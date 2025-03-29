use crate::api;
use crate::errors::{CacheError, UpdateError};
use crate::models::alerts::AlertsGrouper;
use crate::models::{
    alerts::{AlertsDataGroup, AlertsGroup},
    jams::JamsGroup,
};
use crate::server::CacheState;
use crate::utils::connect_to_db;

use std::cmp::{max, min};
use std::sync::Arc;

use chrono::Utc;
use serde::Deserialize;

use axum::{
    Json,
    extract::{Query, State},
};
use memcache::{CommandError, MemcacheError};

pub const ALERTS_CACHE_KEY: &str = "alerts_data";
const ALERTS_CACHE_EXP: u32 = 604800; // One week

const ALERTS_GROUPER_CACHE_KEY: &str = "alerts_grouper";
const ALERTS_GROUPER_CACHE_EXP: u32 = 604800; // One week 

pub const MIN_PUB_MILLIS: &str = "min_pub_millis";
pub const MAX_PUB_MILLIS: &str = "max_pub_millis";

const ALERTS_BEGIN_TIMESTAMP: i64 = 1727740800000;

// Threshold between last update and until param in request
const UPDATE_UNTIL_THRESHOLD: i64 = 600000; // 10 minutes

// AXUM HANDLERS

// Filters from url request args
#[derive(Debug, Deserialize)]
pub struct FilterParams {
    pub since: Option<i64>, // Millis
    pub until: Option<i64>, // Millis
}

/// Fetch data from the Waze API and store it in the database and cache
/// if data exists in cache, add to it and keep unique registers.
///
/// # Params
/// * cache_state: [`std::sync::Arc`] with the pointer to global cache state
///
/// # Returns
/// * Tuple containing alerts and jams retrieved from the API
pub async fn update_data_from_api(
    State(cache_state): State<Arc<CacheState>>,
) -> Result<Json<(AlertsDataGroup, JamsGroup)>, UpdateError> {
    tracing::info!("Starting update_data_from_api request");

    // Get data from API
    let (alerts, jams) = api::request_and_parse().await.map_err(|e| {
        tracing::error!("API Error: {:?}", e);
        UpdateError::Api(e)
    })?;

    insert_and_update_data(&alerts, &jams).await?;

    let alerts = group_alerts(alerts, Arc::clone(&cache_state)).await?;
    let alerts = concat_alerts_and_storage_to_cache(cache_state, alerts)?;

    // TODO: Isolate the retrieved alerts

    Ok(Json((alerts, jams)))
}

/// Retrieve data from the cache if it exists; otherwise get it from the database
///
/// # Params
/// * params: url request aruments
/// * cache_state: [`std::sync::Arc`] with the pointer to global cache state
///
/// # Returns
/// * [`AlertsDataGroup`]: Grouped alerts data
pub async fn get_data(
    Query(params): Query<FilterParams>,
    State(cache_state): State<Arc<CacheState>>,
) -> Result<Json<AlertsDataGroup>, UpdateError> {
    // Check for data in cache

    tracing::info!("Params received: {:?}", params);
    let mut alerts: Option<_> = None;

    let now = Utc::now().timestamp() * 1000;
    let since = max(ALERTS_BEGIN_TIMESTAMP, params.since.unwrap_or(ALERTS_BEGIN_TIMESTAMP));
    let until = min(now, params.until.unwrap_or(now));

    let min_millis = cache_state
        .client
        .get::<i64>(MIN_PUB_MILLIS)
        .map_err(|_| {
            UpdateError::Cache(CacheError::Request(MemcacheError::CommandError(
                CommandError::KeyNotFound,
            )))
        })?
        .unwrap_or(since);

    let max_millis = cache_state
        .client
        .get::<i64>(MAX_PUB_MILLIS)
        .map_err(|_| {
            UpdateError::Cache(CacheError::Request(MemcacheError::CommandError(
                CommandError::KeyNotFound,
            )))
        })?
        .unwrap_or(until);

    tracing::info!("Retrieving data from cache with params");
    tracing::info!("min pub_millis: {}", min_millis);
    tracing::info!("max pub_millis: {}", max_millis);

    // If the data from cache is larger than the data requested, return the cache
    // Only retrieve from cache if since is present
    if min_millis <= since && max_millis >= until - UPDATE_UNTIL_THRESHOLD {
        // Try to get data from cache
        alerts = match get_data_from_cache(&cache_state.client, &params).await {
            Ok(alerts) => Some(alerts),
            Err(e) => {
                tracing::info!("Data not found in cache, retrieving from database...");
                tracing::info!("{:?}", e);
                None
            }
        };
    }

    // Calculate params
    let params = calculate_params(min_millis, max_millis, params);

    // If alerts is `None` retrieve data from database.
    let alerts = match alerts {
        Some(a) => a,
        None => get_data_from_database(&params, Arc::clone(&cache_state)).await?,
    };

    // Set the minimum `since` query to the cache
    cache_state
        .client
        .set(MIN_PUB_MILLIS, since, ALERTS_CACHE_EXP)
        .map_err(|e| UpdateError::Cache(CacheError::Request(e)))?;
    tracing::info!("Set min pub_millis: {}", since);

    // Set the maximum `until` query to the cache if it exists
    cache_state
        .client
        .set(MAX_PUB_MILLIS, until, ALERTS_CACHE_EXP)
        .map_err(|e| UpdateError::Cache(CacheError::Request(e)))?;
    tracing::info!("Set max pub_millis: {}", until);

    Ok(Json(alerts))
}

/// Calculate the min and max pub_millis from params and match since/until
/// to the max or min pub_millis, if params.since and params.until are larger
/// than the cache, returns `params_since` and `params_until`
///
/// # Example
/// ```
/// use antof_traffic::data::FilterParams;
/// use antof_traffic::data::calculate_params;
///
/// let since = 10000;
/// let until = 20000;
/// let params = FilterParams { since: Some(5000), until: Some(15000)};
///
/// let result = calculate_params(since, until, params);
/// assert_eq!(result.since.unwrap(), 5000);
/// assert_eq!(result.until.unwrap(), since); // Until the initial `since`
///
/// let since = 10000;
/// let until = 20000;
/// let params = FilterParams { since: Some(15000), until: Some(25000)};
///
/// let result = calculate_params(since, until, params);
/// assert_eq!(result.since.unwrap(), until); // Since the initial `until`
/// assert_eq!(result.until.unwrap(), 25000);
///
/// let since = 10000;
/// let until = 20000;
/// let params = FilterParams { since: Some(5000), until: Some(25000)};
///
/// let result = calculate_params(since, until, params);
/// // The range is larger than the initial params
/// assert_eq!(result.since.unwrap(), 5000);
/// assert_eq!(result.until.unwrap(), 25000);
/// ```
pub fn calculate_params(min_millis: i64, max_millis: i64, params: FilterParams) -> FilterParams {
    let params_since = params.since.unwrap_or(min_millis);
    let params_until = params.until.unwrap_or(max_millis);

    // Get the edge of requested data
    let mut since = min(min_millis, params_since);
    let mut until = max(max_millis, params_until);

    // Only retrieve the least amount of data
    if since < min_millis && until <= max_millis {
        until = min_millis
    } else if since >= min_millis && until > max_millis {
        since = max_millis
    }

    FilterParams {
        since: Some(since),
        until: Some(until),
    }
}

/// Retrieve data from cache if it exists
///
/// # Params
/// * `memclient`: The reference to memcache client connection
/// * `params`: Parameters of request, that shoud contain `since` and `until`
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_cache(
    memclient: &memcache::Client,
    params: &FilterParams,
) -> Result<AlertsDataGroup, CacheError> {
    tracing::info!("Retrieving data...");

    let now = Utc::now().timestamp() * 1000;
    let since = max(ALERTS_BEGIN_TIMESTAMP, params.since.unwrap_or(ALERTS_BEGIN_TIMESTAMP));
    let until = min(now, params.until.unwrap_or(now));

    let mut alerts: AlertsDataGroup = match memclient.get(ALERTS_CACHE_KEY) {
        Ok(Some(alerts)) => alerts,
        Ok(None) => {
            return Err(CacheError::NotFound(MemcacheError::CommandError(
                CommandError::InvalidArguments,
            )));
        }
        Err(e) => {
            tracing::error!("Failed to retrieve alerts from cache: {}", e);
            return Err(CacheError::Request(e));
        }
    };

    alerts.filter_range(since, until);
    tracing::info!("Data found: {}", alerts.alerts.len());

    Ok(alerts)
}

/// Get data from database, based on filters passed as args
///
/// # Params
/// * params: Filters from url request
/// * cache_state: Global state with cache connection
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_database(
    params: &FilterParams,
    cache_state: Arc<CacheState>,
) -> Result<AlertsDataGroup, UpdateError> {
    tracing::info!("Retrieving data from database...");
    let pool = connect_to_db().await.map_err(UpdateError::Database)?;

    let now = Utc::now().timestamp() * 1000;
    let since = max(ALERTS_BEGIN_TIMESTAMP, params.since.unwrap_or(ALERTS_BEGIN_TIMESTAMP));
    let until = min(now, params.until.unwrap_or(now));

    let query = format!(
        r#"
    SELECT a.*, l.x, l.y
    FROM alerts a
    LEFT JOIN alerts_location l ON a.location_id = l.id
    WHERE pub_millis
    BETWEEN {} and {}
    "#,
        since, until
    );

    let alerts: AlertsGroup = match sqlx::query_as(&query).fetch_all(&pool).await {
        Ok(alerts) => AlertsGroup { alerts },
        Err(e) => {
            tracing::error!("Error retrieving data from database {}", e);
            return Err(UpdateError::Database(e));
        }
    };

    tracing::info!("Grouping...");

    let alerts = group_alerts(alerts, Arc::clone(&cache_state)).await?;

    // Update data in cache and concatenate
    let mut alerts = concat_alerts_and_storage_to_cache(cache_state, alerts)?;
    alerts.filter_range(since, until);

    tracing::info!("Data found: {}", alerts.alerts.len());

    Ok(alerts)
}

/// Insert the values to database and update end_pub_millis fields
///
/// # Params
/// * alerts: New alerts from API
/// * jams: New jams from API
async fn insert_and_update_data(alerts: &AlertsGroup, jams: &JamsGroup) -> Result<(), UpdateError> {
    // Database insertions
    let alerts_inserted = alerts.bulk_insert().await.map_err(|e| {
        tracing::error!("Database Error (alerts): {:?}", e);
        UpdateError::Database(e)
    })?;

    let j́ams_inserted = jams.bulk_insert().await.map_err(|e| {
        tracing::error!("Database Error (jams): {:?}", e);
        UpdateError::Database(e)
    })?;

    // End pub millis update
    let alerts_ends = alerts
        .fill_end_pub_millis()
        .await
        .map_err(UpdateError::Database)?;
    let jams_ends = jams.fill_end_pub_millis().await.map_err(UpdateError::Database)?;

    tracing::info!(
        "{} alerts and {} jams inserted to database",
        alerts_inserted,
        j́ams_inserted
    );
    tracing::info!("{} alerts and {} jams end time updated", alerts_ends, jams_ends);

    Ok(())
}

/// Group the alerts and generate aggregate data, try get grouper from cache
///
/// # Params
/// * alerts: Alerts to group
/// * cache_state: Global state with cache connection
///
/// # Returns
/// * Result with new [`AlertsDataGroup`] or an [`UpdateError`]
pub async fn group_alerts(
    alerts: AlertsGroup,
    cache_state: Arc<CacheState>,
) -> Result<AlertsDataGroup, UpdateError> {
    // Group data, try get from cache the grouper
    let alerts_grouper = match cache_state
        .client
        .get(ALERTS_GROUPER_CACHE_KEY)
        .map_err(|e| UpdateError::Cache(CacheError::Request(e)))?
    {
        Some(alerts_grouper) => alerts_grouper,
        None => {
            let alerts_grouper =
                AlertsGrouper::new((10, 20)).map_err(|e| UpdateError::Cache(CacheError::Grouping(e)))?;
            cache_state
                .client
                .set(
                    ALERTS_GROUPER_CACHE_KEY,
                    &alerts_grouper,
                    ALERTS_GROUPER_CACHE_EXP,
                )
                .map_err(|e| UpdateError::Cache(CacheError::Request(e)))?;

            alerts_grouper
        }
    };

    let alerts = alerts_grouper
        .group(alerts, Arc::clone(&cache_state))
        .await
        .map_err(|e| UpdateError::Cache(CacheError::Grouping(e)))?;

    Ok(alerts)
}

/// Concatenate alerts with cache data and update the cache with the result
///
/// # Parameters
/// * `cache_state`: Pointer to the cache state
/// * `alerts`: New alerts to concatenate
///
/// # Returns
/// * The concatenated alerts.
pub fn concat_alerts_and_storage_to_cache(
    cache_state: Arc<CacheState>,
    alerts: AlertsDataGroup,
) -> Result<AlertsDataGroup, UpdateError> {
    let prev_alerts: AlertsDataGroup = match cache_state.client.get(ALERTS_CACHE_KEY) {
        Ok(Some(prev_alerts)) => prev_alerts,
        Ok(None) => AlertsDataGroup { alerts: vec![] },
        Err(_) => AlertsDataGroup { alerts: vec![] },
    };

    // Concat to previuos alerts in cache
    let alerts = prev_alerts.concat(alerts);

    tracing::info!("Setting data of alerts in cache");
    cache_state
        .client
        .set(ALERTS_CACHE_KEY, &alerts, ALERTS_CACHE_EXP)
        .map_err(|e| {
            tracing::error!("Error setting alerts data in cache: {}", e);
            UpdateError::Cache(CacheError::Request(e))
        })?;
    tracing::info!("Data inserted to cache");

    Ok(alerts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::{setup_alerts, setup_cache, setup_test_db};
    use serial_test::serial;

    #[tokio::test]
    async fn test_get_data_from_cache_empty() {
        let cache_state = setup_cache().await;
        cache_state.client.delete(ALERTS_CACHE_KEY).unwrap();

        let until = Some(Utc::now().timestamp() * 1000);

        // Is empty, should return an error
        let result = get_data_from_cache(
            &cache_state.client,
            &FilterParams {
                since: Some(ALERTS_BEGIN_TIMESTAMP),
                until,
            },
        )
        .await;

        // Should return an error indicating doesnt exist
        assert!(matches!(
            result,
            Err(CacheError::NotFound(MemcacheError::CommandError(
                CommandError::InvalidArguments,
            )))
        ))
    }

    #[tokio::test]
    #[serial]
    async fn test_get_data_from_cache_with_data() {
        setup_test_db().await;
        let cache_state = setup_cache().await;
        let alerts = setup_alerts();

        // Insert two alerts to database
        alerts.bulk_insert().await.unwrap();

        // Remove any cache data
        cache_state.client.delete(ALERTS_CACHE_KEY).unwrap();

        // Group and insert data to cache
        let alerts = group_alerts(alerts, Arc::clone(&cache_state)).await.unwrap();
        cache_state
            .client
            .set(ALERTS_CACHE_KEY, &alerts, ALERTS_CACHE_EXP)
            .unwrap();

        let until = Some(Utc::now().timestamp() * 1000);

        // Only retrieve one alert
        let since = Some(
            max(
                alerts.alerts.first().unwrap().alert.pub_millis,
                alerts.alerts.get(1).unwrap().alert.pub_millis,
            ) - 1000,
        );

        println!("{:?} - {:?}", since, until);

        // Only retrieve one alert - since limits to the max pub_millis - 1000
        let alerts = get_data_from_cache(&cache_state.client, &FilterParams { since, until })
            .await
            .unwrap();

        // Clean
        cache_state.client.delete(ALERTS_CACHE_KEY).unwrap();

        assert_eq!(alerts.alerts.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_data_from_database() {
        setup_test_db().await;
        let cache_state = setup_cache().await;
        let alerts = setup_alerts();

        // Insert two alerts to database
        alerts.bulk_insert().await.unwrap();

        // This since value is lower than only one alert
        let filters = FilterParams {
            since: Some(1735980027000),
            until: None,
        };

        let alerts = get_data_from_database(&filters, Arc::clone(&cache_state))
            .await
            .unwrap();

        // Should return only one because the since filter
        assert_eq!(alerts.alerts.len(), 1);

        // Second test case

        // This since value is lower than two alerts
        let filters = FilterParams {
            since: Some(1730980027000),
            until: None,
        };

        let alerts = get_data_from_database(&filters, cache_state).await.unwrap();

        // Should return both alerts
        assert_eq!(alerts.alerts.len(), 2);
    }
}
