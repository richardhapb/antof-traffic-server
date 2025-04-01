use crate::api;
use crate::cache::CacheService;
use crate::errors::{CacheError, UpdateError};
use crate::models::{
    alerts::{AlertsDataGroup, AlertsGroup},
    jams::JamsGroup,
};
use crate::utils::{calculate_params, connect_to_db, group_alerts};

use std::cmp::{max, min};
use std::sync::Arc;

use chrono::Utc;
use serde::Deserialize;

use axum::{
    Json,
    extract::{Query, State},
};

pub const ALERTS_CACHE_KEY: &str = "alerts_data";
pub const ALERTS_CACHE_EXP: u32 = 604800; // One week

pub const MIN_PUB_MILLIS: &str = "min_pub_millis";
pub const MAX_PUB_MILLIS: &str = "max_pub_millis";

const ALERTS_BEGIN_TIMESTAMP: i64 = 1727740800000; // 2024-10-01

// Threshold between last update and until param in request
const UPDATE_UNTIL_THRESHOLD: i64 = 600000; // 10 minutes

/// Get the range of the [`FilterParams`] and return
/// `since` and `until` with the correct values
///
/// The return value must be between `ALERTS_BEGIN_TIMESTAMP`
/// and the current time, inclusively.
macro_rules! get_time_range {
    ($params:expr) => {{
        let now = Utc::now().timestamp() * 1000;
        let since = max(
            ALERTS_BEGIN_TIMESTAMP,
            $params.since.unwrap_or(ALERTS_BEGIN_TIMESTAMP),
        );
        let until = min(now, $params.until.unwrap_or(now));
        (since, until)
    }};
}

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
/// * cache_service: [`std::sync::Arc`] with the pointer to global cache state
///
/// # Returns
/// * Tuple containing alerts and jams retrieved from the API
pub async fn update_data_from_api(
    State(cache_service): State<Arc<CacheService>>,
) -> Result<Json<(AlertsDataGroup, JamsGroup)>, UpdateError> {
    tracing::info!("Starting update_data_from_api request");

    // Get data from API
    let (alerts, jams) = api::request_and_parse().await.map_err(|e| {
        tracing::error!("API Error: {:?}", e);
        UpdateError::Api(e)
    })?;

    insert_and_update_data(&alerts, &jams).await?;

    let alerts = group_alerts(alerts, Arc::clone(&cache_service)).await?;
    let alerts = concat_alerts_and_storage_to_cache(cache_service, alerts)?;

    // TODO: Isolate the retrieved alerts

    Ok(Json((alerts, jams)))
}

/// Retrieve data from the cache if it exists; otherwise get it from the database
///
/// # Params
/// * params: url request aruments
/// * cache_service: [`std::sync::Arc`] with the pointer to global cache state
///
/// # Returns
/// * [`AlertsDataGroup`]: Grouped alerts data
pub async fn get_data(
    Query(params): Query<FilterParams>,
    State(cache_service): State<Arc<CacheService>>,
) -> Result<Json<AlertsDataGroup>, UpdateError> {
    // Check for data in cache

    tracing::info!("Params received: {:?}", params);
    let mut alerts: Option<_> = None;

    let (since, until) = get_time_range!(params);

    let min_millis = cache_service.get_or_default(MIN_PUB_MILLIS, since);
    let max_millis = cache_service.get_or_default(MAX_PUB_MILLIS, until);

    tracing::info!("Retrieving data from cache with params");
    tracing::info!("min pub_millis: {}", min_millis);
    tracing::info!("max pub_millis: {}", max_millis);

    // If the data from cache is larger than the data requested, return the cache
    // Only retrieve from cache if since is present
    if min_millis <= since && max_millis >= until - UPDATE_UNTIL_THRESHOLD {
        // Try to get data from cache
        alerts = match get_data_from_cache(Arc::clone(&cache_service), &params).await {
            Ok(alerts) => Some(alerts),
            Err(e) => {
                tracing::info!("Data not found in cache, retrieving from database...");
                tracing::info!("{:?}", e);
                None
            }
        };
    }

    // If alerts is `None` retrieve data from database.
    let alerts = match alerts {
        Some(a) => a,
        None => get_data_from_database(&params, Arc::clone(&cache_service)).await?,
    };

    // Set the minimum `since` query to the cache
    cache_service.store_alerts(&alerts)?;
    tracing::info!("Set min pub_millis: {}", since);

    // Set the maximum `until` query to the cache if it exists
    cache_service.store_alerts(&alerts)?;
    tracing::info!("Set max pub_millis: {}", until);

    Ok(Json(alerts))
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
    cache_service: Arc<CacheService>,
    params: &FilterParams,
) -> Result<AlertsDataGroup, CacheError> {
    tracing::info!("Retrieving data...");

    let (since, until) = get_time_range!(params);

    let mut alerts: AlertsDataGroup = cache_service.get_or_cache_err(ALERTS_CACHE_KEY)?;

    alerts.filter_range(since, until);
    tracing::info!("Data found: {}", alerts.alerts.len());

    Ok(alerts)
}

/// Get data from database, based on filters passed as args
///
/// # Params
/// * params: Filters from url request
/// * cache_service: Global state with cache connection
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_database(
    params: &FilterParams,
    cache_service: Arc<CacheService>,
) -> Result<AlertsDataGroup, UpdateError> {
    tracing::info!("Retrieving data from database...");
    let pool = connect_to_db().await.map_err(UpdateError::Database)?;

    let (since, until) = get_time_range!(params);

    let min_millis = cache_service.get_or_default(MIN_PUB_MILLIS, since);
    let max_millis = cache_service.get_or_default(MAX_PUB_MILLIS, until);

    // Calculate params
    let params = calculate_params(min_millis, max_millis, params);
    tracing::info!("Params calculated: {:?}", params);

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

    let alerts = group_alerts(alerts, Arc::clone(&cache_service)).await?;

    // Update data in cache and concatenate
    let mut alerts = concat_alerts_and_storage_to_cache(cache_service, alerts)?;
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

/// Concatenate alerts with cache data and update the cache with the result
///
/// # Parameters
/// * `cache_service`: Pointer to the cache state
/// * `alerts`: New alerts to concatenate
///
/// # Returns
/// * The concatenated alerts.
pub fn concat_alerts_and_storage_to_cache(
    cache_service: Arc<CacheService>,
    alerts: AlertsDataGroup,
) -> Result<AlertsDataGroup, UpdateError> {
    let prev_alerts: AlertsDataGroup =
        cache_service.get_or_default(ALERTS_CACHE_KEY, AlertsDataGroup { alerts: vec![] });

    // Concat to previuos alerts in cache
    let alerts = prev_alerts.concat(alerts);

    tracing::info!("Setting data of alerts in cache");
    cache_service.store_alerts(&alerts)?;
    tracing::info!("Data inserted to cache");

    Ok(alerts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::CacheError;
    use crate::utils::test::{setup_alerts, setup_cache, setup_test_db};
    use memcache::{CommandError, MemcacheError};
    use serial_test::serial;

    #[tokio::test]
    async fn test_get_data_from_cache_empty() {
        let cache_service = setup_cache().await;
        cache_service.client.delete(ALERTS_CACHE_KEY).unwrap();

        let until = Some(Utc::now().timestamp() * 1000);

        // Is empty, should return an error
        let result = get_data_from_cache(
            cache_service,
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
                CommandError::KeyNotFound,
            )))
        ))
    }

    #[tokio::test]
    #[serial]
    async fn test_get_data_from_cache_with_data() {
        setup_test_db().await;
        let cache_service = setup_cache().await;
        let alerts = setup_alerts();

        // Insert two alerts to database
        alerts.bulk_insert().await.unwrap();

        // Remove any cache data
        cache_service.client.delete(ALERTS_CACHE_KEY).unwrap();

        // Group and insert data to cache
        let alerts = group_alerts(alerts, Arc::clone(&cache_service)).await.unwrap();
        cache_service.store_alerts(&alerts).unwrap();

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
        let alerts = get_data_from_cache(cache_service.clone(), &FilterParams { since, until })
            .await
            .unwrap();

        // Clean
        cache_service.client.delete(ALERTS_CACHE_KEY).unwrap();

        assert_eq!(alerts.alerts.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_data_from_database() {
        setup_test_db().await;
        let cache_service = setup_cache().await;
        let alerts = setup_alerts();

        // Insert two alerts to database
        alerts.bulk_insert().await.unwrap();

        // This since value is lower than only one alert
        let filters = FilterParams {
            since: Some(1735980027000),
            until: None,
        };

        let alerts = get_data_from_database(&filters, Arc::clone(&cache_service))
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

        let alerts = get_data_from_database(&filters, cache_service).await.unwrap();

        // Should return both alerts
        assert_eq!(alerts.alerts.len(), 2);
    }

    #[test]
    fn test_time_range_macro() {
        let now = Utc::now().timestamp() * 1000;
        let params = FilterParams {
            since: Some(ALERTS_BEGIN_TIMESTAMP - 20),
            until: Some(now + 100),
        };
        let (since, until) = get_time_range!(params);

        // `since` can't be lower than `ALERTS_BEGIN_TIMESTAMP`
        assert_eq!(since, ALERTS_BEGIN_TIMESTAMP);
        // `until` can't be upper than current time
        assert_eq!(until, now);

        // Second case

        let params = FilterParams {
            since: Some(ALERTS_BEGIN_TIMESTAMP + 20),
            until: Some(now - 100),
        };

        let (since, until) = get_time_range!(params);

        assert_eq!(since, ALERTS_BEGIN_TIMESTAMP + 20);
        assert_eq!(until, now - 100);
    }
}
