use crate::api;
use crate::errors::{CacheError, UpdateError};
use crate::models::alerts::AlertsGrouper;
use crate::models::{
    alerts::{AlertsDataGroup, AlertsGroup},
    jams::JamsGroup,
};
use crate::server::CacheState;
use crate::utils::connect_to_db;

use chrono::Utc;
use std::sync::Arc;

use serde::Deserialize;

use axum::{
    Json,
    extract::{Query, State},
};
use memcache::{CommandError, MemcacheError};

pub const ALERTS_CACHE_KEY: &str = "alerts_data";
const ALERTS_CACHE_EXP: u32 = 3600; // 1 hour

pub const LAST_UPDATE_KEY: &str = "last_request_millis";
const LAST_UPDATE_EXP: u32 = 3600; // 1 hour

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
        UpdateError::ApiError(e)
    })?;

    insert_and_update_data(&alerts, &jams).await?;

    let alerts = group_alerts(alerts, cache_state).await?;

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

    tracing::info!("Retrieving last request millis");
    let min_millis = cache_state
        .client
        .get::<i64>(LAST_UPDATE_KEY)
        .map_err(|_| {
            UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                CommandError::KeyNotFound,
            )))
        })?
        .unwrap_or(-1);
    tracing::info!("Last request millis: {}", min_millis);

    // If the request `since` argument is upper than `LAST_UPDATE_KEY`,
    // we send data from cache if it is present. Otherwise send data from database.
    match &params.since {
        // Only retrieve from cache if since is present
        Some(since) => {
            if min_millis > 0 && min_millis <= *since {
                // Try to get data from cache
                alerts = match get_data_from_cache(&cache_state.client).await {
                    Ok(alerts) => Some(alerts),
                    Err(e) => {
                        tracing::info!("Data not found in cache, retrieving from database...");
                        tracing::info!("{:?}", e);
                        None
                    }
                };

                // Clear data for avoid send again same data
                cache_state.client.delete(ALERTS_CACHE_KEY).map_err(|_| {
                    UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                        CommandError::KeyNotFound,
                    )))
                })?;

                // Clear last register, this indicates that there is not new data in cache
                cache_state.client.delete(LAST_UPDATE_KEY).map_err(|_| {
                    UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                        CommandError::KeyNotFound,
                    )))
                })?;
            }
        }
        None => {}
    };

    // If alerts is `None` retrieve data from database.
    let alerts = match alerts {
        Some(alerts) => alerts,
        None => {
            let alerts = match get_data_from_database(params, cache_state).await {
                Ok(alerts) => alerts,
                Err(e) => {
                    tracing::error!("Error retrieving data from database.");
                    return Err(e);
                }
            };

            alerts
        }
    };

    Ok(Json(alerts))
}

/// Retrieve data from cache if it exists
///
/// # Params
/// * memclient: The reference to memcache client connection
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_cache(
    memclient: &memcache::Client,
) -> Result<AlertsDataGroup, CacheError> {
    tracing::info!("Retrieving data...");
    let alerts: AlertsDataGroup = match memclient.get(ALERTS_CACHE_KEY) {
        Ok(Some(alerts)) => alerts,
        Ok(None) => {
            return Err(CacheError::NotFound(MemcacheError::CommandError(
                CommandError::InvalidArguments,
            )));
        }
        Err(e) => {
            tracing::error!("Failed to retrieve alerts from cache: {}", e);
            return Err(CacheError::RequestError(e));
        }
    };

    tracing::info!("Data found: {}", alerts.alerts.len());

    Ok(alerts)
}

/// Get data from database, based on filters passed as args
///
/// # Params
/// * filters: Filters from url request
/// * cache_state: Global state with cache connection
///
/// # Returns
/// * Result enum with [`AlertsDataGroup`] or [`CacheError`]
pub async fn get_data_from_database(
    filters: FilterParams,
    cache_state: Arc<CacheState>,
) -> Result<AlertsDataGroup, UpdateError> {
    tracing::info!("Retrieving data from database...");
    let pool = connect_to_db()
        .await
        .map_err(|e| UpdateError::DatabaseError(e))?;

    let since = filters.since.unwrap_or(1722470400000); // 24-08-01 by default

    let query = r#"
    SELECT a.*, l.x, l.y
    FROM alerts a
    LEFT JOIN alerts_location l ON a.location_id = l.id
    WHERE pub_millis
    "#;

    let time_filter = match filters.until {
        Some(until) => format!("BETWEEN {} and {}", since, until),
        None => format!(">= {}", since),
    };

    let query = format!("{} {}", query, time_filter);

    let alerts: AlertsGroup = match sqlx::query_as(&query).fetch_all(&pool).await {
        Ok(alerts) => AlertsGroup { alerts },
        Err(e) => {
            tracing::error!("Error retrieving data from database {}", e);
            return Err(UpdateError::DatabaseError(e));
        }
    };

    tracing::info!("Data found: {}", alerts.alerts.len());
    tracing::info!("Grouping...");

    let alerts_grouper = match cache_state
        .client
        .get("alerts_grouper")
        .map_err(|e| UpdateError::CacheError(CacheError::RequestError(e)))?
    {
        Some(alerts_grouper) => alerts_grouper,
        None => AlertsGrouper::new((10, 20))
            .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?,
    };

    let alerts = alerts_grouper
        .group(alerts, Arc::clone(&cache_state))
        .await
        .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?;

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
        UpdateError::DatabaseError(e)
    })?;

    let j́ams_inserted = jams.bulk_insert().await.map_err(|e| {
        tracing::error!("Database Error (jams): {:?}", e);
        UpdateError::DatabaseError(e)
    })?;

    // End pub millis update
    let alerts_ends = alerts
        .fill_end_pub_millis()
        .await
        .map_err(UpdateError::DatabaseError)?;
    let jams_ends = jams
        .fill_end_pub_millis()
        .await
        .map_err(UpdateError::DatabaseError)?;

    tracing::info!(
        "{} alerts and {} jams inserted to database",
        alerts_inserted,
        j́ams_inserted
    );
    tracing::info!(
        "{} alerts and {} jams end time updated",
        alerts_ends,
        jams_ends
    );

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
async fn group_alerts(
    alerts: AlertsGroup,
    cache_state: Arc<CacheState>,
) -> Result<AlertsDataGroup, UpdateError> {
    // Group data, try get from cache the grouper
    let alerts_grouper = match cache_state
        .client
        .get("alerts_grouper")
        .map_err(|e| UpdateError::CacheError(CacheError::RequestError(e)))?
    {
        Some(alerts_grouper) => alerts_grouper,
        None => AlertsGrouper::new((10, 20))
            .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?,
    };

    let alerts = alerts_grouper
        .group(alerts, Arc::clone(&cache_state))
        .await
        .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?;

    let prev_alerts: AlertsDataGroup = match cache_state.client.get(ALERTS_CACHE_KEY) {
        Ok(Some(prev_alerts)) => prev_alerts,
        Ok(None) => AlertsDataGroup { alerts: vec![] },
        Err(_) => AlertsDataGroup { alerts: vec![] },
    };

    // Concat to previuos alerts in cache
    let alerts = prev_alerts.concat(alerts);

    // Only set the cache if it not exists
    // because this is reset if the client request the data
    if cache_state
        .client
        .get::<i64>(LAST_UPDATE_KEY)
        .unwrap_or(None)
        .is_none()
    {
        let now = Utc::now().timestamp() * 1000;
        cache_state
            .client
            .set(LAST_UPDATE_KEY, now, LAST_UPDATE_EXP)
            .map_err(|e| UpdateError::CacheError(CacheError::RequestError(e)))?;
        tracing::info!("Set last request millis: {}", now);
    }

    tracing::info!("Setting data of alerts in cache");
    cache_state
        .client
        .set(ALERTS_CACHE_KEY, &alerts, ALERTS_CACHE_EXP)
        .map_err(|e| {
            tracing::error!("Error setting alerts data in cache: {}", e);
            UpdateError::CacheError(CacheError::RequestError(e))
        })?;

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

        // Is empty, should return an error
        let result = get_data_from_cache(&cache_state.client).await;

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
        let alerts = group_alerts(alerts, Arc::clone(&cache_state))
            .await
            .unwrap();
        cache_state
            .client
            .set(ALERTS_CACHE_KEY, &alerts, ALERTS_CACHE_EXP)
            .unwrap();

        let alerts = get_data_from_cache(&cache_state.client).await.unwrap();

        // Clean
        cache_state.client.delete(ALERTS_CACHE_KEY).unwrap();

        assert_eq!(alerts.alerts.len(), 2);
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

        let alerts = get_data_from_database(filters, Arc::clone(&cache_state))
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

        let alerts = get_data_from_database(filters, cache_state).await.unwrap();

        // Should return both alerts
        assert_eq!(alerts.alerts.len(), 2);
    }
}
