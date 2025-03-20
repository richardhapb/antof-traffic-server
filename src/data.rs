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

const ALERTS_CACHE_KEY: &str = "alerts_data";
const ALERTS_CACHE_EXP: u32 = 3600; // 1 hour

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
/// * cache_state: `Arc` with the pointer to global cache state
///
/// # Returns
/// * Tuple containing alerts and jams retrieved from the API
pub async fn update_data_from_api(
    State(cache_state): State<Arc<CacheState>>,
) -> Result<Json<(AlertsDataGroup, JamsGroup)>, UpdateError> {
    tracing::info!("Starting update_data_from_api request");

    let (alerts, jams) = api::request_and_parse().await.map_err(|e| {
        tracing::error!("API Error: {:?}", e);
        UpdateError::ApiError(e)
    })?;

    let alerts_inserted = alerts.bulk_insert().await.map_err(|e| {
        tracing::error!("Database Error (alerts): {:?}", e);
        UpdateError::DatabaseError(e)
    })?;

    let j́ams_inserted = jams.bulk_insert().await.map_err(|e| {
        tracing::error!("Database Error (jams): {:?}", e);
        UpdateError::DatabaseError(e)
    })?;

    let alerts_ends = alerts.fill_end_pub_millis()
        .await
        .map_err(UpdateError::DatabaseError)?;
    let jams_ends = jams.fill_end_pub_millis()
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

    // Group data
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
        .get::<i64>("last_request_millis")
        .unwrap_or(None)
        .is_none()
    {
        let now = Utc::now().timestamp() * 1000;
        cache_state
            .client
            .set("last_request_millis", now, 600)
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

    Ok(Json((alerts, jams)))
}

/// Retrieve data from the cache if it exists; otherwise get it from the database
///
/// # Params
/// * params: url request aruments
/// * cache_state: `Arc` with the pointer to global cache state
///
/// # Returns
/// * AlertsDataGroup: Grouped alerts data
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
        .get::<i64>("last_request_millis")
        .map_err(|_| {
            UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                CommandError::KeyNotFound,
            )))
        })?
        .unwrap_or(-1);
    tracing::info!("Last request millis: {}", min_millis);

    match &params.since {
        // Only retrieve from cache if since is present
        Some(since) => {
            if min_millis > 0 && min_millis <= *since {
                tracing::info!("Connected to memcache successfully!");
                alerts = match get_data_from_cache(&cache_state.client).await {
                    Ok(alerts) => Some(alerts),
                    Err(e) => {
                        tracing::info!("Data not found in cache, retrieving from database...");
                        tracing::info!("{:?}", e);
                        None
                    }
                };
                cache_state.client.delete(ALERTS_CACHE_KEY).map_err(|_| {
                    UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                        CommandError::KeyNotFound,
                    )))
                })?;
                cache_state
                    .client
                    .delete("last_request_millis")
                    .map_err(|_| {
                        UpdateError::CacheError(CacheError::RequestError(
                            MemcacheError::CommandError(CommandError::KeyNotFound),
                        ))
                    })?;
            }
        }
        None => {}
    };

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
/// * Result enum with Grouped alerts by location or Cache error
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
/// * Result enum with groupeda alerts by location or Cache error
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


