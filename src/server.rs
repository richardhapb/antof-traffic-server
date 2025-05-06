use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use chrono::Utc;
use std::{
    cmp::{max, min},
    sync::Arc,
};

use crate::{
    api,
    cache::{CacheService, MAX_PUB_MILLIS_CACHE_KEY, MIN_PUB_MILLIS_CACHE_KEY},
    data::{
        ALERTS_BEGIN_TIMESTAMP, UPDATE_UNTIL_THRESHOLD, concat_alerts_and_storage_to_cache,
        get_data_from_cache, get_data_from_database, insert_and_update_data,
    },
    errors::{CacheError, UpdateError},
    get_time_range,
    models::{
        alerts::{AlertsDataGroup, AlertsGroup},
        jams::JamsGroup,
    },
    utils::group_alerts,
};
use serde::Deserialize;

/// Main server handler
pub async fn create_server() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting server on 0.0.0.0:7070");

    let cache_service = CacheService::init_cache().await;

    let app = Router::new()
        .route("/update-data", get(update_data_from_api))
        .route("/get-data", get(get_data))
        .route("/clear-cache", get(clear_cache))
        .route("/aggregate", post(aggregate))
        .with_state(cache_service);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7070").await?;
    tracing::info!("Server is running on http://0.0.0.0:7070");
    axum::serve(listener, app).await?;
    Ok(())
}

// AXUM HANDLERS

// Filters from url request args
#[derive(Debug, Deserialize)]
pub struct FilterParams {
    pub since: Option<i64>, // Millis
    pub until: Option<i64>, // Millis
}

// Cache key passed as url request arg
#[derive(Debug, Deserialize)]
pub struct CacheKey {
    pub key: String,
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

    let min_millis = cache_service.get_or_default(MIN_PUB_MILLIS_CACHE_KEY, since);
    let max_millis = cache_service.get_or_default(MAX_PUB_MILLIS_CACHE_KEY, until);

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

    // Store the new data
    cache_service.store_alerts(&alerts)?;

    // Set the minimum `since` and `until` query to the cache
    cache_service.update_millis(&alerts).map_err(UpdateError::Cache)?;
    tracing::info!("Set min pub_millis: {}", since);
    tracing::info!("Set max pub_millis: {}", until);

    Ok(Json(alerts))
}

/// Remove a key passed from an Http request
pub async fn clear_cache(
    Query(cache_key): Query<CacheKey>,
    State(cache_service): State<Arc<CacheService>>,
) -> Result<(StatusCode, Json<&'static str>), CacheError> {
    if cache_key.key.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json("Key is required, use localhost:7070/clear-cache?key=<your-key>"),
        ));
    }

    if cache_service.remove_key(&cache_key.key)? {
        Ok((StatusCode::OK, Json("Key removed")))
    } else {
        Ok((StatusCode::NOT_FOUND, Json("Key not found")))
    }
}

/// Transform [`AlertsGroup`] to [`AlertsDataGroup`] adding
/// aggregate data and grouping.
pub async fn aggregate(
    State(cache_service): State<Arc<CacheService>>,
    Json(alerts_group): Json<AlertsGroup>,
) -> Result<(StatusCode, Json<AlertsDataGroup>), UpdateError> {
    // If is empty returns an empty array
    if alerts_group.alerts.is_empty() {
        tracing::info!("Received empty data, returning empty data");
        return Ok((StatusCode::NO_CONTENT, Json(AlertsDataGroup { alerts: vec![] })));
    }

    // Write a sample ot the received data
    tracing::info!("Received data to aggregate: {} ", alerts_group.alerts.len());
    if let Some(first_alert) = alerts_group.alerts.first() {
        if let Ok(sample) = serde_json::to_string(first_alert) {
            tracing::info!("Sample: {:?} ", sample);
        } else {
            tracing::warn!("Failed to serialize first alert for logging");
        }
    } else {
        tracing::warn!("No alerts to sample in received data");
    }

    let alerts = group_alerts(alerts_group, cache_service).await;

    match alerts {
        Ok(alerts) => {
            // Write a sample of the transformed data
            tracing::info!(
                "Data transformed successfully, returning {} elements",
                alerts.alerts.len()
            );
            if let Some(first_alert) = alerts.alerts.first() {
                if let Ok(sample) = serde_json::to_string(first_alert) {
                    tracing::info!("Sample: {} ", sample);
                } else {
                    tracing::warn!("Failed to serialize first transformed alert for logging");
                }
            }

            Ok((StatusCode::OK, Json(alerts)))
        }
        Err(err) => {
            tracing::error!("Error generating agregate data: {:?}", err);
            Err(err)
        }
    }
}
