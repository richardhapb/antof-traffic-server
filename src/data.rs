use crate::api;
use crate::models::alerts::AlertsGrouper;
use crate::models::{
    alerts::{Alert, AlertsDataGroup, AlertsGroup},
    jams::{Jam, JamsGroup},
};
use chrono::Utc;
use sqlx::PgPool;
use std::env;

use serde::Deserialize;

use axum::{Json, extract::Query, http::StatusCode, response::IntoResponse};
use memcache::{CommandError, MemcacheError};
use serde_json::json;

#[derive(Debug)]
pub enum UpdateError {
    ApiError(Box<dyn std::error::Error>),
    DatabaseError(sqlx::Error),
    CacheError(CacheError),
}

#[derive(Debug)]
pub enum CacheError {
    ConnectionError(Box<dyn std::error::Error>),
    RequestError(memcache::MemcacheError),
    NotFound(memcache::MemcacheError),
    GroupingError(Box<dyn std::error::Error>),
}

impl IntoResponse for UpdateError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            UpdateError::ApiError(e) => (StatusCode::BAD_GATEWAY, format!("API error: {}", e)),
            UpdateError::DatabaseError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            ),
            UpdateError::CacheError(e) => {
                (StatusCode::BAD_GATEWAY, format!("Cache error: {:?}", e))
            }
        };

        (status, Json(json!({ "error": error_message }))).into_response()
    }
}

impl IntoResponse for CacheError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            CacheError::ConnectionError(e) => {
                (StatusCode::BAD_GATEWAY, format!("Connection error: {}", e))
            }
            CacheError::RequestError(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Request error: {}", e),
            ),
            CacheError::NotFound(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Key not found: {}", e),
            ),
            CacheError::GroupingError(e) => (
                StatusCode::BAD_GATEWAY,
                format!("Error grouping data: {}", e),
            ),
        };

        (status, Json(json!({ "error": error_message }))).into_response()
    }
}

// AXUM HANDLERS

#[derive(Debug, Deserialize)]
pub struct FilterParams {
    pub since: Option<i64>, // Millis
    pub until: Option<i64>, // Millis
}

/// Fetch data from the Waze API and store it in the database
///
/// # Returns
/// * Tuple containing alerts and jams retrieved from the API
pub async fn update_data_from_api() -> Result<Json<(AlertsDataGroup, JamsGroup)>, UpdateError> {
    tracing::info!("Starting update_data_from_api request");

    tracing::info!("Connecting to memcache...");
    let client = match memcache::connect("memcache://127.0.0.1:11211") {
        Ok(client) => client,
        Err(e) => {
            tracing::error!("Failed to connect to memcache: {}", e);
            return Err(UpdateError::CacheError(CacheError::ConnectionError(
                Box::new(e),
            )));
        }
    };

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

    let alerts_ends = Alert::fill_end_pub_millis(&alerts)
        .await
        .map_err(UpdateError::DatabaseError)?;
    let jams_ends = Jam::fill_end_pub_millis(&jams)
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
    let alerts_grouper = match client
        .get("alerts_grouper")
        .map_err(|e| UpdateError::CacheError(CacheError::RequestError(e)))?
    {
        Some(alerts_grouper) => alerts_grouper,
        None => AlertsGrouper::new((10, 20))
            .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?,
    };

    let alerts = alerts_grouper
        .group(alerts, &client)
        .await
        .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?;

    let prev_alerts: AlertsDataGroup = match client.get("alerts") {
        Ok(Some(prev_alerts)) => prev_alerts,
        Ok(None) => AlertsDataGroup { alerts: vec![] },
        Err(_) => AlertsDataGroup { alerts: vec![] },
    };

    // Concat to previuos alerts in cache
    let alerts = prev_alerts.concat(alerts);

    // Only set the cache if it not exists
    // because this is reset if the client request the data
    if client
        .get::<i64>("last_request_millis")
        .unwrap_or(None)
        .is_none()
    {
        let now = Utc::now().timestamp() * 1000;
        client
            .set("last_request_millis", now, 600)
            .map_err(|e| UpdateError::CacheError(CacheError::RequestError(e)))?;
        tracing::info!("Set last request millis: {}", now);
    }

    tracing::info!("Setting data of alerts in cache");
    client.set("alerts", &alerts, 3600).map_err(|e| {
        tracing::error!("Error setting alerts data in cache: {}", e);
        UpdateError::CacheError(CacheError::RequestError(e))
    })?;

    Ok(Json((alerts, jams)))
}

/// Retrieve data from the cache if it exists; otherwise get it from the database
///
/// # Returns
/// * AlertsDataGroup: Grouped alerts data
pub async fn get_data(
    Query(params): Query<FilterParams>,
) -> Result<Json<AlertsDataGroup>, UpdateError> {
    // Check for data in cache

    tracing::info!("Params received: {:?}", params);
    let mut alerts: Option<_> = None;

    tracing::info!("Connecting to memcache...");
    let client = match memcache::connect("memcache://127.0.0.1:11211") {
        Ok(client) => client,
        Err(e) => {
            tracing::error!("Failed to connect to memcache: {}", e);
            return Err(UpdateError::CacheError(CacheError::ConnectionError(
                Box::new(e),
            )));
        }
    };

    tracing::info!("Retrieving last request millis");
    let min_millis = client
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
                alerts = match get_data_from_cache(&client).await {
                    Ok(alerts) => Some(alerts),
                    Err(e) => {
                        tracing::info!("Data not found in cache, retrieving from database...");
                        tracing::info!("{:?}", e);
                        None
                    }
                };
                client.delete("alerts").map_err(|_| {
                    UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                        CommandError::KeyNotFound,
                    )))
                })?;
                client.delete("last_request_millis").map_err(|_| {
                    UpdateError::CacheError(CacheError::RequestError(MemcacheError::CommandError(
                        CommandError::KeyNotFound,
                    )))
                })?;
            }
        }
        None => {}
    };

    let alerts = match alerts {
        Some(alerts) => alerts,
        None => {
            let alerts = match get_data_from_database(params, &client).await {
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

/// Retrieve data from database or cache if it exists
///
/// # Returns
/// * Grouped alerts by location
pub async fn get_data_from_cache(client: &memcache::Client) -> Result<AlertsDataGroup, CacheError> {
    tracing::info!("Retrieving data...");
    let alerts: AlertsDataGroup = match client.get("alerts") {
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
pub async fn get_data_from_database(
    filters: FilterParams,
    memclient: &memcache::Client,
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

    let alerts_grouper = match memclient
        .get("alerts_grouper")
        .map_err(|e| UpdateError::CacheError(CacheError::RequestError(e)))?
    {
        Some(alerts_grouper) => alerts_grouper,
        None => AlertsGrouper::new((10, 20))
            .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?,
    };

    let alerts = alerts_grouper
        .group(alerts, &memclient)
        .await
        .map_err(|e| UpdateError::CacheError(CacheError::GroupingError(e)))?;

    Ok(alerts)
}

/// Create pool connection with postgres
pub async fn connect_to_db() -> Result<PgPool, sqlx::Error> {
    let database_url =
        env::var("DATABASE_URL").expect("Environment variable DATABASE_URL must be set");
    PgPool::connect(&database_url).await
}
