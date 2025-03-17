use crate::api;
use crate::models::alerts::AlertsGrouper;
use crate::models::{
    alerts::{Alert, AlertsDataGroup, AlertsGroup},
    jams::{Jam, JamsGroup},
};
use sqlx::PgPool;
use std::env;

use axum::{Json, http::StatusCode, response::IntoResponse};
use memcache::{CommandError, MemcacheError};
use serde_json::json;

#[allow(dead_code)]
#[derive(Debug)]
pub enum UpdateError {
    ApiError(Box<dyn std::error::Error>),
    DatabaseError(sqlx::Error),
}

#[allow(dead_code)]
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

/// Fetch data from the Waze API and store it in the database
///
/// # Returns
/// * Tuple containing alerts and jams retrieved from the API
pub async fn update_data_from_api() -> Result<Json<(AlertsGroup, JamsGroup)>, UpdateError> {
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

    Ok(Json((alerts, jams)))
}

/// Retrieve data from the cache if it exists; otherwise get it from the database
///
#[axum::debug_handler]
pub async fn get_data() -> Result<Json<AlertsDataGroup>, CacheError> {
    // Check for data in cache

    tracing::info!("Connecting to memcache...");
    let client = match memcache::connect("memcache://127.0.0.1:11211") {
        Ok(client) => client,
        Err(e) => {
            tracing::error!("Failed to connect to memcache: {}", e);
            return Err(CacheError::ConnectionError(Box::new(e)));
        }
    };

    tracing::info!("Connected to memcache successfully!");
    let alerts: Option<_> = match get_data_from_cache(&client).await {
        Ok(alerts) => Some(alerts),
        Err(e) => {
            tracing::info!("Data not found in cache, retrieving from database...");
            tracing::info!("{:?}", e);
            None
        }
    };

    let alerts = match alerts {
        Some(alerts) => alerts,
        None => {
            let alerts = match get_data_from_database().await {
                Ok(alerts) => alerts,
                Err(e) => {
                    tracing::error!("Error retrieving data from database.");
                    return Err(CacheError::ConnectionError(Box::new(e)));
                }
            };
            // Set in cache
            tracing::info!("Setting data in cache");
            if let Err(e) = client
                .set("alerts", &alerts, 40)
                .map_err(|e| CacheError::ConnectionError(Box::new(e)))
            {
                tracing::error!("Error setting data in cache: {:?}", e);
            }

            alerts
        }
    };

    let alerts_grouper = AlertsGrouper::new((10, 20)).map_err(|e| CacheError::GroupingError(e))?;
    let alerts: AlertsDataGroup = alerts_grouper
        .group(alerts, &client)
        .await
        .map_err(|e| CacheError::GroupingError(e))?;

    Ok(Json(alerts))
}

/// Retrieve data from database or cache if it exists
///
/// # Returns
/// * Grouped alerts by location
pub async fn get_data_from_cache(client: &memcache::Client) -> Result<AlertsGroup, CacheError> {
    tracing::info!("Retrieving data...");
    let alerts = match client.get("alerts") {
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

    tracing::info!("Data found");

    Ok(alerts)
}

pub async fn get_data_from_database() -> Result<AlertsGroup, sqlx::Error> {
    tracing::info!("Retrieving data from database...");
    let pool = connect_to_db().await?;

    let query = r#"
    SELECT a.*, l.x, l.y
    FROM alerts a
    LEFT JOIN alerts_location l ON a.location_id = l.id
    "#;

    let alerts: AlertsGroup = match sqlx::query_as(&query).fetch_all(&pool).await {
        Ok(alerts) => AlertsGroup { alerts },
        Err(e) => {
            tracing::error!("Error retrieving data from database {}", e);
            return Err(e);
        }
    };

    tracing::info!("Data found");

    Ok(alerts)
}

pub async fn connect_to_db() -> Result<PgPool, sqlx::Error> {
    let database_url =
        env::var("DATABASE_URL").expect("Environment variable DATABASE_URL must be set");
    PgPool::connect(&database_url).await
}
