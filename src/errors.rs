use std::{error::Error, fmt::Display};

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;

/// # Custom errors for Events models
pub enum EventError<'a> {
    ApiRequest,
    Database(&'a str),
    Serialize(&'a str),
    Deserialize(&'a str),
    RequestData(&'a str),
}

/// # Customs errors for Axus handlers
/// Errors while update data
#[derive(Debug)]
pub enum UpdateError {
    Api(Box<dyn std::error::Error>),
    Database(sqlx::Error),
    Cache(CacheError),
}

/// Errors while interact with cache
#[derive(Debug)]
pub enum CacheError {
    Request(memcache::MemcacheError),
    NotFound(memcache::MemcacheError),
    Grouping(Box<dyn std::error::Error>),
}

impl Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::Request(e) => write!(f, "Cache request error: {}", e),
            CacheError::NotFound(e) => write!(f, "Cache key not found: {}", e),
            CacheError::Grouping(e) => write!(f, "Cache grouping error: {}", e),
        }
    }
}

impl Error for CacheError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CacheError::Request(e) => Some(e),
            CacheError::NotFound(e) => Some(e),
            CacheError::Grouping(e) => Some(&**e),
        }
    }
}

unsafe impl Send for CacheError {}
unsafe impl Sync for CacheError {}

// Adjust error for work with Axum errors type

impl IntoResponse for UpdateError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            UpdateError::Api(e) => (StatusCode::BAD_GATEWAY, format!("API error: {}", e)),
            UpdateError::Database(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database error: {}", e),
            ),
            UpdateError::Cache(e) => (StatusCode::BAD_GATEWAY, format!("Cache error: {:?}", e)),
        };

        (status, Json(json!({ "error": error_message }))).into_response()
    }
}

impl IntoResponse for CacheError {
    fn into_response(self) -> axum::response::Response {
        let (status, error_message) = match self {
            CacheError::Request(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Request error: {}", e)),
            CacheError::NotFound(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Key not found: {}", e)),
            CacheError::Grouping(e) => (StatusCode::BAD_GATEWAY, format!("Error grouping data: {}", e)),
        };

        (status, Json(json!({ "error": error_message }))).into_response()
    }
}
