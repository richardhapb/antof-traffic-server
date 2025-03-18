use axum::{
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

/// # Custom errors for Events models
pub enum EventError<'a> {
    ApiRequestError,
    DatabaseError(&'a str),
    SerializeError(&'a str),
    DeserializeError(&'a str),
    RequestDataError(&'a str),
}

/// # Customs errors for Axus handlers
/// Errors while update data
#[derive(Debug)]
pub enum UpdateError {
    ApiError(Box<dyn std::error::Error>),
    DatabaseError(sqlx::Error),
    CacheError(CacheError),
}

/// Errors while interact with cache
#[derive(Debug)]
pub enum CacheError {
    RequestError(memcache::MemcacheError),
    NotFound(memcache::MemcacheError),
    GroupingError(Box<dyn std::error::Error>),
}

// Adjust error for work with Axum errors type

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

