use crate::api;
use crate::models::{
    events::{Alert, AlertsGroup, Jam, JamsGroup},
    grouper::GroupedAlerts,
};

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;

#[allow(dead_code)]
#[derive(Debug)]
pub enum UpdateError {
    ApiError(Box<dyn std::error::Error>),
    DatabaseError(sqlx::Error),
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

/// Fetch data from the Waze API and store it in the database
///
/// # Returns
/// * Tuple containing alerts and jams retrieved from the API
pub async fn update_data_from_api() -> Result<Json<(AlertsGroup, JamsGroup)>, UpdateError> {
    // Use tracing instead of println for better logging
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

    tracing::info!("Successfully updated data");

    Ok(Json((alerts, jams)))
}

// /// Get data from database or cache if it exists
// ///
// /// # Returns
// /// * Grouped alerts by location
// pub async fn get_data() -> Result<Json<GroupedAlerts>, GetDataError> {
//     return Ok(Json(alerts));
// }
