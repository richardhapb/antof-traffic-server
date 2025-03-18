use crate::models::{alerts, jams};
use reqwest::Client;
use std::env;
use std::error::Error;

/// Get data from Waze API
///
/// # Returns
/// * Result enum with the raw json or an error
pub async fn request_waze_data() -> Result<String, Box<dyn Error>> {
    let client = Client::new();
    let url = env::var("WAZE_API_URL").expect("WAZE_API_URL must be available");

    let response = client.get(url).send().await?;

    Ok(response.text().await?)
}

/// Transform the raw data to `AlertsGroup`
///
/// # Returns
/// * Result enum with the `AlertsGroup` instance or an error
pub async fn request_and_parse() -> Result<(alerts::AlertsGroup, jams::JamsGroup), Box<dyn std::error::Error>> {
    let response = request_waze_data().await?;

    let alerts: alerts::AlertsGroup = serde_json::from_str(&response)?;
    let jams: jams::JamsGroup = serde_json::from_str(&response)?;

    Ok((alerts, jams))
}
