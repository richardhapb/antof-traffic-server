use crate::models::events;
use reqwest::Client;
use std::env;
use std::error::Error;

pub async fn request_waze_data() -> Result<String, Box<dyn Error>> {
    let client = Client::new();
    let url = env::var("WAZE_API_URL").expect("WAZE_API_URL must be available");

    let response = client.get(url).send().await?;

    Ok(response.text().await?)
}

pub async fn request_and_parse() -> Result<(events::AlertsGroup, events::JamsGroup), Box<dyn std::error::Error>> {
    let response = request_waze_data().await?;

    let alerts: events::AlertsGroup = serde_json::from_str(&response)?;
    let jams: events::JamsGroup = serde_json::from_str(&response)?;

    Ok((alerts, jams))
}
