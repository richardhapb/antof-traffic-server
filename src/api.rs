use crate::models::events;
use reqwest::blocking::Client;
use std::env;
use std::error::Error;

pub fn request_waze_data() -> Result<String, Box<dyn Error>> {
    let client = Client::new();
    let url = env::var("WAZE_API_URL").expect("WAZE_API_URL must be available");

    let response = client.get(url).send()?;

    Ok(response.text()?)
}

pub fn request_and_parse() -> Result<events::AlertsGroup, Box<dyn std::error::Error>> {
    let response = request_waze_data()?;

    let alerts: events::AlertsGroup = serde_json::from_str(&response)?;

    Ok(alerts)
}
