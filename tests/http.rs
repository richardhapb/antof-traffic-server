use antof_traffic::server::{init_cache, create_server};
use antof_traffic::data::{LAST_UPDATE_KEY, ALERTS_CACHE_KEY};
use reqwest;
use tokio::task::JoinHandle;
use chrono::Utc;
use serial_test::serial;

const TEST_SERVER_URL_UPDATE: &str = "http://0.0.0.0:7070/update-data";
const TEST_SERVER_URL_GET: &str = "http://0.0.0.0:7070/get-data?since={since}";

fn start_http_server() -> JoinHandle<()> {
    dotenv::dotenv().ok();
    tokio::task::spawn(async {
        create_server().await.unwrap();
    })
}

#[tokio::test]
#[serial]
async fn test_update_data_from_api() {
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let response = reqwest::get(TEST_SERVER_URL_UPDATE).await.unwrap();

    assert!(response.text().await.unwrap().contains("alerts"));
    handler.abort();
    let _ = handler.await;
}

#[tokio::test]
#[serial]
async fn test_get_data() {
    let handler = start_http_server();

    let now = Utc::now().timestamp() * 1000;

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let url = TEST_SERVER_URL_GET.replace("{since}", &now.to_string());

    let response = reqwest::get(url).await.unwrap();

    assert!(response.text().await.unwrap().contains("alerts"));
    handler.abort();
    let _ = handler.await;
}

#[tokio::test]
#[serial]
async fn test_integrity() {
    let cache = init_cache().await.client.clone();
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear cache before test
    cache.delete(ALERTS_CACHE_KEY).unwrap();
    cache.delete(LAST_UPDATE_KEY).unwrap();

    // If the request is after the last update
    // returns all data from cache
    let now = Utc::now().timestamp() * 1000 + 10000;

    let response_update = reqwest::get(TEST_SERVER_URL_UPDATE).await.unwrap().text().await.unwrap();
    let url = TEST_SERVER_URL_GET.replace("{since}", &now.to_string());
    let response_get = reqwest::get(url).await.unwrap().text().await.unwrap();

    let alerts_length = response_get.len();

    // The response from API contains `jams` data, and cache only alerts
    // and first character contains `[` because is a list of keys `alerts` and `jams`
    assert_eq!(response_update[1..=alerts_length], response_get);
    handler.abort();
    let _ = handler.await;
}

#[tokio::test]
#[serial]
async fn test_old_data() {
    let cache = init_cache().await.client.clone();
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear cache before test
    cache.delete(ALERTS_CACHE_KEY).unwrap();
    cache.delete(LAST_UPDATE_KEY).unwrap();

    // Get old data that don't exist in cache
    // 120 days ago
    let now = Utc::now().timestamp() * 1000 + 10368000000;


    let url = TEST_SERVER_URL_GET.replace("{since}", &now.to_string());
    let response_get = reqwest::get(url).await.unwrap().text().await.unwrap();

    // Should be contain data
    assert!(response_get.len() > 0);
    handler.abort();
    let _ = handler.await;
}
