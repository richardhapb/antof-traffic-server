use antof_traffic::api::request_waze_data;
use antof_traffic::cache::CacheService;
use antof_traffic::data::{ALERTS_CACHE_KEY, MIN_PUB_MILLIS, concat_alerts_and_storage_to_cache};
use antof_traffic::models::alerts::{AlertsDataGroup, AlertsGroup};
use antof_traffic::server::create_server;
use antof_traffic::utils::group_alerts;
use chrono::Utc;
use serial_test::serial;
use tokio::task::JoinHandle;

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
    let cache = CacheService::init_cache().await;
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear cache before test
    cache.client.delete(ALERTS_CACHE_KEY).unwrap();
    cache.client.delete(MIN_PUB_MILLIS).unwrap();

    let response_update = request_waze_data().await.unwrap();

    let alerts_update: AlertsGroup = serde_json::from_str(&response_update).unwrap();
    let alerts_update: AlertsDataGroup = group_alerts(alerts_update, cache.clone()).await.unwrap();

    let alerts_update = concat_alerts_and_storage_to_cache(cache, alerts_update).unwrap();

    let min_pub_millis = alerts_update
        .alerts
        .iter()
        .map(|a| a.alert.pub_millis)
        .min()
        .unwrap();

    let url = TEST_SERVER_URL_GET.replace("{since}", &min_pub_millis.to_string());
    let response_get = reqwest::get(url).await.unwrap().text().await.unwrap();
    let alerts_get: AlertsDataGroup = serde_json::from_str(&response_get).unwrap();

    // The response from API contains `jams` data, and cache only alerts
    // and first character contains `[` because is a list of keys `alerts` and `jams`
    assert_eq!(alerts_update.alerts.len(), alerts_get.alerts.len());
    handler.abort();
    let _ = handler.await;
}

#[tokio::test]
#[serial]
async fn test_old_data() {
    let cache = CacheService::init_cache().await.client.clone();
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear cache before test
    cache.delete(ALERTS_CACHE_KEY).unwrap();
    cache.delete(MIN_PUB_MILLIS).unwrap();

    // Get old data that don't exist in cache
    // 120 days ago
    let now = Utc::now().timestamp() * 1000 + 10368000000;

    let url = TEST_SERVER_URL_GET.replace("{since}", &now.to_string());
    let response_get = reqwest::get(url).await.unwrap().text().await.unwrap();

    // Should be contain data
    assert!(!response_get.is_empty());
    handler.abort();
    let _ = handler.await;
}
