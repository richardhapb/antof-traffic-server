use antof_traffic::cache::{ALERTS_CACHE_KEY, CacheService};
use antof_traffic::data::MIN_PUB_MILLIS_CACHE_KEY;
use antof_traffic::models::{
    alerts::{Alert, AlertType, AlertsDataGroup, AlertsGroup, Location},
    jams::JamsGroup,
};
use antof_traffic::server::create_server;
use chrono::Utc;
use reqwest::Client;
use serial_test::serial;
use tokio::task::JoinHandle;

const TEST_SERVER_URL_UPDATE: &str = "http://0.0.0.0:7070/update-data";
const TEST_SERVER_URL_GET: &str = "http://0.0.0.0:7070/get-data?since={since}";
const TEST_CLEAR_CACHE_URL: &str = "http://0.0.0.0:7070/clear-cache?key={key}";
const TEST_AGGREGATE_URL: &str = "http://0.0.0.0:7070/aggregate";

fn start_http_server() -> JoinHandle<()> {
    dotenv::dotenv().ok();
    tokio::task::spawn(async {
        create_server().await.unwrap();
    })
}

// Create test data
pub fn setup_alerts() -> AlertsGroup {
    let alerts: Vec<Alert> = vec![
        Alert {
            uuid: uuid::Uuid::parse_str("a0f93cf6-9099-4962-8f9a-72c30186571c").unwrap(),
            reliability: Some(2),
            alert_type: Some(AlertType::Accident),
            road_type: Some(2),
            magvar: Some(3.0),
            subtype: Some("Some accident".to_string()),
            location: Some(Location {
                id: 0,
                x: -70.39831,
                y: -23.651636,
            }),
            street: Some("Av. Pedro Aguirre Cerda".to_string()),
            pub_millis: 1736980027000,
            end_pub_millis: None,
        },
        Alert {
            uuid: uuid::Uuid::parse_str("a123f22e-e5e0-4c6c-8a4e-7434c4fd2110").unwrap(),
            reliability: Some(2),
            alert_type: Some(AlertType::Accident),
            road_type: Some(2),
            magvar: Some(3.3),
            subtype: Some("Some accident".to_string()),
            location: Some(Location {
                id: 0,
                x: -70.37841,
                y: -23.625319,
            }),
            street: Some("Av. Pedro Aguirre Cerda".to_string()),
            pub_millis: 1731210357000,
            end_pub_millis: None,
        },
    ];

    AlertsGroup { alerts }
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
    CacheService::init_cache().await;
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear cache before test
    let url = TEST_CLEAR_CACHE_URL.replace("{key}", ALERTS_CACHE_KEY);
    reqwest::get(url).await.unwrap();
    let url = TEST_CLEAR_CACHE_URL.replace("{key}", MIN_PUB_MILLIS_CACHE_KEY);
    reqwest::get(url).await.unwrap();

    let response_update = reqwest::get(TEST_SERVER_URL_UPDATE)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let (alerts_update, _) = serde_json::from_str::<(AlertsDataGroup, JamsGroup)>(&response_update).unwrap();

    let min_pub_millis = alerts_update
        .alerts
        .iter()
        .map(|a| a.alert.pub_millis)
        .min()
        .unwrap();

    let url = TEST_SERVER_URL_GET.replace("{since}", &min_pub_millis.to_string());
    let response_get = reqwest::get(url).await.unwrap().text().await.unwrap();
    let alerts_get: AlertsDataGroup = serde_json::from_str(&response_get).unwrap();

    // The retrieved data should contain the data obtained from the API because
    // it should be inserted into the cache and `concat` method stores unique values
    assert_eq!(
        alerts_get.alerts.len(),
        alerts_get.concat(alerts_update).alerts.len()
    );
    handler.abort();
    let _ = handler.await;
}

#[tokio::test]
#[serial]
async fn test_aggregate() {
    // Test the aggregate endpoint
    CacheService::init_cache().await;
    let handler = start_http_server();

    // Add a delay to ensure server is ready before making requests
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let client = Client::new();
    let alerts = setup_alerts();

    let response = client
        .post(TEST_AGGREGATE_URL)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&alerts).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let result = response.text().await.unwrap();
    let res_alerts: AlertsDataGroup = serde_json::from_str(&result).unwrap();

    assert_eq!(res_alerts.alerts.len(), alerts.alerts.len());
    let alert = res_alerts.alerts.first().unwrap();

    // Aggregate must be some
    assert!(alert.group.is_some());
    assert!(alert.week_day.is_some());
    assert!(alert.day_type.is_some());
    assert!(alert.day.is_some());
    assert!(alert.hour.is_some());
    assert!(alert.minute.is_some());
    handler.abort();
    let _ = handler.await;
}

#[tokio::test]
#[serial]
async fn test_old_data() {
    let cache = CacheService::init_cache().await;
    let handler = start_http_server();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clear cache before test
    cache.remove_key(ALERTS_CACHE_KEY).unwrap();
    cache.remove_key(MIN_PUB_MILLIS_CACHE_KEY).unwrap();

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
