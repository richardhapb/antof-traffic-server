use crate::cache::{CacheService, ALERTS_GROUPER_CACHE_KEY, ALERTS_GROUPER_CACHE_EXP};
use crate::server::FilterParams;
use crate::errors::{CacheError, UpdateError};
use crate::models::alerts::{AlertsDataGroup, AlertsGroup, AlertsGrouper};
use std::cmp::{max, min};
use std::sync::Arc;

/// Get the range of the [`FilterParams`] and return
/// `since` and `until` with the correct values
///
/// The return value must be between `ALERTS_BEGIN_TIMESTAMP`
/// and the current time, inclusively.
#[macro_export] macro_rules! get_time_range {
    ($params:expr) => {{
        let now = Utc::now().timestamp() * 1000;
        let since = max(
            ALERTS_BEGIN_TIMESTAMP,
            $params.since.unwrap_or(ALERTS_BEGIN_TIMESTAMP),
        );
        let until = min(now, $params.until.unwrap_or(now));
        (since, until)
    }};
}

/// Calculate the min and max pub_millis from params and match since/until
/// to the max or min pub_millis, if params.since and params.until are larger
/// than the cache, returns `params_since` and `params_until`
///
/// # Example
/// ```
/// use antof_traffic::data::FilterParams;
/// use antof_traffic::utils::calculate_params;
///
/// let since = 10000;
/// let until = 20000;
/// let params = FilterParams { since: Some(5000), until: Some(15000)};
///
/// let result = calculate_params(since, until, &params);
/// assert_eq!(result.since.unwrap(), 5000);
/// assert_eq!(result.until.unwrap(), since); // Until the initial `since`
///
/// let since = 10000;
/// let until = 20000;
/// let params = FilterParams { since: Some(15000), until: Some(25000)};
///
/// let result = calculate_params(since, until, &params);
/// assert_eq!(result.since.unwrap(), until); // Since the initial `until`
/// assert_eq!(result.until.unwrap(), 25000);
///
/// let since = 10000;
/// let until = 20000;
/// let params = FilterParams { since: Some(5000), until: Some(25000)};
///
/// let result = calculate_params(since, until, &params);
/// // The range is larger than the initial params
/// assert_eq!(result.since.unwrap(), 5000);
/// assert_eq!(result.until.unwrap(), 25000);
/// ```
pub fn calculate_params(min_millis: i64, max_millis: i64, params: &FilterParams) -> FilterParams {
    let params_since = params.since.unwrap_or(min_millis);
    let params_until = params.until.unwrap_or(max_millis);

    // Get the edge of requested data
    let mut since = min(min_millis, params_since);
    let mut until = max(max_millis, params_until);

    // Only retrieve the least amount of data
    if since < min_millis && until <= max_millis {
        until = min_millis
    } else if since >= min_millis && until > max_millis {
        since = max_millis
    }

    FilterParams {
        since: Some(since),
        until: Some(until),
    }
}

/// Group the alerts and generate aggregate data, try get grouper from cache
///
/// # Params
/// * alerts: Alerts to group
/// * cache_service: Global state with cache connection
///
/// # Returns
/// * Result with new [`AlertsDataGroup`] or an [`UpdateError`]
pub async fn group_alerts(
    alerts: AlertsGroup,
    cache_service: Arc<CacheService>,
) -> Result<AlertsDataGroup, UpdateError> {
    // Group data, try get from cache the grouper
    let alerts_grouper = match cache_service
        .client
        .get(ALERTS_GROUPER_CACHE_KEY)
        .map_err(|e| UpdateError::Cache(CacheError::Request(e)))?
    {
        Some(alerts_grouper) => alerts_grouper,
        None => {
            let alerts_grouper =
                AlertsGrouper::new((10, 20)).map_err(|e| UpdateError::Cache(CacheError::Grouping(e)))?;
            cache_service
                .client
                .set(
                    ALERTS_GROUPER_CACHE_KEY,
                    &alerts_grouper,
                    ALERTS_GROUPER_CACHE_EXP,
                )
                .map_err(|e| UpdateError::Cache(CacheError::Request(e)))?;

            alerts_grouper
        }
    };

    let alerts = alerts_grouper
        .group(alerts, Arc::clone(&cache_service))
        .await
        .map_err(|e| UpdateError::Cache(CacheError::Grouping(e)))?;

    Ok(alerts)
}

/// Create pool connection with postgres
///
/// # Returns
/// * Result enum with pool connection or sqlx error
pub async fn connect_to_db() -> Result<sqlx::Pool<sqlx::Postgres>, sqlx::Error> {
    #[cfg(test)]
    use test::get_test_db_pool;
    #[cfg(test)]
    return Ok(get_test_db_pool().await);

    #[cfg(not(test))]
    {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
    }
}

// Test utils
#[cfg(test)]
pub mod test {
    use sqlx::Postgres;
    use std::sync::Once;

    use crate::models::alerts::{Alert, AlertType, AlertsGroup, Location};

    static INIT: Once = Once::new();

    // Read .env file for testing
    pub fn setup_test_env() {
        INIT.call_once(|| {
            dotenv::from_filename(".env.test").ok();
        });
    }

    // Get test database pool
    pub async fn get_test_db_pool() -> sqlx::Pool<sqlx::Postgres> {
        setup_test_env();

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env.test");

        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to create test database pool")
    }

    use std::sync::Arc;

    use crate::cache::CacheService;

    static CACHE: tokio::sync::OnceCell<Arc<CacheService>> = tokio::sync::OnceCell::const_new();

    // Get the cache connection instance
    pub async fn setup_cache() -> Arc<CacheService> {
        CACHE
            .get_or_init(|| async {
                // Initialize test cache
                CacheService::init_cache().await
            })
            .await
            .clone()
    }

    // Clean test database
    pub async fn setup_test_db() -> sqlx::Pool<Postgres> {
        let pool = get_test_db_pool().await;

        // Clear existing data
        sqlx::raw_sql("DELETE FROM alerts; DELETE FROM alerts_location;")
            .execute(&pool)
            .await
            .expect("Failed to clear test database");

        pool
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
}
