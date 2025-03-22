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
    use std::sync::Once;
    use sqlx::Postgres;

    use crate::models::alerts::{AlertsGroup, Location, Alert, AlertType};

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

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env.test");

        sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to create test database pool")
    }

    use std::sync::Arc;

    use crate::server::{CacheState, init_cache};

    static CACHE: tokio::sync::OnceCell<Arc<CacheState>> = tokio::sync::OnceCell::const_new();

    // Get the cache connection instance
    pub async fn setup_cache() -> Arc<CacheState> {
        CACHE
            .get_or_init(|| async {
                // Initialize test cache
                init_cache().await
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
