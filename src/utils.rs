#[cfg(test)]
pub mod test {
    use std::sync::Once;

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
