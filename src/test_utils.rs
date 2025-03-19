#[cfg(test)]
pub mod database {
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn setup_test_env() {
        INIT.call_once(|| {
            dotenv::from_filename(".env.test").ok();
        });
    }

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
}

#[cfg(test)]
pub mod cache {
    use std::sync::Arc;

    use crate::server::{CacheState, init_cache};

    static CACHE: tokio::sync::OnceCell<Arc<CacheState>> = tokio::sync::OnceCell::const_new();

    pub async fn setup_cache() -> Arc<CacheState> {
        CACHE.get_or_init(|| async {
            // Initialize test cache
            init_cache().await
        })
        .await.clone()
    }
}
