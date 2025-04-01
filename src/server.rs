use axum::{
    routing::get,
    Router,
};

use crate::data;
use crate::cache;
use crate::cache::CacheService;

/// Main server handler
pub async fn create_server() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting server on 0.0.0.0:7070");
    
    let cache_service = CacheService::init_cache().await;
    
    let app = Router::new()
        .route("/update-data", get(data::update_data_from_api))
        .route("/get-data", get(data::get_data))
        .route("/clear-cache", get(cache::clear_cache))
        .with_state(cache_service);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7070").await?;
    tracing::info!("Server is running on http://0.0.0.0:7070");
    axum::serve(listener, app).await?;
    Ok(())
}

