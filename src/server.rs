use axum::{
    routing::get,
    Router,
};

use std::sync::{Arc, OnceLock};

use crate::data;
use crate::models::cache::MEMCACHE_URI;


pub struct CacheState {
    pub client: memcache::Client
}

static CACHE_STATE: OnceLock<Arc<CacheState>> = OnceLock::new();

pub async fn init_cache() -> Arc<CacheState> {
    CACHE_STATE.get_or_init(|| {
        Arc::new(CacheState { 
            client: memcache::Client::connect(MEMCACHE_URI).unwrap() 
        })
    }).clone()
}

pub async fn create_server() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting server on 0.0.0.0:7070");
    
    let cache_state = init_cache().await;
    
    let app = Router::new()
        .route("/update-data", get(data::update_data_from_api))
        .route("/get-data", get(data::get_data))
        .with_state(cache_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7070").await?;
    tracing::info!("Server is running on http://0.0.0.0:7070");
    axum::serve(listener, app).await?;
    Ok(())
}

