extern crate chrono;
extern crate dotenv;

mod api;
mod data;
mod models;
mod server;
mod errors;

use dotenv::dotenv;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    dotenv().ok();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    server::create_server().await.unwrap_or_else(|e| {
        tracing::error!("Error in server: {}", e);
    });
}
