use axum::{
    routing::get,
    Router
};

use crate::data;

pub async fn create_server() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!("Starting server on 0.0.0.0:7070");
    let app = Router::new()
        .route("/update-data", get(data::update_data_from_api))
        .route("/get-data", get(data::get_data));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7070").await?;
    tracing::info!("Server is running on http://0.0.0.0:7070");
    axum::serve(listener, app).await?;
    Ok(())
}
