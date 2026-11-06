mod db;
mod api;
mod nfo;

use axum::{routing::{get, post}, Router, middleware};
use tower_http::{services::ServeDir, cors::CorsLayer};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    let conn = rusqlite::Connection::open("rust_em.db").unwrap();
    db::init_db(&conn);
    let db_pool = Arc::new(Mutex::new(conn));

    let api_routes = Router::new()
        .route("/login", post(api::login))
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/items", get(api::get_items))
        // 修复：不需要用 from_fn_with_state，直接用 from_fn
        .layer(middleware::from_fn(api::auth))
        .with_state(db_pool.clone());

    let app = Router::new()
        .nest("/api", api_routes)
        .route("/System/Info", get(api::emby_info))
        .route("/Users/{userId}/Items", get(api::emby_items))
        .fallback_service(ServeDir::new("public"))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 rust-em running on 8096");
    axum::serve(listener, app).await.unwrap();
}
