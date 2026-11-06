mod db;
mod api;
mod nfo;

use axum::{routing::{get, post}, Router, middleware};
use tower_http::{services::ServeDir, cors::CorsLayer};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    std::fs::create_dir_all("/app/data").ok();
    
    let conn = rusqlite::Connection::open("/app/data/rust_em.db").unwrap();
    db::init_db(&conn);
    let db_pool = Arc::new(Mutex::new(conn));

    let protected_routes = Router::new()
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/items", get(api::get_items))
        .route("/browse", get(api::browse_dir)) // 已改为 GET
        .route("/user/update", post(api::update_user))
        .layer(middleware::from_fn(api::auth))
        .with_state(db_pool.clone());

    let public_routes = Router::new()
        .route("/login", post(api::login))
        .with_state(db_pool.clone());

    let api_routes = Router::new()
        .merge(public_routes)
        .merge(protected_routes);

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