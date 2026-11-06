mod db;
mod api;
mod nfo;

use axum::{routing::{get, post}, Router, middleware};
use tower_http::{services::ServeDir, cors::CorsLayer};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    // 自动创建 data 目录，防止报错
    std::fs::create_dir_all("/app/data").ok();
    
    // 数据库文件改到 /app/data 下
    let conn = rusqlite::Connection::open("/app/data/rust_em.db").unwrap();
    db::init_db(&conn);
    let db_pool = Arc::new(Mutex::new(conn));

    let api_routes = Router::new()
        .route("/login", post(api::login))
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/items", get(api::get_items))
        .layer(middleware::from_fn(api::auth))
        .with_state(db_pool);

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
