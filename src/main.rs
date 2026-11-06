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
        .route("/browse", get(api::browse_dir)) 
        .route("/user/update", post(api::update_user))
        .route("/images/{id}/{type}", get(api::get_image))
        .layer(middleware::from_fn(api::auth))
        .with_state(db_pool.clone());

    let public_routes = Router::new()
        .route("/login", post(api::login))
        .with_state(db_pool.clone());

    let api_routes = Router::new()
        .merge(public_routes)
        .merge(protected_routes);

    // 【新增】极简 Emby 兼容路由
    let emby_routes = Router::new()
        // 1. 登录认证
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        // 2. 获取列表
        .route("/Users/{userId}/Items", get(api::emby_items))
        // 3. 播放信息
        .route("/Items/{itemId}/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        // 4. 视频流直链提取 (302重定向)
        .route("/Videos/{itemId}/stream", get(api::emby_stream).head(api::emby_stream))
        .route("/Videos/{itemId}/original.strm", get(api::emby_stream).head(api::emby_stream))
        .route("/System/Info", get(api::emby_info))
        .route("/System/Info/Public", get(api::emby_info))
        .with_state(db_pool.clone());

    let app = Router::new()
        .nest("/api", api_routes)
        .nest("/emby", emby_routes.clone()) // 客户端通常请求带 /emby 前缀
        .merge(emby_routes) // 也兼容不带前缀的请求
        .fallback_service(ServeDir::new("public"))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 rust-em running on 8096");
    axum::serve(listener, app).await.unwrap();
}