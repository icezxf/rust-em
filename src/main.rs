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

    // 1. 我们自己的 Web UI API (带鉴权)
    let protected_routes = Router::new()
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/items", get(api::get_items))
        .route("/browse", get(api::browse_dir)) 
        .route("/user/update", post(api::update_user))
        .route("/images/{id}/{type}", get(api::get_image))
        .layer(middleware::from_fn(api::auth))
        .with_state(db_pool.clone());

    // 2. 公开 API
    let public_routes = Router::new()
        .route("/login", post(api::login))
        .with_state(db_pool.clone());

    let api_routes = Router::new()
        .merge(public_routes)
        .merge(protected_routes);

    // 3. Emby 协议兼容层 (Infuse/播放器专用)
    let emby_routes = Router::new()
        .route("/System/Info", get(api::emby_info))
        .route("/System/Info/Public", get(api::emby_info))
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        .route("/Users/{userId}/Items", get(api::emby_items))
        .route("/Items/{itemId}/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        // 兼容播放器不同的流请求路径
        .route("/Videos/{itemId}/stream", get(api::emby_stream).head(api::emby_stream))
        .route("/Videos/{itemId}/original.strm", get(api::emby_stream).head(api::emby_stream))
        .with_state(db_pool.clone());

    // 4. 组装 App
    let app = Router::new()
        .nest("/api", api_routes)
        .nest("/emby", emby_routes.clone()) // 兼容 /emby/Users/...
        .merge(emby_routes)                 // 兼容直接 /Users/... 请求
        .fallback_service(ServeDir::new("public")) // 挂载前端网页
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 rust-em running on 8096");
    axum::serve(listener, app).await.unwrap();
}