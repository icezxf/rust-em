mod db;
mod api;
mod nfo;

use axum::{routing::{get, post, delete}, Router, middleware};
use tower_http::{services::ServeDir, cors::CorsLayer};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    std::fs::create_dir_all("/app/data").ok();
    let conn = rusqlite::Connection::open("/app/data/rust_em.db").unwrap();
    db::init_db(&conn);
    let db_pool = Arc::new(Mutex::new(conn));

    // 1. Web UI 后端 API
    let protected_routes = Router::new()
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/libraries/{id}", delete(api::delete_library))
        .route("/libraries/{id}/scan", post(api::rescan_library))
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

    // 2. Emby 协议兼容层
    let emby_routes = Router::new()
        // 系统与探测
        .route("/System/Info", get(api::emby_info))
        .route("/System/Info/Public", get(api::emby_public_info))
        .route("/System/Ping", get(|| async { "Emby Server" }))
        .route("/Auth/Keys", get(api::emby_auth_keys))
        
        // 登录与鉴权
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        
        // 媒体库展示与获取
        .route("/Users/{userId}/Views", get(api::emby_views))
        .route("/Users/{userId}/Items", get(api::emby_items))
        .route("/Items/{itemId}", get(api::emby_item_detail))
        .route("/Items/{itemId}/Images/{imageType}", get(api::emby_item_image))
        .route("/Items/{itemId}/Images/{imageType}/{imageIndex}", get(api::emby_item_image))
        
        // 隐藏继续观看 (兼容两种路径)
        .route("/Users/{userId}/Items/{itemId}/HideFromResume", post(api::emby_hide_from_resume))
        .route("/Items/{itemId}/HideFromResume", post(api::emby_hide_from_resume_without_user))
        
        // 播放信息与视频流
        .route("/Items/{itemId}/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        .route("/Videos/{itemId}/stream", get(api::emby_stream).head(api::emby_stream))
        .route("/Videos/{itemId}/original.strm", get(api::emby_stream).head(api::emby_stream))
        
        .with_state(db_pool.clone());

    let app = Router::new()
        .nest("/api", api_routes)
        .nest("/emby", emby_routes.clone())
        .merge(emby_routes)
        .fallback_service(ServeDir::new("public"))
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 rust-em running on 8096");
    axum::serve(listener, app).await.unwrap();
}