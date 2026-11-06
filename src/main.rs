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

    let protected_routes = Router::new()
        // 媒体库管理
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/libraries/{id}", delete(api::delete_library))
        .route("/libraries/{id}/scan", post(api::rescan_library))
        // 媒体内容
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

    let emby_routes = Router::new()
        .route("/System/Info", get(api::emby_info))
        .route("/System/Info/Public", get(api::emby_info))
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        .route("/Users/{userId}/Items", get(api::emby_items))
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