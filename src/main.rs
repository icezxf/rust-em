#![recursion_limit = "256"]

mod covers;

use axum::{
    routing::{get, post, delete},
    Router, middleware,
};
use std::sync::{Arc, Mutex};
use rusqlite::Connection;
use tokio::net::TcpListener;

mod api;
mod auth;
mod scanner;
mod streamer;
mod images;
mod db;
mod nfo;
mod probe;
mod utils;

pub type AppState = Arc<Mutex<Connection>>;

#[tokio::main]
async fn main() {
    // 1. 初始化数据库
    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "/app/data/data.db".to_string());
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let conn = Connection::open(&db_path).expect("无法打开数据库");
    
    // 💡 所有底层建表、初始管理员全部交给 db.rs
    db::init_db(&conn);
    db::ensure_extra_columns(&conn);
    db::init_settings_table(&conn);
    
    println!("✅ [DB] 数据库初始化完成");

    let state = Arc::new(Mutex::new(conn));

    // 2. 组装路由
    let app = Router::new()
        // ===== Web 网页端 API =====
        .route("/api/login", post(api::login))
        .route("/api/browse", get(api::browse_dir))
        .route("/api/libraries", get(api::get_libraries).post(api::add_library))
        .route("/api/libraries/{id}", delete(api::delete_library))
        .route("/api/libraries/{id}/scan", post(api::rescan_library))
        .route("/api/items", get(api::get_items))
        .route("/api/images/{id}/{img_type}", get(api::get_image).head(api::get_image))
        .route("/api/user/update", post(api::update_user))
        .route("/api/settings", get(api::get_settings).post(api::update_settings))
        .route("/api/users", get(api::api_get_users).post(api::api_create_user))
        .route("/api/users/{id}", delete(api::api_delete_user))
        .route("/api/generate_covers", post(api::api_generate_covers))
        .route("/api/probe/scan", post(api::probe_scan))
        
        // ===== Emby API 路由 =====
        .nest("/emby", emby_routes())
        .merge(emby_routes())

        // 3. 静态文件服务
        .fallback_service(tower_http::services::ServeDir::new("public").append_index_html_on_directories(true))

        // 4. 中间件挂载
        .layer(middleware::from_fn(auth::logger_middleware))
        .layer(middleware::from_fn(auth::auth))
        .with_state(state);

    // 5. 启动服务
    let listener = TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 Server running on http://0.0.0.0:8096");
    axum::serve(listener, app).await.unwrap();
}

fn emby_routes() -> Router<AppState> {
    Router::new()
        .route("/System/Info/Public", get(api::emby_public_info))
        .route("/System/Info", get(api::emby_info))
        .route("/System/ActivityLog/Entries", get(api::emby_activity_log))
        .route("/System/Ping", get(api::emby_ping).post(api::emby_ping))
        .route("/Users/Public", get(api::emby_public_users))
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        .route("/Auth/Keys", get(api::emby_auth_keys))
        .route("/ScheduledTasks", get(api::emby_scheduled_tasks))
        .route("/ScheduledTasks/Running/RefreshLibrary", post(api::emby_refresh_library))
        .route("/Users/{user_id}", get(api::emby_user))
        .route("/DisplayPreferences/{id}", get(api::emby_display_preferences))
        .route("/Library/VirtualFolders", get(api::emby_virtual_folders))
        .route("/Users/{user_id}/Views", get(api::emby_views))
        .route("/Users/{user_id}/Items/Root", get(api::emby_items)) 
        .route("/Items/Root", get(api::emby_items))
        .route("/Users/{user_id}/Items", get(api::emby_items_with_user))
        .route("/Users/{user_id}/Items/Latest", get(api::emby_items_latest))
        .route("/Users/{user_id}/Items/Resume", get(api::emby_items_resume))
        .route("/Users/{user_id}/Items/NextUp", get(api::emby_items_next_up))
        .route("/Items", get(api::emby_items))
        .route("/Items/Counts", get(api::emby_item_counts))
        .route("/Items/Filters", get(api::emby_items_filters))
        .route("/Genres", get(api::emby_genres))
        .route("/Tags", get(api::emby_tags))
        .route("/Years", get(api::emby_years))
        .route("/Studios", get(api::emby_studios))
        .route("/OfficialRatings", get(api::emby_official_ratings))
        .route("/Search/Hints", get(api::emby_search_hints))
        .route("/Users/{user_id}/Items/{item_id}", get(api::emby_item_detail_with_user))
        .route("/Items/{item_id}", get(api::emby_item_detail).head(api::emby_item_detail))
        .route("/Items/{item_id}/Similar", get(api::emby_similar))
        .route("/Items/{item_id}/ThumbnailSet", get(api::emby_thumbnail_set))
        .route("/Persons/{person_id}", get(api::emby_person))
        .route("/Persons/{person_id}/Images/{image_type}", get(images::emby_person_image))
        .route("/Items/{item_id}/Images/{image_type}", get(images::emby_item_image).head(images::emby_item_image))
        .route("/Items/{item_id}/Images/{image_type}/{index}", get(images::emby_item_image_with_index).head(images::emby_item_image_with_index))
        .route("/Users/{user_id}/Images/{image_type}", get(images::emby_item_image_user_only))
        .route("/Shows/NextUp", get(api::emby_shows_next_up))
        .route("/Shows/{series_id}/Seasons", get(api::emby_shows_seasons))
        .route("/Shows/{series_id}/Episodes", get(api::emby_shows_episodes))
        .route("/Items/{item_id}/PlaybackInfo", get(streamer::emby_playback_info).post(streamer::emby_playback_info))
        .route("/Items/{item_id}/Download", get(streamer::emby_download).head(streamer::emby_download))
        .route("/Videos/{item_id}/stream", get(streamer::emby_stream).head(streamer::emby_stream))
        .route("/Videos/{item_id}/original.mp4", get(streamer::emby_stream).head(streamer::emby_stream))
        .route("/Videos/{item_id}/original.strm", get(streamer::emby_stream).head(streamer::emby_stream))
        .route("/Videos/{item_id}/original.{ext}", get(streamer::emby_stream_with_ext).head(streamer::emby_stream_with_ext))
        .route("/Videos/{item_id}/stream.{ext}", get(streamer::emby_stream_with_ext).head(streamer::emby_stream_with_ext))
        .route("/Videos/{item_id}/{source_id}/stream", get(streamer::emby_stream_with_source).head(streamer::emby_stream_with_source))
        .route("/Videos/{item_id}/{source_id}/stream.{ext}", get(streamer::emby_stream_with_source_ext).head(streamer::emby_stream_with_source_ext))
        .route("/videos/{item_id}/stream", get(streamer::emby_stream).head(streamer::emby_stream))
        .route("/videos/{item_id}/original.mp4", get(streamer::emby_stream).head(streamer::emby_stream))
        .route("/videos/{item_id}/original.strm", get(streamer::emby_stream).head(streamer::emby_stream))
        .route("/videos/{item_id}/original.{ext}", get(streamer::emby_stream_with_ext).head(streamer::emby_stream_with_ext))
        .route("/videos/{item_id}/stream.{ext}", get(streamer::emby_stream_with_ext).head(streamer::emby_stream_with_ext))
        .route("/videos/{item_id}/{source_id}/stream", get(streamer::emby_stream_with_source).head(streamer::emby_stream_with_source))
        .route("/videos/{item_id}/{source_id}/stream.{ext}", get(streamer::emby_stream_with_source_ext).head(streamer::emby_stream_with_source_ext))
        .route("/Videos/{item_id}/{source_id}/Subtitles/{stream_index}/Stream", get(api::emby_subtitle_stream_with_source))
        .route("/Items/{item_id}/Subtitles/{stream_index}/Stream", get(api::emby_subtitle_stream))
        .route("/Sessions", get(api::emby_sessions))
        .route("/Sessions/Playing", post(api::emby_playing))
        .route("/Sessions/Playing/Progress", post(api::emby_playing_progress))
        .route("/Sessions/Playing/Stopped", post(api::emby_playing_stopped))
        .route("/Users/{user_id}/Items/{item_id}/HideFromResume", post(api::emby_hide_from_resume))
        .route("/Items/{item_id}/Refresh", post(api::emby_action))
        .route("/Users/{user_id}/PlayedItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        .route("/Users/{user_id}/FavoriteItems/{item_id}", post(api::emby_mark_favorite).delete(api::emby_unmark_favorite))
        .route("/Sessions/Logout", post(api::emby_action))
        .route("/api/danmu/{item_id}", get(api::emby_danmu))
}
