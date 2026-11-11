#![recursion_limit = "256"]

use axum::{
    routing::{get, post, delete},
    Router, middleware,
};
use std::sync::{Arc, Mutex};
use rusqlite::Connection;
use tokio::net::TcpListener;

mod api;
mod db;
mod nfo;
mod probe;

pub type AppState = Arc<Mutex<Connection>>;

#[tokio::main]
async fn main() {
    // 1. 初始化数据库 (适配 Docker 挂载路径)
    let db_path = std::env::var("DATABASE_PATH").unwrap_or_else(|_| "/app/data/data.db".to_string());
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let conn = Connection::open(&db_path).expect("无法打开数据库");
    db::init_db(&conn);
    db::ensure_extra_columns(&conn);
    db::init_settings_table(&conn);
    
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
        .route("/api/probe/scan", post(api::probe_scan))
        
        // ===== Emby API 路由 (嵌套在 /emby 下) =====
        .nest("/emby", emby_routes())
        
        // ===== Emby 路由 (兼容未嵌套 /emby 的情况) =====
        .merge(emby_routes())

        // 3. 静态文件服务（网页端 index.html 等）
        .fallback_service(tower_http::services::ServeDir::new("public").append_index_html_on_directories(true))

        // 4. 中间件挂载
        .layer(middleware::from_fn(api::logger_middleware))
        .layer(middleware::from_fn(api::auth))
        .with_state(state);

    // 5. 启动服务
    let listener = TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 Server running on http://0.0.0.0:8096");
    axum::serve(listener, app).await.unwrap();
}

fn emby_routes() -> Router<AppState> {
    Router::new()
        // 服务发现与公共认证
        .route("/System/Info/Public", get(api::emby_public_info))
        .route("/System/Info", get(api::emby_info))
        .route("/System/Ping", get(api::emby_ping).post(api::emby_ping))
        .route("/Users/Public", get(api::emby_public_users))
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        .route("/Users/{user_id}/Authenticate", post(api::emby_authenticate)) 
        
        // 用户与鉴权
        .route("/Auth/Keys", get(api::emby_auth_keys))
        .route("/Users/{user_id}", get(api::emby_user))
        .route("/DisplayPreferences/{id}", get(api::emby_display_preferences))
        
        // 媒体库、浏览与搜索
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
        .route("/Search/Hints", get(api::emby_search_hints))
        
        // 详情、元数据与图片
        .route("/Users/{user_id}/Items/{item_id}", get(api::emby_item_detail_with_user))
        .route("/Items/{item_id}", get(api::emby_item_detail).head(api::emby_item_detail))
        .route("/Items/{item_id}/Similar", get(api::emby_similar))
        .route("/Items/{item_id}/ThumbnailSet", get(api::emby_thumbnail_set))
        .route("/Items/{item_id}/Images/{image_type}", 
            get(api::emby_item_image).head(api::emby_item_image))
        .route("/Items/{item_id}/Images/{image_type}/{index}", 
            get(api::emby_item_image_with_index).head(api::emby_item_image_with_index))
        .route("/Users/{user_id}/Images/{image_type}", get(api::emby_item_image_user_only))
        .route("/Persons/{person_id}", get(api::emby_person))
        .route("/Persons/{person_id}/Images/{image_type}", get(api::emby_person_image))
        
        // 剧集相关（返回空或默认处理）
        .route("/Shows/NextUp", get(api::emby_shows_next_up))
        .route("/Shows/{series_id}/Seasons", get(api::emby_shows_seasons))
        .route("/Shows/{series_id}/Episodes", get(api::emby_shows_episodes))
        
        // 播放与视频流 (含 HEAD 支持)
        .route("/Items/{item_id}/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        .route("/Items/{item_id}/Download", get(api::emby_download).head(api::emby_download))
        
        .route("/Videos/{item_id}/stream", get(api::emby_stream).head(api::emby_stream))
        .route("/Videos/{item_id}/original.mp4", get(api::emby_stream).head(api::emby_stream))
        .route("/Videos/{item_id}/original.strm", get(api::emby_stream).head(api::emby_stream))
        .route("/Videos/{item_id}/stream.{ext}", get(api::emby_stream_with_ext).head(api::emby_stream_with_ext))
        .route("/Videos/{item_id}/{source_id}/stream", get(api::emby_stream_with_source).head(api::emby_stream_with_source))
        .route("/Videos/{item_id}/{source_id}/stream.{ext}", get(api::emby_stream_with_source_ext).head(api::emby_stream_with_source_ext))
        
        // 小写路由兼容 (部分客户端重定向会转小写)
        .route("/videos/{item_id}/stream", get(api::emby_stream).head(api::emby_stream))
        .route("/videos/{item_id}/original.mp4", get(api::emby_stream).head(api::emby_stream))
        .route("/videos/{item_id}/original.strm", get(api::emby_stream).head(api::emby_stream))
        .route("/videos/{item_id}/stream.{ext}", get(api::emby_stream_with_ext).head(api::emby_stream_with_ext))
        .route("/videos/{item_id}/{source_id}/stream", get(api::emby_stream_with_source).head(api::emby_stream_with_source))
        .route("/videos/{item_id}/{source_id}/stream.{ext}", get(api::emby_stream_with_source_ext).head(api::emby_stream_with_source_ext))
        
        // 字幕
        .route("/Videos/{item_id}/{source_id}/Subtitles/{stream_index}/Stream", get(api::emby_subtitle_stream_with_source))
        .route("/Items/{item_id}/Subtitles/{stream_index}/Stream", get(api::emby_subtitle_stream))
        
        // 会话与播放进度
        .route("/Sessions", get(api::emby_sessions))
        .route("/Sessions/Playing", post(api::emby_playing))
        .route("/Sessions/Playing/Progress", post(api::emby_playing_progress))
        .route("/Sessions/Playing/Stopped", post(api::emby_playing_stopped))
        .route("/Users/{user_id}/Items/{item_id}/HideFromResume", post(api::emby_hide_from_resume))
        
        // 杂项操作 (统一返回 OK，防止客户端报错)
        .route("/Items/{item_id}/Refresh", post(api::emby_action))
        .route("/Users/{user_id}/PlayedItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        .route("/Users/{user_id}/FavoriteItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        .route("/Sessions/Logout", post(api::emby_action))
        .route("/api/danmu/{item_id}", get(api::emby_danmu))
}
