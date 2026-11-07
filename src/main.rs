use axum::{
    routing::{get, post, any},
    Router,
    response::IntoResponse,
};
use std::sync::{Arc, Mutex};
use tower_http::services::ServeDir;
use tower_http::cors::{Any, CorsLayer};

mod api;
mod db;
mod nfo;

#[tokio::main]
async fn main() {
    let conn = rusqlite::Connection::open("data.db").unwrap();
    db::init_db(&conn);
    let pool = Arc::new(Mutex::new(conn));

    // 移除 credentials 限制，避免 CORS 通配符冲突导致的 Panic
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Emby 兼容路由 (全面使用 Axum 0.8 的 {param} 语法)
    let emby_routes = Router::new()
        // 1. 系统与健康检查
        .route("/System/Info/Public", get(api::emby_public_info))
        .route("/System/Info", get(api::emby_info))
        .route("/System/Ping", get(api::emby_ping).post(api::emby_ping))
        .route("/Auth/Keys", get(api::emby_auth_keys))
        .route("/Users/Public", get(api::emby_public_users))
        
        // 2. 认证
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        
        // 3. 用户与视图
        .route("/Users/{user_id}", get(api::emby_user))
        .route("/Users/{user_id}/Views", get(api::emby_views))
        .route("/DisplayPreferences/{display_preferences_id}", get(api::emby_display_preferences))
        
        // 4. 列表与最新
        .route("/Users/{user_id}/Items/Latest", get(api::emby_items_latest))
        .route("/Users/{user_id}/Items/Resume", get(api::emby_items_resume))
        .route("/Users/{user_id}/Items/NextUp", get(api::emby_items_next_up))
        .route("/Users/{user_id}/Items", get(api::emby_items))
        .route("/Items", get(api::emby_items))
        .route("/Items/Latest", get(api::emby_items_latest))
        
        // 5. 详情
        .route("/Items/{item_id}", get(api::emby_item_detail))
        .route("/Users/{user_id}/Items/{item_id}", get(api::emby_item_detail))
        
        // 6. 图片
        .route("/Items/{item_id}/Images/{image_type}", get(api::emby_item_image))
        .route("/Items/{item_id}/Images/{image_type}/{index}", get(api::emby_item_image))
        .route("/Users/{user_id}/Items/{item_id}/Images/{image_type}", get(api::emby_item_image))
        .route("/Users/{user_id}/Images/{image_type}", get(api::emby_item_image))

        // 7. 播放
        .route("/Items/{item_id}/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        .route("/Videos/{item_id}/stream", get(api::emby_stream))
        .route("/Videos/{item_id}/stream.{ext}", get(api::emby_stream))
        .route("/Videos/{item_id}/original", get(api::emby_stream))
        .route("/Videos/{item_id}/original.{ext}", get(api::emby_stream))
        .route("/Videos/{item_id}/{source_id}/stream", get(api::emby_stream))
        .route("/Videos/{item_id}/{source_id}/stream.{ext}", get(api::emby_stream))
        .route("/Items/{item_id}/Download", get(api::emby_download))
        
        // 8. 会话与进度
        .route("/Sessions/Playing", post(api::emby_playing))
        .route("/Sessions/Playing/Progress", post(api::emby_playing_progress))
        .route("/Sessions/Playing/Stopped", post(api::emby_playing_stopped))
        
        // 9. 用户动作
        .route("/Users/{user_id}/Items/{item_id}/HideFromResume", post(api::emby_hide_from_resume))
        .route("/Users/{user_id}/FavoriteItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        .route("/Users/{user_id}/PlayedItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        
        // 10. 终极兜底：捕获所有未注册的 Emby 路径
        .fallback(any(|req: axum::extract::Request| async move {
            println!("⚠️ [EMBY 未捕获路由] {} {}", req.method(), req.uri().path());
            axum::http::StatusCode::OK.into_response()
        }));

    // Web 路由
    let web_routes = Router::new()
        .route("/api/login", post(api::login))
        .route("/api/libraries", get(api::get_libraries).post(api::add_library))
        .route("/api/libraries/{id}", axum::routing::delete(api::delete_library))
        .route("/api/libraries/{id}/scan", post(api::rescan_library))
        .route("/api/items", get(api::get_items))
        .route("/api/browse", get(api::browse_dir))
        .route("/api/images/{id}/{type}", get(api::get_image))
        .route("/api/user/update", post(api::update_user));

    // 多重前缀嵌套
    let app = Router::new()
        .merge(web_routes)
        .merge(emby_routes.clone())
        .nest("/emby", emby_routes.clone())
        .nest("/emby/emby", emby_routes.clone())
        .fallback_service(ServeDir::new("public"))
        .layer(cors)
        .layer(axum::middleware::from_fn(api::logger_middleware))
        .layer(axum::middleware::from_fn(api::auth))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 Server running on http://0.0.0.0:8096");
    axum::serve(listener, app).await.unwrap();
}
