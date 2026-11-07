use axum::{
    routing::{get, post, any},
    Router,
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

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
        .expose_headers(Any)
        .allow_credentials(true);

    // 核心 Emby 路由集合 (极度宽容，覆盖所有可能的路径变体)
    let emby_routes = Router::new()
        .route("/System/Info/Public", get(api::emby_public_info))
        .route("/System/Info", get(api::emby_info))
        .route("/System/Ping", get(api::emby_ping).post(api::emby_ping))
        .route("/Auth/Keys", get(api::emby_auth_keys))
        .route("/Users/Public", get(api::emby_public_users))
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        .route("/DisplayPreferences/:id", get(api::emby_display_preferences))
        
        // 用户与视图
        .route("/Users/:user_id", get(api::emby_user))
        .route("/Users/:user_id/Views", get(api::emby_views))
        .route("/Users/:user_id/Items", get(api::emby_items))
        .route("/Users/:user_id/Items/Latest", get(api::emby_items_latest))
        .route("/Users/:user_id/Items/Resume", get(api::emby_items_resume))
        .route("/Users/:user_id/Items/NextUp", get(api::emby_items_next_up))
        .route("/Users/:user_id/Items/:item_id", get(api::emby_item_detail))
        .route("/Users/:user_id/Items/:item_id/HideFromResume", post(api::emby_hide_from_resume))
        .route("/Users/:user_id/Items/:item_id/Images/:image_type", get(api::emby_item_image))
        .route("/Users/:user_id/Items/:item_id/Images/:image_type/:index", get(api::emby_item_image))
        .route("/Users/:user_id/FavoriteItems/:item_id", post(api::emby_favorite).delete(api::emby_unfavorite))
        .route("/Users/:user_id/PlayedItems/:item_id", post(api::emby_played).delete(api::emby_unplayed))

        // 列表与详情通用
        .route("/Items", get(api::emby_items))
        .route("/Items/Latest", get(api::emby_items_latest))
        .route("/Items/Resume", get(api::emby_items_resume))
        .route("/Items/:item_id", get(api::emby_item_detail))
        .route("/Items/:item_id/Images/:image_type", get(api::emby_item_image))
        .route("/Items/:item_id/Images/:image_type/:index", get(api::emby_item_image))
        .route("/Items/:item_id/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        .route("/Items/:item_id/HideFromResume", post(api::emby_hide_from_resume_without_user))

        // 视频流
        .route("/Videos/:item_id/stream", get(api::emby_stream))
        .route("/Videos/:item_id/stream.:ext", get(api::emby_stream))
        .route("/Videos/:item_id/original", get(api::emby_stream))
        .route("/Videos/:item_id/original.:ext", get(api::emby_stream))
        .route("/Videos/:item_id/:source_id/stream", get(api::emby_stream))
        .route("/Videos/:item_id/:source_id/stream.:ext", get(api::emby_stream))
        
        // 会话与进度上报
        .route("/Sessions/Playing", post(api::emby_playing))
        .route("/Sessions/Playing/Progress", post(api::emby_playing_progress))
        .route("/Sessions/Playing/Stopped", post(api::emby_playing_stopped))
        
        // 兜底，防止未知的 Emby 路由直接吃 404
        .fallback(any(|req: axum::extract::Request| async move {
            println!("⚠️ [EMBY 未知路由] {} {}", req.method(), req.uri().path());
            axum::http::StatusCode::OK.into_response()
        }));

    // Web 端 API
    let web_routes = Router::new()
        .route("/api/login", post(api::login))
        .route("/api/libraries", get(api::get_libraries).post(api::add_library))
        .route("/api/libraries/:id", axum::routing::delete(api::delete_library))
        .route("/api/libraries/:id/scan", post(api::rescan_library))
        .route("/api/items", get(api::get_items))
        .route("/api/browse", get(api::browse_dir))
        .route("/api/images/:id/:type", get(api::get_image))
        .route("/api/user/update", post(api::update_user));

    // 注册所有路由，并加上极其详尽的日志中间件
    let app = Router::new()
        .merge(web_routes)
        .merge(emby_routes.clone())
        .nest("/emby", emby_routes.clone())
        .nest("/emby/emby", emby_routes.clone())
        .fallback_service(ServeDir::new("public"))
        .layer(cors)
        .layer(axum::middleware::from_fn(api::logger_middleware)) // 把日志放在最外层
        .layer(axum::middleware::from_fn(api::auth))
        .with_state(pool);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 Server running on http://0.0.0.0:8096");
    axum::serve(listener, app).await.unwrap();
}
