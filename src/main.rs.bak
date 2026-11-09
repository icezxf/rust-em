#![recursion_limit = "512"]
#![recursion_limit = "512"]
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
mod probe;

#[tokio::main]
async fn main() {
    std::fs::create_dir_all("data").unwrap();
    let conn = rusqlite::Connection::open("data/data.db").unwrap();
    db::init_db(&conn);
    db::ensure_extra_columns(&conn);
    db::init_settings_table(&conn);
    let pool = Arc::new(Mutex::new(conn));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let emby_routes = Router::new()
        .route("/System/Info/Public", get(api::emby_public_info))
        .route("/System/Info", get(api::emby_info))
        .route("/System/Ping", get(api::emby_ping).post(api::emby_ping))
        .route("/Auth/Keys", get(api::emby_auth_keys))
        .route("/Users/Public", get(api::emby_public_users))
        .route("/Users/AuthenticateByName", post(api::emby_authenticate))
        
        .route("/Users/{user_id}", get(api::emby_user))
        .route("/Users/{user_id}/Views", get(api::emby_views))
        .route("/DisplayPreferences/{display_preferences_id}", get(api::emby_display_preferences))
        
        .route("/Users/{user_id}/Items/Latest", get(api::emby_items_latest))
        .route("/Users/{user_id}/Items/Resume", get(api::emby_items_resume))
        .route("/Users/{user_id}/Items/NextUp", get(api::emby_items_next_up))
        .route("/Users/{user_id}/Items", get(api::emby_items))
        .route("/Items", get(api::emby_items))
        .route("/Items/Latest", get(api::emby_items_latest))
        
        .route("/Items/{item_id}", get(api::emby_item_detail))
        .route("/Users/{user_id}/Items/{item_id}", get(api::emby_item_detail_with_user))
        
        .route("/Items/{item_id}/Images/{image_type}", get(api::emby_item_image))
        .route("/Items/{item_id}/Images/{image_type}/{index}", get(api::emby_item_image_with_index))
        .route("/Users/{user_id}/Items/{item_id}/Images/{image_type}", get(api::emby_item_image_with_user))
        .route("/Users/{user_id}/Images/{image_type}", get(api::emby_item_image))
        .route("/Persons/{person_id}/Images/{image_type}", get(api::emby_person_image))

        .route("/Items/{item_id}/PlaybackInfo", get(api::emby_playback_info).post(api::emby_playback_info))
        .route("/Videos/{item_id}/stream", get(api::emby_stream))
        .route("/Videos/{item_id}/stream.{ext}", get(api::emby_stream_with_ext))
        .route("/Videos/{item_id}/original", get(api::emby_stream))
        .route("/Videos/{item_id}/original.{ext}", get(api::emby_stream_with_ext))
        .route("/Videos/{item_id}/{source_id}/stream", get(api::emby_stream_with_source))
        .route("/Videos/{item_id}/{source_id}/stream.{ext}", get(api::emby_stream_with_source_ext))
        .route("/Items/{item_id}/Download", get(api::emby_download))
        
        .route("/Items/{item_id}/Subtitles/{stream_index}/Stream", get(api::emby_subtitle_stream))
        .route("/Videos/{item_id}/{source_id}/Subtitles/{stream_index}/Stream", get(api::emby_subtitle_stream_with_source))
        
        .route("/Sessions", get(api::emby_sessions))
        .route("/Sessions/Playing", post(api::emby_playing))
        .route("/Sessions/Playing/Progress", post(api::emby_playing_progress))
        .route("/Sessions/Playing/Stopped", post(api::emby_playing_stopped))
        
        .route("/Users/{user_id}/Items/{item_id}/HideFromResume", post(api::emby_hide_from_resume))
        .route("/Users/{user_id}/FavoriteItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        .route("/Users/{user_id}/PlayedItems/{item_id}", post(api::emby_action).delete(api::emby_action))
        
        .route("/Items/{item_id}/Similar", get(api::emby_similar))
        .route("/Items/{item_id}/ThumbnailSet", get(api::emby_thumbnail_set))
        .route("/Library/VirtualFolders", get(api::emby_virtual_folders))
        .route("/Items/Counts", get(api::emby_item_counts))
        .route("/Search/Hints", get(api::emby_search_hints))
        .route("/Shows/NextUp", get(api::emby_shows_next_up))
        .route("/Shows/{series_id}/Seasons", get(api::emby_shows_seasons))
        .route("/Shows/{series_id}/Episodes", get(api::emby_shows_episodes))
        .route("/api/danmu/{item_id}", get(api::emby_danmu))
        
        .fallback(any(|req: axum::extract::Request| async move {
            println!("⚠️ [EMBY 未捕获路由] {} {}", req.method(), req.uri().path());
            axum::http::StatusCode::OK.into_response()
        }));

    let web_routes = Router::new()
        .route("/api/login", post(api::login))
        .route("/api/libraries", get(api::get_libraries).post(api::add_library))
        .route("/api/libraries/{id}", axum::routing::delete(api::delete_library))
        .route("/api/libraries/{id}/scan", post(api::rescan_library))
        .route("/api/items", get(api::get_items))
        .route("/api/browse", get(api::browse_dir))
        .route("/api/images/{id}/{type}", get(api::get_image))
        .route("/api/user/update", post(api::update_user))
        .route("/api/settings", get(api::get_settings).post(api::update_settings))
        .route("/api/probe/scan", post(api::probe_scan));

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
