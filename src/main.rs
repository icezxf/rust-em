mod emby;
mod nfo;
mod probe;

use axum::{routing::get, Router, middleware};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/System/Info", get(emby::system_info))
        .route("/Users/{userId}/Items", get(emby::list_items))
        .route("/Videos/{itemId}/stream", get(emby::stream))
        .layer(middleware::from_fn(emby::auth));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("rust-em listening on 8096");
    axum::serve(listener, app).await.unwrap();
}
