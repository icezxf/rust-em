use axum::{extract::{Path, Request}, http::StatusCode, middleware::Next, response::{IntoResponse, Redirect, Response}, Json};
use serde_json::json;

pub async fn auth(req: Request, next: Next) -> Result<Response, StatusCode> {
    if let Some(token) = req.headers().get("X-Emby-Token") {
        if token == "secret_token" { return Ok(next.run(req).await); }
    }
    Err(StatusCode::UNAUTHORIZED)
}

pub async fn system_info() -> impl IntoResponse {
    Json(json!({"ServerName": "rust-em", "Version": "0.1.0"}))
}

pub async fn list_items() -> impl IntoResponse {
    Json(json!({"Items": [], "TotalRecordCount": 0}))
}

pub async fn stream(Path(item_id): Path<String>) -> impl IntoResponse {
    let path = format!("file:///media/{}", item_id);
    Redirect::temporary(&path)
}
