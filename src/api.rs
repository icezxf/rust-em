use axum::{
    extract::State,
    http::{StatusCode, Request},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use rusqlite::Connection;
use sha2::{Digest, Sha256};

static TOKENS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

#[derive(Deserialize)]
pub struct LoginReq { username: String, password: String }

pub async fn login(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<LoginReq>) -> impl IntoResponse {
    let mut hasher = Sha256::new();
    hasher.update(&payload.password);
    let hashed_pwd = format!("{:x}", hasher.finalize());

    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id FROM users WHERE username = ?1 AND password = ?2").unwrap();
    let exists = stmt.exists([&payload.username, &hashed_pwd]).unwrap();

    if exists {
        let token = uuid::Uuid::new_v4().to_string();
        TOKENS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap().push(token.clone());
        (StatusCode::OK, Json(json!({ "token": token })))
    } else {
        (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" })))
    }
}

// 修复：去掉了 State 参数，直接使用全局静态变量
pub async fn auth(req: Request, next: Next) -> Result<Response, StatusCode> {
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Some(token) = auth_header.to_str().unwrap_or("").strip_prefix("Bearer ") {
            if let Some(tokens) = TOKENS.get() {
                if tokens.lock().unwrap().contains(&token.to_string()) {
                    return Ok(next.run(req).await);
                }
            }
        }
    }
    Err(StatusCode::UNAUTHORIZED)
}

#[derive(Deserialize)]
pub struct AddLibReq { name: String, path: String, #[serde(rename = "type")] lib_type: String }

pub async fn add_library(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<AddLibReq>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    conn.execute("INSERT INTO libraries (name, path, type) VALUES (?1, ?2, ?3)", 
        [&payload.name, &payload.path, &payload.lib_type]).unwrap();
    
    let path = payload.path.clone();
    let lib_type = payload.lib_type.clone();
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        scan_strm(&path, &lib_type, pool_clone);
    });

    Json(json!({"status": "scanning"}))
}

pub async fn get_libraries() -> impl IntoResponse {
    Json(json!([]))
}

pub async fn get_items(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT title, type FROM items").unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        Ok(json!({"title": row.get::<_, String>(0)?, "type": row.get::<_, String>(1)?}))
    }).unwrap().filter_map(Result::ok).collect();

    Json(json!({ "items": items }))
}

fn scan_strm(path: &str, lib_type: &str, pool: Arc<Mutex<Connection>>) {
    let conn = pool.lock().unwrap();
    for entry in walkdir::WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        let file_path = entry.path();
        if file_path.extension().map_or(false, |ext| ext == "strm") {
            let nfo_path = file_path.with_extension("nfo");
            let mut title = file_path.file_stem().unwrap().to_string_lossy().to_string();
            if let Some(nfo) = crate::nfo::read_nfo(&nfo_path) {
                if let Some(t) = nfo.title { title = t; }
            }
            
            // 修复：用 params! 宏解决类型和生命周期问题
            let path_str = file_path.to_string_lossy().to_string();
            let _ = conn.execute(
                "INSERT INTO items (library_id, title, file_path, type) VALUES (1, ?1, ?2, ?3)",
                rusqlite::params![title, path_str, lib_type]
            );
        }
    }
}

pub async fn emby_info() -> impl IntoResponse {
    Json(json!({"ServerName": "Rust-Em", "Version": "0.1.0"}))
}
pub async fn emby_items() -> impl IntoResponse {
    Json(json!({"Items": [], "TotalRecordCount": 0}))
}
