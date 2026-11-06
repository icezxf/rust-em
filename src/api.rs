use axum::{
    extract::{State, Request, Json, Query},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::Path;

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
pub struct BrowseReq { path: String }

pub async fn browse_dir(Query(req): Query<BrowseReq>) -> impl IntoResponse {
    let base_path = req.path;
    
    let result = tokio::task::spawn_blocking(move || {
        let dir_path = Path::new(&base_path);
        let mut dirs = Vec::new();
        let mut error_msg = None;

        match std::fs::read_dir(dir_path) {
            Ok(entries) => {
                for entry in entries.filter_map(|e| e.ok()) {
                    if entry.path().is_dir() {
                        dirs.push(json!({
                            "name": entry.file_name().to_string_lossy(),
                            "path": entry.path().to_string_lossy()
                        }));
                    }
                }
            }
            Err(e) => {
                error_msg = Some(format!("无法读取目录 [{}]: {}", base_path, e));
            }
        }
        
        dirs.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        (dirs, error_msg, base_path)
    }).await.unwrap();

    let (dirs, error, current) = result;
    Json(json!({
        "current": current,
        "dirs": dirs,
        "error": error
    }))
}

#[derive(Deserialize)]
pub struct AddLibReq { name: String, paths: Vec<String>, #[serde(rename = "type")] lib_type: String }

pub async fn add_library(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<AddLibReq>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let paths_json = serde_json::to_string(&payload.paths).unwrap();
    
    conn.execute("INSERT INTO libraries (name, path, type) VALUES (?1, ?2, ?3)", 
        [&payload.name, &paths_json, &payload.lib_type]).unwrap();
    
    let paths = payload.paths.clone();
    let lib_type = payload.lib_type.clone();
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        scan_strm(paths, lib_type, pool_clone);
    });

    Json(json!({"status": "scanning"}))
}

pub async fn get_libraries() -> impl IntoResponse { Json(json!([])) }

pub async fn get_items(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT title, type FROM items").unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        Ok(json!({"title": row.get::<_, String>(0)?, "type": row.get::<_, String>(1)?}))
    }).unwrap().filter_map(Result::ok).collect();

    Json(json!({ "items": items }))
}

fn scan_strm(paths: Vec<String>, lib_type: String, pool: Arc<Mutex<Connection>>) {
    let conn = pool.lock().unwrap();
    for path in paths {
        for entry in walkdir::WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
            let file_path = entry.path();
            if file_path.extension().map_or(false, |ext| ext == "strm") {
                let nfo_path = file_path.with_extension("nfo");
                let mut title = file_path.file_stem().unwrap().to_string_lossy().to_string();
                if let Some(nfo) = crate::nfo::read_nfo(&nfo_path) {
                    if let Some(t) = nfo.title { title = t; }
                }
                
                let path_str = file_path.to_string_lossy().to_string();
                let _ = conn.execute(
                    "INSERT INTO items (library_id, title, file_path, type) VALUES (1, ?1, ?2, ?3)",
                    rusqlite::params![title, path_str, lib_type]
                );
            }
        }
    }
}

#[derive(Deserialize)]
pub struct UpdateUserReq { old_password: String, new_username: String, new_password: String }

pub async fn update_user(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<UpdateUserReq>) -> impl IntoResponse {
    let mut hasher_old = Sha256::new();
    hasher_old.update(&payload.old_password);
    let hashed_old = format!("{:x}", hasher_old.finalize());

    let mut hasher_new = Sha256::new();
    hasher_new.update(&payload.new_password);
    let hashed_new = format!("{:x}", hasher_new.finalize());

    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id FROM users WHERE username = 'admin' AND password = ?1").unwrap();
    if stmt.exists([&hashed_old]).unwrap() {
        conn.execute(
            "UPDATE users SET username = ?1, password = ?2 WHERE username = 'admin'", 
            [&payload.new_username, &hashed_new]
        ).unwrap();
        if let Some(tokens) = TOKENS.get() { tokens.lock().unwrap().clear(); }
        (StatusCode::OK, Json(json!({"status": "success"})))
    } else {
        (StatusCode::BAD_REQUEST, Json(json!({"error": "旧密码错误"})))
    }
}

pub async fn emby_info() -> impl IntoResponse { Json(json!({"ServerName": "Rust-Em", "Version": "0.1.0"})) }
pub async fn emby_items() -> impl IntoResponse { Json(json!({"Items": [], "TotalRecordCount": 0})) }