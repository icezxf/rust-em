use axum::{
    extract::{State, Request, Json, Query, Path as AxumPath},
    http::{StatusCode, header},
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

// 支持从 Header 或 URL 参数 (针对 img 标签) 获取 Token
pub async fn auth(req: Request, next: Next) -> Result<Response, StatusCode> {
    // 1. 从 Header 获取
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Some(token) = auth_header.to_str().unwrap_or("").strip_prefix("Bearer ") {
            if let Some(tokens) = TOKENS.get() {
                if tokens.lock().unwrap().contains(&token.to_string()) {
                    return Ok(next.run(req).await);
                }
            }
        }
    }
    // 2. 从 URL 参数获取
    if let Some(query) = req.uri().query() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                if key == "token" {
                    if let Some(tokens) = TOKENS.get() {
                        if tokens.lock().unwrap().contains(&value.to_string()) {
                            return Ok(next.run(req).await);
                        }
                    }
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
        let mut files = Vec::new();
        let mut error_msg = None;

        match std::fs::read_dir(dir_path) {
            Ok(entries) => {
                for entry in entries.filter_map(|e| e.ok()) {
                    let path = entry.path();
                    let name = entry.file_name().to_string_lossy().to_string();
                    let path_str = path.to_string_lossy().to_string();
                    if path.is_dir() {
                        dirs.push(json!({ "name": name, "path": path_str }));
                    } else if path.is_file() {
                        files.push(json!({ "name": name, "path": path_str }));
                    }
                }
            }
            Err(e) => { error_msg = Some(format!("无法读取目录 [{}]: {}", base_path, e)); }
        }
        dirs.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        files.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        (dirs, files, error_msg, base_path)
    }).await.unwrap();

    let (dirs, files, error, current) = result;
    Json(json!({ "current": current, "dirs": dirs, "files": files, "error": error }))
}

#[derive(Deserialize, Clone)]
pub struct AddLibReq { name: String, paths: Vec<String>, #[serde(rename = "type")] lib_type: String }

pub async fn add_library(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<AddLibReq>) -> impl IntoResponse {
    let pool_clone = pool.clone();
    tokio::task::spawn_blocking(move || {
        let conn = pool_clone.lock().unwrap();
        let paths_json = serde_json::to_string(&payload.paths).unwrap();
        conn.execute("INSERT INTO libraries (name, path, type) VALUES (?1, ?2, ?3)", 
            [&payload.name, &paths_json, &payload.lib_type]).unwrap();
        let lib_id = conn.last_insert_rowid();

        for path_str in payload.paths {
            let path = Path::new(&path_str);
            if path.is_dir() {
                for entry in walkdir::WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
                    if entry.file_type().is_file() {
                        process_file(entry.path(), &payload.lib_type, lib_id, &conn);
                    }
                }
            } else if path.is_file() {
                process_file(path, &payload.lib_type, lib_id, &conn);
            }
        }
    }).await.unwrap();

    Json(json!({"status": "scanning"}))
}

fn process_file(file_path: &Path, lib_type: &str, lib_id: i64, conn: &Connection) {
    if file_path.extension().map_or(false, |ext| ext == "strm") {
        let stem = file_path.file_stem().unwrap().to_string_lossy().to_string();
        let parent = file_path.parent().unwrap_or(Path::new(""));
        
        // 解析 NFO
        let nfo_path = file_path.with_extension("nfo");
        let mut title = stem.clone();
        let mut plot = "".to_string();
        let mut year = "".to_string();
        
        if let Some(nfo) = crate::nfo::read_nfo(&nfo_path) {
            if let Some(t) = nfo.title { title = t; }
            if let Some(p) = nfo.plot { plot = p; }
            if let Some(y) = nfo.year { year = y.to_string(); }
        }

        // 寻找海报和背景图
        let mut poster_path = "".to_string();
        let mut fanart_path = "".to_string();
        
        let poster_names = [format!("{}-poster.jpg", stem), "poster.jpg".to_string(), "folder.jpg".to_string()];
        for name in poster_names {
            let p = parent.join(&name);
            if p.exists() { poster_path = p.to_string_lossy().to_string(); break; }
        }

        let fanart_names = [format!("{}-fanart.jpg", stem), "fanart.jpg".to_string(), "background.jpg".to_string()];
        for name in fanart_names {
            let p = parent.join(&name);
            if p.exists() { fanart_path = p.to_string_lossy().to_string(); break; }
        }

        let path_str = file_path.to_string_lossy().to_string();
        let _ = conn.execute(
            "INSERT OR REPLACE INTO items (library_id, title, file_path, type, plot, year, poster_path, fanart_path) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![lib_id, title, path_str, lib_type, plot, year, poster_path, fanart_path]
        );
    }
}

pub async fn get_libraries() -> impl IntoResponse { Json(json!([])) }

pub async fn get_items(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, title, type, year, plot, poster_path FROM items").unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let has_poster = !row.get::<_, String>(5).unwrap_or_default().is_empty();
        Ok(json!({
            "id": id,
            "title": row.get::<_, String>(1).unwrap_or_default(),
            "type": row.get::<_, String>(2).unwrap_or_default(),
            "year": row.get::<_, String>(3).unwrap_or_default(),
            "plot": row.get::<_, String>(4).unwrap_or_default(),
            "has_poster": has_poster,
        }))
    }).unwrap().filter_map(Result::ok).collect();

    Json(json!({ "items": items }))
}

pub async fn get_image(
    State(pool): State<Arc<Mutex<Connection>>>,
    AxumPath((id, img_type)): AxumPath<(i64, String)>,
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let query = if img_type == "poster" { "SELECT poster_path FROM items WHERE id = ?1" } else { "SELECT fanart_path FROM items WHERE id = ?1" };
    
    let mut stmt = conn.prepare(query).unwrap();
    let path_opt: Result<String, _> = stmt.query_row([id], |row| row.get(0));
    
    if let Ok(path_str) = path_opt {
        if !path_str.is_empty() {
            if let Ok(bytes) = tokio::fs::read(&path_str).await {
                // 简单根据后缀判断，默认 jpeg
                let mime = if path_str.to_lowercase().ends_with(".png") { "image/png" } else { "image/jpeg" };
                return (StatusCode::OK, [(header::CONTENT_TYPE, mime)], bytes).into_response();
            }
        }
    }
    StatusCode::NOT_FOUND.into_response()
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