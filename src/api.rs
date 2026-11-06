use axum::{
    extract::{State, Request, Json, Query, Path as AxumPath},
    http::{StatusCode, header::{LOCATION, USER_AGENT, CONTENT_TYPE}, HeaderMap},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::Path;

static TOKENS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

// 全局缓存：Key 是 (URL+IP+UA)，Value 是 (解析后的直链, 过期时间)
static STRM_CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();

// ================= 鉴权与登录 =================

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
    // 支持从 URL 参数获取 Token (用于 img 标签)
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

// ================= 浏览目录与扫描入库 =================

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
        
        let nfo_path = file_path.with_extension("nfo");
        let mut title = stem.clone();
        let mut plot = "".to_string();
        let mut year = "".to_string();
        
        if let Some(nfo) = crate::nfo::read_nfo(&nfo_path) {
            if let Some(t) = nfo.title { title = t; }
            if let Some(p) = nfo.plot { plot = p; }
            if let Some(y) = nfo.year { year = y.to_string(); }
        }

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

// ================= 前端 UI 数据读取 =================

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
) -> Response {
    let path_opt = {
        let conn = pool.lock().unwrap();
        let query = if img_type == "poster" { "SELECT poster_path FROM items WHERE id = ?1" } else { "SELECT fanart_path FROM items WHERE id = ?1" };
        
        let mut stmt = conn.prepare(query).unwrap();
        let res: Result<String, _> = stmt.query_row([id], |row| row.get(0));
        res.ok()
    };
    
    if let Some(path_str) = path_opt {
        if !path_str.is_empty() {
            if let Ok(bytes) = tokio::fs::read(&path_str).await {
                let mime = if path_str.to_lowercase().ends_with(".png") { "image/png" } else { "image/jpeg" };
                return (StatusCode::OK, [(CONTENT_TYPE, mime)], bytes).into_response();
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

// ================= Emby 协议兼容层 =================

pub async fn emby_info() -> impl IntoResponse { 
    Json(json!({
        "ServerName": "Rust-Em",
        "Version": "0.1.0",
        "Id": "rust-em-server",
        "OperatingSystem": "Linux",
        "HasPendingRestart": false,
        "IsShuttingDown": false
    })) 
}

#[derive(Deserialize)]
pub struct EmbyAuthReq {
    #[serde(rename = "Username")]
    pub username: String,
    #[serde(rename = "Pw")]
    pub password: String,
}

pub async fn emby_authenticate(
    State(pool): State<Arc<Mutex<Connection>>>,
    Json(payload): Json<EmbyAuthReq>,
) -> Response {
    let mut hasher = Sha256::new();
    hasher.update(&payload.password);
    let hashed_pwd = format!("{:x}", hasher.finalize());

    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, username FROM users WHERE username = ?1 AND password = ?2").unwrap();
    let user_opt = stmt.query_row([&payload.username, &hashed_pwd], |row| {
        Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, String>(1).unwrap()))
    }).ok();

    if let Some((id, name)) = user_opt {
        let token = uuid::Uuid::new_v4().to_string();
        TOKENS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap().push(token.clone());
        
        (StatusCode::OK, Json(json!({
            "User": { "Id": id.to_string(), "Name": name },
            "AccessToken": token,
            "ServerId": "rust-em-server"
        }))).into_response()
    } else {
        (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" }))).into_response()
    }
}

pub async fn emby_items(
    State(pool): State<Arc<Mutex<Connection>>>,
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, title, type, year, plot, poster_path FROM items").unwrap();
    
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let has_poster = !row.get::<_, String>(5).unwrap_or_default().is_empty();
        Ok(json!({
            "Id": id.to_string(),
            "Name": row.get::<_, String>(1).unwrap_or_default(),
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(),
            "ImageTags": if has_poster { json!({"Primary": "tag"}) } else { json!({}) },
            "MediaSources": [{ "Id": id.to_string(), "SupportsDirectPlay": true }]
        }))
    }).unwrap().filter_map(Result::ok).collect();

    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_playback_info(
    State(pool): State<Arc<Mutex<Connection>>>, 
    AxumPath(item_id): AxumPath<i64>
) -> Response {
    let path_opt = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT file_path FROM items WHERE id = ?1").unwrap();
        let res: Result<String, _> = stmt.query_row([item_id], |row| row.get(0));
        res.ok()
    };

    if let Some(path_str) = path_opt {
        Json(json!({
            "MediaSources": [{
                "Id": item_id.to_string(),
                "Path": path_str,
                "Protocol": "Http",
                "SupportsDirectPlay": true,
                "SupportsDirectStream": true,
                "SupportsTranscoding": false
            }]
        })).into_response()
    } else {
        (StatusCode::NOT_FOUND, Json(json!({ "error": "Item not found" }))).into_response()
    }
}

// 【终极版本】：带缓存机制解析 OpenList/网盘 直链
pub async fn emby_stream(
    State(pool): State<Arc<Mutex<Connection>>>, 
    AxumPath(item_id): AxumPath<i64>,
    headers: HeaderMap, // 提取请求头，用于区分不同客户端
) -> Response {
    // 1. 从数据库获取 .strm 文件的路径
    let path_opt = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT file_path FROM items WHERE id = ?1").unwrap();
        let res: Result<String, _> = stmt.query_row([item_id], |row| row.get(0));
        res.ok()
    };

    let Some(strm_path) = path_opt else {
        return StatusCode::NOT_FOUND.into_response();
    };

    // 2. 读取 .strm 里的原始 URL
    let Ok(url) = tokio::fs::read_to_string(&strm_path).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let url = url.trim().to_string();

    if !url.starts_with("http") {
        return StatusCode::NOT_FOUND.into_response();
    }

    // 3. 构建缓存 Key（结合 UA 和 IP，防止不同客户端互相干扰）
    let ua = headers.get(USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    let ip = headers.get("X-Real-IP").and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    let cache_key = format!("{}|{}|{}", url, ip, ua);

    // 4. 检查缓存
    let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    {
        let mut cache_lock = cache.lock().unwrap();
        if let Some((cached_url, expire_at)) = cache_lock.get(&cache_key) {
            if Instant::now() < *expire_at {
                // 缓存命中，直接返回 302
                return (StatusCode::FOUND, [(LOCATION, cached_url.clone())], "").into_response();
            } else {
                cache_lock.remove(&cache_key);
            }
        }
        // 防止内存泄漏清理：如果超过 512 个缓存，清理过期的
        if cache_lock.len() > 512 {
            cache_lock.retain(|_, (_, expire)| Instant::now() < *expire);
        }
    }

    // 5. 缓存未命中，请求 OpenList 获取真实直链
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none()) // 禁止自动跟随，捕获 302
        .timeout(Duration::from_secs(15))
        .build() {
            Ok(c) => c,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };

    let mut final_url = url.clone(); // 兜底策略：解析失败则返回原链接

    if let Ok(resp) = client.get(&url)
        .header(USER_AGENT, ua)
        .send().await 
    {
        if resp.status().is_redirection() {
            if let Some(location) = resp.headers().get("location") {
                if let Ok(loc_str) = location.to_str() {
                    final_url = loc_str.to_string();
                }
            }
        }
    }

    // 6. 写入缓存（有效期 5 秒）
    {
        let mut cache_lock = cache.lock().unwrap();
        cache_lock.insert(cache_key, (final_url.clone(), Instant::now() + Duration::from_secs(5)));
    }

    // 7. 返回 302 给客户端
    (StatusCode::FOUND, [(LOCATION, final_url)], "").into_response()
}