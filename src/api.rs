use axum::{
    extract::{State, Json, Query, Path as AxumPath},
    http::{StatusCode, header::CONTENT_TYPE, HeaderMap},
    response::{IntoResponse, Response}, body::Bytes,
    Extension,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

use crate::utils::{decode_url, format_iso8601, sanitize_media_streams};
use crate::auth::{TOKENS, get_allowed_libs};

static SCAN_STATUS: OnceLock<Mutex<bool>> = OnceLock::new();
static LAST_SCAN_TIME: OnceLock<Mutex<String>> = OnceLock::new();

fn build_external_urls(tmdb_id: &str, imdb_id: &str, item_type: &str) -> Value {
    let mut urls = Vec::new();
    if !tmdb_id.is_empty() && tmdb_id != "0" {
        let path = if item_type == "Series" || item_type == "tvshows" { "tv" } else { "movie" };
        urls.push(json!({ "Name": "MovieDb", "Url": format!("https://www.themoviedb.org/{}/{}", path, tmdb_id) }));
    }
    if !imdb_id.is_empty() && imdb_id != "0" && imdb_id.starts_with("tt") {
        urls.push(json!({ "Name": "IMDb", "Url": format!("https://www.imdb.com/title/{}", imdb_id) }));
    }
    json!(urls)
}

// ======= Web API =======
#[derive(Deserialize)]
pub struct LoginReq { username: String, password: String }
pub async fn login(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<LoginReq>) -> impl IntoResponse {
    let mut hasher = Sha256::new(); hasher.update(&payload.password);
    let conn = pool.lock().unwrap();
    if conn.prepare("SELECT id FROM users WHERE username = ?1 AND password = ?2").unwrap().exists([&payload.username, &format!("{:x}", hasher.finalize())]).unwrap() {
        let user_id: i64 = conn.query_row("SELECT id FROM users WHERE username = ?1", [&payload.username], |r| r.get(0)).unwrap_or(0);
        if user_id != 1 {
            return (StatusCode::FORBIDDEN, Json(json!({ "error": "只有管理员可以登录网页端" }))).into_response();
        }
        let token = uuid::Uuid::new_v4().to_string();
        TOKENS.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap().insert(token.clone(), user_id);
        (StatusCode::OK, Json(json!({ "token": token }))).into_response()
    } else { 
        (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" }))).into_response() 
    }
}

#[derive(Deserialize)]
pub struct BrowseReq { path: String }
pub async fn browse_dir(Query(req): Query<BrowseReq>) -> impl IntoResponse {
    let base_path = req.path;
    let result = tokio::task::spawn_blocking(move || {
        let dir_path = Path::new(&base_path);
        let mut dirs = Vec::new(); let mut files = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path(); let name = entry.file_name().to_string_lossy().to_string(); let path_str = path.to_string_lossy().to_string();
                if path.is_dir() { dirs.push(json!({ "name": name, "path": path_str })); } else if path.is_file() { files.push(json!({ "name": name, "path": path_str })); }
            }
        }
        (dirs, files, base_path)
    }).await.unwrap();
    Json(json!({ "current": result.2, "dirs": result.0, "files": result.1, "error": null }))
}

#[derive(Deserialize, Clone)]
pub struct AddLibReq { name: String, paths: Vec<String>, #[serde(rename = "type")] lib_type: String }
pub async fn add_library(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<AddLibReq>) -> impl IntoResponse {
    let pool_clone = pool.clone();
    tokio::task::spawn_blocking(move || {
        let lib_id = { let conn = pool_clone.lock().unwrap(); conn.execute("INSERT INTO libraries (name, path, type) VALUES (?1, ?2, ?3)", [&payload.name, &serde_json::to_string(&payload.paths).unwrap(), &payload.lib_type]).unwrap(); conn.last_insert_rowid() };
        for path_str in payload.paths { crate::scanner::scan_directory(pool_clone.clone(), lib_id, payload.lib_type.clone(), path_str); }
    }).await.unwrap();
    Json(json!({"status": "scanning"}))
}

pub async fn get_libraries(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, name, type FROM libraries").unwrap();
    let libs: Vec<Value> = stmt.query_map([], |r| Ok(json!({ "id": r.get::<_, i64>(0)?, "name": r.get::<_, String>(1)?, "type": r.get::<_, String>(2)? }))).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "libraries": libs }))
}

pub async fn delete_library(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let _ = conn.execute("DELETE FROM items WHERE library_id = ?1", [id]);
    let _ = conn.execute("DELETE FROM libraries WHERE id = ?1", [id]);
    Json(json!({"status": "success"}))
}

pub async fn rescan_library(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    let pool_clone = pool.clone();
    tokio::task::spawn_blocking(move || {
        if let Ok((paths_json, lib_type)) = { let conn = pool_clone.lock().unwrap(); conn.query_row("SELECT path, type FROM libraries WHERE id = ?1", [id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))) } {
            let paths: Vec<String> = serde_json::from_str(&paths_json).unwrap_or_default();
            for path_str in paths { crate::scanner::scan_directory(pool_clone.clone(), id, lib_type.clone(), path_str); }
        }
    }).await.unwrap();
    Json(json!({"status": "scanning"}))
}

#[derive(Deserialize)]
pub struct ItemQuery { 
    library_id: Option<i64>, 
    lib_type: Option<String>,
    actor: Option<String>, 
    genre: Option<String>, 
    tag: Option<String>, 
    studio: Option<String> 
}

pub async fn get_items(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<ItemQuery>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, title, num, type, year, plot, poster_path, rating, mpaa, genres, tags, directors, actors, runtime, premiered, tagline, studios, tmdb_id, imdb_id, size, series, label, set_name, file_path, strm_content, stills, parts, media_streams FROM items WHERE 1=1".to_string();
    let mut params: Vec<String> = Vec::new();
    
    if let Some(lid) = query.library_id { sql.push_str(" AND library_id = ?"); params.push(lid.to_string()); }
    if let Some(lt) = &query.lib_type { 
        if lt == "tvshows" { sql.push_str(" AND type = 'Series'"); } 
        else if lt == "movies" { sql.push_str(" AND type = 'movies'"); }
    }
    
    if let Some(a) = &query.actor { 
        sql.push_str(" AND (actors LIKE ? OR actors LIKE ? OR directors = ? OR directors LIKE ? OR directors LIKE ? OR directors LIKE ?)");
        params.push(format!("%\"name\":\"{}\"%", a)); params.push(format!("%\"name\": \"{}\"%", a));
        params.push(a.clone()); params.push(format!("{} / %", a)); params.push(format!("% / {}", a)); params.push(format!("% / {} / %", a));
    }
    if let Some(g) = &query.genre { 
        sql.push_str(" AND (genres = ? OR genres LIKE ? OR genres LIKE ? OR genres LIKE ?)");
        params.push(g.clone()); params.push(format!("{} / %", g)); params.push(format!("% / {}", g)); params.push(format!("% / {} / %", g));
    }
    if let Some(t) = &query.tag { 
        sql.push_str(" AND (tags = ? OR tags LIKE ? OR tags LIKE ? OR tags LIKE ?)");
        params.push(t.clone()); params.push(format!("{} / %", t)); params.push(format!("% / {}", t)); params.push(format!("% / {} / %", t));
    }
    if let Some(st) = &query.studio { 
        sql.push_str(" AND (studios = ? OR studios LIKE ? OR studios LIKE ? OR studios LIKE ?)");
        params.push(st.clone()); params.push(format!("{} / %", st)); params.push(format!("% / {}", st)); params.push(format!("% / {} / %", st));
    }
    
    sql.push_str(" ORDER BY title ASC");
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("SQL Prepare Error: {}", e)).into_response() };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let actors_json: String = row.get(12).unwrap_or_else(|_| "[]".to_string());
        let actors: Value = serde_json::from_str(&actors_json).unwrap_or(json!([]));
        let stills_json: String = row.get(25).unwrap_or_else(|_| "[]".to_string());
        let stills: Value = serde_json::from_str(&stills_json).unwrap_or(json!([]));
        let parts_json: String = row.get(26).unwrap_or_else(|_| "[]".to_string());
        let parts: Value = serde_json::from_str(&parts_json).unwrap_or(json!([]));
        let mut media_streams: Value = serde_json::from_str(&row.get::<_, String>(27).unwrap_or_else(|_| "[]".to_string())).unwrap_or(json!([]));
        sanitize_media_streams(&mut media_streams);
        Ok(json!({
            "id": row.get::<_, i64>(0).unwrap_or_default().to_string(), 
            "title": row.get::<_, String>(1).unwrap_or_default(), "num": row.get::<_, String>(2).unwrap_or_default(), 
            "type": row.get::<_, String>(3).unwrap_or_default(), "year": row.get::<_, String>(4).unwrap_or_default(), 
            "plot": row.get::<_, String>(5).unwrap_or_default(), "has_poster": !row.get::<_, String>(6).unwrap_or_default().is_empty(), 
            "rating": row.get::<_, f64>(7).unwrap_or(0.0), "mpaa": row.get::<_, String>(8).unwrap_or_default(), 
            "genres": row.get::<_, String>(9).unwrap_or_default(), "tags": row.get::<_, String>(10).unwrap_or_default(), 
            "directors": row.get::<_, String>(11).unwrap_or_default(), "actors": actors, 
            "runtime": row.get::<_, i32>(13).unwrap_or(0), "premiered": row.get::<_, String>(14).unwrap_or_default(), 
            "tagline": row.get::<_, String>(15).unwrap_or_default(), "studios": row.get::<_, String>(16).unwrap_or_default(), 
            "tmdb_id": row.get::<_, String>(17).unwrap_or_default(), "imdb_id": row.get::<_, String>(18).unwrap_or_default(), 
            "size": row.get::<_, i64>(19).unwrap_or(0), "series": row.get::<_, String>(20).unwrap_or_default(), 
            "label": row.get::<_, String>(21).unwrap_or_default(), "set_name": row.get::<_, String>(22).unwrap_or_default(), 
            "file_path": row.get::<_, String>(23).unwrap_or_default(), "strm_content": row.get::<_, String>(24).unwrap_or_default(), 
            "stills": stills, "parts": parts, "media_streams": media_streams
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "items": items })).into_response()
}

#[derive(Deserialize)]
pub struct ImageQuery { index: Option<usize> }
pub async fn get_image(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((id, img_type)): AxumPath<(i64, String)>, Query(params): Query<ImageQuery>) -> Response {
    let path_opt = {
        let conn = pool.lock().unwrap();
        if img_type == "stills" {
            let idx = params.index.unwrap_or(0);
            let mut stmt = conn.prepare("SELECT stills FROM items WHERE id = ?1").unwrap();
            let stills_json: Option<String> = stmt.query_row([id], |r| r.get(0)).ok();
            if let Some(json) = stills_json {
                if let Ok(arr) = serde_json::from_str::<Vec<String>>(&json) { arr.get(idx).cloned() } else { None }
            } else { None }
        } else {
            let query = match img_type.as_str() {
                "poster" => "SELECT poster_path FROM items WHERE id = ?1", "fanart" => "SELECT fanart_path FROM items WHERE id = ?1",
                "logo" => "SELECT logo_path FROM items WHERE id = ?1", "thumb" => "SELECT thumb_path FROM items WHERE id = ?1",
                "banner" => "SELECT banner_path FROM items WHERE id = ?1", _ => "SELECT poster_path FROM items WHERE id = ?1",
            };
            let mut stmt = conn.prepare(query).unwrap();
            stmt.query_row([id], |row| row.get::<_, String>(0)).ok()
        }
    };
    if let Some(path_str) = path_opt { 
        if !path_str.is_empty() { 
            return crate::images::serve_file(&path_str).await; 
        } 
    }
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Deserialize)]
pub struct UpdateUserReq { old_password: String, new_username: String, new_password: String }
pub async fn update_user(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<UpdateUserReq>) -> impl IntoResponse {
    let mut hasher_old = Sha256::new(); hasher_old.update(&payload.old_password);
    let mut hasher_new = Sha256::new(); hasher_new.update(&payload.new_password);
    let conn = pool.lock().unwrap();
    
    if conn.prepare("SELECT id FROM users WHERE id = 1 AND password = ?1").unwrap().exists([&format!("{:x}", hasher_old.finalize())]).unwrap() {
        conn.execute("UPDATE users SET username = ?1, password = ?2 WHERE id = 1", [&payload.new_username, &format!("{:x}", hasher_new.finalize())]).unwrap();
        if let Some(tokens) = TOKENS.get() { tokens.lock().unwrap().clear(); }
        (StatusCode::OK, Json(json!({"status": "success"})))
    } else { 
        (StatusCode::BAD_REQUEST, Json(json!({"error": "旧密码错误"}))) 
    }
}

pub async fn get_settings(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT key, value FROM settings").unwrap();
    let mut settings = serde_json::Map::new();
    let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))).unwrap();
    for r in rows.flatten() { settings.insert(r.0, json!(r.1)); }
    Json(json!(settings))
}

#[derive(Deserialize)]
pub struct UpdateSettingsReq { pub prefetch_playback: bool }
pub async fn update_settings(State(pool): State<Arc<Mutex<Connection>>>, Json(req): Json<UpdateSettingsReq>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let val = if req.prefetch_playback { "true" } else { "false" };
    let _ = conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('prefetch_playback', ?1)", [val]);
    Json(json!({"status": "success"}))
}

#[derive(Deserialize)]
pub struct ProbeScanReq { pub path: String, pub interval_ms: Option<u64> }
pub async fn probe_scan(State(_pool): State<Arc<Mutex<Connection>>>, Json(req): Json<ProbeScanReq>) -> impl IntoResponse {
    let root = PathBuf::from(&req.path);
    if !root.is_dir() { return (StatusCode::BAD_REQUEST, Json(json!({"error":"路径不存在"}))).into_response(); }
    let limiter = crate::probe::ProbeLimiter::new(req.interval_ms.unwrap_or(1000));
    let client = reqwest::Client::builder().timeout(Duration::from_secs(40)).redirect(reqwest::redirect::Policy::none()).build().unwrap();
    tokio::spawn(async move {
        let mut ok = 0u32; let mut skip = 0u32; let mut fail = 0u32;
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            let mut rd = match tokio::fs::read_dir(&dir).await { Ok(r) => r, Err(_) => continue };
            while let Ok(Some(entry)) = rd.next_entry().await {
                let p = entry.path();
                if p.is_dir() { stack.push(p); continue; }
                if p.extension().and_then(|e| e.to_str()).map_or(false, |e| e.eq_ignore_ascii_case("strm")) {
                    let json_path = crate::probe::get_mediainfo_json_path(&p);
                    if json_path.exists() {
                        if let Some(src) = crate::probe::load_cached_source(&json_path) {
                            if src.get("MediaStreams").and_then(|m| m.as_array()).map(|a| !a.is_empty()).unwrap_or(false) { skip += 1; continue; }
                        }
                    }
                    match crate::probe::probe_strm(&p, limiter.clone(), &client).await {
                        Ok(_) => ok += 1, Err(_) => fail += 1,
                    }
                }
            }
        }
        println!("🏁 [PROBE] 完成 ok={} skip={} fail={}", ok, skip, fail);
    });
    (StatusCode::OK, Json(json!({"status":"started"}))).into_response()
}

pub async fn api_generate_covers(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let pool_clone = pool.clone();
    tokio::spawn(async move { crate::covers::generate_all_covers(pool_clone).await; });
    Json(json!({"status": "started"}))
}

pub async fn api_get_users(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, username FROM users WHERE id != 1").unwrap();
    let users: Vec<Value> = stmt.query_map([], |r| {
        let id: i64 = r.get(0).unwrap_or_default();
        let name: String = r.get(1).unwrap_or_default();
        Ok((id, name))
    }).unwrap().filter_map(Result::ok).map(|(id, name)| {
        let mut access_stmt = conn.prepare("SELECT library_id FROM user_library_access WHERE user_id = ?1").unwrap();
        let libs: Vec<i64> = access_stmt.query_map([id], |r| r.get(0)).unwrap().filter_map(Result::ok).collect();
        json!({ "id": id, "username": name, "libraries": libs })
    }).collect();
    Json(users)
}

#[derive(Deserialize)]
pub struct CreateUserReq { username: String, password: String, libraries: Vec<i64> }
pub async fn api_create_user(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<CreateUserReq>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let user_id: i64 = conn.query_row("SELECT id FROM users WHERE username = ?1", [&payload.username], |r| r.get(0)).unwrap_or(0);
    if user_id == 0 {
        if payload.password.is_empty() { return (StatusCode::BAD_REQUEST, "新建用户密码不能为空").into_response(); }
        let mut hasher = Sha256::new(); hasher.update(&payload.password);
        let hash = format!("{:x}", hasher.finalize());
        let _ = conn.execute("INSERT INTO users (username, password) VALUES (?1, ?2)", [&payload.username, &hash]);
    } else if user_id > 1 {
        if !payload.password.is_empty() {
            let mut hasher = Sha256::new(); hasher.update(&payload.password);
            let hash = format!("{:x}", hasher.finalize());
            let _ = conn.execute("UPDATE users SET password = ?1 WHERE id = ?2", [&hash, &user_id.to_string()]);
        }
    } else { return (StatusCode::FORBIDDEN, "不能修改管理员").into_response(); }
    let user_id: i64 = conn.query_row("SELECT id FROM users WHERE username = ?1", [&payload.username], |r| r.get(0)).unwrap_or(0);
    if user_id > 1 {
        let _ = conn.execute("DELETE FROM user_library_access WHERE user_id = ?1", [user_id]);
        for lib_id in payload.libraries { let _ = conn.execute("INSERT INTO user_library_access (user_id, library_id) VALUES (?1, ?2)", [user_id, lib_id]); }
    }
    StatusCode::OK.into_response()
}

pub async fn api_delete_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(id): AxumPath<i64>) -> impl IntoResponse {
    if id == 1 { return StatusCode::BAD_REQUEST.into_response(); }
    let conn = pool.lock().unwrap();
    let _ = conn.execute("DELETE FROM users WHERE id = ?1", [id]);
    let _ = conn.execute("DELETE FROM user_library_access WHERE user_id = ?1", [id]);
    StatusCode::OK.into_response()
}

// ======= Emby API =======
pub async fn emby_public_info() -> impl IntoResponse { Json(json!({ "ServerName": "Rust-Em", "Version": "4.8.0.0", "Id": "rust-em-server", "OperatingSystem": "Linux", "StartupWizardCompleted": true })) }
pub async fn emby_info() -> impl IntoResponse { Json(json!({ "ServerName": "Rust-Em", "Version": "4.8.0.0", "Id": "rust-em-server", "OperatingSystem": "Linux", "OperatingSystemDisplayName": "Linux", "HasPendingRestart": false, "IsShuttingDown": false, "SupportsLibraryMonitor": false, "SupportsHttps": true, "HttpServerPortNumber": 8096 })) }
pub async fn emby_ping() -> impl IntoResponse { "Emby Server" }
pub async fn emby_public_users() -> impl IntoResponse { Json(json!([])) }
pub async fn emby_auth_keys() -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }
pub async fn emby_user(_: AxumPath<String>) -> impl IntoResponse { Json(json!({"Id": "1", "Name": "admin", "ServerId": "rust-em-server", "HasPassword": true, "Policy": {"IsAdministrator": true, "EnableAllFolders": true}})) }

pub async fn emby_authenticate(State(pool): State<Arc<Mutex<Connection>>>, headers: HeaderMap, body: Bytes) -> Response {
    let content_type = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let mut username = String::new(); let mut password = String::new();
    if content_type.contains("application/json") {
        if let Ok(val) = serde_json::from_slice::<Value>(&body) { username = val["Username"].as_str().unwrap_or("").to_string(); password = val["Pw"].as_str().unwrap_or("").to_string(); }
    } else if content_type.contains("application/x-www-form-urlencoded") {
        for (k, v) in url::form_urlencoded::parse(&body) { if k == "Username" { username = v.to_string(); } if k == "Pw" { password = v.to_string(); } }
    }
    let mut hasher = Sha256::new(); hasher.update(&password);
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, username FROM users WHERE username = ?1 AND password = ?2").unwrap();
    let user_opt = stmt.query_row([&username, &format!("{:x}", hasher.finalize())], |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, String>(1).unwrap()))).ok();
    if let Some((id, name)) = user_opt {
        let token = uuid::Uuid::new_v4().to_string();
        TOKENS.get_or_init(|| Mutex::new(HashMap::new())).lock().unwrap().insert(token.clone(), id);
        (StatusCode::OK, Json(json!({ "User": { "Id": id.to_string(), "Name": name, "ServerId": "rust-em-server", "HasPassword": true, "Policy": { "IsAdministrator": id == 1, "EnableAllFolders": id == 1 } }, "SessionInfo": { "Id": uuid::Uuid::new_v4().to_string(), "UserId": id.to_string() }, "AccessToken": token, "ServerId": "rust-em-server" }))).into_response()
    } else { (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" }))).into_response() }
}

pub async fn emby_views(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, name, type FROM libraries".to_string();
    if req_user_id != 1 { sql.push_str(&format!(" WHERE id IN (SELECT library_id FROM user_library_access WHERE user_id = {})", req_user_id)); }
    let mut stmt = conn.prepare(&sql).unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default(); let name: String = row.get(1).unwrap_or_default(); let lib_type: String = row.get(2).unwrap_or_default();
        Ok(json!({ "Name": name, "Id": format!("lib_{}", id), "ServerId": "rust-em-server", "Type": "CollectionFolder", "IsFolder": true, "MediaType": "Video", "CollectionType": if lib_type == "movies" { "movies" } else { "tvshows" }, "ImageTags": {"Primary": crate::images::get_cover_tag("library", "lib", &id.to_string())}, "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false } }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_virtual_folders(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, name, type FROM libraries").unwrap();
    let folders: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default(); let name: String = row.get(1).unwrap_or_default(); let lib_type: String = row.get(2).unwrap_or_default();
        Ok(json!({ "Name": name, "Id": format!("lib_{}", id), "Guid": format!("lib_{}", id), "Path": format!("/media/lib_{}", id), "CollectionType": lib_type, "LibraryOptions": { "PathInfos": [], "EnablePhotos": true, "EnableRealtimeMonitor": false, "SaveLocalMetadata": true }, "ItemId": format!("lib_{}", id), "PrimaryImageItemId": "", "RefreshStatus": "Idle" }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(folders)
}

pub async fn emby_items_latest(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let allowed_libs = get_allowed_libs(&conn, req_user_id);
    if let Some(libs) = &allowed_libs { if libs.is_empty() { return Json(json!([])).into_response(); } }

    let mut sql = format!("SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, fanart_path, (SELECT COUNT(*) FROM items e WHERE e.series_id = items.id AND e.type = 'Episode') as recursive_item_count, (SELECT COUNT(*) FROM items s WHERE s.series_id = items.id AND s.type = 'Season') as total_seasons, IFNULL(ud.play_count, 0) as play_count, IFNULL(ud.is_favorite, 0) as is_favorite, IFNULL(ud.playback_position_ticks, 0) as playback_position_ticks FROM items LEFT JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = {} WHERE 1=1", req_user_id);
    let mut params: Vec<String> = Vec::new();

    if let Some(libs) = &allowed_libs {
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }

    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(" AND library_id = ?");
                params.push(lib_id.to_string());
                let lib_type: String = conn.query_row("SELECT type FROM libraries WHERE id = ?1", [lib_id], |r| r.get(0)).unwrap_or_default();
                if lib_type == "tvshows" { sql.push_str(" AND type = 'Series'"); }
                else if lib_type == "movies" { sql.push_str(" AND type = 'movies'"); }
            }
        }
    }
    
    sql.push_str(" ORDER BY id DESC LIMIT 16"); 
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(_) => return Json(json!([])).into_response() };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let id: i64 = row.get("id").unwrap_or_default();
        let r_type: String = row.get("type").unwrap_or_default();
        let playback_ticks: i64 = row.get("playback_position_ticks").unwrap_or(0);
        let play_count: i32 = row.get("play_count").unwrap_or(0);
        let runtime_secs: i32 = row.get("runtime").unwrap_or(0);
        let total_ticks: i64 = (runtime_secs as i64) * 10_000_000;
        
        let unplayed_count = if r_type == "Series" || r_type == "tvshows" || r_type == "Season" {
            if play_count > 0 { 0 } else { 1 }
        } else {
            0
        };

        let poster_path: String = row.get("poster_path").unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
        let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        let poster_tag = if has_poster { format!("tag_{}_{}", id, &hash_hex[..8]) } else { String::new() };

        let fanart_path: String = row.get("fanart_path").unwrap_or_default();
        let has_fanart = !fanart_path.is_empty();
        let mut hf = Sha256::new(); hf.update(fanart_path.as_bytes()); let hashf = hf.finalize();
        let hash_hexf: String = hashf.iter().map(|b| format!("{:02x}", b)).collect();
        let backdrop_tag = if has_fanart { format!("backdrop_{}_{}", id, &hash_hexf[..8]) } else { String::new() };

        let size: i64 = row.get("size").unwrap_or(0);
        let mtime: i64 = row.get("mtime").unwrap_or(0);
        let date_str = format_iso8601(mtime);
        let tmdb_id: String = row.get("tmdb_id").unwrap_or_default();
        let imdb_id: String = row.get("imdb_id").unwrap_or_default();
        let recursive_count: i64 = row.get("recursive_item_count").unwrap_or(0);
        let total_seasons: i64 = row.get("total_seasons").unwrap_or(0);
        
        Ok(json!({
            "Name": row.get::<_, String>("title").unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if r_type == "movies" { "Movie" } else if r_type == "Series" || r_type == "tvshows" { "Series" } else { "Episode" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>("year").unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>("plot").unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>("rating").unwrap_or(0.0).max(0.0), "RunTimeTicks": total_ticks, "Size": size,
            "DateCreated": date_str.clone(), "EndDate": date_str, "PrimaryImageAspectRatio": 0.6666666666666666,
            "Container": "mp4", "CanDelete": false, "CanDownload": false, "LocationType": "FileSystem", "Status": "Released",
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>("file_path").unwrap_or_default()),
            "ExternalUrls": build_external_urls(&tmdb_id, &imdb_id, &r_type),
            "RecursiveItemCount": if r_type == "Series" || r_type == "tvshows" || r_type == "Season" { recursive_count } else { 0 },
            "ChildCount": if r_type == "Series" || r_type == "tvshows" { total_seasons } else if r_type == "Season" { recursive_count } else { 0 },
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Size": size, "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks }],
            "UserData": {
                "Played": play_count > 0,
                "PlayCount": play_count,
                "IsFavorite": row.get::<_, i32>("is_favorite").unwrap_or(0) > 0,
                "PlaybackPositionTicks": playback_ticks,
                "PlayedPercentage": if total_ticks > 0 { (playback_ticks as f64 / total_ticks as f64) * 100.0 } else { 0.0 },
                "UnplayedItemCount": unplayed_count
            }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(items).into_response()
}

pub async fn emby_items_resume(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut where_clauses = vec!["ud.playback_position_ticks > 0".to_string()];
    let mut params: Vec<String> = Vec::new();

    if let Some(types) = query.get("IncludeItemTypes") {
        if types.contains("Movie") { where_clauses.push("items.type = 'movies'".to_string()); }
        else if types.contains("Episode") { where_clauses.push("items.type = 'Episode'".to_string()); }
    }

    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                where_clauses.push("items.library_id = ?".to_string());
                params.push(lib_id.to_string());
            }
        }
    }
    
    let where_str = where_clauses.join(" AND ");
    let sql = format!("SELECT items.id, items.title, items.type, items.year, items.plot, items.poster_path, items.file_path, items.runtime, items.rating, items.mpaa, items.genres, items.premiered, items.tmdb_id, items.imdb_id, items.size, items.mtime, items.sort_title, items.original_title, ud.playback_position_ticks, items.fanart_path, ud.play_count, ud.last_played_date, items.series_id, items.parent_id, items.season_number, items.episode_number FROM items JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = {} WHERE {} ORDER BY ud.last_played_date DESC LIMIT 30", req_user_id, where_str);
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response() };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let item_type: String = row.get(2).unwrap_or_default();
        let runtime_secs: i32 = row.get(7).unwrap_or(0);
        let total_ticks = (runtime_secs as i64) * 10_000_000;
        let playback_ticks: i64 = row.get(18).unwrap_or(0);
        let play_count: i32 = row.get(20).unwrap_or(0);
        
        let poster_path: String = row.get(5).unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
        let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        let poster_tag = if has_poster { format!("tag_{}_{}", id, &hash_hex[..8]) } else { String::new() };

        let fanart_path: String = row.get(19).unwrap_or_default();
        let has_fanart = !fanart_path.is_empty();
        let mut hf = Sha256::new(); hf.update(fanart_path.as_bytes()); let hashf = hf.finalize();
        let hash_hexf: String = hashf.iter().map(|b| format!("{:02x}", b)).collect();
        let backdrop_tag = if has_fanart { format!("backdrop_{}_{}", id, &hash_hexf[..8]) } else { String::new() };

        let played_pct = if total_ticks > 0 { (playback_ticks as f64 / total_ticks as f64) * 100.0 } else { 0.0 };

        let tmdb_id: String = row.get(12).unwrap_or_default();
        let imdb_id: String = row.get(13).unwrap_or_default();
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if item_type == "movies" { "Movie" } else if item_type == "Series" { "Series" } else { "Episode" },
            "IsFolder": false, "MediaType": "Video", "RunTimeTicks": total_ticks,
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0),
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "ExternalUrls": build_external_urls(&tmdb_id, &imdb_id, &item_type),
            "SeriesId": if item_type == "Episode" { json!(row.get::<_, i64>(22).unwrap_or(0).to_string()) } else { json!(null) },
            "SeasonId": if item_type == "Episode" { json!(row.get::<_, i64>(23).unwrap_or(0).to_string()) } else { json!(null) },
            "IndexNumber": if item_type == "Episode" { json!(row.get::<_, i32>(25).unwrap_or(0)) } else { json!(null) },
            "ParentIndexNumber": if item_type == "Episode" { json!(row.get::<_, i32>(24).unwrap_or(0)) } else { json!(null) },
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "RunTimeTicks": total_ticks }],
            "UserData": { 
                "Played": play_count > 0, 
                "PlaybackPositionTicks": playback_ticks,
                "PlayedPercentage": played_pct,
                "PlayCount": play_count
            }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response()
}

pub async fn emby_items_next_up(_: AxumPath<String>) -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }

pub async fn emby_shows_next_up(State(pool): State<Arc<Mutex<Connection>>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let sql = "SELECT e.id, e.title, e.season_number, e.episode_number, e.plot, e.runtime, e.poster_path, e.series_id, e.parent_id, e.series FROM items e JOIN user_data ud ON ud.item_id = CAST(e.id AS TEXT) AND ud.user_id = ?1 WHERE e.type = 'Episode' AND ud.playback_position_ticks > 0 ORDER BY ud.last_played_date DESC LIMIT 20";
    let mut items = Vec::new();
    if let Ok(mut stmt) = conn.prepare(sql) {
        if let Ok(mapped) = stmt.query_map([req_user_id], |r| {
            let id: i64 = r.get(0).unwrap_or_default();
            let s_num: i32 = r.get(2).unwrap_or(1);
            let e_num: i32 = r.get(3).unwrap_or(1);
            let runtime: i32 = r.get(5).unwrap_or(0);
            let poster: String = r.get(6).unwrap_or_default();
            Ok(json!({
                "Id": id.to_string(), "Name": r.get::<_, String>(1).unwrap_or_default(), "Type": "Episode", "IsFolder": false, "MediaType": "Video",
                "IndexNumber": e_num, "ParentIndexNumber": s_num,
                "SeriesId": r.get::<_, i64>(7).unwrap_or(0).to_string(), "SeasonId": r.get::<_, i64>(8).unwrap_or(0).to_string(),
                "SeriesName": r.get::<_, String>(9).unwrap_or_default(), "SeasonName": format!("Season {}", s_num),
                "Overview": r.get::<_, String>(4).unwrap_or_default(), "RunTimeTicks": (runtime as i64) * 10_000_000,
                "PrimaryImageAspectRatio": 1.7777777777777777,
                "ImageTags": if !poster.is_empty() { json!({"Primary": format!("tag_{}", id)}) } else { json!({}) },
                "UserData": { "Played": false, "PlaybackPositionTicks": 0, "UnplayedItemCount": 1 }
            }))
        }) { for res in mapped.flatten() { items.push(res); } }
    }
    Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response()
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct EmbyItemsQuery { 
    #[serde(rename = "ParentId")] parent_id: Option<String>,
    #[serde(rename = "ListItemIds")] list_item_ids: Option<String>,
    #[serde(rename = "StartIndex")] start_index: Option<i64>,
    #[serde(rename = "Limit")] limit: Option<i64>,
    #[serde(rename = "PersonIds")] person_ids: Option<String>,
    #[serde(rename = "Genres")] genres: Option<String>,
    #[serde(rename = "GenreIds")] genre_ids: Option<String>,
    #[serde(rename = "Tags")] tags: Option<String>,
    #[serde(rename = "TagIds")] tag_ids: Option<String>,
    #[serde(rename = "StudioIds")] studio_ids: Option<String>,
    #[serde(rename = "Studios")] studios: Option<String>,
    #[serde(rename = "IncludeItemTypes")] include_item_types: Option<String>,
    #[serde(rename = "Filters")] filters: Option<String>,
    #[serde(rename = "SortBy")] sort_by: Option<String>,
    #[serde(rename = "SortOrder")] sort_order: Option<String>,
    #[serde(rename = "Recursive")] recursive: Option<bool>,
    #[serde(rename = "SearchTerm", alias = "searchTerm", alias = "searchterm")]
    search_term: Option<String>,
}

pub async fn emby_items(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<EmbyItemsQuery>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse { emby_items_inner(pool, query, req_user_id).await }
pub async fn emby_items_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<EmbyItemsQuery>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse { emby_items_inner(pool, query, req_user_id).await }

async fn emby_items_inner(pool: Arc<Mutex<Connection>>, query: EmbyItemsQuery, req_user_id: i64) -> Response {
    let conn = pool.lock().unwrap();
    
    if let Some(list_ids) = &query.list_item_ids {
        if let Ok(item_id) = list_ids.parse::<i64>() {
            let set_name_str: String = conn.query_row("SELECT set_name FROM items WHERE id = ?1", [item_id], |r| r.get(0)).unwrap_or_default();
            let mut boxsets = Vec::new();
            for name in set_name_str.split(" / ").filter(|s| !s.trim().is_empty()) {
                let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                let boxset_id = format!("boxset_{:x}", hash);
                let is_fav = conn.query_row("SELECT COUNT(1) FROM boxset_favorites WHERE user_id = ?1 AND boxset_id = ?2", rusqlite::params![req_user_id, &item_id.to_string()], |r| r.get::<_, i64>(0)).map(|c| c > 0).unwrap_or(false);
                boxsets.push(json!({ "Name": name, "Id": boxset_id, "ServerId": "rust-em-server", "Type": "BoxSet", "IsFolder": true, "MediaType": "Video", "ImageTags": {"Primary": format!("{}_cover", boxset_id)}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": is_fav, "Played": false } }));
            }
            return Json(json!({ "Items": boxsets, "TotalRecordCount": boxsets.len() })).into_response();
        }
        return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
    }

    if let Some(types) = &query.include_item_types {
        let t_lower = types.to_lowercase();
        let is_dict_only = !t_lower.contains("movie") && !t_lower.contains("series") && !t_lower.contains("episode") && !t_lower.contains("season");

        if is_dict_only && t_lower.contains("tag") {
            let mut sql = "SELECT tags FROM items WHERE tags != ''".to_string();
            if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
                if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
                let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
                sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
            }
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
            }
            let mut set = std::collections::HashSet::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    for t_str in mapped.flatten() { for tag in t_str.split(" / ") { if !tag.trim().is_empty() { set.insert(tag.trim().to_string()); } } }
                }
            }
            let mut names: Vec<String> = set.into_iter().collect();
            names.sort();
            let items: Vec<Value> = names.into_iter().map(|name| {
                json!({ "Name": name, "Id": format!("tag_{}", name), "ServerId": "rust-em-server", "Type": "Tag", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": crate::images::get_cover_tag("tag", "tag", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } })
            }).collect();
            return Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response();
        }
        
        if is_dict_only && t_lower.contains("genre") {
            let mut sql = "SELECT genres FROM items WHERE genres != ''".to_string();
            if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
                if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
                let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
                sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
            }
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
            }
            let mut set = std::collections::HashSet::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    for g_str in mapped.flatten() { for g in g_str.split(" / ") { if !g.trim().is_empty() { set.insert(g.trim().to_string()); } } }
                }
            }
            let mut names: Vec<String> = set.into_iter().collect();
            names.sort();
            let items: Vec<Value> = names.into_iter().map(|name| {
                json!({ "Name": name, "Id": format!("genre_{}", name), "ServerId": "rust-em-server", "Type": "Genre", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": crate::images::get_cover_tag("genre", "genre", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } })
            }).collect();
            return Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response();
        }

        if is_dict_only && t_lower.contains("year") {
            let mut sql = "SELECT DISTINCT year FROM items WHERE year != ''".to_string();
            if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
                if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
                let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
                sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
            }
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
            }
            sql.push_str(" ORDER BY year DESC");
            let mut items = Vec::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    for y in mapped.flatten() {
                        items.push(json!({ "Name": y, "Id": y, "ServerId": "rust-em-server", "Type": "Year", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } }));
                    }
                }
            }
            return Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response();
        }

        if is_dict_only && t_lower.contains("officialrating") {
            let mut sql = "SELECT DISTINCT mpaa FROM items WHERE mpaa != ''".to_string();
            if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
                if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
                let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
                sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
            }
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
            }
            sql.push_str(" ORDER BY mpaa ASC");
            let mut items = Vec::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    for m in mapped.flatten() {
                        items.push(json!({ "Name": m, "Id": m, "ServerId": "rust-em-server", "Type": "OfficialRating", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } }));
                    }
                }
            }
            return Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response();
        }

        if types.to_lowercase().contains("boxset") {
            let mut sql = "SELECT set_name FROM items WHERE set_name != ''".to_string();
            if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
                if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
                let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
                sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
            }
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
            }
            let mut boxsets = Vec::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    let mut set_names = std::collections::HashSet::new();
                    for s in mapped.flatten() { for name in s.split(" / ").filter(|s| !s.trim().is_empty()) { set_names.insert(name.to_string()); } }
                    let is_fav_query = query.filters.as_deref().map_or(false, |f| f.contains("IsFavorite"));
                    for name in set_names {
                        let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                        let hash_hex = format!("{:x}", hash);
                        let boxset_id = format!("boxset_{}", hash_hex);
                        
                        let is_fav = if let Ok(count) = conn.query_row("SELECT COUNT(1) FROM boxset_favorites WHERE user_id = ?1 AND boxset_id = ?2", rusqlite::params![req_user_id, &boxset_id], |r| r.get::<_, i64>(0)) { count > 0 } else { false };
                        if is_fav_query && !is_fav { continue; }
                        boxsets.push(json!({ "Name": name, "Id": boxset_id, "ServerId": "rust-em-server", "Type": "BoxSet", "IsFolder": true, "MediaType": "Video", "ImageTags": {"Primary": crate::images::get_cover_tag("boxset", "boxset", &hash_hex)}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": is_fav, "Played": false } }));
                    }
                }
            }
            return Json(json!({ "Items": boxsets, "TotalRecordCount": boxsets.len() })).into_response();
        }
    }

    let is_folder_view = query.recursive == Some(false) || query.sort_by.as_deref().map_or(false, |s| s.contains("IsFolder")) || query.include_item_types.as_deref().map_or(false, |t| t.to_lowercase().contains("folder"));
    if is_folder_view {
        let base_path = if let Some(pid) = &query.parent_id {
            if pid.starts_with("lib_") {
                if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                    if req_user_id != 1 {
                        let is_allowed = conn.query_row("SELECT COUNT(1) FROM user_library_access WHERE user_id = ?1 AND library_id = ?2", [req_user_id, lib_id], |r| r.get::<_, i64>(0)).unwrap_or(0) > 0;
                        if !is_allowed { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
                    }
                    if let Ok(path_json) = conn.query_row("SELECT path FROM libraries WHERE id = ?1", [lib_id], |r| r.get::<_, String>(0)) { serde_json::from_str::<Vec<String>>(&path_json).ok().and_then(|p| p.first().cloned()) } else { None }
                } else { None }
            } else { None }
        } else { None };

        if let Some(base) = base_path {
            let base_clean = base.trim_end_matches('/');
            let mut folders_set = HashSet::new();
            if let Ok(mut stmt) = conn.prepare("SELECT file_path FROM items WHERE file_path LIKE ?") {
                let like_pattern = format!("{}/%", base_clean);
                if let Ok(mapped) = stmt.query_map([&like_pattern], |r| r.get::<_, String>(0)) {
                    for f_path in mapped.flatten() {
                        let rel_path = f_path[base_clean.len()..].trim_start_matches('/');
                        if let Some(first_dir) = rel_path.split('/').next() {
                            if !first_dir.is_empty() && first_dir != rel_path { folders_set.insert((first_dir.to_string(), format!("{}/{}", base_clean, first_dir))); }
                        }
                    }
                }
            }
            if !folders_set.is_empty() {
                let mut folder_items = Vec::new();
                for (name, _abs_path) in folders_set {
                    folder_items.push(json!({ "Name": name, "Id": format!("folder_{}", name), "ServerId": "rust-em-server", "Type": "Folder", "IsFolder": true, "MediaType": "Video", "ImageTags": {}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false } }));
                }
                return Json(json!({ "Items": folder_items, "TotalRecordCount": folder_items.len() })).into_response();
            }
        }
    }

    let allowed_libs = get_allowed_libs(&conn, req_user_id);
    if let Some(libs) = &allowed_libs { if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); } }

    let mut where_clauses = vec!["1=1".to_string()];
    let mut params: Vec<String> = Vec::new();

    if let Some(libs) = &allowed_libs {
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        where_clauses.push(format!("library_id IN ({})", libs_str.join(",")));
    }

    if let Some(types) = &query.include_item_types {
        let t = types.to_lowercase();
        let mut type_conditions = Vec::new();
        if t.contains("movie") { type_conditions.push("type = 'movies'"); }
        if t.contains("series") { type_conditions.push("type = 'Series'"); }
        if t.contains("episode") { type_conditions.push("type = 'Episode'"); }
        if t.contains("season") { type_conditions.push("type = 'Season'"); }
        if !type_conditions.is_empty() { where_clauses.push(format!("({})", type_conditions.join(" OR "))); }
    } else {
        if let Some(pid) = &query.parent_id {
            if pid.starts_with("lib_") { 
                if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                    let lib_type: String = conn.query_row("SELECT type FROM libraries WHERE id = ?1", [lib_id], |r| r.get(0)).unwrap_or_default();
                    if lib_type == "tvshows" { where_clauses.push("type = 'Series'".to_string()); }
                    else if lib_type == "movies" { where_clauses.push("type = 'movies'".to_string()); }
                }
            }
        } else if query.recursive == Some(true) {
            where_clauses.push("(type = 'movies' OR type = 'Series')".to_string());
        }
    }

    if let Some(filters) = &query.filters {
        if filters.contains("IsPlayed") { where_clauses.push("ud.play_count > 0".to_string()); } 
        else if filters.contains("IsUnplayed") { where_clauses.push("IFNULL(ud.play_count, 0) = 0".to_string()); }
        if filters.contains("IsFavorite") { where_clauses.push("ud.is_favorite = 1".to_string()); }
    }

    if let Some(pid) = &query.parent_id {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { 
                where_clauses.push("library_id = ?".to_string());
                params.push(lib_id.to_string()); 
            }
        } else if pid.starts_with("boxset_") {
            where_clauses.push("set_name LIKE '%' ".to_string());
        }
    }

    let search_term_raw = query.search_term.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let mut is_search = false;

    if let Some(term) = search_term_raw {
        let len = term.chars().count();
        if len <= 1 { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
        is_search = true;
        let pattern = format!("%{}%", term);
        where_clauses.push("(title LIKE ? OR original_title LIKE ? OR tags LIKE ? OR actors LIKE ? OR directors LIKE ?)".to_string());
        for _ in 0..5 { params.push(pattern.clone()); }
    }

    let split_ids = |val: &str| -> Vec<String> { decode_url(val).split(|c| c == ',' || c == '|').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect() };
    let mut genre_filters = Vec::new(); let mut studio_filters = Vec::new(); let mut person_filters = Vec::new(); let mut tag_filters = Vec::new();

    if let Some(p) = &query.person_ids { person_filters.extend(split_ids(p)); }
    if let Some(g) = &query.genre_ids { genre_filters.extend(split_ids(g)); }
    if let Some(g) = &query.genres { genre_filters.extend(split_ids(g)); }
    if let Some(t) = &query.tag_ids { tag_filters.extend(split_ids(t)); }
    if let Some(t) = &query.tags { tag_filters.extend(split_ids(t)); }
    if let Some(s) = &query.studio_ids { studio_filters.extend(split_ids(s)); }
    if let Some(s) = &query.studios { studio_filters.extend(split_ids(s)); }

    if !genre_filters.is_empty() {
        let mut or_clauses = Vec::new();
        for raw_id in genre_filters {
            let name = raw_id.replace("genre_", "").trim().to_string();
            if name.is_empty() { continue; }
            or_clauses.push("(genres = ? OR genres LIKE ? OR genres LIKE ? OR genres LIKE ?)");
            params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name));
        }
        if !or_clauses.is_empty() { where_clauses.push(format!("({})", or_clauses.join(" OR "))); }
    }

    if !studio_filters.is_empty() {
        let mut or_clauses = Vec::new();
        for raw_id in studio_filters {
            let name = raw_id.replace("studio_", "").trim().to_string();
            if name.is_empty() { continue; }
            or_clauses.push("(studios = ? OR studios LIKE ? OR studios LIKE ? OR studios LIKE ?)");
            params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name));
        }
        if !or_clauses.is_empty() { where_clauses.push(format!("({})", or_clauses.join(" OR "))); }
    }

    if !person_filters.is_empty() {
        let mut or_clauses = Vec::new();
        for raw_id in person_filters {
            let name = raw_id.replace("person_", "").trim().to_string();
            if name.is_empty() { continue; }
            or_clauses.push("(directors = ? OR directors LIKE ? OR directors LIKE ? OR directors LIKE ? OR actors LIKE ? OR actors LIKE ?)");
            params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name)); params.push(format!("%\"name\":\"{}\"%", name)); params.push(format!("%\"name\": \"{}\"%", name));
        }
        if !or_clauses.is_empty() { where_clauses.push(format!("({})", or_clauses.join(" OR "))); }
    }

    for raw_id in tag_filters {
        let name = raw_id.replace("tag_", "").trim().to_string();
        if name.is_empty() { continue; }
        where_clauses.push("(tags = ? OR tags LIKE ? OR tags LIKE ? OR tags LIKE ?)".to_string());
        params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name));
    }

    let where_str = where_clauses.join(" AND ");
    
    let total: i64 = if is_search { 0 } else {
        let count_sql = format!("SELECT COUNT(*) FROM items LEFT JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = {} WHERE {}", req_user_id, where_str);
        conn.query_row(&count_sql, rusqlite::params_from_iter(params.iter()), |r| r.get(0)).unwrap_or(0)
    };
    
    let mut order_clause = " ORDER BY id DESC".to_string();
    if let Some(sort_by) = &query.sort_by {
        let is_desc = query.sort_order.as_deref().unwrap_or("Ascending").to_lowercase() == "descending";
        let dir = if is_desc { " DESC" } else { " ASC" };
        if sort_by.contains("DateCreated") { order_clause = format!(" ORDER BY date_created {}", dir); }
        else if sort_by.contains("SortName") || sort_by.contains("Filename") { order_clause = format!(" ORDER BY title {}", dir); }
        else if sort_by.contains("ProductionYear") { order_clause = format!(" ORDER BY year {}", dir); }
        else if sort_by.contains("CommunityRating") { order_clause = format!(" ORDER BY rating {}", dir); }
    }
    
    let start = query.start_index.unwrap_or(0);
    let limit = query.limit.unwrap_or(100);
    
    let data_sql = format!("SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, IFNULL(ud.playback_position_ticks, 0) as playback_position_ticks, fanart_path, date_created, actors, directors, tags, studios, IFNULL(ud.play_count, 0) as play_count, IFNULL(ud.is_favorite, 0) as is_favorite, IFNULL(ud.last_played_date, '') as last_played_date, (SELECT COUNT(*) FROM items e WHERE e.series_id = items.id AND e.type = 'Episode') as recursive_item_count, (SELECT COUNT(*) FROM items s WHERE s.series_id = items.id AND s.type = 'Season') as total_seasons FROM items LEFT JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = {} WHERE {} {} LIMIT {} OFFSET {}", req_user_id, where_str, order_clause, limit, start);

    let mut stmt = match conn.prepare(&data_sql) { Ok(s) => s, Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response() };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let id: i64 = row.get("id").unwrap_or_default();
        let poster_path: String = row.get("poster_path").unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let poster_tag = if has_poster {
            let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
            let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            format!("tag_{}_{}", id, &hash_hex[..8])
        } else { String::new() };
        
        let playback_ticks: i64 = row.get("playback_position_ticks").unwrap_or(0);
        let fanart_path: String = row.get("fanart_path").unwrap_or_default();
        let has_fanart = !fanart_path.is_empty();
        let mut hf = Sha256::new(); hf.update(fanart_path.as_bytes()); let hashf = hf.finalize();
        let hash_hexf: String = hashf.iter().map(|b| format!("{:02x}", b)).collect();
        let backdrop_tag = if has_fanart { format!("backdrop_{}_{}", id, &hash_hexf[..8]) } else { String::new() };
        
        let runtime_secs: i32 = row.get("runtime").unwrap_or(0);
        let total_ticks: i64 = (runtime_secs as i64) * 10_000_000;
        let size: i64 = row.get("size").unwrap_or(0);
        let play_count: i32 = row.get("play_count").unwrap_or(0);
        let recursive_count: i64 = row.get("recursive_item_count").unwrap_or(0);
        let total_seasons: i64 = row.get("total_seasons").unwrap_or(0);
        let tmdb_id: String = row.get("tmdb_id").unwrap_or_default();
        let imdb_id: String = row.get("imdb_id").unwrap_or_default();
        let r_type: String = row.get("type").unwrap_or_default();

        Ok(json!({
            "Name": row.get::<_, String>("title").unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if r_type == "movies" { "Movie" } else if r_type == "Series" || r_type == "tvshows" { "Series" } else if r_type == "Season" { "Season" } else { "Episode" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>("year").unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>("plot").unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>("rating").unwrap_or(0.0).max(0.0), "RunTimeTicks": total_ticks, "Size": size,
            "DateCreated": format_iso8601(row.get::<_, i64>("date_created").unwrap_or(0)), "PrimaryImageAspectRatio": 0.6666666666666666,
            "Container": "mp4", "CanDelete": false, "CanDownload": false, "LocationType": "FileSystem", "Status": "Released",
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "RecursiveItemCount": if r_type == "Series" || r_type == "tvshows" || r_type == "Season" { recursive_count } else { 0 },
            "ChildCount": if r_type == "Series" || r_type == "tvshows" { total_seasons } else if r_type == "Season" { recursive_count } else { 0 },
            "Path": decode_url(&row.get::<_, String>("file_path").unwrap_or_default()),
            "ExternalUrls": build_external_urls(&tmdb_id, &imdb_id, &r_type),
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Size": size, "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks }],
            "UserData": if r_type == "Series" || r_type == "tvshows" || r_type == "Season" {
                let unplayed = conn.query_row("SELECT COUNT(*) FROM items e LEFT JOIN user_data ud2 ON ud2.item_id = CAST(e.id AS TEXT) AND ud2.user_id = ?1 WHERE e.series_id = ?2 AND e.type = 'Episode' AND IFNULL(ud2.play_count, 0) = 0", rusqlite::params![req_user_id, id], |r| r.get::<_, i64>(0)).unwrap_or(0);
                json!({ 
                    "PlaybackPositionTicks": playback_ticks, 
                    "PlayCount": play_count, 
                    "IsFavorite": row.get::<_, i32>("is_favorite").unwrap_or(0) > 0, 
                    "Played": unplayed == 0, 
                    "PlayedPercentage": if total_ticks > 0 { (playback_ticks as f64 / total_ticks as f64) * 100.0 } else { 0.0 },
                    "UnplayedItemCount": unplayed
                })
            } else {
                json!({ 
                    "PlaybackPositionTicks": playback_ticks, 
                    "PlayCount": play_count, 
                    "IsFavorite": row.get::<_, i32>("is_favorite").unwrap_or(0) > 0, 
                    "Played": play_count > 0, 
                    "PlayedPercentage": if total_ticks > 0 { (playback_ticks as f64 / total_ticks as f64) * 100.0 } else { 0.0 }
                })
            }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    
    let final_total = if is_search { items.len() as i64 } else { total };
    Json(json!({ "Items": items, "TotalRecordCount": final_total })).into_response()
}

pub async fn emby_shows_seasons(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(series_id): AxumPath<String>) -> impl IntoResponse {
    let Ok(sid) = series_id.parse::<i64>() else { return Json(json!({"Items": [], "TotalRecordCount": 0})); };
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, title, season_number, poster_path, (SELECT COUNT(*) FROM items e WHERE e.parent_id = items.id AND e.type = 'Episode') as ep_count FROM items WHERE type = 'Season' AND parent_id = ?1 ORDER BY season_number ASC").unwrap();
    
    let items: Vec<Value> = stmt.query_map([sid], |r| {
        let id: i64 = r.get(0).unwrap_or_default();
        let ep_count: i64 = r.get(4).unwrap_or(0);
        Ok(json!({
            "Name": r.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server", 
            "Type": "Season", "IsFolder": true, "MediaType": "Video", "IndexNumber": r.get::<_, i32>(2).unwrap_or(1), "SeriesId": series_id,
            "ChildCount": ep_count, "RecursiveItemCount": ep_count, "UserData": { "Played": false, "UnplayedItemCount": ep_count }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct EpisodesQuery { 
    #[serde(rename = "SeasonId")] season_id: Option<String>,
    #[serde(rename = "ParentId")] parent_id: Option<String>,
}

pub async fn emby_shows_episodes(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(series_id): AxumPath<String>, Query(query): Query<EpisodesQuery>) -> impl IntoResponse {
    let Ok(sid) = series_id.parse::<i64>() else { return Json(json!({"Items": [], "TotalRecordCount": 0})); };
    let conn = pool.lock().unwrap();
    let series_name: String = conn.query_row("SELECT title FROM items WHERE id = ?1", [sid], |row| row.get::<_, String>(0)).unwrap_or_default();
    
    let mut sql = "SELECT id, title, season_number, episode_number, plot, runtime, poster_path, fanart_path, directors, actors FROM items WHERE type = 'Episode' AND series_id = ?1".to_string();
    let mut params = vec![sid.to_string()];
    if let Some(season_id) = &query.season_id {
        sql.push_str(" AND parent_id = ?");
        params.push(season_id.clone());
    }
    sql.push_str(" ORDER BY season_number ASC, episode_number ASC");
    
    let mut stmt = conn.prepare(&sql).unwrap();
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |r| {
        let id: i64 = r.get(0).unwrap_or_default();
        let runtime: i32 = r.get(5).unwrap_or(0);
        let run_time_ticks = (runtime as i64) * 10_000_000;
        let poster: String = r.get(6).unwrap_or_default();
        let mut image_tags = json!({});
        if !poster.is_empty() { image_tags["Primary"] = json!(format!("tag_{}", id)); }

        Ok(json!({
            "Name": r.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": "Episode", "IsFolder": false, "MediaType": "Video",
            "IndexNumber": r.get::<_, i32>(3).unwrap_or(1), "ParentIndexNumber": r.get::<_, i32>(2).unwrap_or(1),
            "SeriesId": sid.to_string(), "SeasonId": query.season_id.clone().unwrap_or_default(),
            "SeriesName": series_name.clone(), "SeasonName": format!("Season {}", r.get::<_, i32>(2).unwrap_or(1)),
            "Overview": r.get::<_, String>(4).unwrap_or_default(), "RunTimeTicks": run_time_ticks,
            "ImageTags": image_tags, "PrimaryImageAspectRatio": 1.7777777777777777,
            "UserData": { "Played": false, "PlaybackPositionTicks": 0, "UnplayedItemCount": 1 }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_item_detail(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<String>, Extension(req_user_id): Extension<i64>) -> Response { emby_item_detail_inner(pool, item_id, req_user_id).await }
pub async fn emby_item_detail_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>, Extension(req_user_id): Extension<i64>) -> Response { emby_item_detail_inner(pool, item_id, req_user_id).await }

async fn emby_item_detail_inner(pool: Arc<Mutex<Connection>>, item_id: String, req_user_id: i64) -> Response {
    if item_id.starts_with("person_") || item_id.starts_with("studio_") { return StatusCode::NOT_FOUND.into_response(); }
    let Ok(id) = item_id.parse::<i64>() else { return StatusCode::NOT_FOUND.into_response(); };
    let conn = pool.lock().unwrap();
    
    let query_str = format!(
        "SELECT items.id, items.title, items.type, items.year, items.plot, items.poster_path, items.file_path, items.studios, items.parts, items.runtime, items.actors, items.rating, items.mpaa, items.genres, items.premiered, items.tmdb_id, items.imdb_id, items.size, items.mtime, items.series, items.label, items.set_name, items.tagline, items.tags, items.directors, items.num, items.stills, items.releasedate, items.original_title, items.sort_title, items.country, items.language, items.writers, items.trailer, items.media_streams, items.date_created, items.primary_image_aspect_ratio, items.original_language, items.fanart_path, items.logo_path, items.thumb_path, items.banner_path, items.strm_content, IFNULL(ud.playback_position_ticks, 0), IFNULL(ud.play_count, 0), IFNULL(ud.is_favorite, 0), IFNULL(ud.last_played_date, ''), (SELECT COUNT(*) FROM items e WHERE e.series_id = items.id AND e.type = 'Episode') as total_eps, (SELECT COUNT(*) FROM items s WHERE s.series_id = items.id AND s.type = 'Season') as total_seasons, (SELECT COUNT(*) FROM items e JOIN user_data ud2 ON ud2.item_id = CAST(e.id AS TEXT) AND ud2.user_id = {} WHERE e.series_id = items.id AND e.type = 'Episode' AND ud2.play_count > 0) as played_eps, (SELECT COUNT(*) FROM items e WHERE e.parent_id = items.id AND e.type = 'Episode') as season_total_eps, (SELECT COUNT(*) FROM items e JOIN user_data ud3 ON ud3.item_id = CAST(e.id AS TEXT) AND ud3.user_id = {} WHERE e.parent_id = items.id AND e.type = 'Episode' AND ud3.play_count > 0) as season_played_eps FROM items LEFT JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = {} WHERE items.id = ?",
        req_user_id, req_user_id, req_user_id
    );

    let item_opt = conn.query_row(&query_str, rusqlite::params![id], |row| {
        let title: String = row.get(1).unwrap_or_default(); let typ: String = row.get(2).unwrap_or_default();
        let year_str: String = row.get(3).unwrap_or_default(); let plot: String = row.get(4).unwrap_or_default();
        let poster_path: String = row.get(5).unwrap_or_default(); let studios_str: String = row.get(7).unwrap_or_default();
        let parts_json: String = row.get(8).unwrap_or_default(); let runtime: i32 = row.get(9).unwrap_or(0);
        let actors_json: String = row.get(10).unwrap_or_default(); let rating: f64 = row.get(11).unwrap_or(0.0);
        let mpaa: String = row.get(12).unwrap_or_default(); let genres_str: String = row.get(13).unwrap_or_default();
        let premiered: String = row.get(14).unwrap_or_default(); let tmdb_id: String = row.get(15).unwrap_or_default();
        let imdb_id: String = row.get(16).unwrap_or_default(); let size: i64 = row.get(17).unwrap_or(0);
        let mtime: i64 = row.get(18).unwrap_or(0); let series: String = row.get(19).unwrap_or_default();
        let set_name: String = row.get(21).unwrap_or_default(); let tagline: String = row.get(22).unwrap_or_default();
        let tags_str: String = row.get(23).unwrap_or_default(); let directors_str: String = row.get(24).unwrap_or_default();
        let num: String = row.get(25).unwrap_or_default(); let stills_json: String = row.get(26).unwrap_or_default();
        let stills_vec: Vec<String> = serde_json::from_str(&stills_json).unwrap_or_default();
        let releasedate: String = row.get(27).unwrap_or_default(); let original_title: String = row.get(28).unwrap_or_default();
        let sort_title: String = row.get(29).unwrap_or_default();
        let sort_title = if sort_title.is_empty() { title.clone() } else { sort_title };
        let country: String = row.get(30).unwrap_or_default(); let language: String = row.get(31).unwrap_or_default();
        let _writers_str: String = row.get(32).unwrap_or_default(); let media_streams_json: String = row.get(34).unwrap_or_default();
        let date_created: i64 = row.get(35).unwrap_or(0); let aspect_ratio: f64 = row.get(36).unwrap_or(0.0);
        let original_language: String = row.get(37).unwrap_or_default(); let fanart_path: String = row.get(38).unwrap_or_default();
        let logo_path: String = row.get(39).unwrap_or_default(); let thumb_path: String = row.get(40).unwrap_or_default();
        let banner_path: String = row.get(41).unwrap_or_default(); let strm_content: String = row.get(42).unwrap_or_default();
        let playback_ticks: i64 = row.get(43).unwrap_or(0); let play_count: i32 = row.get(44).unwrap_or(0); let is_favorite: i32 = row.get(45).unwrap_or(0);
        let last_played_str: String = row.get(46).unwrap_or_default();
        let total_eps: i64 = row.get(47).unwrap_or(0);
        let total_seasons: i64 = row.get(48).unwrap_or(0);
        let played_eps: i64 = row.get(49).unwrap_or(0);
        let season_total_eps: i64 = row.get(50).unwrap_or(0);
        let season_played_eps: i64 = row.get(51).unwrap_or(0);

        let parts: Vec<String> = serde_json::from_str(&parts_json).unwrap_or_default();
        let actors: Vec<Value> = serde_json::from_str(&actors_json).unwrap_or_default();
        let mut media_streams: Value = serde_json::from_str(&media_streams_json).unwrap_or(json!([]));
        sanitize_media_streams(&mut media_streams);
        let real_path = strm_content.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("").to_string();

        let genres_vec: Vec<&str> = genres_str.split(" / ").filter(|s| !s.is_empty()).collect();
        let mut genre_items = Vec::new();
        for g in &genres_vec { genre_items.push(json!({"Id": format!("genre_{}", g), "Name": g})); }
        
        let tags_vec: Vec<&str> = tags_str.split(" / ").filter(|s| !s.is_empty()).collect();
        let mut tag_items = Vec::new();
        for t in &tags_vec { tag_items.push(json!({"Id": format!("tag_{}", t), "Name": t})); }

        let studios: Vec<Value> = studios_str.split(" / ").filter(|s| !s.trim().is_empty()).map(|s| json!({ "Name": s, "Id": format!("studio_{}", s) })).collect();

        let mut people: Vec<Value> = Vec::new();
        let director_names: HashSet<String> = directors_str.split(" / ").map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        for d in &director_names {
            people.push(json!({ "Name": d, "Type": "Director", "Role": "", "Id": format!("person_{}", d), "PrimaryImageTag": "" }));
        }
        for a in actors.iter() {
            let name = a["name"].as_str().unwrap_or_default();
            if !director_names.contains(name) {
                people.push(json!({ "Name": name, "Role": a["role"].as_str().unwrap_or(""), "Type": "Actor", "Id": format!("person_{}", name), "PrimaryImageTag": "" }));
            }
        }

        let mut image_tags = serde_json::Map::new();
        if !poster_path.is_empty() {
            let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
            let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            image_tags.insert("Primary".to_string(), json!(format!("tag_{}_{}", id, &hash_hex[..8])));
        }
        if !logo_path.is_empty() { image_tags.insert("Logo".to_string(), json!(format!("logo_{}", id))); }
        if !thumb_path.is_empty() { image_tags.insert("Thumb".to_string(), json!(format!("thumb_{}", id))); }
        if !banner_path.is_empty() { image_tags.insert("Banner".to_string(), json!(format!("banner_{}", id))); }

        let mut backdrops: Vec<String> = Vec::new();
        if !fanart_path.is_empty() { backdrops.push(fanart_path.clone()); }
        backdrops.extend(stills_vec.iter().cloned());
        let mut backdrop_tags: Vec<Value> = Vec::new();
        for (i, p) in backdrops.iter().enumerate() {
            let mut h = Sha256::new(); h.update(p.as_bytes()); let hash = h.finalize();
            let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            backdrop_tags.push(json!(format!("backdrop_{}_{}_{}", id, i, &hash_hex[..8])));
        }

        let date_created_ts = if date_created > 0 { date_created } else { mtime };
        let run_time_ticks = (runtime as i64) * 10_000_000;
        let est_bitrate = if size > 0 && run_time_ticks > 0 { (size * 8) / (run_time_ticks / 10_000_000) } else { 0 };

        let media_sources: Vec<Value> = if parts.is_empty() { vec![] } else {
            parts.iter().enumerate().map(|(i, _path)| {
                let stream_url = format!("/Videos/{}/original.mp4?MediaSourceId={}_cd{}", id, id, i);
                json!({
                    "Id": format!("{}_cd{}", id, i), "ItemId": id.to_string(), "Path": decode_url(&real_path), "DirectStreamUrl": stream_url,
                    "Protocol": "Http", "Container": "mp4", "Name": title.clone(), "Type": "Default", "VideoType": "VideoFile", "IsRemote": true,
                    "Bitrate": est_bitrate, "RunTimeTicks": run_time_ticks, "Size": size, "DefaultAudioStreamIndex": 1, "DefaultSubtitleStreamIndex": -1,
                    "SupportsDirectPlay": true, "SupportsDirectStream": true, "SupportsProbing": true, "SupportsTranscoding": false,
                    "AddApiKeyToDirectStreamUrl": false, "RequiresClosing": false, "RequiresOpening": false, "ProbeEngine": "rust-native", "MediaStreams": media_streams.clone()
                })
            }).collect()
        };

        let played = if typ == "Series" { total_eps > 0 && total_eps == played_eps } 
                     else if typ == "Season" { season_total_eps > 0 && season_total_eps == season_played_eps } 
                     else { play_count > 0 };
        let unplayed = if typ == "Series" { total_eps - played_eps } 
                       else if typ == "Season" { season_total_eps - season_played_eps } 
                       else { 0 };
        let _recursive_count = if typ == "Series" { total_eps } else if typ == "Season" { season_total_eps } else { 0 };
        let _child_count = if typ == "Series" { total_seasons } else if typ == "Season" { season_total_eps } else { 0 };

        Ok(json!({
            "Id": id.to_string(), "ServerId": "rust-em-server", "Name": title.clone(),
            "OriginalTitle": json!(original_title), "SortName": json!(sort_title.clone()), "ForcedSortName": json!(sort_title.clone()),
            "Type": if typ == "movies" { "Movie" } else if typ == "Series" || typ == "tvshows" { "Series" } else if typ == "Season" { "Season" } else { "Episode" },
            "MediaType": "Video", "IsFolder": false, "Overview": plot, "ProductionYear": year_str.parse::<i32>().unwrap_or(0), "PremiereDate": premiered, "ReleaseDate": releasedate,
            "DateCreated": format_iso8601(date_created_ts), "CommunityRating": rating, "OfficialRating": mpaa, "RunTimeTicks": run_time_ticks, "Size": size, "Bitrate": est_bitrate,
            "Genres": genres_vec, "GenreItems": genre_items, "Tags": tags_vec, "TagItems": tag_items,
            "Taglines": if tagline.is_empty() { json!([]) } else { json!([tagline]) }, "Studios": studios, "People": people,
            "ProviderIds": json!({ "Tmdb": tmdb_id, "Imdb": imdb_id, "num": num }),
            "ImageTags": image_tags, "BackdropImageTags": backdrop_tags, "PrimaryImageAspectRatio": if typ == "Episode" { json!(1.7777777777777777) } else if aspect_ratio > 0.0 { json!(aspect_ratio) } else { json!(0.6666666666666666) },
            "ProductionLocations": country.split(" / ").filter(|s| !s.is_empty()).collect::<Vec<_>>(), "OriginalLanguage": json!(if !original_language.is_empty() { original_language } else { language }),
            "SetName": json!(set_name), "SeriesName": json!(if typ == "tvshows" { series } else { "".to_string() }),
            "Path": decode_url(&real_path), "Container": "mp4", "MediaSourceCount": media_sources.len(), "MediaSources": media_sources,
            
            
            "RecursiveItemCount": if typ == "Series" || typ == "tvshows" { total_eps } else if typ == "Season" { season_total_eps } else { 0 },
            "ChildCount": if typ == "Series" || typ == "tvshows" { total_seasons } else if typ == "Season" { season_total_eps } else { 0 },
            "SeasonCount": if typ == "Series" || typ == "tvshows" { total_seasons } else { 0 },
            "ExternalUrls": build_external_urls(&tmdb_id, &imdb_id, &typ),
            "ProbeEngine": "rust-native", "LocationType": "FileSystem", "CanDelete": false, "CanDownload": false, "CanDeleteFromFolders": [], "HideMissingActorImages": true,
            "UserData": if typ == "Series" || typ == "tvshows" || typ == "Season" {
                json!({ 
                    "ItemId": id.to_string(), "Key": id.to_string(), 
                    "PlaybackPositionTicks": playback_ticks, "PlayCount": play_count, 
                    "IsFavorite": is_favorite > 0, "Played": played, 
                    "PlayedPercentage": if run_time_ticks > 0 { playback_ticks * 100 / run_time_ticks } else { 0 },
                    "LastPlayedDate": if !last_played_str.is_empty() {
                        if let Ok(ts) = last_played_str.parse::<i64>() { json!(format_iso8601(ts)) } else { json!(null) }
                    } else { json!(null) },
                    "UnplayedItemCount": unplayed
                })
            } else {
                json!({ 
                    "ItemId": id.to_string(), "Key": id.to_string(), 
                    "PlaybackPositionTicks": playback_ticks, "PlayCount": play_count, 
                    "IsFavorite": is_favorite > 0, "Played": played, 
                    "PlayedPercentage": if run_time_ticks > 0 { playback_ticks * 100 / run_time_ticks } else { 0 },
                    "LastPlayedDate": if !last_played_str.is_empty() {
                        if let Ok(ts) = last_played_str.parse::<i64>() { json!(format_iso8601(ts)) } else { json!(null) }
                    } else { json!(null) }
                })
            }
        }))
    }).ok();

    if let Some(item) = item_opt { Json(item).into_response() } else { StatusCode::NOT_FOUND.into_response() }
}

pub async fn emby_similar(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let (genres_str, tags_str): (String, String) = conn.query_row("SELECT genres, tags FROM items WHERE id = ?1", [item_id], |r| Ok((r.get::<_, String>(0).unwrap_or_default(), r.get::<_, String>(1).unwrap_or_default()))).unwrap_or_default();
    let mut terms: Vec<(String, i64)> = Vec::new();
    for t in tags_str.split(" / ").map(str::trim).filter(|s| !s.is_empty()).take(8) { terms.push((t.to_string(), 2)); }
    for g in genres_str.split(" / ").map(str::trim).filter(|s| !s.is_empty()).take(8) { terms.push((g.to_string(), 1)); }

    if terms.is_empty() {
        let mut stmt = conn.prepare("SELECT id, title, type, year, plot, poster_path, rating, runtime FROM items WHERE id != ?1 ORDER BY RANDOM() LIMIT 6").unwrap();
        let items: Vec<Value> = stmt.query_map([item_id], |row| {
            let id: i64 = row.get(0).unwrap_or_default();
            let r_type: String = row.get(2).unwrap_or_default();
            Ok(json!({ "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "Type": if r_type == "movies" { "Movie" } else { "Series" }, "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0), "ImageTags": {"Primary": format!("tag_{}", id)}, "BackdropImageTags": [], "CommunityRating": row.get::<_, f64>(6).unwrap_or(0.0) }))
        }).unwrap().filter_map(Result::ok).collect();
        return Json(json!({ "Items": items, "TotalRecordCount": items.len() }));
    }

    let mut score_expr = String::new(); let mut where_expr = String::new();
    for (i, (_term, weight)) in terms.iter().enumerate() {
        let p = i + 2;
        if i > 0 { score_expr.push_str(" + "); where_expr.push_str(" OR "); }
        score_expr.push_str(&format!("((CASE WHEN tags LIKE ?{} THEN {} ELSE 0 END) + (CASE WHEN genres LIKE ?{} THEN {} ELSE 0 END))", p, weight * 2, p, weight));
        where_expr.push_str(&format!("genres LIKE ?{} OR tags LIKE ?{}", p, p));
    }
    let sql = format!("SELECT id, title, type, year, plot, poster_path, rating, runtime, ({}) AS score FROM items WHERE id != ?1 AND ({}) ORDER BY score DESC, RANDOM() LIMIT 6", score_expr, where_expr);
    let mut params_vec: Vec<rusqlite::types::Value> = Vec::with_capacity(terms.len() + 1);
    params_vec.push(rusqlite::types::Value::Integer(item_id));
    for (term, _) in &terms { params_vec.push(rusqlite::types::Value::Text(format!("%{}%", term))); }
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(_) => return Json(json!({ "Items": [], "TotalRecordCount": 0 })) };
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let r_type: String = row.get(2).unwrap_or_default();
        Ok(json!({ "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "Type": if r_type == "movies" { "Movie" } else { "Series" }, "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0), "ImageTags": {"Primary": format!("tag_{}", id)}, "BackdropImageTags": [], "CommunityRating": row.get::<_, f64>(6).unwrap_or(0.0) }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_person(AxumPath(person_id): AxumPath<String>) -> impl IntoResponse {
    let decoded = decode_url(&person_id); let name = decoded.replace("person_", "");
    Json(json!({ "Name": name, "ServerId": "rust-em-server", "Id": person_id, "Type": "Person", "IsFolder": false, "ImageTags": {}, "BackdropImageTags": [], "UserData": { "PlayCount": 0, "IsFavorite": false, "Played": false } }))
}

pub async fn emby_studio(AxumPath(studio_id): AxumPath<String>) -> impl IntoResponse {
    let decoded = decode_url(&studio_id); let name = decoded.replace("studio_", "");
    Json(json!({ "Name": name, "ServerId": "rust-em-server", "Id": studio_id, "Type": "Studio", "IsFolder": false, "ImageTags": {}, "BackdropImageTags": [], "UserData": { "PlayCount": 0, "IsFavorite": false, "Played": false } }))
}

pub async fn emby_studio_items(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(studio_id): AxumPath<String>, Query(mut query): Query<EmbyItemsQuery>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    query.studio_ids = Some(studio_id); query.studios = None; emby_items_inner(pool, query, req_user_id).await
}

pub async fn emby_genres(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT genres FROM items WHERE genres != ''".to_string();
    if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
        if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })); }
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
    }
    let mut set = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for g_str in mapped.flatten() { for g in g_str.split(" / ") { if !g.trim().is_empty() { set.insert(g.trim().to_string()); } } }
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    let items: Vec<Value> = names.into_iter().map(|name| {
        json!({ "Name": name, "Id": format!("genre_{}", name), "ServerId": "rust-em-server", "Type": "Genre", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": crate::images::get_cover_tag("genre", "genre", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_tags(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT tags FROM items WHERE tags != ''".to_string();
    if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
        if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })); }
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
    }
    let mut set = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) { for t_str in mapped.flatten() { for t in t_str.split(" / ") { if !t.trim().is_empty() { set.insert(t.trim().to_string()); } } } }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    let items: Vec<Value> = names.into_iter().map(|name| {
        json!({ "Name": name, "Id": format!("tag_{}", name), "ServerId": "rust-em-server", "Type": "Tag", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": crate::images::get_cover_tag("tag", "tag", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_years(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT DISTINCT year FROM items WHERE year != ''".to_string();
    if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
        if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })); }
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
    }
    sql.push_str(" ORDER BY year DESC");
    let mut items = Vec::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for y in mapped.flatten() {
                items.push(json!({ "Name": y, "Id": y, "ServerId": "rust-em-server", "Type": "Year", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } }));
            }
        }
    }
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_studios(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT studios FROM items WHERE studios != ''".to_string();
    if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
        if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })); }
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
    }
    let mut set = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) { for st_str in mapped.flatten() { for st in st_str.split(" / ") { if !st.trim().is_empty() { set.insert(st.trim().to_string()); } } } }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    let items: Vec<Value> = names.into_iter().map(|name| {
        json!({ "Name": name, "Id": name, "ServerId": "rust-em-server", "Type": "Studio", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_official_ratings(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT DISTINCT mpaa FROM items WHERE mpaa != ''".to_string();
    if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
        if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })); }
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
    }
    sql.push_str(" ORDER BY mpaa ASC");
    let mut items = Vec::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for m in mapped.flatten() {
                items.push(json!({ "Name": m, "Id": m, "ServerId": "rust-em-server", "Type": "OfficialRating", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0, "UnplayedItemCount": 1 } }));
            }
        }
    }
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_items_filters(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT genres, tags, mpaa, year FROM items WHERE 1=1".to_string();
    let mut params = Vec::new();
    if let Some(libs) = get_allowed_libs(&conn, req_user_id) {
        if libs.is_empty() { return Json(json!({ "Genres": [], "Tags": [], "OfficialRatings": [], "Years": [] })); }
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(" AND library_id = ?"); params.push(lib_id.to_string()); } }
    }
    let mut genres = std::collections::HashSet::new(); let mut tags = std::collections::HashSet::new(); let mut ratings = std::collections::HashSet::new(); let mut years = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
        if let Ok(mapped) = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
            Ok((row.get::<_, String>(0).unwrap_or_default(), row.get::<_, String>(1).unwrap_or_default(), row.get::<_, String>(2).unwrap_or_default(), row.get::<_, String>(3).unwrap_or_default()))
        }) {
            for (g_str, t_str, m_str, y_str) in mapped.flatten() {
                for g in g_str.split(" / ") { if !g.trim().is_empty() { genres.insert(g.trim().to_string()); } }
                for t in t_str.split(" / ") { if !t.trim().is_empty() { tags.insert(t.trim().to_string()); } }
                if !m_str.is_empty() { ratings.insert(m_str); }
                if let Ok(y) = y_str.parse::<i32>() { years.insert(y); }
            }
        }
    }
    let mut genre_items: Vec<Value> = genres.into_iter().map(|g| json!({"Name": g, "Id": g})).collect();
    genre_items.sort_by(|a, b| a["Name"].as_str().unwrap_or("").cmp(b["Name"].as_str().unwrap_or("")));
    let mut tag_items: Vec<Value> = tags.into_iter().map(|t| json!({"Name": t, "Id": t})).collect();
    tag_items.sort_by(|a, b| a["Name"].as_str().unwrap_or("").cmp(b["Name"].as_str().unwrap_or("")));
    let mut rating_items: Vec<Value> = ratings.into_iter().map(|r| json!(r)).collect();
    rating_items.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
    let mut year_list: Vec<i32> = years.into_iter().collect();
    year_list.sort_by(|a, b| b.cmp(a));
    Json(json!({ "Genres": genre_items, "Tags": tag_items, "OfficialRatings": rating_items, "Years": year_list }))
}

pub async fn emby_activity_log() -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }

pub async fn emby_scheduled_tasks() -> impl IntoResponse {
    let is_running = *SCAN_STATUS.get_or_init(|| Mutex::new(false)).lock().unwrap();
    let state_str = if is_running { "Running" } else { "Idle" };
    let last_time = LAST_SCAN_TIME.get_or_init(|| Mutex::new("2024-01-01T00:00:00.0000000Z".to_string())).lock().unwrap().clone();
    
    Json(json!([
        {
            "Name": "Scan Media Library", "State": state_str, "Id": "RefreshLibrary",
            "LastExecutionResult": { "StartTimeUtc": last_time.clone(), "EndTimeUtc": last_time.clone(), "Status": "Completed", "Error": "" },
            "Triggers": [{ "Type": "IntervalTrigger", "IntervalTicks": 864000000000i64 }],
            "Description": "Scans your media library for new and updated files.", "Category": "Library", "IsHidden": false, "Key": "RefreshLibrary"
        }
    ]))
}

pub async fn emby_refresh_library(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let status = SCAN_STATUS.get_or_init(|| Mutex::new(false));
    *status.lock().unwrap() = true;
    println!("📡 [TASK] 客户端触发了计划任务：全库扫描");
    let pool_clone = pool.clone();
    tokio::task::spawn_blocking(move || {
        let libs = { let conn = pool_clone.lock().unwrap(); let mut stmt = match conn.prepare("SELECT id, path, type FROM libraries") { Ok(s) => s, Err(_) => return }; let libs: Vec<(i64, String, String)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().filter_map(Result::ok).collect(); libs }; 
        for (id, path_json, lib_type) in libs {
            if let Ok(paths) = serde_json::from_str::<Vec<String>>(&path_json) {
                for path in paths { crate::scanner::scan_directory(pool_clone.clone(), id, lib_type.clone(), path); }
            }
        }
        let status = SCAN_STATUS.get_or_init(|| Mutex::new(false));
        *status.lock().unwrap() = false;
        let time = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S.0000000Z").to_string();
        *LAST_SCAN_TIME.get_or_init(|| Mutex::new("".to_string())).lock().unwrap() = time;
        println!("✅ [TASK] 计划任务扫描完成");
    });
    StatusCode::NO_CONTENT.into_response()
}

pub async fn emby_mark_played(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let _ = conn.execute(
        "INSERT INTO user_data (user_id, item_id, play_count, playback_position_ticks, last_played_date) VALUES (?1, ?2, 1, 0, ?3) 
         ON CONFLICT(user_id, item_id) DO UPDATE SET play_count = 1, playback_position_ticks = 0, last_played_date = excluded.last_played_date", 
        rusqlite::params![req_user_id, item_id, now.to_string()]
    );
    Json(json!({ "Played": true, "PlayCount": 1, "PlaybackPositionTicks": 0 })).into_response()
}

pub async fn emby_mark_unplayed(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let _ = conn.execute(
        "INSERT INTO user_data (user_id, item_id, play_count, playback_position_ticks) VALUES (?1, ?2, 0, 0) 
         ON CONFLICT(user_id, item_id) DO UPDATE SET play_count = 0, playback_position_ticks = 0", 
        rusqlite::params![req_user_id, item_id]
    );
    Json(json!({ "Played": false, "PlayCount": 0, "PlaybackPositionTicks": 0 })).into_response()
}

pub async fn emby_mark_favorite(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    if item_id.starts_with("boxset_") {
        let _ = conn.execute("INSERT OR IGNORE INTO boxset_favorites (user_id, boxset_id) VALUES (?1, ?2)", rusqlite::params![req_user_id, &item_id]);
    } else if let Ok(_numeric_id) = item_id.parse::<i64>() {
        let _ = conn.execute("INSERT INTO user_data (user_id, item_id, is_favorite) VALUES (?1, ?2, 1) ON CONFLICT(user_id, item_id) DO UPDATE SET is_favorite = 1", rusqlite::params![req_user_id, &item_id]);
    }
    Json(json!({ "IsFavorite": true, "ItemId": item_id })).into_response()
}

pub async fn emby_unmark_favorite(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    if item_id.starts_with("boxset_") {
        let _ = conn.execute("DELETE FROM boxset_favorites WHERE user_id = ?1 AND boxset_id = ?2", rusqlite::params![req_user_id, &item_id]);
    } else if let Ok(_numeric_id) = item_id.parse::<i64>() {
        let _ = conn.execute("INSERT INTO user_data (user_id, item_id, is_favorite) VALUES (?1, ?2, 0) ON CONFLICT(user_id, item_id) DO UPDATE SET is_favorite = 0", rusqlite::params![req_user_id, &item_id]);
    }
    Json(json!({ "IsFavorite": false, "ItemId": item_id })).into_response()
}

pub async fn emby_action() -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_sessions() -> impl IntoResponse { Json(json!([])) }
pub async fn emby_playing() -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_playing_stopped() -> impl IntoResponse { StatusCode::OK.into_response() }

pub async fn emby_playing_progress(State(pool): State<Arc<Mutex<Connection>>>, Extension(req_user_id): Extension<i64>, body: Bytes) -> impl IntoResponse {
    if let Ok(val) = serde_json::from_slice::<Value>(&body) {
        let item_id_str = val["ItemId"].as_str().map(|s| s.to_string()).or_else(|| val["ItemId"].as_i64().map(|i| i.to_string()));
        if let Some(item_id_str) = item_id_str {
            if let Ok(item_id) = item_id_str.parse::<i64>() {
                let ticks = val["PositionTicks"].as_i64().unwrap_or(0);
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                let conn = pool.lock().unwrap();
                
                let _runtime_secs: i32 = conn.query_row("SELECT runtime FROM items WHERE id = ?1", [item_id], |r| r.get(0)).unwrap_or(0);
                
                // 💡 核心修复：绝不自动打勾！只负责记录真实进度
                if ticks > 0 {
                    let _ = conn.execute(
                        "INSERT INTO user_data (user_id, item_id, play_count, playback_position_ticks, last_played_date) VALUES (?1, ?2, 0, ?3, ?4) 
                         ON CONFLICT(user_id, item_id) DO UPDATE SET playback_position_ticks = excluded.playback_position_ticks, last_played_date = excluded.last_played_date", 
                        rusqlite::params![req_user_id, item_id.to_string(), ticks, now.to_string()]
                    );
                }
            }
        }
    }
    StatusCode::OK.into_response()
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ResumeVisibilityQuery { #[serde(rename = "Hide")] pub hide: Option<String> }
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ResumeVisibilityBody { #[serde(rename = "Hide")] pub hide: bool }

pub async fn emby_hide_from_resume(AxumPath((_user_id, _item_id)): AxumPath<(String, String)>, Query(query): Query<ResumeVisibilityQuery>, body: Bytes) -> Response {
    let query_hide = match query.hide.as_deref().map(str::parse::<bool>).transpose() { Ok(value) => value, Err(_) => return StatusCode::BAD_REQUEST.into_response() };
    let body_hide = if body.is_empty() { None } else { match serde_json::from_slice::<ResumeVisibilityBody>(&body) { Ok(body) => Some(body.hide), Err(_) => return StatusCode::BAD_REQUEST.into_response() } };
    let hidden = match (query_hide, body_hide) { (Some(q), Some(b)) if q != b => return StatusCode::BAD_REQUEST.into_response(), (Some(h), _) | (_, Some(h)) => h, (None, None) => return StatusCode::BAD_REQUEST.into_response() };
    Json(json!({ "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "HideFromResume": hidden })).into_response()
}

pub async fn emby_thumbnail_set(AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse { Json(json!({ "Id": item_id.to_string(), "ThumbnailSet": {} })) }

pub async fn emby_item_counts(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let movie_count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE type='movies'", [], |r| r.get(0)).unwrap_or(0);
    let series_count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE type='tvshows' OR type='Series'", [], |r| r.get(0)).unwrap_or(0);
    Json(json!({ "MovieCount": movie_count, "SeriesCount": series_count, "EpisodeCount": 0, "ArtistCount": 0, "SongCount": 0, "AlbumCount": 0, "MusicVideoCount": 0, "BoxSetCount": 0, "BookCount": 0, "TrailerCount": 0 }))
}

#[derive(Deserialize)]
pub struct SearchQuery { #[serde(rename = "SearchTerm")] pub search_term: Option<String>, #[serde(rename = "Limit")] pub limit: Option<i64> }
pub async fn emby_search_hints(State(pool): State<Arc<Mutex<Connection>>>, Query(q): Query<SearchQuery>) -> impl IntoResponse {
    let term = q.search_term.unwrap_or_default();
    if term.is_empty() { return Json(json!({ "SearchHints": [], "TotalRecordCount": 0 })); }
    let limit = q.limit.unwrap_or(20);
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, title, type, year, poster_path FROM items WHERE title LIKE ?1 ORDER BY id DESC LIMIT ?2").unwrap();
    let pattern = format!("%{}%", term);
    let hints: Vec<Value> = stmt.query_map(rusqlite::params![pattern, limit], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let r_type: String = row.get(2).unwrap_or_default();
        let has_poster = !row.get::<_, String>(4).unwrap_or_default().is_empty();
        Ok(json!({ "ItemId": id.to_string(), "Id": id.to_string(), "Name": row.get::<_, String>(1).unwrap_or_default(), "Type": if r_type == "movies" { "Movie" } else { "Series" }, "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0), "ImageTags": if has_poster { json!({"Primary": format!("tag_{}", id)}) } else { json!({}) }, "MatchedTerm": term, "PrimaryImageTag": if has_poster { format!("tag_{}", id) } else { "".to_string() } }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "SearchHints": hints, "TotalRecordCount": hints.len() }))
}

pub async fn emby_danmu(_: AxumPath<String>) -> impl IntoResponse { Json(json!([])) }

pub async fn emby_person_image(_: AxumPath<(String, String)>) -> impl IntoResponse {
    let png_bytes: [u8; 67] = [
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
        0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
        0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
        0x42, 0x60, 0x82
    ];
    (StatusCode::OK, [(CONTENT_TYPE, "image/png")], png_bytes).into_response()
}

pub async fn emby_subtitle_stream(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, stream_index)): AxumPath<(String, String)>) -> Response {
    serve_subtitle_inner(pool, item_id, stream_index).await
}

pub async fn emby_subtitle_stream_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, stream_index, _ext)): AxumPath<(String, String, String)>) -> Response {
    serve_subtitle_inner(pool, item_id, stream_index).await
}

pub async fn emby_subtitle_stream_with_source(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id, stream_index)): AxumPath<(String, String, String)>) -> Response {
    serve_subtitle_inner(pool, item_id, stream_index).await
}

pub async fn emby_subtitle_stream_with_source_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id, stream_index, _ext)): AxumPath<(String, String, String, String)>) -> Response {
    serve_subtitle_inner(pool, item_id, stream_index).await
}

async fn serve_subtitle_inner(pool: Arc<Mutex<Connection>>, item_id: String, stream_index: String) -> Response {
    let idx = stream_index.parse::<usize>().unwrap_or(0);
    let path_opt = {
        let conn = pool.lock().unwrap();
        if let Ok(id) = item_id.parse::<i64>() {
            let ms_json: String = conn.query_row("SELECT media_streams FROM items WHERE id = ?1", [id], |r| r.get(0)).unwrap_or_else(|_| "[]".to_string());
            let ms: Value = serde_json::from_str(&ms_json).unwrap_or(json!([]));
            ms.as_array().and_then(|arr| arr.get(idx)).and_then(|stream| stream.get("Path")).and_then(|p| p.as_str()).map(|s| s.to_string())
        } else { None }
    };

    if let Some(path) = path_opt {
        if let Ok(meta) = tokio::fs::metadata(&path).await {
            if meta.len() > 10 * 1024 * 1024 { return StatusCode::PAYLOAD_TOO_LARGE.into_response(); }
        }
        if let Ok(bytes) = tokio::fs::read(&path).await {
            let mime = if path.ends_with(".vtt") { "text/vtt" } else if path.ends_with(".srt") { "application/x-subrip" } else { "text/plain; charset=utf-8" };
            return (StatusCode::OK, [(CONTENT_TYPE, mime)], bytes).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

pub async fn emby_display_preferences(_: AxumPath<String>) -> impl IntoResponse {
    Json(json!({"Id": "default", "ViewType": "Poster", "SortBy": "SortName", "PrimaryImageHeight": 250, "PrimaryImageWidth": 250, "ScrollDirection": "Horizontal"}))
}

pub async fn emby_item_image_user_only(_: AxumPath<(String, String)>) -> Response { StatusCode::NOT_FOUND.into_response() }
