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

use crate::utils::{decode_url, to_hex, from_hex, format_iso8601, sanitize_media_streams};
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
    tokio::spawn(async move {
        crate::covers::generate_all_covers(pool_clone).await;
    });
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
    } else {
        return (StatusCode::FORBIDDEN, "不能修改管理员").into_response();
    }
    
    let user_id: i64 = conn.query_row("SELECT id FROM users WHERE username = ?1", [&payload.username], |r| r.get(0)).unwrap_or(0);
    
    if user_id > 1 {
        let _ = conn.execute("DELETE FROM user_library_access WHERE user_id = ?1", [user_id]);
        for lib_id in payload.libraries {
            let _ = conn.execute("INSERT INTO user_library_access (user_id, library_id) VALUES (?1, ?2)", [user_id, lib_id]);
        }
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
        (StatusCode::OK, Json(json!({
            "User": {
                "Id": id.to_string(),
                "Name": name,
                "ServerId": "rust-em-server",
                "HasPassword": true,
                "Policy": { "IsAdministrator": id == 1, "EnableAllFolders": id == 1 }
            },
            "SessionInfo": { "Id": uuid::Uuid::new_v4().to_string(), "UserId": id.to_string() },
            "AccessToken": token,
            "ServerId": "rust-em-server"
        }))).into_response()
    } else { (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" }))).into_response() }
}

pub async fn emby_views(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    
    let mut sql = "SELECT id, name, type FROM libraries".to_string();
    if req_user_id != 1 {
        sql.push_str(&format!(" WHERE id IN (SELECT library_id FROM user_library_access WHERE user_id = {})", req_user_id));
    }
    
    let mut stmt = conn.prepare(&sql).unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default(); let name: String = row.get(1).unwrap_or_default(); let lib_type: String = row.get(2).unwrap_or_default();
        Ok(json!({ "Name": name, "Id": format!("lib_{}", id), "ServerId": "rust-em-server", "Type": "CollectionFolder", "IsFolder": true, "MediaType": "Video", "CollectionType": if lib_type == "movies" { "movies" } else { "tvshows" }, "ImageTags": {"Primary": crate::images::get_cover_tag("library", "lib", &id.to_string())}, "UserData": {
                "PlaybackPositionTicks": 0,
                "PlayCount": 0,
                "IsFavorite": false,
                "Played": false,
                "PlayedPercentage": 0.0,
                "UnplayedItemCount": 1
            }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    
    items.extend(search_person_items);
    let final_total = if is_search { if search_term_len < 4 { 0 } else { items.len() as i64 } } else { total };
    Json(json!({ "Items": items, "TotalRecordCount": final_total })).into_response()
}

pub async fn emby_shows_seasons(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(series_id): AxumPath<String>) -> impl IntoResponse {
    let Ok(sid) = series_id.parse::<i64>() else { return Json(json!({"Items": [], "TotalRecordCount": 0})); };
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, title, season_number, poster_path, (SELECT COUNT(*) FROM items e WHERE e.parent_id = items.id AND e.type = 'Episode') as ep_count FROM items WHERE type = 'Season' AND parent_id = ?1 ORDER BY season_number ASC").unwrap();
    
    let items: Vec<Value> = stmt.query_map([sid], |r| {
        let id: i64 = r.get(0).unwrap_or_default();
        let title: String = r.get(1).unwrap_or_default();
        let num: i32 = r.get(2).unwrap_or(1);
        let poster: String = r.get(3).unwrap_or_default();
        let ep_count: i64 = r.get(4).unwrap_or(0);
        
        let mut image_tags = serde_json::Map::new();
        if !poster.is_empty() { image_tags.insert("Primary".to_string(), json!(format!("season_{}", id))); }

        Ok(json!({
            "Name": title, "Id": id.to_string(), "ServerId": "rust-em-server", 
            "Type": "Season", "IsFolder": true, "MediaType": "Video",
            "IndexNumber": num, "SeriesId": series_id,
            "ChildCount": ep_count, "RecursiveItemCount": null,
            "ChildCount": null, 
            "SeasonCount": if typ == "Series" { total_seasons } else { 0 },
            "SeriesStudio": if typ == "Series" { json!({ "Name": studios_str.split(" / ").next().unwrap_or(""), "Id": "" }) } else { json!(null) },
            "ExternalUrls": build_external_urls(&tmdb_id, &imdb_id, &typ),
            "ProbeEngine": "rust-native", "LocationType": "FileSystem", "CanDelete": false, "CanDownload": false, "CanDeleteFromFolders": [], "HideMissingActorImages": true,
            "UserData": { 
                "ItemId": id.to_string(), "Key": id.to_string(), 
                "PlaybackPositionTicks": playback_ticks, "PlayCount": play_count, 
                "IsFavorite": is_favorite > 0, "Played": played, 
                "PlayedPercentage": if run_time_ticks > 0 { playback_ticks * 100 / run_time_ticks } else { 0 },
                "LastPlayedDate": if !last_played_str.is_empty() {
                    if let Ok(ts) = last_played_str.parse::<i64>() { json!(format_iso8601(ts)) } else { json!(null) }
                } else { json!(null) },
                "UnplayedItemCount": unplayed
            }
        }))
    }).ok();

    if let Some(item) = item_opt {
        Json(item).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

pub async fn emby_similar(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let (genres_str, tags_str): (String, String) = conn.query_row("SELECT genres, tags FROM items WHERE id = ?1", [item_id], |r| Ok((r.get::<_, String>(0).unwrap_or_default(), r.get::<_, String>(1).unwrap_or_default()))).unwrap_or_default();
    let mut terms: Vec<(String, i64)> = Vec::new();
    for t in tags_str.split(" / ").map(str::trim).filter(|s| !s.is_empty()).take(8) { terms.push((t.to_string(), 2)); }
    for g in genres_str.split(" / ").map(str::trim).filter(|s| !s.is_empty()).take(8) { terms.push((g.to_string(), 1)); }

    // 通用的行数据解析闭包
    let parse_row = |row: &rusqlite::Row| -> rusqlite::Result<Value> {
        let id: i64 = row.get(0).unwrap_or_default();
        let r_type: String = row.get(2).unwrap_or_default();
        let poster_path: String = row.get(5).unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
        let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        let poster_tag = if has_poster { format!("tag_{}_{}", id, &hash_hex[..8]) } else { String::new() };
        
        let runtime_secs: i32 = row.get("runtime").unwrap_or(0);
        let total_ticks = (runtime_secs as i64) * 10_000_000;
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if r_type == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(),
            "CommunityRating": row.get::<_, f64>("rating").unwrap_or(0.0), "RunTimeTicks": total_ticks,
            "PrimaryImageAspectRatio": 0.6666666666666666,
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": [],
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks }],
            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
        }))
    };

    if terms.is_empty() {
        let mut stmt = conn.prepare("SELECT id, title, type, year, plot, poster_path, rating, runtime FROM items WHERE id != ?1 ORDER BY RANDOM() LIMIT 6").unwrap();
        let items: Vec<Value> = stmt.query_map([item_id], |row| parse_row(row)).unwrap().filter_map(Result::ok).collect();
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
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(params_vec.iter()), |row| parse_row(row)).unwrap().filter_map(Result::ok).collect();
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
        json!({ "Name": name, "Id": format!("genre_{}", name), "ServerId": "rust-em-server", "Type": "Genre", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": crate::images::get_cover_tag("genre", "genre", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
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
        json!({ "Name": name, "Id": format!("tag_{}", name), "ServerId": "rust-em-server", "Type": "Tag", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": crate::images::get_cover_tag("tag", "tag", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
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
                items.push(json!({ "Name": y, "Id": y, "ServerId": "rust-em-server", "Type": "Year", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } }));
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
        json!({ "Name": name, "Id": name, "ServerId": "rust-em-server", "Type": "Studio", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
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
                items.push(json!({ "Name": m, "Id": m, "ServerId": "rust-em-server", "Type": "OfficialRating", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } }));
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
        let libs = {
            let conn = pool_clone.lock().unwrap();
            let mut stmt = match conn.prepare("SELECT id, path, type FROM libraries") { Ok(s) => s, Err(_) => return };
            let libs: Vec<(i64, String, String)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().filter_map(Result::ok).collect();
            libs
        }; 
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
                
                let mut runtime_secs: i32 = conn.query_row("SELECT runtime FROM items WHERE id = ?1", [item_id], |r| r.get(0)).unwrap_or(0);
                let _current_ticks: i64 = conn.query_row("SELECT playback_position_ticks FROM user_data WHERE user_id = ?1 AND item_id = ?2", rusqlite::params![req_user_id, item_id.to_string()], |r| r.get(0)).unwrap_or(0);
                
                if let Some(client_ticks) = val["RunTimeTicks"].as_i64() {
                    if client_ticks > 0 {
                        runtime_secs = (client_ticks / 10_000_000) as i32;
                        let _ = conn.execute("UPDATE items SET runtime = ?1 WHERE id = ?2", rusqlite::params![runtime_secs, item_id]);
                    }
                }
                
                if runtime_secs <= 0 && ticks > 0 {
                    runtime_secs = (ticks / 10_000_000) as i32 + 1800;
                    let _ = conn.execute("UPDATE items SET runtime = ?1 WHERE id = ?2", rusqlite::params![runtime_secs, item_id]);
                    
                    let pool_clone = pool.clone();
                    tokio::spawn(async move {
                        let (strm_path_opt, _) = {
                            let conn = pool_clone.lock().unwrap();
                            let mut stmt = match conn.prepare("SELECT file_path FROM items WHERE id = ?1") { Ok(s) => s, Err(_) => return };
                            let path: String = stmt.query_row([item_id], |r| r.get(0)).unwrap_or_default();
                            (path, ())
                        };
                        if !strm_path_opt.is_empty() {
                            let strm_path = std::path::PathBuf::from(strm_path_opt);
                            let limiter = crate::probe::ProbeLimiter::new(1000);
                            let client = reqwest::Client::builder().timeout(Duration::from_secs(40)).build().unwrap();
                            let _ = crate::probe::probe_strm(&strm_path, limiter, &client).await;
                        }
                    });
                }
                
                let total_ticks = (runtime_secs as i64) * 10_000_000;
                if total_ticks > 0 && ticks > (total_ticks * 90 / 100) {
                    let _ = conn.execute("INSERT INTO user_data (user_id, item_id, play_count, playback_position_ticks, last_played_date) VALUES (?1, ?2, 1, 0, ?3) ON CONFLICT(user_id, item_id) DO UPDATE SET play_count = play_count + 1, playback_position_ticks = 0, last_played_date = excluded.last_played_date", rusqlite::params![req_user_id, item_id.to_string(), now.to_string()]);
                } else if ticks > 0 {
                    let _ = conn.execute("INSERT INTO user_data (user_id, item_id, playback_position_ticks, last_played_date) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(user_id, item_id) DO UPDATE SET playback_position_ticks = excluded.playback_position_ticks, last_played_date = excluded.last_played_date", rusqlite::params![req_user_id, item_id.to_string(), ticks, now.to_string()]);
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

pub async fn emby_subtitle_stream_with_source(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id, stream_index)): AxumPath<(String, String, String)>) -> Response {
    serve_subtitle_inner(pool, item_id, stream_index).await
}

pub async fn emby_subtitle_stream_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, stream_index, _ext)): AxumPath<(String, String, String)>) -> Response {
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
        // 💡 加上文件大小限制，防止恶意超大文件打爆内存 (限制 10MB)
        if let Ok(meta) = tokio::fs::metadata(&path).await {
            if meta.len() > 10 * 1024 * 1024 {
                return StatusCode::PAYLOAD_TOO_LARGE.into_response();
            }
        }

        // 原样读取，不进行格式转换
        if let Ok(bytes) = tokio::fs::read(&path).await {
            let mime = if path.ends_with(".vtt") { "text/vtt" } 
                       else if path.ends_with(".srt") { "application/x-subrip" } 
                       else { "text/plain; charset=utf-8" };
            return (StatusCode::OK, [(CONTENT_TYPE, mime)], bytes).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

pub async fn emby_display_preferences(_: AxumPath<String>) -> impl IntoResponse {
    Json(json!({"Id": "default", "ViewType": "Poster", "SortBy": "SortName", "PrimaryImageHeight": 250, "PrimaryImageWidth": 250, "ScrollDirection": "Horizontal"}))
}

pub async fn emby_item_image_user_only(_: AxumPath<(String, String)>) -> Response { StatusCode::NOT_FOUND.into_response() }
