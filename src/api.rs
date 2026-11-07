use axum::{
    extract::{State, Request, Json, Query, Path as AxumPath},
    http::{StatusCode, header::{LOCATION, USER_AGENT, CONTENT_TYPE}, HeaderMap, HeaderValue, Method},
    middleware::Next, response::{IntoResponse, Response}, body::Bytes,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::collections::HashMap;
use std::time::{Duration, Instant, UNIX_EPOCH};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

static TOKENS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static STRM_CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
const MAX_STRM_SIZE: u64 = 16 * 1024;
const NORMALIZED_CREDENTIAL_HEADER: &str = "x-lux-internal-emby-credential";

pub async fn logger_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();
    if path.ends_with(".css") || path.ends_with(".js") || path.ends_with(".png") || path.ends_with(".jpg") {
        return next.run(req).await;
    }
    let ua = req.headers().get("User-Agent").and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    let query = uri.query().unwrap_or("");
    println!("📥 [REQ] {} {}?{} | UA: {}", method, path, query, ua);
    let res = next.run(req).await;
    let status = res.status();
    if status.is_success() || status.is_redirection() { 
        println!("📤 [RES] {} {} | 状态: {}", method, path, status); 
    } else { 
        println!("❌ [RES] {} {} | 状态: {} (异常)", method, path, status); 
    }
    res
}

pub async fn auth(req: Request, next: Next) -> Result<Response, StatusCode> {
    if req.method() == Method::OPTIONS { return Ok(next.run(req).await); }
    let path = req.uri().path();
    
    if !path.starts_with("/api") && !path.starts_with("/emby") && !path.starts_with("/Items") && !path.starts_with("/Users") && !path.starts_with("/Videos") && !path.starts_with("/System") && !path.starts_with("/Auth") && !path.starts_with("/auth") && !path.starts_with("/Sessions") && !path.starts_with("/DisplayPreferences") {
        return Ok(next.run(req).await);
    }
    if path.contains("/Images/") || path.ends_with("/Ping") || path.ends_with("/Info/Public") || path.ends_with("/Users/Public") || path.ends_with("/AuthenticateByName") || path == "/api/login" {
        return Ok(next.run(req).await);
    }
    
    let mut token_opt = None;
    if let Some(val) = req.headers().get("Authorization").and_then(|v| v.to_str().ok()) {
        if let Some(t) = val.strip_prefix("Bearer ") {
            token_opt = Some(t.trim().to_string());
        }
    }
    if token_opt.is_none() {
        if let Some(val) = req.headers().get("X-Emby-Token").and_then(|v| v.to_str().ok()) {
            token_opt = Some(val.to_string());
        }
    }
    if token_opt.is_none() {
        if let Some(val) = req.headers().get(NORMALIZED_CREDENTIAL_HEADER) {
            if let Ok(t) = val.to_str() { token_opt = Some(t.to_string()); }
        }
    }
    if token_opt.is_none() {
        if let Some(query) = req.uri().query() {
            for pair in query.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    let k_lower = k.to_lowercase();
                    if k_lower == "token" || k_lower == "api_key" || k_lower == "apikey" || k_lower == "x-emby-token" {
                        token_opt = Some(v.to_string());
                        break;
                    }
                }
            }
        }
    }

    if let Some(token) = token_opt {
        if let Some(tokens) = TOKENS.get() {
            if tokens.lock().unwrap().contains(&token) {
                return Ok(next.run(req).await);
            }
        }
    }
    
    Err(StatusCode::UNAUTHORIZED)
}

#[derive(Deserialize)]
pub struct LoginReq { username: String, password: String }

pub async fn login(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<LoginReq>) -> impl IntoResponse {
    let mut hasher = Sha256::new(); hasher.update(&payload.password);
    let conn = pool.lock().unwrap();
    if conn.prepare("SELECT id FROM users WHERE username = ?1 AND password = ?2").unwrap().exists([&payload.username, &format!("{:x}", hasher.finalize())]).unwrap() {
        let token = uuid::Uuid::new_v4().to_string();
        TOKENS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap().push(token.clone());
        (StatusCode::OK, Json(json!({ "token": token })))
    } else { (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" }))) }
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
                let path = entry.path();
                let name = entry.file_name().to_string_lossy().to_string();
                let path_str = path.to_string_lossy().to_string();
                if path.is_dir() { dirs.push(json!({ "name": name, "path": path_str })); }
                else if path.is_file() { files.push(json!({ "name": name, "path": path_str })); }
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
        let lib_id = {
            let conn = pool_clone.lock().unwrap();
            conn.execute("INSERT INTO libraries (name, path, type) VALUES (?1, ?2, ?3)", [&payload.name, &serde_json::to_string(&payload.paths).unwrap(), &payload.lib_type]).unwrap();
            conn.last_insert_rowid()
        };
        for path_str in payload.paths { scan_directory(pool_clone.clone(), lib_id, payload.lib_type.clone(), path_str); }
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
            for path_str in paths { scan_directory(pool_clone.clone(), id, lib_type.clone(), path_str); }
        }
    }).await.unwrap();
    Json(json!({"status": "scanning"}))
}

fn get_image_type(stem: &str) -> Option<&'static str> {
    let lower = stem.to_lowercase();
    let base = if let Some((b, suffix)) = lower.rsplit_once('-') { if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) { b } else { &lower } } else { &lower };
    match base { "poster" => Some("poster"), "fanart" | "backdrop" => Some("fanart"), "logo" | "clearlogo" => Some("logo"), "thumb" | "thumbnail" => Some("thumb"), "banner" => Some("banner"), _ => None }
}

fn get_base_stem(stem: &str) -> String {
    let lower = stem.to_lowercase();
    for p in &["-cd", "-disc", "-part", "-pt"] {
        if let Some(idx) = lower.rfind(p) {
            let suffix = &lower[idx + p.len()..];
            if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) { return stem[..idx].to_string(); }
        }
    }
    stem.to_string()
}

fn scan_directory(pool: Arc<Mutex<Connection>>, lib_id: i64, lib_type: String, dir_path: String) {
    println!("🔍 开始扫描目录: {:?}", dir_path);
    let mut count = 0;
    let mut strm_files = Vec::new();
    for entry in walkdir::WalkDir::new(&dir_path).into_iter().filter_map(|e| e.ok()) {
        if entry.path().is_file() && entry.path().extension().and_then(|e| e.to_str()).map_or(false, |e| e.eq_ignore_ascii_case("strm")) { strm_files.push(entry.path().to_path_buf()); }
    }
    let mut grouped: HashMap<(PathBuf, String), Vec<PathBuf>> = HashMap::new();
    for path in strm_files {
        let stem = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
        let base_stem = get_base_stem(&stem);
        let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
        grouped.entry((parent, base_stem)).or_default().push(path);
    }
    for ((parent, base_stem), mut paths) in grouped {
        count += 1;
        paths.sort();
        let first_path = &paths[0];
        let metadata = match std::fs::metadata(first_path) { Ok(m) => m, Err(_) => continue };
        let size: i64 = paths.iter().filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len() as i64)).sum();
        let mtime = metadata.modified().unwrap_or(UNIX_EPOCH).duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        let virtual_path = parent.join(format!("{}.strm", base_stem)).to_string_lossy().to_string();
        let mut combined_content = String::new();
        for p in &paths { if let Ok(c) = std::fs::read_to_string(p) { combined_content.push_str(c.trim()); combined_content.push('\n'); } }
        let strm_content = combined_content.trim().to_string();
        let parts_json = serde_json::to_string(&paths.iter().map(|p| p.to_string_lossy().to_string()).collect::<Vec<_>>()).unwrap_or_default();

        let nfo_candidates = vec![parent.join(format!("{}.nfo", base_stem)), parent.join("movie.nfo")];
        let mut title = base_stem.clone(); let mut plot = String::new(); let mut year = String::new();
        let mut rating = 0.0; let mut mpaa = String::new(); let mut genres = String::new();
        let mut directors = String::new(); let mut actors_json = String::new();
        let mut runtime = 0; let mut premiered = String::new(); let mut tagline = String::new();
        let mut studios = String::new(); let mut tmdb_id = String::new(); let mut imdb_id = String::new();
        let mut series = String::new(); let mut label = String::new(); let mut set_name = String::new();
        let mut num = String::new(); let mut tags = String::new();
        let mut nfo_poster = String::new(); let mut nfo_fanart = String::new();

        for nfo in nfo_candidates {
            if nfo.exists() {
                if let Some(meta) = crate::nfo::read_nfo(&nfo) {
                    if let Some(t) = &meta.title { title = t.clone(); }
                    if let Some(p) = &meta.plot { plot = p.clone(); }
                    if let Some(r) = meta.rating { rating = r; }
                    if let Some(m) = &meta.mpaa { mpaa = m.clone(); }
                    if let Some(rt) = meta.runtime { runtime = rt; }
                    if let Some(tg) = &meta.tagline { tagline = tg.clone(); }
                    if let Some(s) = &meta.series { series = s.clone(); }
                    if let Some(l) = &meta.label { label = l.clone(); }
                    if let Some(set) = &meta.set { set_name = set.clone(); }
                    if let Some(n) = &meta.num { num = n.clone(); }
                    if let Some(pr) = &meta.premiered { premiered = pr.clone(); }
                    if let Some(y) = &meta.year { year = y.clone(); } 
                    else if let Some(pr) = &meta.premiered { if pr.len() >= 4 { year = pr[0..4].to_string(); } } 
                    else if let Some(rd) = &meta.releasedate { if rd.len() >= 4 { year = rd[0..4].to_string(); } }
                    if !meta.genres.is_empty() { genres = meta.genres.join(" / "); }
                    if !meta.tags.is_empty() { tags = meta.tags.join(" / "); }
                    if !meta.directors.is_empty() { directors = meta.directors.join(" / "); }
                    if !meta.studios.is_empty() { studios = meta.studios.join(" / "); }
                    let actors_arr: Vec<Value> = meta.actors.into_iter().filter(|a| a.name.is_some()).map(|a| json!({ "name": a.name.unwrap_or_default(), "role": a.role.unwrap_or_default(), "thumb": a.thumb.unwrap_or_default() })).collect();
                    actors_json = serde_json::to_string(&actors_arr).unwrap_or_default();
                    for uid in meta.unique_ids { if uid.0 == "tmdb" { tmdb_id = uid.1.clone(); } if uid.0 == "imdb" { imdb_id = uid.1.clone(); } }
                    let resolve_path = |p: &str| -> String { if p.is_empty() { return String::new(); } if p.starts_with("http") || p.starts_with("/") { return p.to_string(); } parent.join(p).to_string_lossy().to_string() };
                    if let Some(poster) = &meta.poster { nfo_poster = resolve_path(poster); }
                    if let Some(fanart) = &meta.fanart { nfo_fanart = resolve_path(fanart); }
                    break;
                }
            }
        }
        let mut final_poster = nfo_poster; let mut final_fanart = nfo_fanart;
        let mut logo = String::new(); let mut thumb = String::new(); let mut banner = String::new();
        if let Ok(entries) = std::fs::read_dir(&parent) {
            for entry in entries.flatten() {
                let p = entry.path();
                if !p.is_file() { continue; }
                if let (Some(ext), Some(file_stem)) = (p.extension().and_then(|e| e.to_str()), p.file_stem().and_then(|s| s.to_str())) {
                    let ext = ext.to_lowercase();
                    if !["jpg", "jpeg", "png", "webp"].contains(&ext.as_str()) { continue; }
                    let is_match_media = file_stem.starts_with(&format!("{}-", base_stem));
                    let test_stem = if is_match_media { file_stem.trim_start_matches(&format!("{}-", base_stem)) } else { file_stem };
                    if let Some(img_type) = get_image_type(test_stem) {
                        let p_str = p.to_string_lossy().to_string();
                        match img_type { "poster" => final_poster = p_str, "fanart" => final_fanart = p_str, "logo" => if logo.is_empty() { logo = p_str }, "thumb" => if thumb.is_empty() { thumb = p_str }, "banner" => if banner.is_empty() { banner = p_str }, _ => {} }
                    }
                }
            }
        }
        let mut stills = Vec::new();
        let extrafanart_dir = parent.join("extrafanart");
        if extrafanart_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&extrafanart_dir) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let p = entry.path();
                    if p.is_file() {
                        if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                            if ["jpg", "jpeg", "png", "webp"].contains(&ext.to_lowercase().as_str()) { stills.push(p.to_string_lossy().to_string()); }
                        }
                    }
                }
            }
        }
        stills.sort();
        let stills_json = serde_json::to_string(&stills).unwrap_or_default();
        {
            let conn = match pool.lock() { Ok(c) => c, Err(_) => continue };
            let _ = conn.execute(
                "INSERT INTO items (library_id, title, num, file_path, type, plot, year, poster_path, fanart_path, logo_path, thumb_path, banner_path, size, mtime, rating, mpaa, genres, tags, directors, actors, runtime, premiered, tagline, studios, tmdb_id, imdb_id, series, label, set_name, strm_content, stills, parts) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32)
                 ON CONFLICT(file_path) DO UPDATE SET title=excluded.title, num=excluded.num, size=excluded.size, mtime=excluded.mtime, plot=excluded.plot, year=excluded.year, poster_path=excluded.poster_path, fanart_path=excluded.fanart_path, logo_path=excluded.logo_path, thumb_path=excluded.thumb_path, banner_path=excluded.banner_path, rating=excluded.rating, mpaa=excluded.mpaa, genres=excluded.genres, tags=excluded.tags, directors=excluded.directors, actors=excluded.actors, runtime=excluded.runtime, premiered=excluded.premiered, tagline=excluded.tagline, studios=excluded.studios, tmdb_id=excluded.tmdb_id, imdb_id=excluded.imdb_id, series=excluded.series, label=excluded.label, set_name=excluded.set_name, strm_content=excluded.strm_content, stills=excluded.stills, parts=excluded.parts",
                rusqlite::params![lib_id, title, num, virtual_path, lib_type, plot, year, final_poster, final_fanart, logo, thumb, banner, size, mtime, rating, mpaa, genres, tags, directors, actors_json, runtime, premiered, tagline, studios, tmdb_id, imdb_id, series, label, set_name, strm_content, stills_json, parts_json]
            );
        }
    }
    println!("✅ 扫描完成。共处理 {} 个媒体项。", count);
}

#[derive(Deserialize)]
pub struct ItemQuery { library_id: Option<i64>, lib_type: Option<String> }

pub async fn get_items(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<ItemQuery>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, title, num, type, year, plot, poster_path, rating, mpaa, genres, tags, directors, actors, runtime, premiered, tagline, studios, tmdb_id, imdb_id, size, series, label, set_name, file_path, strm_content, stills, parts FROM items WHERE 1=1".to_string();
    if let Some(lid) = query.library_id { sql.push_str(&format!(" AND library_id = {}", lid)); }
    if let Some(lt) = query.lib_type { sql.push_str(&format!(" AND type = '{}'", lt)); }
    sql.push_str(" ORDER BY title ASC");
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("SQL Prepare Error: {}", e)).into_response() };
    let items: Vec<Value> = stmt.query_map([], |row| {
        let actors_json: String = row.get(12).unwrap_or_else(|_| "[]".to_string());
        let actors: Value = serde_json::from_str(&actors_json).unwrap_or(json!([]));
        let stills_json: String = row.get(25).unwrap_or_else(|_| "[]".to_string());
        let stills: Value = serde_json::from_str(&stills_json).unwrap_or(json!([]));
        let parts_json: String = row.get(26).unwrap_or_else(|_| "[]".to_string());
        let parts: Value = serde_json::from_str(&parts_json).unwrap_or(json!([]));
        Ok(json!({
            "id": row.get::<_, i64>(0).unwrap_or_default(), "title": row.get::<_, String>(1).unwrap_or_default(),
            "num": row.get::<_, String>(2).unwrap_or_default(), "type": row.get::<_, String>(3).unwrap_or_default(),
            "year": row.get::<_, String>(4).unwrap_or_default(), "plot": row.get::<_, String>(5).unwrap_or_default(),
            "has_poster": !row.get::<_, String>(6).unwrap_or_default().is_empty(), "rating": row.get::<_, f64>(7).unwrap_or(0.0),
            "mpaa": row.get::<_, String>(8).unwrap_or_default(), "genres": row.get::<_, String>(9).unwrap_or_default(),
            "tags": row.get::<_, String>(10).unwrap_or_default(), "directors": row.get::<_, String>(11).unwrap_or_default(),
            "actors": actors, "runtime": row.get::<_, i32>(13).unwrap_or(0),
            "premiered": row.get::<_, String>(14).unwrap_or_default(), "tagline": row.get::<_, String>(15).unwrap_or_default(),
            "studios": row.get::<_, String>(16).unwrap_or_default(), "tmdb_id": row.get::<_, String>(17).unwrap_or_default(),
            "imdb_id": row.get::<_, String>(18).unwrap_or_default(), "size": row.get::<_, i64>(19).unwrap_or(0),
            "series": row.get::<_, String>(20).unwrap_or_default(), "label": row.get::<_, String>(21).unwrap_or_default(),
            "set_name": row.get::<_, String>(22).unwrap_or_default(), "file_path": row.get::<_, String>(23).unwrap_or_default(),
            "strm_content": row.get::<_, String>(24).unwrap_or_default(), "stills": stills, "parts": parts
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
    if let Some(path_str) = path_opt { if !path_str.is_empty() { return serve_file(&path_str).await; } }
    StatusCode::NOT_FOUND.into_response()
}

async fn serve_file(path_str: &str) -> Response {
    if path_str.starts_with("http") { return (StatusCode::FOUND, [(LOCATION, path_str.to_string())], "").into_response(); }
    if let Ok(bytes) = tokio::fs::read(&path_str).await {
        let mime = if path_str.to_lowercase().ends_with(".png") { "image/png" } else { "image/jpeg" };
        return (StatusCode::OK, [(CONTENT_TYPE, mime)], bytes).into_response();
    }
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Deserialize)]
pub struct UpdateUserReq { old_password: String, new_username: String, new_password: String }

pub async fn update_user(State(pool): State<Arc<Mutex<Connection>>>, Json(payload): Json<UpdateUserReq>) -> impl IntoResponse {
    let mut hasher_old = Sha256::new(); hasher_old.update(&payload.old_password);
    let mut hasher_new = Sha256::new(); hasher_new.update(&payload.new_password);
    let conn = pool.lock().unwrap();
    if conn.prepare("SELECT id FROM users WHERE username = 'admin' AND password = ?1").unwrap().exists([&format!("{:x}", hasher_old.finalize())]).unwrap() {
        conn.execute("UPDATE users SET username = ?1, password = ?2 WHERE username = 'admin'", [&payload.new_username, &format!("{:x}", hasher_new.finalize())]).unwrap();
        if let Some(tokens) = TOKENS.get() { tokens.lock().unwrap().clear(); }
        (StatusCode::OK, Json(json!({"status": "success"})))
    } else { (StatusCode::BAD_REQUEST, Json(json!({"error": "旧密码错误"}))) }
}

// ======= Emby API =======
pub async fn emby_public_info() -> impl IntoResponse { Json(json!({ "ServerName": "Rust-Em", "Version": "4.8.0.0", "Id": "rust-em-server", "OperatingSystem": "Linux", "StartupWizardCompleted": true })) }
pub async fn emby_info() -> impl IntoResponse { Json(json!({ "ServerName": "Rust-Em", "Version": "4.8.0.0", "Id": "rust-em-server", "OperatingSystem": "Linux", "OperatingSystemDisplayName": "Linux", "HasPendingRestart": false, "IsShuttingDown": false, "SupportsLibraryMonitor": false, "SupportsHttps": true, "HttpServerPortNumber": 8096 })) }
pub async fn emby_ping() -> impl IntoResponse { "Emby Server" }
pub async fn emby_public_users() -> impl IntoResponse { Json(json!([])) }
pub async fn emby_auth_keys() -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }
pub async fn emby_display_preferences() -> impl IntoResponse { Json(json!({"Id": "default", "ViewType": "Poster", "SortBy": "SortName", "PrimaryImageHeight": 250, "PrimaryImageWidth": 250, "ScrollDirection": "Horizontal"})) }
pub async fn emby_user() -> impl IntoResponse { Json(json!({"Id": "1", "Name": "admin", "ServerId": "rust-em-server", "HasPassword": true, "Policy": {"IsAdministrator": true, "EnableAllFolders": true}})) }

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
        TOKENS.get_or_init(|| Mutex::new(Vec::new())).lock().unwrap().push(token.clone());
        (StatusCode::OK, Json(json!({ "User": { "Id": id.to_string(), "Name": name, "ServerId": "rust-em-server", "HasPassword": true, "Policy": { "IsAdministrator": true, "EnableAllFolders": true } }, "SessionInfo": { "Id": uuid::Uuid::new_v4().to_string(), "UserId": id.to_string() }, "AccessToken": token, "ServerId": "rust-em-server" }))).into_response()
    } else { (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid credentials" }))).into_response() }
}

pub async fn emby_views(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, name, type FROM libraries").unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default(); let name: String = row.get(1).unwrap_or_default(); let lib_type: String = row.get(2).unwrap_or_default();
        Ok(json!({ "Name": name, "Id": format!("lib_{}", id), "ServerId": "rust-em-server", "Type": "CollectionFolder", "IsFolder": true, "MediaType": "Video", "CollectionType": if lib_type == "movies" { "movies" } else { "tvshows" }, "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false } }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct EmbyItemsQuery {
    #[serde(rename = "ParentId")] parent_id: Option<String>,
    #[serde(rename = "StartIndex")] start_index: Option<i64>,
    #[serde(rename = "Limit")] limit: Option<i64>,
}

pub async fn emby_items_latest(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<EmbyItemsQuery>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, title, type, year, plot, poster_path FROM items WHERE 1=1".to_string();
    if let Some(pid) = &query.parent_id {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(&format!(" AND library_id = {}", lib_id));
            }
        }
    }
    sql.push_str(" ORDER BY id DESC LIMIT 16");
    let mut stmt = conn.prepare(&sql).unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let has_poster = !row.get::<_, String>(5).unwrap_or_default().is_empty();
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(),
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(),
            "ImageTags": if has_poster { json!({"Primary": "tag"}) } else { json!({}) },
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(items).into_response()
}

pub async fn emby_items_resume() -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }
pub async fn emby_items_next_up() -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }

pub async fn emby_items(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<EmbyItemsQuery>) -> impl IntoResponse { emby_items_inner(pool, query).await }
async fn emby_items_inner(pool: Arc<Mutex<Connection>>, query: EmbyItemsQuery) -> Response {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, title, type, year, plot, poster_path, file_path FROM items WHERE 1=1".to_string();
    if let Some(pid) = &query.parent_id {
        if pid.starts_with("lib_") { if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { sql.push_str(&format!(" AND library_id = {}", lib_id)); } }
    }
    sql.push_str(" ORDER BY id DESC");
    if let Some(limit) = query.limit {
        sql.push_str(&format!(" LIMIT {}", limit));
    } else {
        sql.push_str(" LIMIT 100");
    }
    if let Some(start) = query.start_index {
        sql.push_str(&format!(" OFFSET {}", start));
    }
    let mut stmt = conn.prepare(&sql).unwrap();
    let items: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let has_poster = !row.get::<_, String>(5).unwrap_or_default().is_empty();
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), "ImageTags": if has_poster { json!({"Primary": "tag"}) } else { json!({}) },
            "Path": row.get::<_, String>(6).unwrap_or_default(),
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true }],
            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response()
}

pub async fn emby_item_detail(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<String>) -> Response {
    emby_item_detail_inner(pool, item_id).await
}
pub async fn emby_item_detail_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>) -> Response {
    emby_item_detail_inner(pool, item_id).await
}
async fn emby_item_detail_inner(pool: Arc<Mutex<Connection>>, item_id: String) -> Response {
    let Ok(id) = item_id.parse::<i64>() else { return StatusCode::NOT_FOUND.into_response(); };
    let conn = pool.lock().unwrap();
    let item_opt = conn.query_row("SELECT id, title, type, year, plot, poster_path, file_path, studios, parts, runtime, actors FROM items WHERE id = ?1", [id], |row| {
        let has_poster = !row.get::<_, String>(5).unwrap_or_default().is_empty();
        let studios_str: String = row.get::<_, String>(7).unwrap_or_default();
        let studios: Vec<Value> = studios_str.split(" / ").filter(|s| !s.trim().is_empty()).map(|s| json!({ "Name": s, "Id": format!("studio_{}", s) })).collect();
        let parts_json: String = row.get::<_, String>(8).unwrap_or_default();
        let parts: Vec<String> = serde_json::from_str(&parts_json).unwrap_or_default();
        let runtime: i32 = row.get(9).unwrap_or(0);
        let actors_json: String = row.get(10).unwrap_or_else(|_| "[]".to_string());
        let actors: Vec<Value> = serde_json::from_str(&actors_json).unwrap_or_default();
        let people: Vec<Value> = actors.into_iter().map(|a| {
            json!({
                "Name": a["name"].as_str().unwrap_or_default(),
                "Role": a["role"].as_str().unwrap_or_default(),
                "Type": "Actor"
            })
        }).collect();
        let media_sources: Vec<Value> = parts.iter().enumerate().map(|(i, path)| {
            json!({ "Id": format!("{}_cd{}", id, i), "Path": path, "Name": if parts.len() > 1 { format!("CD{}", i + 1) } else { "Default".to_string() }, "Protocol": "Http", "SupportsDirectPlay": true, "SupportsDirectStream": true, "SupportsTranscoding": false, "DirectStreamUrl": format!("/Videos/{}/stream?MediaSourceId={}_cd{}", id, id, i) })
        }).collect();
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), "ImageTags": if has_poster { json!({"Primary": "tag"}) } else { json!({}) },
            "Path": row.get::<_, String>(6).unwrap_or_default(), "Studios": studios,
            "MediaSources": media_sources,
            "RunTimeTicks": (runtime as i64) * 60 * 10_000_000,
            "People": people,
            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
        }))
    }).ok();
    if let Some(item) = item_opt { Json(item).into_response() } else { StatusCode::NOT_FOUND.into_response() }
}

pub async fn emby_item_image(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, image_type)): AxumPath<(String, String)>) -> Response {
    emby_item_image_inner(pool, item_id, image_type).await
}
pub async fn emby_item_image_with_index(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, image_type, _index)): AxumPath<(String, String, String)>) -> Response {
    emby_item_image_inner(pool, item_id, image_type).await
}
pub async fn emby_item_image_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id, image_type)): AxumPath<(String, String, String)>) -> Response {
    emby_item_image_inner(pool, item_id, image_type).await
}
async fn emby_item_image_inner(pool: Arc<Mutex<Connection>>, item_id: String, image_type: String) -> Response {
    let Ok(id) = item_id.parse::<i64>() else { return StatusCode::NOT_FOUND.into_response(); };
    let path_opt = {
        let conn = pool.lock().unwrap();
        let query = match image_type.to_lowercase().as_str() {
            "primary" | "poster" | "thumb" => "SELECT poster_path FROM items WHERE id = ?1",
            "backdrop" | "fanart" => "SELECT fanart_path FROM items WHERE id = ?1",
            "logo" => "SELECT logo_path FROM items WHERE id = ?1", "banner" => "SELECT banner_path FROM items WHERE id = ?1",
            _ => "SELECT poster_path FROM items WHERE id = ?1",
        };
        let mut stmt = conn.prepare(query).unwrap();
        stmt.query_row([id], |row| row.get::<_, String>(0)).ok()
    };
    if let Some(path_str) = path_opt {
        if !path_str.is_empty() {
            if path_str.starts_with("http") { return (StatusCode::FOUND, [(LOCATION, path_str)], "").into_response(); }
            if let Ok(bytes) = tokio::fs::read(&path_str).await {
                let mime = if path_str.to_lowercase().ends_with(".png") { "image/png" } else { "image/jpeg" };
                return (StatusCode::OK, [(CONTENT_TYPE, mime)], bytes).into_response();
            }
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

pub async fn emby_playback_info(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> Response {
    let parts_json_opt = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts FROM items WHERE id = ?1").unwrap();
        stmt.query_row([item_id], |row| row.get::<_, String>(0)).ok()
    };
    let parts: Vec<String> = parts_json_opt.and_then(|json| serde_json::from_str(&json).ok()).unwrap_or_default();
    let media_sources: Vec<Value> = parts.iter().enumerate().map(|(i, path)| {
        json!({
            "Id": format!("{}_cd{}", item_id, i),
            "Path": path,
            "Name": if parts.len() > 1 { format!("CD{}", i + 1) } else { "Default".to_string() },
            "Protocol": "Http",
            "SupportsDirectPlay": true,
            "SupportsDirectStream": true,
            "SupportsTranscoding": false,
            "DirectStreamUrl": format!("/Videos/{}/stream?MediaSourceId={}_cd{}", item_id, item_id, i)
        })
    }).collect();
    Json(json!({ "MediaSources": media_sources })).into_response()
}

pub async fn emby_stream(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response {
    emby_stream_inner(pool, item_id, query, headers).await
}
pub async fn emby_stream_with_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _ext)): AxumPath<(i64, String)>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response {
    emby_stream_inner(pool, item_id, query, headers).await
}
pub async fn emby_stream_with_source(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id)): AxumPath<(i64, String)>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response {
    emby_stream_inner(pool, item_id, query, headers).await
}
pub async fn emby_stream_with_source_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id, _ext)): AxumPath<(i64, String, String)>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response {
    emby_stream_inner(pool, item_id, query, headers).await
}
async fn emby_stream_inner(pool: Arc<Mutex<Connection>>, item_id: i64, query: HashMap<String, String>, headers: HeaderMap) -> Response {
    let ua = headers.get(USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    let ip = headers.get("X-Real-IP").and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    
    let part_idx = if let Some(p) = query.get("part") { p.parse::<usize>().unwrap_or(0) }
    else if let Some(msid) = query.get("MediaSourceId").or_else(|| query.get("mediaSourceId")) {
        msid.split('_').last().and_then(|s| s.replace("cd", "").parse::<usize>().ok()).unwrap_or(0)
    } else { 0 };

    let cache_key = format!("{}|{}|{}|{}", item_id, part_idx, ip, ua);
    let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    { 
        let cache_lock = cache.lock().unwrap(); 
        if let Some((cached_url, expire_at)) = cache_lock.get(&cache_key) { 
            if Instant::now() < *expire_at { 
                return (StatusCode::FOUND, [(LOCATION, cached_url.clone())], "").into_response(); 
            } 
        } 
    }

    let strm_path_opt = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts FROM items WHERE id = ?1").unwrap();
        stmt.query_row([item_id], |row| row.get::<_, String>(0)).ok()
            .and_then(|json| serde_json::from_str::<Vec<String>>(&json).ok())
            .and_then(|arr| arr.get(part_idx).cloned())
    };
    
    let Some(strm_path) = strm_path_opt else { return StatusCode::NOT_FOUND.into_response(); };
    let file = match tokio::fs::File::open(&strm_path).await { Ok(f) => f, Err(_) => return StatusCode::NOT_FOUND.into_response() };
    let mut contents = String::new();
    if file.take(MAX_STRM_SIZE).read_to_string(&mut contents).await.is_err() { return StatusCode::INTERNAL_SERVER_ERROR.into_response(); }
    let url = contents.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
    if !url.starts_with("http") { return StatusCode::NOT_FOUND.into_response(); }

    let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(15)).build().unwrap();
    let mut final_url = url.to_string();
    if let Ok(resp) = client.get(url).header(USER_AGENT, ua).send().await {
        if resp.status().is_redirection() { if let Some(location) = resp.headers().get("location") { if let Ok(loc_str) = location.to_str() { final_url = loc_str.to_string(); } } }
    }
    
    { let mut cache_lock = cache.lock().unwrap(); cache_lock.insert(cache_key, (final_url.clone(), Instant::now() + Duration::from_secs(300))); }
    (StatusCode::FOUND, [(LOCATION, final_url)], "").into_response()
}

pub async fn emby_download(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> Response {
    let path_opt = { let conn = pool.lock().unwrap(); let mut stmt = conn.prepare("SELECT file_path FROM items WHERE id = ?1").unwrap(); stmt.query_row([item_id], |row| row.get::<_, String>(0)).ok() };
    let Some(strm_path) = path_opt else { return StatusCode::NOT_FOUND.into_response(); };
    let file = match tokio::fs::File::open(&strm_path).await { Ok(f) => f, Err(_) => return StatusCode::NOT_FOUND.into_response() };
    let mut contents = String::new();
    if file.take(MAX_STRM_SIZE).read_to_string(&mut contents).await.is_err() { return StatusCode::INTERNAL_SERVER_ERROR.into_response(); }
    let url = contents.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
    if url.starts_with("http") { return (StatusCode::FOUND, [(LOCATION, url.to_string())], "").into_response(); }
    StatusCode::NOT_FOUND.into_response()
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ResumeVisibilityQuery { #[serde(rename = "Hide")] pub hide: Option<String> }
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ResumeVisibilityBody { #[serde(rename = "Hide")] pub hide: bool }

pub async fn emby_hide_from_resume(Query(query): Query<ResumeVisibilityQuery>, body: Bytes) -> Response {
    let query_hide = match query.hide.as_deref().map(str::parse::<bool>).transpose() { Ok(value) => value, Err(_) => return StatusCode::BAD_REQUEST.into_response() };
    let body_hide = if body.is_empty() { None } else { match serde_json::from_slice::<ResumeVisibilityBody>(&body) { Ok(body) => Some(body.hide), Err(_) => return StatusCode::BAD_REQUEST.into_response() } };
    let hidden = match (query_hide, body_hide) { (Some(q), Some(b)) if q != b => return StatusCode::BAD_REQUEST.into_response(), (Some(h), _) | (_, Some(h)) => h, (None, None) => return StatusCode::BAD_REQUEST.into_response() };
    Json(json!({ "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "HideFromResume": hidden })).into_response()
}

pub async fn emby_action() -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_playing() -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_playing_progress() -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_playing_stopped() -> impl IntoResponse { StatusCode::OK.into_response() }
