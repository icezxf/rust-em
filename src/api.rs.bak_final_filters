use axum::{
    extract::{State, Request, Json, Query, Path as AxumPath},
    http::{StatusCode, header::{LOCATION, USER_AGENT, CONTENT_TYPE}, HeaderMap, Method},
    middleware::Next, response::{IntoResponse, Response}, body::Bytes,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::collections::HashMap;
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

static TOKENS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static STRM_CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
static GLOBAL_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
static SCAN_STATUS: OnceLock<Mutex<bool>> = OnceLock::new();
static LAST_SCAN_TIME: OnceLock<Mutex<String>> = OnceLock::new();
const MAX_STRM_SIZE: u64 = 16 * 1024;

pub async fn logger_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();
    
    // 过滤掉不需要打印的干扰请求
    if path.ends_with(".css") || path.ends_with(".js") || path.ends_with(".png") || path.ends_with(".jpg") 
        || path.contains("/Images/") || path.contains("/api/images/") 
        || path.contains("/Sessions/Playing/Progress")
        || path.contains("websocket") || path.contains("webhook") || path.contains("Webhook") {
        return next.run(req).await; 
    }

    let ua = req.headers().get("User-Agent").and_then(|v| v.to_str().ok()).unwrap_or("unknown");
    let query = uri.query().unwrap_or("");
    println!("📥 [REQ] {} {}?{} | UA: {}", method, path, query, ua);
    let res = next.run(req).await;
    let status = res.status();
    if status.is_success() || status.is_redirection() { println!("📤 [RES] {} {} | 状态: {}", method, path, status); }
    else { println!("❌ [RES] {} {} | 状态: {} (异常)", method, path, status); }
    res
}

pub async fn auth(req: Request, next: Next) -> Result<Response, StatusCode> {
    if req.method() == Method::OPTIONS { return Ok(next.run(req).await); }
    let path = req.uri().path();
    if path.contains("/Images/") || path.ends_with("/Ping") || path.ends_with("/Info/Public") || path.ends_with("/Users/Public") || path.ends_with("/AuthenticateByName") || path == "/api/login" {
        return Ok(next.run(req).await);
    }
    if !path.starts_with("/api") && !path.starts_with("/emby") && !path.starts_with("/Items") && !path.starts_with("/Users") && !path.starts_with("/Videos") && !path.starts_with("/System") && !path.starts_with("/Auth") && !path.starts_with("/Sessions") && !path.starts_with("/Persons") {
        return Ok(next.run(req).await);
    }
    
    let mut token_opt = None;
    if let Some(val) = req.headers().get("Authorization").and_then(|v| v.to_str().ok()) {
        if let Some(t) = val.strip_prefix("Bearer ") { token_opt = Some(t.trim().to_string()); }
    }
    if token_opt.is_none() {
        if let Some(val) = req.headers().get("X-Emby-Token").and_then(|v| v.to_str().ok()) { token_opt = Some(val.to_string()); }
    }
    if token_opt.is_none() {
        if let Some(query) = req.uri().query() {
            for pair in query.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    let k_lower = k.to_lowercase();
                    if k_lower == "token" || k_lower == "api_key" || k_lower == "apikey" || k_lower == "x-emby-token" { token_opt = Some(v.to_string()); break; }
                }
            }
        }
    }

    if let Some(token) = token_opt {
        if let Some(tokens) = TOKENS.get() {
            if tokens.lock().unwrap().contains(&token) { return Ok(next.run(req).await); }
        }
    }
    Err(StatusCode::UNAUTHORIZED)
}


fn to_hex(s: &str) -> String { s.as_bytes().iter().map(|b| format!("{:02x}", b)).collect() }
fn from_hex(s: &str) -> Option<String> {
    let mut bytes = Vec::new();
    for i in (0..s.len()).step_by(2) {
        if i + 1 < s.len() {
            if let Ok(b) = u8::from_str_radix(&s[i..i+2], 16) { bytes.push(b); } else { return None; }
        }
    }
    String::from_utf8(bytes).ok()
}

fn decode_url(input: &str) -> String {
    let mut result = Vec::new();
    let mut bytes = input.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            if let (Some(c1), Some(c2)) = (bytes.next(), bytes.next()) {
                if let Ok(hex_str) = std::str::from_utf8(&[c1, c2]) {
                    if let Ok(hex_val) = u8::from_str_radix(hex_str, 16) {
                        result.push(hex_val);
                        continue;
                    }
                }
                result.push(b'%'); result.push(c1); result.push(c2);
            } else {
                result.push(b'%');
            }
        } else if b == b'+' {
            result.push(b' ');
        } else {
            result.push(b);
        }
    }
    String::from_utf8_lossy(&result).into_owned()
}

fn format_iso8601(unix_secs: i64) -> String {
    if let Some(dt) = chrono::DateTime::from_timestamp(unix_secs, 0) { dt.format("%Y-%m-%dT%H:%M:%S.0000000Z").to_string() } 
    else { "2024-01-01T00:00:00.0000000Z".to_string() }
}

fn extract_extension_from_url(url: &str) -> String {
    let path_part = url.split('?').next().unwrap_or(url);
    if let Some(idx) = path_part.rfind('.') {
        let ext = &path_part[idx+1..];
        if !ext.is_empty() && ext.len() <= 5 && ext.chars().all(|c| c.is_ascii_alphanumeric()) { return ext.to_lowercase(); }
    }
    "mp4".to_string()
}

pub async fn resolve_qmediasync_url(client: &reqwest::Client, url: &str, ua: &str) -> Option<String> {
    let resp = client.get(url).header(USER_AGENT, ua).send().await.ok()?;
    let status = resp.status();
    if status.is_redirection() {
        println!("🎯 [RESOLVE] 成功获取 302 跳转！");
        resp.headers().get(reqwest::header::LOCATION).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
    } else {
        let text = resp.text().await.unwrap_or_default();
        if let Some(start) = text.find("href=\"") {
            let rest = &text[start + 6..];
            if let Some(end) = rest.find('\"') { Some(rest[..end].replace("&amp;", "&")) } else { None }
        } else if text.starts_with("http") { Some(text.trim().to_string()) } 
        else { 
            println!("❌ [RESOLVE] 解析失败！状态码: {}", status);
            None 
        }
    }
}

fn sanitize_media_streams(streams: &mut Value) {
    if let Some(arr) = streams.as_array_mut() {
        for (idx, stream) in arr.iter_mut().enumerate() {
            if let Some(obj) = stream.as_object_mut() {
                obj.insert("Index".to_string(), json!(idx));
                if let Some(Value::String(t)) = obj.get_mut("Type") {
                    *t = if t.eq_ignore_ascii_case("video") { "Video".to_string() }
                         else if t.eq_ignore_ascii_case("audio") { "Audio".to_string() }
                         else if t.eq_ignore_ascii_case("subtitle") { "Subtitle".to_string() }
                         else { t.clone() };
                }
                let stream_type = obj.get("Type").and_then(|v| v.as_str()).unwrap_or("");
                let codec = obj.get("Codec").and_then(|v| v.as_str()).unwrap_or("unknown").to_uppercase();
                let mut display_title = obj.get("DisplayTitle").and_then(|v| v.as_str()).unwrap_or("").to_string();
                if stream_type == "Video" {
                    let depth = obj.get("BitDepth").and_then(|v| v.as_i64()).unwrap_or(0);
                    if display_title.is_empty() {
                        let w = obj.get("Width").and_then(|v| v.as_i64()).unwrap_or(0);
                        let h = obj.get("Height").and_then(|v| v.as_i64()).unwrap_or(0);
                        let base = std::cmp::max(w, h);
                        let p = if base >= 2160 { "2160p" } else if base >= 1080 { "1080p" } else if base >= 720 { "720p" } else { "SD" };
                        display_title = format!("{} {}", p, codec);
                    }
                    if !display_title.contains("bit") && depth > 0 { display_title = format!("{} {}bit", display_title, depth); }
                } else if stream_type == "Audio" {
                    if display_title.is_empty() || display_title.contains("Unknown") {
                        let ch = obj.get("Channels").and_then(|v| v.as_i64()).unwrap_or(2);
                        let layout = obj.get("ChannelLayout").and_then(|v| v.as_str()).unwrap_or(if ch == 2 { "stereo" } else if ch == 6 { "5.1" } else { "" });
                        display_title = format!("{} {}", codec, layout).trim().to_string();
                    }
                }
                obj.insert("DisplayTitle".to_string(), json!(display_title));
                obj.remove("Title");
                for k in &["AverageFrameRate", "RealFrameRate"] {
                    if let Some(v) = obj.get_mut(*k) {
                        let f = match v {
                            Value::String(s) => { let s = s.trim(); if s.is_empty() || s == "0/0" { 0.0 } else if let Some((num, den)) = s.split_once('/') { num.parse::<f64>().unwrap_or(0.0) / den.parse::<f64>().unwrap_or(1.0) } else { s.parse::<f64>().unwrap_or(0.0) } },
                            Value::Number(n) => n.as_f64().unwrap_or(0.0),
                            _ => 0.0,
                        };
                        *v = json!(f);
                    }
                }
                for k in &["BitRate", "BitDepth", "Width", "Height", "Level", "Channels", "SampleRate", "Index"] {
                    if let Some(v) = obj.get_mut(*k) {
                        let i = match v { Value::String(s) => s.trim().parse::<i64>().unwrap_or(0), Value::Number(n) => n.as_i64().unwrap_or(0), _ => 0 };
                        *v = json!(i);
                    }
                }
                for k in &["IsDefault", "IsForced", "IsExternal"] {
                    let b = match obj.get(*k) { Some(Value::Bool(b)) => *b, Some(Value::String(s)) => s.to_lowercase() == "true", _ => false };
                    obj.insert(k.to_string(), json!(b));
                }
            }
        }
    }
    fn replace_null(v: &mut Value) {
        match v {
            Value::Object(map) => { for (_, val) in map.iter_mut() { replace_null(val); } }
            Value::Array(arr) => { for val in arr.iter_mut() { replace_null(val); } }
            Value::Null => { *v = Value::String("".to_string()); }
            _ => {}
        }
    }
    replace_null(streams);
}

// ======= Web API =======
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
        let lib_id = { let conn = pool_clone.lock().unwrap(); conn.execute("INSERT INTO libraries (name, path, type) VALUES (?1, ?2, ?3)", [&payload.name, &serde_json::to_string(&payload.paths).unwrap(), &payload.lib_type]).unwrap(); conn.last_insert_rowid() };
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

// ======= 扫描与解析 =======
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
        let mut title = base_stem.clone(); let mut num = String::new(); let mut plot = String::new(); let mut year = String::new();
        let mut rating = 0.0; let mut mpaa = String::new(); let mut genres = String::new(); let mut tags = String::new();
        let mut directors = String::new(); let mut actors_json = String::new(); let mut runtime = 0; let mut premiered = String::new();
        let mut releasedate = String::new(); let mut tagline = String::new(); let mut studios = String::new(); let mut tmdb_id = String::new();
        let mut imdb_id = String::new(); let mut series = String::new(); let mut label = String::new(); let mut set_name = String::new();
        let mut nfo_poster = String::new(); let mut nfo_fanart = String::new();
        let mut original_title = String::new(); let mut sort_title = String::new(); let mut country = String::new(); let mut language = String::new();
        let mut writers = String::new(); let mut trailer = String::new();

        for nfo in nfo_candidates {
            if nfo.exists() {
                if let Some(meta) = crate::nfo::read_nfo(&nfo) {
                    if let Some(t) = &meta.title { title = t.clone(); }
                    if let Some(n) = &meta.num { num = n.clone(); }
                    if let Some(p) = &meta.plot { plot = p.clone(); }
                    if let Some(r) = meta.rating { rating = r; }
                    if let Some(m) = &meta.mpaa { mpaa = m.clone(); }
                    if let Some(rt) = meta.runtime { runtime = rt; }
                    if let Some(tg) = &meta.tagline { tagline = tg.clone(); }
                    if let Some(s) = &meta.series { series = s.clone(); }
                    if let Some(l) = &meta.label { label = l.clone(); }
                    if !meta.sets.is_empty() { set_name = meta.sets.join(" / "); }
                    if let Some(pr) = &meta.premiered { premiered = pr.clone(); }
                    if let Some(rd) = &meta.releasedate { releasedate = rd.clone(); }
                    if let Some(y) = &meta.year { year = y.clone(); } 
                    else if let Some(pr) = &meta.premiered { if pr.len() >= 4 { year = pr[0..4].to_string(); } } 
                    else if let Some(rd) = &meta.releasedate { if rd.len() >= 4 { year = rd[0..4].to_string(); } }
                    if !meta.genres.is_empty() { genres = meta.genres.join(" / "); }
                    if !meta.tags.is_empty() { tags = meta.tags.join(" / "); }
                    if !meta.directors.is_empty() { directors = meta.directors.join(" / "); }
                    if !meta.studios.is_empty() { studios = meta.studios.join(" / "); }
                    if let Some(v) = &meta.original_title { original_title = v.clone(); }
                    if let Some(v) = &meta.sort_title { sort_title = v.clone(); }
                    if !meta.countries.is_empty() { country = meta.countries.join(" / "); }
                    if !meta.languages.is_empty() { language = meta.languages.join(" / "); }
                    if !meta.writers.is_empty() { writers = meta.writers.join(" / "); }
                    if let Some(v) = &meta.trailer { trailer = v.clone(); }
                    
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
        
        let mediainfo_json_path = crate::probe::get_mediainfo_json_path(first_path);
        let (probe_size, media_streams_json_str) = if mediainfo_json_path.exists() {
            if let Some(src) = crate::probe::load_cached_source(&mediainfo_json_path) {
                let s = src.get("Size").and_then(|v| v.as_i64()).unwrap_or(0);
                let ms = src.get("MediaStreams").cloned().unwrap_or(json!([]));
                let ms_str = serde_json::to_string(&ms).unwrap_or_else(|_| "[]".to_string());
                if let Some(rt_ticks) = src.get("RunTimeTicks").and_then(|v| v.as_i64()) {
                    if rt_ticks > 0 { runtime = (rt_ticks / 10_000_000 / 60) as i32; }
                }
                (s, ms_str)
            } else { (0, "[]".to_string()) }
        } else { (0, "[]".to_string()) };
        let final_size = if probe_size > 0 { probe_size } else { size };
        let updated_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;

        {
            let conn = match pool.lock() { Ok(c) => c, Err(_) => continue };
            let _ = conn.execute(
                "INSERT INTO items (library_id, title, num, file_path, type, plot, year, poster_path, fanart_path, logo_path, thumb_path, banner_path, size, mtime, rating, mpaa, genres, tags, directors, actors, runtime, premiered, tagline, studios, tmdb_id, imdb_id, series, label, set_name, strm_content, stills, parts, original_title, sort_title, country, language, writers, trailer, releasedate, media_streams, updated_at) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40, ?41)
                 ON CONFLICT(file_path) DO UPDATE SET title=excluded.title, num=excluded.num, size=excluded.size, mtime=excluded.mtime, plot=excluded.plot, year=excluded.year, poster_path=excluded.poster_path, fanart_path=excluded.fanart_path, logo_path=excluded.logo_path, thumb_path=excluded.thumb_path, banner_path=excluded.banner_path, rating=excluded.rating, mpaa=excluded.mpaa, genres=excluded.genres, tags=excluded.tags, directors=excluded.directors, actors=excluded.actors, runtime=excluded.runtime, premiered=excluded.premiered, tagline=excluded.tagline, studios=excluded.studios, tmdb_id=excluded.tmdb_id, imdb_id=excluded.imdb_id, series=excluded.series, label=excluded.label, set_name=excluded.set_name, strm_content=excluded.strm_content, stills=excluded.stills, parts=excluded.parts, original_title=excluded.original_title, sort_title=excluded.sort_title, country=excluded.country, language=excluded.language, writers=excluded.writers, trailer=excluded.trailer, releasedate=excluded.releasedate, media_streams=excluded.media_streams, updated_at=excluded.updated_at",
                rusqlite::params![lib_id, title, num, virtual_path, lib_type, plot, year, final_poster, final_fanart, logo, thumb, banner, final_size, mtime, rating, mpaa, genres, tags, directors, actors_json, runtime, premiered, tagline, studios, tmdb_id, imdb_id, series, label, set_name, strm_content, stills_json, parts_json, original_title, sort_title, country, language, writers, trailer, releasedate, media_streams_json_str, updated_at]
            );
        }
    }
    println!("✅ 扫描完成。共处理 {} 个媒体项。", count);
}

// ======= Web 接口 =======
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
    
    if let Some(lid) = query.library_id { 
        sql.push_str(" AND library_id = ?");
        params.push(lid.to_string());
    }
    if let Some(lt) = &query.lib_type { 
        sql.push_str(" AND type = ?");
        params.push(lt.to_string());
    }
    
    if let Some(a) = &query.actor { 
        sql.push_str(" AND (actors LIKE ? OR directors LIKE ?)");
        params.push(format!("%{}%", a));
        params.push(format!("%{}%", a));
    }
    if let Some(g) = &query.genre { 
        sql.push_str(" AND genres LIKE ?");
        params.push(format!("%{}%", g));
    }
    if let Some(t) = &query.tag { 
        sql.push_str(" AND tags LIKE ?");
        params.push(format!("%{}%", t));
    }
    if let Some(st) = &query.studio { 
        sql.push_str(" AND studios LIKE ?");
        params.push(format!("%{}%", st));
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
            "title": row.get::<_, String>(1).unwrap_or_default(),
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
            "strm_content": row.get::<_, String>(24).unwrap_or_default(), "stills": stills, "parts": parts, "media_streams": media_streams
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
    if path_str.starts_with("http") { 
        let png_bytes: [u8; 67] = [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
            0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
            0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
            0x42, 0x60, 0x82
        ];
        return (StatusCode::OK, [(CONTENT_TYPE, "image/png")], png_bytes).into_response(); 
    }
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
                            if src.get("MediaStreams").and_then(|m| m.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
                                skip += 1;
                                println!("⏭️ [PROBE-SKIP] {}", p.display());
                                continue;
                            }
                        }
                    }
                    match crate::probe::probe_strm(&p, limiter.clone(), &client).await {
                        Ok(_) => { ok += 1; println!("✅ [PROBE] {}", p.display()); }
                        Err(e) => { fail += 1; println!("❌ [PROBE] {}: {}", p.display(), e); }
                    }
                }
            }
        }
        println!("🏁 [PROBE] 完成 ok={} skip={} fail={}", ok, skip, fail);
    });
    (StatusCode::OK, Json(json!({"status":"started"}))).into_response()
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

pub async fn emby_items_latest(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<HashMap<String, String>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, fanart_path FROM items WHERE 1=1".to_string();
    let mut params: Vec<String> = Vec::new();
    
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { 
                sql.push_str(" AND library_id = ?");
                params.push(lib_id.to_string());
            }
        }
    }
    sql.push_str(" ORDER BY id DESC LIMIT 16"); 
    
    let mut stmt = conn.prepare(&sql).unwrap();
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let poster_path: String = row.get(5).unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
        let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        let poster_tag = if has_poster { format!("tag_{}_{}", id, &hash_hex[..8]) } else { String::new() };

        let fanart_path: String = row.get(18).unwrap_or_default();
        let has_fanart = !fanart_path.is_empty();
        let mut hf = Sha256::new(); hf.update(fanart_path.as_bytes()); let hashf = hf.finalize();
        let hash_hexf: String = hashf.iter().map(|b| format!("{:02x}", b)).collect();
        let backdrop_tag = if has_fanart { format!("backdrop_{}_{}", id, &hash_hexf[..8]) } else { String::new() };

        let runtime_mins: i32 = row.get(7).unwrap_or(0);
        let total_ticks: i64 = (runtime_mins as i64) * 60 * 10_000_000;
        let size: i64 = row.get(14).unwrap_or(0);
        let mtime = row.get::<_, i64>(15).unwrap_or(0);
        let date_str = format_iso8601(mtime);
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0), 
            "RunTimeTicks": total_ticks,
            "Size": size,
            "DateCreated": date_str.clone(),
            "EndDate": date_str,
            "PrimaryImageAspectRatio": 0.6666666666666666,
            "Container": "mp4",
            "CanDelete": false,
            "CanDownload": false,
            "LocationType": "FileSystem",
            "Status": "Continuing",
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>(6).unwrap_or_default()),
            "MediaSources": [{ 
                "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Size": size,
                "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks
            }],
            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(items).into_response()
}

pub async fn emby_items_resume(
    State(pool): State<Arc<Mutex<Connection>>>, 
    AxumPath(_user_id): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, playback_ticks, fanart_path FROM items WHERE playback_ticks > 0".to_string();
    let mut params: Vec<String> = Vec::new();
    
    // 支持只显示当前媒体库的继续观看
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(" AND library_id = ?");
                params.push(lib_id.to_string());
            }
        }
    }
    sql.push_str(" ORDER BY updated_at DESC LIMIT 30");
    
    let mut stmt = conn.prepare(&sql).unwrap();
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let poster_path: String = row.get(5).unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
        let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        let poster_tag = if has_poster { format!("tag_{}_{}", id, &hash_hex[..8]) } else { String::new() };
        
        let playback_ticks: i64 = row.get(18).unwrap_or(0);
        let fanart_path: String = row.get(19).unwrap_or_default();
        let has_fanart = !fanart_path.is_empty();
        let mut hf = Sha256::new(); hf.update(fanart_path.as_bytes()); let hashf = hf.finalize();
        let hash_hexf: String = hashf.iter().map(|b| format!("{:02x}", b)).collect();
        let backdrop_tag = if has_fanart { format!("backdrop_{}_{}", id, &hash_hexf[..8]) } else { String::new() };

        let runtime_mins: i32 = row.get(7).unwrap_or(0);
        let total_ticks: i64 = (runtime_mins as i64) * 60 * 10_000_000;
        let played_pct = if total_ticks > 0 { (playback_ticks * 100 / total_ticks) as i32 } else { 0 };
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0), 
            "RunTimeTicks": total_ticks,
            "DateCreated": format_iso8601(row.get::<_, i64>(15).unwrap_or(0)),
            "PrimaryImageAspectRatio": 0.6666666666666666,
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>(6).unwrap_or_default()),
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true }],
            "UserData": { 
                "PlaybackPositionTicks": playback_ticks, 
                "PlayCount": 1, 
                "IsFavorite": false, 
                "Played": false,
                "PlayedPercentage": played_pct
            }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response()
}

pub async fn emby_items_next_up(_: AxumPath<String>) -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }

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
}

pub async fn emby_items(State(pool): State<Arc<Mutex<Connection>>>, Query(query): Query<EmbyItemsQuery>) -> impl IntoResponse { emby_items_inner(pool, query).await }
pub async fn emby_items_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<EmbyItemsQuery>) -> impl IntoResponse { emby_items_inner(pool, query).await }
async fn emby_items_inner(pool: Arc<Mutex<Connection>>, query: EmbyItemsQuery) -> Response {
    let conn = pool.lock().unwrap();
    
    // 拦截真正的文件夹请求 (支持无限层级目录)
    // 兼容 Hills (Recursive=false) 和 Yamby (IncludeItemTypes包含Folder)
    let is_folder_view = query.recursive == Some(false)
        || query.sort_by.as_deref().map_or(false, |s| s.contains("IsFolder"))
        || query.include_item_types.as_deref().map_or(false, |t| t.to_lowercase().contains("folder"));

    if is_folder_view {
        {
            // 寻找当前的基路径（取决于点的是第一层 lib_ 还是更深层的 folder_）
            let base_path = if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") {
                    if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                        if let Ok(path_json) = conn.query_row("SELECT path FROM libraries WHERE id = ?1", [lib_id], |r| r.get::<_, String>(0)) {
                            serde_json::from_str::<Vec<String>>(&path_json).ok().and_then(|paths| paths.first().cloned())
                        } else { None }
                    } else { None }
                } else if pid.starts_with("folder_") {
                    from_hex(pid.trim_start_matches("folder_"))
                } else { None }
            } else { None };

            if let Some(base) = base_path {
                let base_clean = base.trim_end_matches('/');
                let mut folders_set = std::collections::HashSet::new();
                
                // 查一下这个基路径下所有的文件
                if let Ok(mut stmt) = conn.prepare("SELECT file_path FROM items WHERE file_path LIKE ?") {
                    let like_pattern = format!("{}/%", base_clean);
                    if let Ok(mapped) = stmt.query_map([&like_pattern], |r| r.get::<_, String>(0)) {
                        for f_path in mapped.flatten() {
                            let rel_path = f_path[base_clean.len()..].trim_start_matches('/');
                            if let Some(first_dir) = rel_path.split('/').next() {
                                // 如果 first_dir != rel_path，说明当前文件还在更深层的子文件夹里
                                if !first_dir.is_empty() && first_dir != rel_path {
                                    let folder_abs_path = format!("{}/{}", base_clean, first_dir);
                                    folders_set.insert((first_dir.to_string(), folder_abs_path));
                                }
                            }
                        }
                    }
                }
                
                // 如果找到了子文件夹，优先返回子文件夹列表，拦截掉平铺电影
                if !folders_set.is_empty() {
                    let mut folder_items = Vec::new();
                    for (name, abs_path) in folders_set {
                        let hex_id = to_hex(&abs_path);
                        folder_items.push(json!({
                            "Name": name, "Id": format!("folder_{}", hex_id), "ServerId": "rust-em-server",
                            "Type": "Folder", "IsFolder": true, "MediaType": "Video",
                            "ImageTags": {}, "BackdropImageTags": [],
                            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
                        }));
                    }
                    return Json(json!({ "Items": folder_items, "TotalRecordCount": folder_items.len() })).into_response();
                }
                
                // 如果 folders_set 为空，说明这已经是最底层目录了，无需 return，让代码继续往下走查电影
            }
        }
    }

    // 拦截合集请求 (BoxSet)
    if let Some(types) = &query.include_item_types {
        if types.to_lowercase().contains("boxset") {
            let mut sql = "SELECT set_name FROM items WHERE set_name != ''".to_string();
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") {
                    if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                        sql.push_str(&format!(" AND library_id = {}", lib_id));
                    }
                }
            }
            
            let mut boxsets = Vec::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    let mut set_names = std::collections::HashSet::new();
                    for s in mapped.flatten() {
                        for name in s.split(" / ").filter(|s| !s.trim().is_empty()) {
                            set_names.insert(name.to_string());
                        }
                    }
                    for name in set_names {
                        let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                        boxsets.push(json!({
                            "Name": name, "Id": format!("boxset_{:x}", hash), "ServerId": "rust-em-server",
                            "Type": "BoxSet", "IsFolder": true, "MediaType": "Video",
                            "ImageTags": {"Primary": "tag_boxset"}, "BackdropImageTags": [],
                            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
                        }));
                    }
                }
            }
            return Json(json!({ "Items": boxsets, "TotalRecordCount": boxsets.len() })).into_response();
        }
    }

    // 2. 拦截标签实体请求 (Yamby 点标签栏时发起的)
    if let Some(types) = &query.include_item_types {
        if types.to_lowercase() == "tag" {
            let mut sql = "SELECT tags FROM items WHERE tags != ''".to_string();
            if let Some(pid) = &query.parent_id {
                if pid.starts_with("lib_") {
                    if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                        sql.push_str(&format!(" AND library_id = {}", lib_id));
                    }
                }
            }
            
            let mut tags_set = std::collections::HashSet::new();
            if let Ok(mut stmt) = conn.prepare(&sql) {
                if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                    for t_str in mapped.flatten() {
                        for t in t_str.split(" / ") {
                            let name = t.trim();
                            if !name.is_empty() { tags_set.insert(name.to_string()); }
                        }
                    }
                }
            }

            let items: Vec<Value> = tags_set.into_iter().map(|t| {
                json!({
                    "Name": t, "Id": format!("tag_{}", t), "ServerId": "rust-em-server",
                    "Type": "Tag", "IsFolder": false, "MediaType": "Video",
                    "ImageTags": {}, "BackdropImageTags": []
                })
            }).collect();
            return Json(json!({ "Items": items, "TotalRecordCount": items.len() })).into_response();
        }
    }

    if let Some(list_ids) = &query.list_item_ids {
        if let Ok(item_id) = list_ids.parse::<i64>() {
            let set_name_str: String = conn.query_row("SELECT set_name FROM items WHERE id = ?1", [item_id], |r| r.get(0)).unwrap_or_default();
            let mut boxsets = Vec::new();
            for name in set_name_str.split(" / ").filter(|s| !s.trim().is_empty()) {
                let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                let boxset_id = format!("boxset_{:x}", hash);
                boxsets.push(json!({
                    "Name": name, "Id": boxset_id, "ServerId": "rust-em-server",
                    "Type": "BoxSet", "IsFolder": true, "MediaType": "Video",
                    "ImageTags": {"Primary": "tag_boxset"}, "BackdropImageTags": [],
                    "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
                }));
            }
            return Json(json!({ "Items": boxsets, "TotalRecordCount": boxsets.len() })).into_response();
        }
        return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
    }

    let mut sql = "SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, playback_ticks, fanart_path, date_created, actors, directors, tags, studios, play_count FROM items WHERE 1=1".to_string();
    let mut params: Vec<String> = Vec::new();

    // 3. 过滤逻辑
    if let Some(types) = &query.include_item_types {
        let t = types.to_lowercase();
        if t.contains("movie") {
            sql.push_str(" AND type = 'movies'");
        } else if t.contains("series") || t.contains("episode") {
            sql.push_str(" AND type = 'tvshows'");
        }
        
        // 允许同时包含 Movie 和 Folder，当在 lib_ 下会展示虚拟文件夹，在 folder_ 下会展示真实电影
    }

    // 2. 处理 ParentId（媒体库/合集/演员主页）
    if let Some(pid) = &query.parent_id {
        if pid.starts_with("folder_") {
            let hex_path = pid.trim_start_matches("folder_");
            if let Some(abs_path) = from_hex(hex_path) {
                sql.push_str(" AND file_path LIKE ?");
                params.push(format!("{}/%", abs_path.trim_end_matches('/')));
            } else {
                return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
            }
        } else {
        let decoded_pid = decode_url(pid).replace('+', " ");
        if decoded_pid.starts_with("lib_") {
            if let Ok(lib_id) = decoded_pid.trim_start_matches("lib_").parse::<i64>() { 
                sql.push_str(" AND library_id = ?"); 
                params.push(lib_id.to_string());
            }
        } else if decoded_pid.starts_with("boxset_") {
            let mut found_set_name = String::new();
            let mut names = Vec::new();
            if let Ok(mut stmt_all) = conn.prepare("SELECT DISTINCT set_name FROM items WHERE set_name != ''") {
                if let Ok(mapped) = stmt_all.query_map([], |r| r.get::<_, String>(0)) {
                    for s in mapped.flatten() { names.push(s); }
                }
            }
            for full_str in names {
                for name in full_str.split(" / ").filter(|s| !s.trim().is_empty()) {
                    let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                    if format!("boxset_{:x}", hash) == decoded_pid { found_set_name = name.to_string(); break; }
                }
                if !found_set_name.is_empty() { break; }
            }
            if !found_set_name.is_empty() { 
                sql.push_str(" AND set_name LIKE ?"); 
                params.push(format!("%{}%", found_set_name));
            } else { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
        } else if decoded_pid.starts_with("person_") {
            let name = decoded_pid.replace("person_", "").trim().to_string();
            if !name.is_empty() {
                sql.push_str(" AND (actors LIKE ? OR directors LIKE ?)");
                params.push(format!("%{}%", name));
                params.push(format!("%{}%", name));
            }
        } else if decoded_pid.starts_with("studio_") {
            let name = decoded_pid.replace("studio_", "").trim().to_string();
            if !name.is_empty() {
                sql.push_str(" AND studios LIKE ?");
                params.push(format!("%{}%", name));
            }
        } else if decoded_pid.starts_with("genre_") {
            let name = decoded_pid.replace("genre_", "").trim().to_string();
            if !name.is_empty() {
                sql.push_str(" AND genres LIKE ?");
                params.push(format!("%{}%", name));
            }
        } else if decoded_pid.starts_with("tag_") {
            let name = decoded_pid.replace("tag_", "").trim().to_string();
            if !name.is_empty() {
                sql.push_str(" AND tags LIKE ?");
                params.push(format!("%{}%", name));
            }
        }
        }
    }

    // 3. 处理 Query 里的参数筛选（演员/工作室/类型/标签）
    if let Some(p) = &query.person_ids { 
        let decoded = decode_url(p).replace('+', " ");
        let name = decoded.replace("person_", "").trim().to_string(); 
        if !name.is_empty() {
            sql.push_str(" AND (actors LIKE ? OR directors LIKE ?)");
            params.push(format!("%{}%", name));
            params.push(format!("%{}%", name));
        }
    }
    
    // 智能纠正 Yamby 的错误：把塞进 GenreIds 里的 studio_ 和 tag_ 剥离出来
    if let Some(g) = &query.genre_ids { 
        let val = decode_url(g).replace('+', " ");
        if val.starts_with("studio_") { 
            let name = val.replace("studio_", "").trim().to_string();
            if !name.is_empty() { sql.push_str(" AND studios LIKE ?"); params.push(format!("%{}%", name)); }
        } else if val.starts_with("tag_") { 
            let name = val.replace("tag_", "").trim().to_string();
            if !name.is_empty() { sql.push_str(" AND tags LIKE ?"); params.push(format!("%{}%", name)); }
        } else if val.starts_with("person_") { 
            let name = val.replace("person_", "").trim().to_string();
            if !name.is_empty() { sql.push_str(" AND actors LIKE ?"); params.push(format!("%{}%", name)); }
        } else { 
            let name = val.replace("genre_", "").trim().to_string();
            if !name.is_empty() { sql.push_str(" AND genres LIKE ?"); params.push(format!("%{}%", name)); }
        }
    }
    if let Some(g) = &query.genres { 
        let name = decode_url(g).replace('+', " ").replace("genre_", "").trim().to_string();
        if !name.is_empty() { sql.push_str(" AND genres LIKE ?"); params.push(format!("%{}%", name)); }
    }
    
    if let Some(t) = &query.tag_ids { 
        let name = decode_url(t).replace('+', " ").replace("tag_", "").trim().to_string();
        if !name.is_empty() { sql.push_str(" AND tags LIKE ?"); params.push(format!("%{}%", name)); }
    }
    if let Some(t) = &query.tags { 
        let name = decode_url(t).replace('+', " ").replace("tag_", "").trim().to_string();
        if !name.is_empty() { sql.push_str(" AND tags LIKE ?"); params.push(format!("%{}%", name)); }
    }
    if let Some(s) = &query.studio_ids { 
        let name = decode_url(s).replace('+', " ").replace("studio_", "").trim().to_string();
        if !name.is_empty() { sql.push_str(" AND studios LIKE ?"); params.push(format!("%{}%", name)); }
    }
    if let Some(s) = &query.studios { 
        let name = decode_url(s).replace('+', " ").replace("studio_", "").trim().to_string();
        if !name.is_empty() { sql.push_str(" AND studios LIKE ?"); params.push(format!("%{}%", name)); }
    }

    // 处理是否已观看、是否收藏的筛选
    if let Some(filters) = &query.filters {
        if filters.contains("IsPlayed") {
            sql.push_str(" AND play_count > 0");
        } else if filters.contains("IsUnplayed") {
            sql.push_str(" AND play_count = 0");
        }
        if filters.contains("IsFavorite") {
            sql.push_str(" AND is_favorite = 1");
        }
    }

    let from_idx = sql.find(" FROM items").unwrap_or(0);
    let count_sql = format!("SELECT COUNT(*){}", &sql[from_idx..]);
    let total: i64 = conn.query_row(&count_sql, rusqlite::params_from_iter(params.iter()), |r| r.get(0)).unwrap_or(0);
    
    let start = query.start_index.unwrap_or(0);
    let limit = query.limit.unwrap_or(100);
    
    // 动态排序处理
    let mut order_clause = " ORDER BY id DESC".to_string();
    if let Some(sort_by) = &query.sort_by {
        let is_desc = query.sort_order.as_deref().unwrap_or("Ascending").to_lowercase() == "descending";
        let dir = if is_desc { " DESC" } else { " ASC" };
        
        if sort_by.contains("DateCreated") {
            order_clause = format!(" ORDER BY date_created {}", dir);
        } else if sort_by.contains("SortName") || sort_by.contains("Filename") {
            order_clause = format!(" ORDER BY title {}", dir);
        } else if sort_by.contains("ProductionYear") {
            order_clause = format!(" ORDER BY year {}", dir);
        } else if sort_by.contains("CommunityRating") {
            order_clause = format!(" ORDER BY rating {}", dir);
        }
    }
    sql.push_str(&format!("{} LIMIT {} OFFSET {}", order_clause, limit, start));
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response() };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let poster_path: String = row.get(5).unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let poster_tag = if has_poster {
            let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
            let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            format!("tag_{}_{}", id, &hash_hex[..8])
        } else { String::new() };
        let playback_ticks: i64 = row.get(18).unwrap_or(0);
        
        let fanart_path: String = row.get(19).unwrap_or_default();
        let has_fanart = !fanart_path.is_empty();
        let mut hf = Sha256::new(); hf.update(fanart_path.as_bytes()); let hashf = hf.finalize();
        let hash_hexf: String = hashf.iter().map(|b| format!("{:02x}", b)).collect();
        let backdrop_tag = if has_fanart { format!("backdrop_{}_{}", id, &hash_hexf[..8]) } else { String::new() };
        
        let runtime_mins: i32 = row.get(7).unwrap_or(0);
        let total_ticks: i64 = (runtime_mins as i64) * 60 * 10_000_000;
        let size: i64 = row.get(14).unwrap_or(0);
        let mtime: i64 = row.get(15).unwrap_or(0);
        let date_created: i64 = row.get(20).unwrap_or(0);
        let final_date_created = if date_created > 0 { date_created } else { mtime };
        let played_pct = if total_ticks > 0 { (playback_ticks * 100 / total_ticks) as i32 } else { 0 };
        let play_count: i32 = row.get(25).unwrap_or(0);
        let date_str = format_iso8601(final_date_created);
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0), 
            "RunTimeTicks": total_ticks,
            "Size": size,
            "DateCreated": date_str.clone(),
            "EndDate": date_str,
            "PrimaryImageAspectRatio": 0.6666666666666666,
            "Container": "mp4",
            "CanDelete": false,
            "CanDownload": false,
            "LocationType": "FileSystem",
            "Status": "Continuing",
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>(6).unwrap_or_default()),
            "MediaSources": [{ 
                "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Size": size,
                "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks
            }],
            "UserData": { "PlaybackPositionTicks": playback_ticks, "PlayCount": play_count, "IsFavorite": false, "Played": play_count > 0, "PlayedPercentage": played_pct }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": total })).into_response()
}

pub async fn emby_item_detail(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<String>) -> Response {
    emby_item_detail_inner(pool, item_id).await
}
pub async fn emby_item_detail_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>) -> Response {
    emby_item_detail_inner(pool, item_id).await
}

async fn emby_item_detail_inner(pool: Arc<Mutex<Connection>>, item_id: String) -> Response {
    if item_id.starts_with("person_") {
        return emby_person(AxumPath(item_id)).await.into_response();
    } else if item_id.starts_with("studio_") {
        return emby_studio(AxumPath(item_id)).await.into_response();
    }
    let conn = pool.lock().unwrap();
    
    if item_id.starts_with("boxset_") {
        let mut found_set_name = String::new();
        let mut names = Vec::new();
        if let Ok(mut stmt_all) = conn.prepare("SELECT DISTINCT set_name FROM items WHERE set_name != ''") {
            if let Ok(mapped) = stmt_all.query_map([], |r| r.get::<_, String>(0)) {
                for s in mapped.flatten() { names.push(s); }
            }
        }
        for full_str in names {
            for name in full_str.split(" / ").filter(|s| !s.trim().is_empty()) {
                let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                if format!("boxset_{:x}", hash) == item_id { found_set_name = name.to_string(); break; }
            }
            if !found_set_name.is_empty() { break; }
        }
        if found_set_name.is_empty() { return StatusCode::NOT_FOUND.into_response(); }
        
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE (set_name = ?1 OR set_name LIKE ?2 OR set_name LIKE ?3 OR set_name LIKE ?4)", [&found_set_name, &format!("{} / %", found_set_name), &format!("% / {}", found_set_name), &format!("% / {} / %", found_set_name)], |r| r.get(0)).unwrap_or(0);
        
        let item = json!({
            "Name": found_set_name, "Id": item_id, "ServerId": "rust-em-server",
            "Type": "BoxSet", "IsFolder": true, "MediaType": "Video",
            "ChildCount": count, "RecursiveItemCount": count,
            "ImageTags": {"Primary": "tag_boxset"}, "BackdropImageTags": [],
            "Overview": "合集",
            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
        });
        return Json(item).into_response();
    }

    let Ok(id) = item_id.parse::<i64>() else { return StatusCode::NOT_FOUND.into_response(); };
    let item_opt = conn.query_row(
        "SELECT id, title, type, year, plot, poster_path, file_path, studios, parts, runtime, actors, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, series, label, set_name, tagline, tags, directors, num, stills, releasedate, original_title, sort_title, country, language, writers, trailer, media_streams, date_created, primary_image_aspect_ratio, original_language, fanart_path, logo_path, thumb_path, banner_path, strm_content, playback_ticks, play_count FROM items WHERE id = ?1",
        [id],
        |row| {
            let title: String = row.get(1).unwrap_or_default();
            let typ: String = row.get(2).unwrap_or_default();
            let year_str: String = row.get(3).unwrap_or_default();
            let plot: String = row.get(4).unwrap_or_default();
            let poster_path: String = row.get(5).unwrap_or_default();
            let studios_str: String = row.get(7).unwrap_or_default();
            let parts_json: String = row.get(8).unwrap_or_default();
            let runtime: i32 = row.get(9).unwrap_or(0);
            let actors_json: String = row.get(10).unwrap_or_default();
            let rating: f64 = row.get(11).unwrap_or(0.0);
            let mpaa: String = row.get(12).unwrap_or_default();
            let genres_str: String = row.get(13).unwrap_or_default();
            let premiered: String = row.get(14).unwrap_or_default();
            let tmdb_id: String = row.get(15).unwrap_or_default();
            let imdb_id: String = row.get(16).unwrap_or_default();
            let size: i64 = row.get(17).unwrap_or(0);
            let mtime: i64 = row.get(18).unwrap_or(0);
            let series: String = row.get(19).unwrap_or_default();
            let set_name: String = row.get(21).unwrap_or_default();
            let tagline: String = row.get(22).unwrap_or_default();
            let tags_str: String = row.get(23).unwrap_or_default();
            let directors_str: String = row.get(24).unwrap_or_default();
            let num: String = row.get(25).unwrap_or_default();
            let stills_json: String = row.get(26).unwrap_or_default();
            let stills_vec: Vec<String> = serde_json::from_str(&stills_json).unwrap_or_default();
            let releasedate: String = row.get(27).unwrap_or_default();
            let original_title: String = row.get(28).unwrap_or_default();
            let sort_title: String = row.get(29).unwrap_or_default();
            let sort_title = if sort_title.is_empty() { title.clone() } else { sort_title };
            let country: String = row.get(30).unwrap_or_default();
            let language: String = row.get(31).unwrap_or_default();
            let _writers_str: String = row.get(32).unwrap_or_default();
            let media_streams_json: String = row.get(34).unwrap_or_default();
            let date_created: i64 = row.get(35).unwrap_or(0);
            let aspect_ratio: f64 = row.get(36).unwrap_or(0.0);
            let original_language: String = row.get(37).unwrap_or_default();
            let fanart_path: String = row.get(38).unwrap_or_default();
            let logo_path: String = row.get(39).unwrap_or_default();
            let thumb_path: String = row.get(40).unwrap_or_default();
            let banner_path: String = row.get(41).unwrap_or_default();
            let strm_content: String = row.get(42).unwrap_or_default();
            let playback_ticks: i64 = row.get(43).unwrap_or(0);
            let play_count: i32 = row.get(44).unwrap_or(0);

            let parts: Vec<String> = serde_json::from_str(&parts_json).unwrap_or_default();
            let actors: Vec<Value> = serde_json::from_str(&actors_json).unwrap_or_default();
            let mut media_streams: Value = serde_json::from_str(&media_streams_json).unwrap_or(json!([]));
            sanitize_media_streams(&mut media_streams);
            let real_path = strm_content.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("").to_string();

            let genres_vec: Vec<&str> = genres_str.split(" / ").filter(|s| !s.is_empty()).collect();
            let mut genre_items = Vec::new();
            for g in &genres_vec {
                genre_items.push(json!({"Id": format!("genre_{}", g), "Name": g}));
            }
            let tags_vec: Vec<&str> = tags_str.split(" / ").filter(|s| !s.is_empty()).collect();
            let mut tag_items = Vec::new();
            for t in &tags_vec {
                tag_items.push(json!({"Id": format!("tag_{}", t), "Name": t}));
            }

            let studios: Vec<Value> = studios_str.split(" / ").filter(|s| !s.trim().is_empty()).map(|s| {
                json!({ "Name": s, "Id": format!("studio_{}", s) })
            }).collect();

            let mut people: Vec<Value> = Vec::new();
            let dir_names: Vec<&str> = directors_str.split(" / ").filter(|s| !s.is_empty()).collect();
            let director_str = dir_names.join(", ");
            if !director_str.is_empty() {
                people.push(json!({ 
                    "Name": director_str, 
                    "Type": "Director", 
                    "Id": format!("person_{}", director_str), 
                    "PrimaryImageTag": "" 
                }));
            }
            
            for a in actors.iter() {
                people.push(json!({
                    "Name": a["name"].as_str().unwrap_or_default(),
                    "Role": a["role"].as_str().unwrap_or_default(),
                    "Type": "Actor",
                    "Id": format!("person_{}", a["name"].as_str().unwrap_or_default()),
                    "PrimaryImageTag": ""
                }));
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
            let run_time_ticks = (runtime as i64) * 60 * 10_000_000;
            let est_bitrate = if size > 0 && run_time_ticks > 0 { (size * 8) / (run_time_ticks / 10_000_000) } else { 0 };

            let media_sources: Vec<Value> = if parts.is_empty() { vec![] } else {
                parts.iter().enumerate().map(|(i, _path)| {
                    let stream_url = format!("/Videos/{}/original.mp4?MediaSourceId={}_cd{}", id, id, i);
                    json!({
                        "Id": format!("{}_cd{}", id, i), "ItemId": id.to_string(),
                        "Path": decode_url(&real_path), "DirectStreamUrl": stream_url,
                        "Protocol": "Http", "Container": "mp4", "Name": title.clone(),
                        "Type": "Default", "VideoType": "VideoFile", "IsRemote": true,
                        "Bitrate": est_bitrate, "RunTimeTicks": run_time_ticks, "Size": size,
                        "DefaultAudioStreamIndex": 1, "DefaultSubtitleStreamIndex": -1,
                        "SupportsDirectPlay": true, "SupportsDirectStream": true,
                        "SupportsProbing": true, "SupportsTranscoding": false,
                        "AddApiKeyToDirectStreamUrl": false, "RequiresClosing": false, "RequiresOpening": false,
                        "ProbeEngine": "rust-native", "MediaStreams": media_streams.clone()
                    })
                }).collect()
            };

            Ok(json!({
                "Id": id.to_string(), "ServerId": "rust-em-server", "Name": title.clone(),
                "OriginalTitle": json!(original_title),
                "SortName": json!(sort_title.clone()),
                "ForcedSortName": json!(sort_title.clone()),
                "Type": if typ == "movies" { "Movie" } else { "Series" },
                "MediaType": "Video", "IsFolder": false, "Overview": plot,
                "ProductionYear": year_str.parse::<i32>().unwrap_or(0), "PremiereDate": premiered, "ReleaseDate": releasedate,
                "DateCreated": format_iso8601(date_created_ts), "CommunityRating": rating, "OfficialRating": mpaa,
                "RunTimeTicks": run_time_ticks, "Size": size, "Bitrate": est_bitrate,
                "Genres": genres_vec, "GenreItems": genre_items, "Tags": tags_vec, "TagItems": tag_items,
                "Taglines": if tagline.is_empty() { json!([]) } else { json!([tagline]) },
                "Studios": studios, "People": people,
                "ProviderIds": json!({ 
                    "Tmdb": tmdb_id, 
                    "Imdb": imdb_id,
                    "num": num
                }),
                "ImageTags": image_tags, "BackdropImageTags": backdrop_tags,
                "PrimaryImageAspectRatio": if aspect_ratio > 0.0 { json!(aspect_ratio) } else { json!(0.6666666666666666) },
                "ProductionLocations": country.split(" / ").filter(|s| !s.is_empty()).collect::<Vec<_>>(),
                "OriginalLanguage": json!(if !original_language.is_empty() { original_language } else { language }),
                "SetName": json!(set_name),
                "SeriesName": json!(if typ == "tvshows" { series } else { "".to_string() }),
                "Path": decode_url(&real_path), "Container": "mp4", "MediaSourceCount": media_sources.len(),
                "MediaSources": media_sources,
                "ProbeEngine": "rust-native", "LocationType": "FileSystem",
                "CanDelete": false, "CanDownload": false, "CanDeleteFromFolders": [], "HideMissingActorImages": true,
                "UserData": { "ItemId": id.to_string(), "Key": id.to_string(), "PlaybackPositionTicks": playback_ticks, "PlayCount": play_count, "IsFavorite": false, "Played": play_count > 0, "PlayedPercentage": if run_time_ticks > 0 { playback_ticks * 100 / run_time_ticks } else { 0 } }
            }))
        }
    ).ok();
    
    if let Some(item) = item_opt { Json(item).into_response() } else { StatusCode::NOT_FOUND.into_response() }
}

pub async fn emby_item_image(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, image_type)): AxumPath<(String, String)>) -> Response {
    emby_item_image_inner(pool, item_id, image_type, 0).await
}
pub async fn emby_item_image_with_index(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, image_type, index)): AxumPath<(String, String, String)>) -> Response {
    let idx = index.parse::<usize>().unwrap_or(0);
    emby_item_image_inner(pool, item_id, image_type, idx).await
}
#[allow(dead_code)]
pub async fn emby_item_image_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id, image_type)): AxumPath<(String, String, String)>) -> Response {
    emby_item_image_inner(pool, item_id, image_type, 0).await
}

pub async fn emby_playback_info(
    State(pool): State<Arc<Mutex<Connection>>>,
    AxumPath(item_id): AxumPath<i64>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let token = query.get("api_key").or_else(|| query.get("token")).or_else(|| query.get("X-Emby-Token"))
        .cloned()
        .or_else(|| headers.get("X-Emby-Token").and_then(|v| v.to_str().ok()).map(|s| s.to_string()))
        .or_else(|| headers.get("Authorization").and_then(|v| v.to_str().ok()).and_then(|v| {
            if let Some(t) = v.strip_prefix("Bearer ") { Some(t.trim().to_string()) }
            else if let Some(idx) = v.find("Token=\"") { let rest = &v[idx+7..]; rest.find('"').map(|e| rest[..e].to_string()) }
            else { None }
        }))
        .unwrap_or_default();
    
    let (parts_json_opt, runtime, media_streams_json, size) = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts, runtime, media_streams, size FROM items WHERE id = ?1").unwrap();
        stmt.query_row([item_id], |row| Ok((
            row.get::<_, String>(0).ok(),
            row.get::<_, i32>(1).unwrap_or(0),
            row.get::<_, String>(2).unwrap_or_else(|_| "[]".to_string()),
            row.get::<_, i64>(3).unwrap_or(0),
        ))).unwrap_or((None, 0, "[]".to_string(), 0))
    };
    let parts: Vec<String> = parts_json_opt.and_then(|json| serde_json::from_str(&json).ok()).unwrap_or_default();
    let mut exts: Vec<String> = Vec::new();
    for path in &parts {
        let ext = match tokio::fs::read_to_string(path).await {
            Ok(content) => { let url = content.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or(""); extract_extension_from_url(url) }
            Err(_) => "mp4".to_string(),
        };
        exts.push(ext);
    }
    let play_session_id = uuid::Uuid::new_v4().to_string();
    let run_time_ticks = (runtime as i64) * 60 * 10_000_000;
    
    let mut parsed_streams: Value = serde_json::from_str(&media_streams_json).unwrap_or(json!([]));
    sanitize_media_streams(&mut parsed_streams);
    let final_streams = parsed_streams;

    // 恢复起播预热逻辑
    let prefetch = {
        let conn = pool.lock().unwrap();
        conn.query_row("SELECT value FROM settings WHERE key = 'prefetch_playback'", [], |r| r.get::<_, String>(0)).unwrap_or_else(|_| "false".to_string()) == "true"
    };
    if prefetch {
        let ip = headers.get("X-Real-IP").and_then(|v| v.to_str().ok()).unwrap_or("unknown").to_string();
        let ua = headers.get(USER_AGENT).and_then(|v| v.to_str().ok()).unwrap_or("unknown").to_string();
        let cache_key = format!("{}|0|{}|{}", item_id, ip, ua);
        let needs_prefetch = {
            let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            let lock = cache.lock().unwrap();
            !lock.contains_key(&cache_key) || lock.get(&cache_key).map(|(_, exp)| Instant::now() >= *exp).unwrap_or(true)
        };

        if needs_prefetch && !parts.is_empty() {
            let path = parts[0].clone();
            let ua_clone = ua.clone();
            let client = GLOBAL_CLIENT.get_or_init(|| reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(15)).build().unwrap()).clone();
            tokio::spawn(async move {
                if let Ok(content) = tokio::fs::read_to_string(&path).await {
                    let url = content.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
                    if url.starts_with("http") {
                        if let Some(final_url) = resolve_qmediasync_url(&client, url, &ua_clone).await {
                            let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
                            let mut lock = cache.lock().unwrap();
                            lock.insert(cache_key, (final_url, Instant::now() + Duration::from_secs(300)));
                            println!("🔥 [PREFETCH] ID:{} 预热成功", item_id);
                        }
                    }
                }
            });
        }
    }

    let media_sources: Vec<Value> = parts.iter().enumerate().map(|(i, _path)| {
        let ext = &exts[i];
        let stream_url = format!("/Videos/{}/original.{}?MediaSourceId={}_cd{}&PlaySessionId={}&api_key={}", item_id, ext, item_id, i, play_session_id, token);
        json!({
            "Id": format!("{}_cd{}", item_id, i), "PlaySessionId": play_session_id.clone(),
            "Path": stream_url.clone(), "DirectStreamUrl": stream_url,
            "Protocol": "Http", "Container": ext, "Name": "Direct Play", "Type": "Default", "VideoType": "VideoFile",
            "IsRemote": true, "RunTimeTicks": run_time_ticks, "Size": size, "Bitrate": 0,
            "DefaultAudioStreamIndex": 1, "DefaultSubtitleStreamIndex": -1,
            "SupportsDirectPlay": true, "SupportsDirectStream": true, "SupportsProbing": true, "SupportsTranscoding": false,
            "SupportsExternalStream": false, "IsInfiniteStream": false,
            "RequiresOpening": false, "RequiresClosing": false, "RequiresLooping": false,
            "ReadAtNativeFramerate": false, "IgnoreDts": false, "IgnoreIndex": false, "GenPtsInput": false,
            "MediaStreams": final_streams.clone()
        })
    }).collect();
    Json(json!({ "MediaSources": media_sources, "PlaySessionId": play_session_id })).into_response()
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
                println!("⚡ [PLAY] 命中缓存，直接 302 到: {}", cached_url);
                return (StatusCode::FOUND, [(LOCATION, cached_url.clone())], "").into_response(); 
            } 
        }
    }

    let (strm_path_opt, item_title) = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts, title FROM items WHERE id = ?1").unwrap();
        let row_res = stmt.query_row([item_id], |row| Ok((
            row.get::<_, String>(0).ok(),
            row.get::<_, String>(1).unwrap_or_else(|_| "未知影片".to_string())
        )));
        match row_res {
            Ok((Some(json), title)) => {
                let path = serde_json::from_str::<Vec<String>>(&json).ok().and_then(|arr| arr.get(part_idx).cloned());
                (path, title)
            }
            _ => (None, "未知影片".to_string())
        }
    };
    
    let Some(strm_path) = strm_path_opt else { return StatusCode::NOT_FOUND.into_response() };
    println!("🎬 [PLAY] 收到播放请求: 《{}》 (ID: {})", item_title, item_id);

    let file = match tokio::fs::File::open(&strm_path).await { Ok(f) => f, Err(_) => return StatusCode::NOT_FOUND.into_response() };
    let mut contents = String::new();
    if file.take(MAX_STRM_SIZE).read_to_string(&mut contents).await.is_err() { return StatusCode::INTERNAL_SERVER_ERROR.into_response(); }
    let url = contents.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
    if !url.starts_with("http") { return StatusCode::NOT_FOUND.into_response(); }

    let client = GLOBAL_CLIENT.get_or_init(|| reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(15)).build().unwrap());
    let final_url = resolve_qmediasync_url(client, url, ua).await.unwrap_or_else(|| url.to_string());
    
    if final_url.contains("openlist") || final_url.contains("127.0.0.1") {
        println!("⚠️ [PLAY] 解析失败，最终地址仍是内网/OpenList 代理，拒绝重定向并跳过缓存。");
        return StatusCode::BAD_GATEWAY.into_response();
    }

    println!("🎯 [302 REDIRECT] 成功解析真实 CDN 地址: {}", &final_url[..std::cmp::min(final_url.len(), 100)]);

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

pub async fn emby_hide_from_resume(AxumPath((_user_id, _item_id)): AxumPath<(String, String)>, Query(query): Query<ResumeVisibilityQuery>, body: Bytes) -> Response {
    let query_hide = match query.hide.as_deref().map(str::parse::<bool>).transpose() { Ok(value) => value, Err(_) => return StatusCode::BAD_REQUEST.into_response() };
    let body_hide = if body.is_empty() { None } else { match serde_json::from_slice::<ResumeVisibilityBody>(&body) { Ok(body) => Some(body.hide), Err(_) => return StatusCode::BAD_REQUEST.into_response() } };
    let hidden = match (query_hide, body_hide) { (Some(q), Some(b)) if q != b => return StatusCode::BAD_REQUEST.into_response(), (Some(h), _) | (_, Some(h)) => h, (None, None) => return StatusCode::BAD_REQUEST.into_response() };
    Json(json!({ "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "HideFromResume": hidden })).into_response()
}

pub async fn emby_action() -> impl IntoResponse { StatusCode::OK.into_response() }
#[allow(dead_code)]
pub async fn emby_action_with_path(_: AxumPath<(String, String)>) -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_sessions() -> impl IntoResponse { Json(json!([])) }

pub async fn emby_playing() -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_playing_stopped() -> impl IntoResponse { StatusCode::OK.into_response() }

pub async fn emby_playing_progress(State(pool): State<Arc<Mutex<Connection>>>, body: Bytes) -> impl IntoResponse {
    if let Ok(val) = serde_json::from_slice::<Value>(&body) {
        if let Some(item_id_str) = val["ItemId"].as_str() {
            if let Ok(item_id) = item_id_str.parse::<i64>() {
                let ticks = val["PositionTicks"].as_i64().unwrap_or(0);
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                let conn = pool.lock().unwrap();
                let (runtime_mins, current_ticks): (i32, i64) = conn.query_row(
                    "SELECT runtime, playback_ticks FROM items WHERE id = ?1", 
                    [item_id], 
                    |r| Ok((r.get(0).unwrap_or(0), r.get(1).unwrap_or(0)))
                ).unwrap_or((0, 0));
                
                let total_ticks = (runtime_mins as i64) * 60 * 10_000_000;
                
                if total_ticks > 0 && ticks > (total_ticks * 90 / 100) {
                    // 超过 90%，标记已看完，并清空继续观看进度
                    let _ = conn.execute("UPDATE items SET play_count = play_count + 1, playback_ticks = 0, updated_at = ?1 WHERE id = ?2", [now, item_id]);
                    println!("💾 [PROGRESS] 标记为已看完: ItemId={}", item_id);
                } else if ticks > 10_000_000 || (ticks == 0 && current_ticks == 0) {
                    let _ = conn.execute("UPDATE items SET playback_ticks = ?1, updated_at = ?2 WHERE id = ?3", [ticks, now, item_id]);
                }
                println!("💾 [PROGRESS] 保存进度: ItemId={}, Ticks={}", item_id, ticks);
            }
        }
    }
    StatusCode::OK.into_response()
}

pub async fn emby_similar(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, title, type, year, plot, poster_path, rating FROM items WHERE id != ?1 ORDER BY RANDOM() LIMIT 6").unwrap();
    let items: Vec<Value> = stmt.query_map([item_id], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let has_poster = !row.get::<_, String>(5).unwrap_or_default().is_empty();
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(),
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(),
            "CommunityRating": row.get::<_, f64>(6).unwrap_or(0.0),
            "ImageTags": if has_poster { json!({"Primary": format!("tag_{}", id)}) } else { json!({}) },
            "BackdropImageTags": []
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_thumbnail_set(AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse {
    Json(json!({ "Id": item_id.to_string(), "ThumbnailSet": {} }))
}

pub async fn emby_virtual_folders(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, name, type FROM libraries").unwrap();
    let folders: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let name: String = row.get(1).unwrap_or_default();
        let lib_type: String = row.get(2).unwrap_or_default();
        Ok(json!({
            "Name": name, "Id": format!("lib_{}", id), "Guid": format!("lib_{}", id),
            "Path": format!("/media/lib_{}", id), "CollectionType": lib_type,
            "LibraryOptions": { "PathInfos": [], "EnablePhotos": true, "EnableRealtimeMonitor": false, "SaveLocalMetadata": true },
            "ItemId": format!("lib_{}", id), "PrimaryImageItemId": "", "RefreshStatus": "Idle"
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(folders)
}

pub async fn emby_item_counts(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let movie_count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE type='movies'", [], |r| r.get(0)).unwrap_or(0);
    let series_count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE type='tvshows'", [], |r| r.get(0)).unwrap_or(0);
    Json(json!({
        "MovieCount": movie_count, "SeriesCount": series_count,
        "EpisodeCount": 0, "ArtistCount": 0, "SongCount": 0, "AlbumCount": 0,
        "MusicVideoCount": 0, "BoxSetCount": 0, "BookCount": 0, "TrailerCount": 0
    }))
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
        let has_poster = !row.get::<_, String>(4).unwrap_or_default().is_empty();
        Ok(json!({
            "ItemId": id.to_string(), "Id": id.to_string(),
            "Name": row.get::<_, String>(1).unwrap_or_default(),
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "ImageTags": if has_poster { json!({"Primary": format!("tag_{}", id)}) } else { json!({}) },
            "MatchedTerm": term,
            "PrimaryImageTag": if has_poster { format!("tag_{}", id) } else { "".to_string() }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "SearchHints": hints, "TotalRecordCount": hints.len() }))
}

pub async fn emby_shows_next_up() -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }
pub async fn emby_shows_seasons(_: AxumPath<String>) -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }
pub async fn emby_shows_episodes(_: AxumPath<String>) -> impl IntoResponse { Json(json!({ "Items": [], "TotalRecordCount": 0 })) }
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
pub async fn emby_subtitle_stream(_: AxumPath<(String, String)>) -> impl IntoResponse { StatusCode::OK.into_response() }
pub async fn emby_subtitle_stream_with_source(_: AxumPath<(String, String, String)>) -> impl IntoResponse { StatusCode::OK.into_response() }

pub async fn emby_display_preferences(_: AxumPath<String>) -> impl IntoResponse {
    Json(json!({"Id": "default", "ViewType": "Poster", "SortBy": "SortName", "PrimaryImageHeight": 250, "PrimaryImageWidth": 250, "ScrollDirection": "Horizontal"}))
}

pub async fn emby_item_image_user_only(_: AxumPath<(String, String)>) -> Response {
    StatusCode::NOT_FOUND.into_response()
}

pub async fn emby_person(AxumPath(person_id): AxumPath<String>) -> impl IntoResponse {
    let decoded = decode_url(&person_id);
    let name = decoded.replace("person_", "");
    Json(json!({
        "Name": name,
        "ServerId": "rust-em-server",
        "Id": person_id,
        "Type": "Person",
        "IsFolder": false,
        "ImageTags": {},
        "BackdropImageTags": [],
        "UserData": { "PlayCount": 0, "IsFavorite": false, "Played": false }
    }))
}

#[allow(dead_code)]
pub async fn emby_person_items(
    State(pool): State<Arc<Mutex<Connection>>>,
    AxumPath(person_id): AxumPath<String>,
    Query(mut query): Query<EmbyItemsQuery>
) -> impl IntoResponse {
    query.person_ids = Some(person_id);
    query.studios = None;
    emby_items_inner(pool, query).await
}

pub async fn emby_studio(AxumPath(studio_id): AxumPath<String>) -> impl IntoResponse {
    let decoded = decode_url(&studio_id);
    let name = decoded.replace("studio_", "");
    Json(json!({
        "Name": name,
        "ServerId": "rust-em-server",
        "Id": studio_id,
        "Type": "Studio",
        "IsFolder": false,
        "ImageTags": {},
        "BackdropImageTags": [],
        "UserData": { "PlayCount": 0, "IsFavorite": false, "Played": false }
    }))
}

#[allow(dead_code)]
pub async fn emby_studio_items(
    State(pool): State<Arc<Mutex<Connection>>>,
    AxumPath(studio_id): AxumPath<String>,
    Query(mut query): Query<EmbyItemsQuery>
) -> impl IntoResponse {
    query.studio_ids = Some(studio_id);
    query.studios = None;
    emby_items_inner(pool, query).await
}

async fn emby_item_image_inner(pool: Arc<Mutex<Connection>>, item_id: String, image_type: String, index: usize) -> Response {
    if item_id.starts_with("person_") || item_id.starts_with("studio_") {
        let png_bytes: [u8; 67] = [
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
            0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
            0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
            0x42, 0x60, 0x82
        ];
        return (StatusCode::OK, [(CONTENT_TYPE, "image/png")], png_bytes).into_response();
    }

    let path_opt = {
        let conn = pool.lock().unwrap();

        if item_id.starts_with("boxset_") {
            let mut found_set_name = String::new();
            let mut names = Vec::new();
            if let Ok(mut stmt_all) = conn.prepare("SELECT DISTINCT set_name FROM items WHERE set_name != ''") {
                if let Ok(mapped) = stmt_all.query_map([], |r| r.get::<_, String>(0)) {
                    for s in mapped.flatten() { names.push(s); }
                }
            }
            for full_str in names {
                for name in full_str.split(" / ").filter(|s| !s.trim().is_empty()) {
                    let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                    if format!("boxset_{:x}", hash) == item_id { found_set_name = name.to_string(); break; }
                }
                if !found_set_name.is_empty() { break; }
            }
            if !found_set_name.is_empty() {
                if let Ok(mut stmt) = conn.prepare("SELECT poster_path FROM items WHERE (set_name = ?1 OR set_name LIKE ?2 OR set_name LIKE ?3 OR set_name LIKE ?4) AND poster_path != '' LIMIT 1") {
                    stmt.query_row([&found_set_name, &format!("{} / %", found_set_name), &format!("% / {}", found_set_name), &format!("% / {} / %", found_set_name)], |r| r.get::<_, String>(0)).ok()
                } else { None }
            } else { None }
        } else {
            let Ok(id) = item_id.parse::<i64>() else { return StatusCode::NOT_FOUND.into_response(); };
            match image_type.to_lowercase().as_str() {
                "primary" | "poster" => conn.query_row("SELECT poster_path FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).ok(),
                "thumb" => conn.query_row("SELECT thumb_path FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).ok(),
                "logo" => conn.query_row("SELECT logo_path FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).ok(),
                "banner" => conn.query_row("SELECT banner_path FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).ok(),
                "backdrop" | "fanart" => {
                    let mut stmt = conn.prepare("SELECT stills, fanart_path FROM items WHERE id = ?1").unwrap();
                    stmt.query_row([id], |row| Ok((
                        row.get::<_, String>(0).unwrap_or_default(),
                        row.get::<_, String>(1).unwrap_or_default()
                    ))).map(|(stills_json, fanart_path)| {
                        let mut backdrops: Vec<String> = Vec::new();
                        if !fanart_path.is_empty() { backdrops.push(fanart_path); }
                        let stills: Vec<String> = serde_json::from_str(&stills_json).unwrap_or_default();
                        backdrops.extend(stills);
                        backdrops.get(index).cloned()
                    }).unwrap_or(None)
                },
                _ => None,
            }
        }
    };

    if let Some(path_str) = path_opt {
        if !path_str.is_empty() {
            return serve_file(&path_str).await;
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

pub async fn emby_scheduled_tasks() -> impl IntoResponse {
    Json(json!([
        {
            "Name": "Scan Media Library",
            "State": "Idle",
            "Id": "RefreshLibrary",
            "LastExecutionResult": {
                "StartTimeUtc": "2024-01-01T00:00:00.0000000Z",
                "EndTimeUtc": "2024-01-01T00:00:01.0000000Z",
                "Status": "Completed",
                "Error": ""
            },
            "Triggers": [
                {
                    "Type": "IntervalTrigger",
                    "IntervalTicks": 864000000000i64
                }
            ],
            "Description": "Scans your media library for new and updated files.",
            "Category": "Library",
            "IsHidden": false,
            "Key": "RefreshLibrary"
        }
    ]))
}

pub async fn emby_refresh_library(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let status = SCAN_STATUS.get_or_init(|| Mutex::new(false));
    *status.lock().unwrap() = true;
    println!("📡 [TASK] 客户端触发了计划任务：全库扫描");

    let pool_clone = pool.clone();
    tokio::task::spawn_blocking(move || {
        // 注意：这里用大括号限制 conn 的作用域，查询完立刻释放数据库锁，防止后续 scan_directory 死锁
        let libs = {
            let conn = pool_clone.lock().unwrap();
            let mut stmt = match conn.prepare("SELECT id, path, type FROM libraries") {
                Ok(s) => s,
                Err(_) => return,
            };
            let libs: Vec<(i64, String, String)> = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .filter_map(Result::ok)
                .collect();
            libs
        }; // 锁在这里释放

        for (id, path_json, lib_type) in libs {
            if let Ok(paths) = serde_json::from_str::<Vec<String>>(&path_json) {
                for path in paths {
                    scan_directory(pool_clone.clone(), id, lib_type.clone(), path);
                }
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

pub async fn emby_activity_log() -> impl IntoResponse {
    Json(json!({ "Items": [], "TotalRecordCount": 0 }))
}

pub async fn emby_genres(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT genres FROM items WHERE genres != ''".to_string();
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(&format!(" AND library_id = {}", lib_id));
            }
        }
    }
    
    let mut genres_set = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for g_str in mapped.flatten() {
                for g in g_str.split(" / ") {
                    let name = g.trim();
                    if !name.is_empty() { genres_set.insert(name.to_string()); }
                }
            }
        }
    }

    let items: Vec<Value> = genres_set.into_iter().map(|g| {
        json!({
            "Name": g, "Id": format!("genre_{}", g), "ServerId": "rust-em-server",
            "Type": "Genre", "IsFolder": false, "MediaType": "Video",
            "ImageTags": {}, "BackdropImageTags": []
        })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_mark_favorite(
    State(pool): State<Arc<Mutex<Connection>>>,
    AxumPath((_user_id, item_id)): AxumPath<(String, i64)>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let _ = conn.execute("UPDATE items SET is_favorite = 1 WHERE id = ?1", [item_id]);
    StatusCode::OK.into_response()
}

pub async fn emby_unmark_favorite(
    State(pool): State<Arc<Mutex<Connection>>>,
    AxumPath((_user_id, item_id)): AxumPath<(String, i64)>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let _ = conn.execute("UPDATE items SET is_favorite = 0 WHERE id = ?1", [item_id]);
    StatusCode::OK.into_response()
}

pub async fn emby_tags(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT tags FROM items WHERE tags != ''".to_string();
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(&format!(" AND library_id = {}", lib_id));
            }
        }
    }
    let mut set = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for t_str in mapped.flatten() {
                for t in t_str.split(" / ") {
                    let name = t.trim();
                    if !name.is_empty() { set.insert(name.to_string()); }
                }
            }
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    
    let items: Vec<Value> = names.into_iter().map(|name| json!({
        "Name": name, "Id": format!("tag_{}", name), "ServerId": "rust-em-server",
        "Type": "Tag", "IsFolder": false, "MediaType": "Video",
        "PrimaryImageAspectRatio": 0.6666666666666666,
        "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {},
        "LocationType": "FileSystem",
        "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 }
    })).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_years(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT DISTINCT year FROM items WHERE year != ''".to_string();
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(&format!(" AND library_id = {}", lib_id));
            }
        }
    }
    sql.push_str(" ORDER BY year DESC");
    let mut items = Vec::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for y in mapped.flatten() {
                items.push(json!({
                    "Name": y, "Id": format!("year_{}", y), "ServerId": "rust-em-server",
                    "Type": "Year", "IsFolder": false, "MediaType": "Video",
                    "PrimaryImageAspectRatio": 0.6666666666666666,
                    "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {},
                    "LocationType": "FileSystem",
                    "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 }
                }));
            }
        }
    }
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_studios(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT studios FROM items WHERE studios != ''".to_string();
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(&format!(" AND library_id = {}", lib_id));
            }
        }
    }
    let mut set = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for st_str in mapped.flatten() {
                for st in st_str.split(" / ") {
                    let name = st.trim();
                    if !name.is_empty() { set.insert(name.to_string()); }
                }
            }
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    
    let items: Vec<Value> = names.into_iter().map(|name| json!({
        "Name": name, "Id": format!("studio_{}", name), "ServerId": "rust-em-server",
        "Type": "Studio", "IsFolder": false, "MediaType": "Video",
        "PrimaryImageAspectRatio": 0.6666666666666666,
        "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {},
        "LocationType": "FileSystem",
        "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 }
    })).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_official_ratings(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>
) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut sql = "SELECT DISTINCT mpaa FROM items WHERE mpaa != ''".to_string();
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(&format!(" AND library_id = {}", lib_id));
            }
        }
    }
    sql.push_str(" ORDER BY mpaa ASC");
    let mut items = Vec::new();
    if let Ok(mut stmt) = conn.prepare(&sql) {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for m in mapped.flatten() {
                items.push(json!({
                    "Name": m, "Id": format!("mpaa_{}", m), "ServerId": "rust-em-server",
                    "Type": "OfficialRating", "IsFolder": false, "MediaType": "Video",
                    "PrimaryImageAspectRatio": 0.6666666666666666,
                    "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {},
                    "LocationType": "FileSystem",
                    "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 }
                }));
            }
        }
    }
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}
