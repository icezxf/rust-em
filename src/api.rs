use axum::{
    extract::{State, Request, Json, Query, Path as AxumPath},
    http::{StatusCode, header::{LOCATION, USER_AGENT, CONTENT_TYPE}, HeaderMap, Method},
    middleware::Next, response::{IntoResponse, Response}, body::Bytes,
    Extension,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex, OnceLock};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

static TOKENS: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();
static STRM_CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
static GLOBAL_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
static SCAN_STATUS: OnceLock<Mutex<bool>> = OnceLock::new();
static LAST_SCAN_TIME: OnceLock<Mutex<String>> = OnceLock::new();
const MAX_STRM_SIZE: u64 = 16 * 1024;

pub async fn logger_middleware(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();
    // 屏蔽垃圾日志
    if path.ends_with(".css") || path.ends_with(".js") || path.ends_with(".png") || path.ends_with(".jpg") 
        || (path.contains("/Images/") && !path.contains("/Images/Primary")) || path.contains("/api/images/") 
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
    if path.contains("/Images/") || path.ends_with("/Ping") || path.ends_with("/Info/Public") || path.ends_with("/Users/Public") || path.ends_with("/AuthenticateByName") || path == "/api/login" || path.contains("websocket") {
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
        let mut user_id_opt = None;
        if let Some(tokens) = TOKENS.get() {
            if let Some(uid) = tokens.lock().unwrap().get(&token).cloned() {
                user_id_opt = Some(uid);
            }
        }
        if let Some(user_id) = user_id_opt {
            if path.starts_with("/api/") && user_id != 1 {
                return Err(StatusCode::FORBIDDEN);
            }
            let mut req = req;
            req.extensions_mut().insert(user_id);
            return Ok(next.run(req).await);
        }
    }
    Err(StatusCode::UNAUTHORIZED)
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
            } else { result.push(b'%'); }
        } else if b == b'+' {
            result.push(b' ');
        } else { result.push(b); }
    }
    String::from_utf8_lossy(&result).into_owned()
}

fn to_hex(s: &str) -> String { s.as_bytes().iter().map(|b| format!("{:02x}", b)).collect() }
fn from_hex(s: &str) -> Option<String> {
    if !s.chars().all(|c| c.is_ascii_hexdigit()) { return None; }
    let mut bytes = Vec::new();
    for i in (0..s.len()).step_by(2) {
        if i + 1 < s.len() {
            if let Ok(b) = u8::from_str_radix(&s[i..i+2], 16) { bytes.push(b); } else { return None; }
        }
    }
    String::from_utf8(bytes).ok()
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
        resp.headers().get(reqwest::header::LOCATION).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
    } else {
        let text = resp.text().await.unwrap_or_default();
        if let Some(start) = text.find("href=\"") {
            let rest = &text[start + 6..];
            if let Some(end) = rest.find('\"') { Some(rest[..end].replace("&amp;", "&")) } else { None }
        } else if text.starts_with("http") { Some(text.trim().to_string()) } 
        else { None }
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
                            Value::Number(n) => n.as_f64().unwrap_or(0.0), _ => 0.0,
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

fn get_cover_tag(folder: &str, prefix: &str, name: &str) -> String {
    let mut sanitized_name = String::new();
    for c in name.chars() {
        if c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|' {
            sanitized_name.push('_');
        } else {
            sanitized_name.push(c);
        }
    }
    let name_trimmed = sanitized_name.trim();
    let tag = format!("{}_{}_cover", prefix, name_trimmed);

    if prefix == "boxset" {
        let dir = std::path::PathBuf::from("data/cache/covers").join(folder);
        if let Ok(entries) = std::fs::read_dir(&dir) {
            let match_prefix = format!("boxset_{}_", name_trimmed);
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.starts_with(&match_prefix) && fname.ends_with(".jpg") {
                    if let Ok(meta) = entry.metadata() {
                        if let Ok(mtime) = meta.modified() {
                            if let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH) {
                                return format!("{}_{}", tag, dur.as_secs());
                            }
                        }
                    }
                }
            }
        }
    } else {
        let path = std::path::PathBuf::from("data/cache/covers").join(folder).join(format!("{}_{}.jpg", prefix, name_trimmed));
        if let Ok(meta) = std::fs::metadata(path) {
            if let Ok(mtime) = meta.modified() {
                if let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH) {
                    return format!("{}_{}", tag, dur.as_secs());
                }
            }
        }
    }
    tag
}

fn get_allowed_libs(conn: &Connection, user_id: i64) -> Option<Vec<i64>> {
    if user_id == 1 { return None; }
    let mut stmt = match conn.prepare("SELECT library_id FROM user_library_access WHERE user_id = ?1") {
        Ok(s) => s,
        Err(_) => return Some(vec![]),
    };
    let mut libs = Vec::new();
    if let Ok(mapped) = stmt.query_map([user_id], |r| r.get::<_, i64>(0)) {
        for id in mapped.flatten() {
            libs.push(id);
        }
    }
    Some(libs)
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

fn parse_nfo_stream_details(nfo_text: &str) -> Value {
    let mut streams = Vec::new();
    let extract_tag = |text: &str, tag: &str| -> Option<String> {
        let start_tag = format!("<{}>", tag); let end_tag = format!("</{}>", tag);
        if let Some(start) = text.find(&start_tag) {
            if let Some(end) = text[start..].find(&end_tag) { return Some(text[start + start_tag.len()..start + end].trim().to_string()); }
        }
        None
    };

    if let Some(v_start) = nfo_text.find("<video>") {
        if let Some(v_end) = nfo_text.find("</video>") {
            let v_text = &nfo_text[v_start..v_end];
            let codec = extract_tag(v_text, "codec").unwrap_or_default();
            let w = extract_tag(v_text, "width").and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let h = extract_tag(v_text, "height").and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let bitrate = extract_tag(v_text, "bitrate").and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let framerate_str = extract_tag(v_text, "framerate").unwrap_or_default();
            let framerate = if let Some((n, d)) = framerate_str.split_once('/') {
                let num = n.parse::<f64>().unwrap_or(0.0); let den = d.parse::<f64>().unwrap_or(1.0);
                if den > 0.0 { num / den } else { 0.0 }
            } else { framerate_str.parse::<f64>().unwrap_or(0.0) };
            streams.push(json!({ "Type": "Video", "Codec": codec, "Width": w, "Height": h, "BitRate": bitrate, "AverageFrameRate": framerate, "RealFrameRate": framerate, "IsDefault": true }));
        }
    }

    let mut search_idx = 0;
    while let Some(a_start_rel) = nfo_text[search_idx..].find("<audio>") {
        let a_start = search_idx + a_start_rel;
        if let Some(a_end_rel) = nfo_text[a_start..].find("</audio>") {
            let a_end = a_start + a_end_rel; let a_text = &nfo_text[a_start..a_end];
            let codec = extract_tag(a_text, "codec").unwrap_or_default();
            let ch = extract_tag(a_text, "channels").and_then(|s| s.parse::<i64>().ok()).unwrap_or(2);
            let bitrate = extract_tag(a_text, "bitrate").and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let sample_rate = extract_tag(a_text, "samplingrate").and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
            let lang = extract_tag(a_text, "language").unwrap_or_default();
            streams.push(json!({ "Type": "Audio", "Codec": codec, "Channels": ch, "BitRate": bitrate, "SampleRate": sample_rate, "Language": lang, "IsDefault": true }));
            search_idx = a_end + 8;
        } else { break; }
    }

    search_idx = 0;
    while let Some(s_start_rel) = nfo_text[search_idx..].find("<subtitle>") {
        let s_start = search_idx + s_start_rel;
        if let Some(s_end_rel) = nfo_text[s_start..].find("</subtitle>") {
            let s_end = s_start + s_end_rel; let s_text = &nfo_text[s_start..s_end];
            let codec = extract_tag(s_text, "codec").unwrap_or_default();
            let lang = extract_tag(s_text, "language").unwrap_or_default();
            streams.push(json!({"Type": "Subtitle", "Codec": codec, "Language": lang, "IsDefault": false}));
            search_idx = s_end + 11;
        } else { break; }
    }
    json!(streams)
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

        for nfo in &nfo_candidates {
            if nfo.exists() {
                if let Some(meta) = crate::nfo::read_nfo(nfo) {
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
                    
                    if runtime <= 0 {
                        if let Ok(nfo_text) = std::fs::read_to_string(nfo) {
                            if let Some(start) = nfo_text.find("<durationinseconds>") {
                                let rest = &nfo_text[start + 19..];
                                if let Some(end) = rest.find("</durationinseconds>") {
                                    if let Ok(secs) = rest[..end].trim().parse::<i32>() { runtime = (secs + 59) / 60; }
                                }
                            }
                            if runtime <= 0 {
                                if let Some(start) = nfo_text.find("<duration>") {
                                    let rest = &nfo_text[start + 10..];
                                    if let Some(end) = rest.find("</duration>") {
                                        if let Ok(mins) = rest[..end].trim().parse::<i32>() { runtime = mins; }
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }
        let mut final_poster = nfo_poster; let mut final_fanart = nfo_fanart;
        let mut logo = String::new(); let mut thumb = String::new(); let mut banner = String::new();
        if let Ok(entries) = std::fs::read_dir(&parent) {
            for entry in entries.flatten() {
                let p = entry.path(); if !p.is_file() { continue; }
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
        let (probe_size, mut media_streams_json_str) = if mediainfo_json_path.exists() {
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

        if media_streams_json_str == "[]" {
            for nfo in &nfo_candidates {
                if nfo.exists() {
                    if let Ok(nfo_text) = std::fs::read_to_string(nfo) {
                        if nfo_text.contains("<streamdetails>") {
                            let parsed_streams = parse_nfo_stream_details(&nfo_text);
                            if parsed_streams.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                                media_streams_json_str = parsed_streams.to_string();
                                break;
                            }
                        }
                    }
                }
            }
        }

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
    
    if let Some(lid) = query.library_id { sql.push_str(" AND library_id = ?"); params.push(lid.to_string()); }
    if let Some(lt) = &query.lib_type { sql.push_str(" AND type = ?"); params.push(lt.to_string()); }
    
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
    
    // 💡 核心修复：改用 id = 1 来验证，这样哪怕管理员改了名字，只要旧密码对，就能继续修改
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
        Ok(json!({ "Name": name, "Id": format!("lib_{}", id), "ServerId": "rust-em-server", "Type": "CollectionFolder", "IsFolder": true, "MediaType": "Video", "CollectionType": if lib_type == "movies" { "movies" } else { "tvshows" }, "ImageTags": {"Primary": get_cover_tag("library", "lib", &id.to_string())}, "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false } }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_items_latest(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    
    let allowed_libs = get_allowed_libs(&conn, req_user_id);
    if let Some(libs) = &allowed_libs {
        if libs.is_empty() { return Json(json!([])).into_response(); }
    }

    let mut sql = "SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, fanart_path FROM items WHERE 1=1".to_string();
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
            }
        }
    }
    
    sql.push_str(" ORDER BY id DESC LIMIT 16"); 
    
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(_) => return Json(json!([])).into_response(),
    };
    
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
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0), "RunTimeTicks": total_ticks, "Size": size,
            "DateCreated": date_str.clone(), "EndDate": date_str, "PrimaryImageAspectRatio": 0.6666666666666666,
            "Container": "mp4", "CanDelete": false, "CanDownload": false, "LocationType": "FileSystem", "Status": "Released",
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>(6).unwrap_or_default()),
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Size": size, "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks }],
            "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0.0 }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    
    Json(items).into_response()
}

pub async fn emby_items_resume(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(_user_id): AxumPath<String>, Query(query): Query<HashMap<String, String>>, Extension(req_user_id): Extension<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let allowed_libs = get_allowed_libs(&conn, req_user_id);
    if let Some(libs) = &allowed_libs {
        if libs.is_empty() { return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); }
    }

    let mut sql = "SELECT items.id, items.title, items.type, items.year, items.plot, items.poster_path, items.file_path, items.runtime, items.rating, items.mpaa, items.genres, items.premiered, items.tmdb_id, items.imdb_id, items.size, items.mtime, items.sort_title, items.original_title, ud.playback_position_ticks, items.fanart_path, ud.play_count, ud.last_played_date FROM items JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = ? WHERE ud.playback_position_ticks > 0".to_string();
    let mut params: Vec<String> = vec![req_user_id.to_string()];

    if let Some(libs) = &allowed_libs {
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND items.library_id IN ({})", libs_str.join(",")));
    }
    if let Some(pid) = query.get("ParentId") {
        if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() {
                sql.push_str(" AND items.library_id = ?");
                params.push(lib_id.to_string());
            }
        }
    }
    sql.push_str(" ORDER BY ud.last_played_date DESC LIMIT 30");
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response() };
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

        let mut runtime_mins: i32 = row.get(7).unwrap_or(0);
        if runtime_mins <= 0 {
            let current_pct = playback_ticks / 10_000_000 / 60;
            runtime_mins = if current_pct > 0 { current_pct as i32 + 30 } else { 100 };
        }
        let total_ticks: i64 = (runtime_mins as i64) * 60 * 10_000_000;
        let play_count: i32 = row.get(20).unwrap_or(0);
        let last_played_str: String = row.get(21).unwrap_or_default();
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0), "RunTimeTicks": total_ticks,
            "DateCreated": format_iso8601(row.get::<_, i64>(15).unwrap_or(0)), "PrimaryImageAspectRatio": 0.6666666666666666,
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>(6).unwrap_or_default()),
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true }],
            "UserData": {
                "PlaybackPositionTicks": playback_ticks,
                "PlayCount": play_count,
                "IsFavorite": false,
                "Played": play_count > 0,
                "PlayedPercentage": if total_ticks > 0 { (playback_ticks as f64 / total_ticks as f64) * 100.0 } else { 0.0 },
                "LastPlayedDate": if !last_played_str.is_empty() {
                    if let Ok(ts) = last_played_str.parse::<i64>() { json!(format_iso8601(ts)) } else { json!(null) }
                } else { json!(null) },
                "UnplayedItemCount": 0
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
                let is_fav = if let Ok(count) = conn.query_row("SELECT COUNT(1) FROM boxset_favorites WHERE user_id = ?1 AND boxset_id = ?2", rusqlite::params![req_user_id, &item_id.to_string()], |r| r.get::<_, i64>(0)) { count > 0 } else { false };
                boxsets.push(json!({ "Name": name, "Id": boxset_id, "ServerId": "rust-em-server", "Type": "BoxSet", "IsFolder": true, "MediaType": "Video", "ImageTags": {"Primary": format!("{}_cover", boxset_id)}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": is_fav, "Played": false } }));
            }
            return Json(json!({ "Items": boxsets, "TotalRecordCount": boxsets.len() })).into_response();
        }
        return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
    }

    if let Some(types) = &query.include_item_types {
        let t_lower = types.to_lowercase();
        let is_dict_only = !t_lower.contains("movie") && !t_lower.contains("series");

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
                json!({ "Name": name, "Id": format!("tag_{}", name), "ServerId": "rust-em-server", "Type": "Tag", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": get_cover_tag("tag", "tag", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
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
                json!({ "Name": name, "Id": format!("genre_{}", name), "ServerId": "rust-em-server", "Type": "Genre", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": get_cover_tag("genre", "genre", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
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
                        items.push(json!({ "Name": y, "Id": y, "ServerId": "rust-em-server", "Type": "Year", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } }));
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
                        items.push(json!({ "Name": m, "Id": m, "ServerId": "rust-em-server", "Type": "OfficialRating", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } }));
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
                        
                        if is_fav_query && !is_fav {
                            continue;
                        }

                        boxsets.push(json!({ "Name": name, "Id": boxset_id, "ServerId": "rust-em-server", "Type": "BoxSet", "IsFolder": true, "MediaType": "Video", "ImageTags": {"Primary": get_cover_tag("boxset", "boxset", &hash_hex)}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": is_fav, "Played": false } }));
                    }
                }
            }
            return Json(json!({ "Items": boxsets, "TotalRecordCount": boxsets.len() })).into_response();
        }
    }

    // 虚拟文件夹递归
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
            } else if pid.starts_with("folder_") { from_hex(pid.trim_start_matches("folder_")) } else { None }
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
                for (name, abs_path) in folders_set {
                    folder_items.push(json!({ "Name": name, "Id": format!("folder_{}", to_hex(&abs_path)), "ServerId": "rust-em-server", "Type": "Folder", "IsFolder": true, "MediaType": "Video", "ImageTags": {}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false } }));
                }
                return Json(json!({ "Items": folder_items, "TotalRecordCount": folder_items.len() })).into_response();
            }
        }
    }

    let allowed_libs = get_allowed_libs(&conn, req_user_id);
    if let Some(libs) = &allowed_libs {
        if libs.is_empty() {
            return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
        }
    }

    let mut sql = "SELECT id, title, type, year, plot, poster_path, file_path, runtime, rating, mpaa, genres, premiered, tmdb_id, imdb_id, size, mtime, sort_title, original_title, IFNULL(ud.playback_position_ticks, 0), fanart_path, date_created, actors, directors, tags, studios, IFNULL(ud.play_count, 0), IFNULL(ud.is_favorite, 0), IFNULL(ud.last_played_date, '') FROM items LEFT JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = ? WHERE 1=1".to_string();
    let mut params: Vec<String> = Vec::new();
    params.push(req_user_id.to_string());

    if let Some(libs) = &allowed_libs {
        let libs_str: Vec<String> = libs.iter().map(|id| id.to_string()).collect();
        sql.push_str(&format!(" AND library_id IN ({})", libs_str.join(",")));
    }

    if let Some(types) = &query.include_item_types {
        let t = types.to_lowercase();
        if t.contains("movie") { sql.push_str(" AND type = 'movies'"); } 
        else if t.contains("series") || t.contains("episode") { sql.push_str(" AND type = 'tvshows'"); }
    }

    if let Some(filters) = &query.filters {
        if filters.contains("IsPlayed") { sql.push_str(" AND ud.play_count > 0"); } 
        else if filters.contains("IsUnplayed") { sql.push_str(" AND IFNULL(ud.play_count, 0) = 0"); }
        if filters.contains("IsFavorite") { sql.push_str(" AND ud.is_favorite = 1"); }
    }

    if let Some(pid) = &query.parent_id {
        if pid.starts_with("folder_") {
            if let Some(abs_path) = from_hex(pid.trim_start_matches("folder_")) { 
                sql.push_str(" AND file_path LIKE ?"); 
                params.push(format!("{}/%", abs_path.trim_end_matches('/'))); 
            } else { 
                return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response(); 
            }
        } else if pid.starts_with("lib_") {
            if let Ok(lib_id) = pid.trim_start_matches("lib_").parse::<i64>() { 
                sql.push_str(" AND library_id = ?"); 
                params.push(lib_id.to_string()); 
            }
        } else if pid.starts_with("boxset_") {
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
                    if format!("boxset_{:x}", hash) == *pid { found_set_name = name.to_string(); break; }
                }
                if !found_set_name.is_empty() { break; }
            }
            if !found_set_name.is_empty() {
                sql.push_str(" AND (set_name = ? OR set_name LIKE ? OR set_name LIKE ? OR set_name LIKE ?)");
                params.push(found_set_name.clone());
                params.push(format!("{} / %", found_set_name));
                params.push(format!("% / {}", found_set_name));
                params.push(format!("% / {} / %", found_set_name));
            } else {
                return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
            }
        }
    }

    let search_term_raw = query.search_term.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let mut search_person_items: Vec<Value> = Vec::new();
    let mut is_search = false;
    let mut search_term_len = 0usize;

    if let Some(term) = search_term_raw {
        search_term_len = term.chars().count();
        if search_term_len <= 1 {
            return Json(json!({ "Items": [], "TotalRecordCount": 0 })).into_response();
        }
        is_search = true;

        let pattern = format!("%{}%", term);
        sql.push_str(" AND (title LIKE ? OR original_title LIKE ? OR tags LIKE ? OR actors LIKE ? OR directors LIKE ?)");
        params.push(pattern.clone()); params.push(pattern.clone()); params.push(pattern.clone()); params.push(pattern.clone()); params.push(pattern.clone());

        let types_lower = query.include_item_types.as_deref().map(|t| t.to_lowercase()).unwrap_or_default();
        let wants_person = types_lower.contains("person");
        let wants_media = types_lower.contains("movie") || types_lower.contains("series");
        let only_person = wants_person && !wants_media;

        if wants_person {
            let lib_id_opt = query.parent_id.as_deref().filter(|p| p.starts_with("lib_")).and_then(|p| p.trim_start_matches("lib_").parse::<i64>().ok());
            let mut person_sql = String::from("SELECT actors, directors FROM items WHERE (actors LIKE ? OR directors LIKE ?)");
            if lib_id_opt.is_some() { person_sql.push_str(" AND library_id = ?"); }
            person_sql.push_str(" LIMIT 500");

            if let Ok(mut pstmt) = conn.prepare(&person_sql) {
                let mut names_set: HashSet<String> = HashSet::new();
                let mut p_params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(pattern.clone()), Box::new(pattern.clone())];
                if let Some(lid) = lib_id_opt { p_params.push(Box::new(lid)); }
                let p_refs: Vec<&dyn rusqlite::ToSql> = p_params.iter().map(|b| b.as_ref()).collect();

                if let Ok(mapped) = pstmt.query_map(rusqlite::params_from_iter(p_refs), |r| Ok((r.get::<_, String>(0).unwrap_or_default(), r.get::<_, String>(1).unwrap_or_default()))) {
                    for (actors, directors) in mapped.flatten() {
                        if let Ok(arr) = serde_json::from_str::<Vec<Value>>(&actors) {
                            for a in arr {
                                if let Some(n) = a.get("name").and_then(|v| v.as_str()) {
                                    if n.contains(term) { names_set.insert(n.to_string()); }
                                }
                            }
                        }
                        for d in directors.split(" / ") {
                            let d = d.trim();
                            if !d.is_empty() && d.contains(term) { names_set.insert(d.to_string()); }
                        }
                    }
                }
                for name in names_set {
                    search_person_items.push(json!({
                        "Name": name, "Id": format!("person_{}", name), "ServerId": "rust-em-server", "Type": "Person", "IsFolder": false, "MediaType": "Video",
                        "ImageTags": {}, "BackdropImageTags": [], "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false }
                    }));
                }
            }
        }

        if only_person {
            let final_total = if search_term_len >= 4 { search_person_items.len() as i64 } else { 0 };
            return Json(json!({ "Items": search_person_items, "TotalRecordCount": final_total })).into_response();
        }
    }

    let split_ids = |val: &str| -> Vec<String> {
        decode_url(val).split(|c| c == ',' || c == '|').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()
    };

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
        if !or_clauses.is_empty() { sql.push_str(&format!(" AND ({})", or_clauses.join(" OR "))); }
    }

    if !studio_filters.is_empty() {
        let mut or_clauses = Vec::new();
        for raw_id in studio_filters {
            let name = raw_id.replace("studio_", "").trim().to_string();
            if name.is_empty() { continue; }
            or_clauses.push("(studios = ? OR studios LIKE ? OR studios LIKE ? OR studios LIKE ?)");
            params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name));
        }
        if !or_clauses.is_empty() { sql.push_str(&format!(" AND ({})", or_clauses.join(" OR "))); }
    }

    if !person_filters.is_empty() {
        let mut or_clauses = Vec::new();
        for raw_id in person_filters {
            let name = raw_id.replace("person_", "").trim().to_string();
            if name.is_empty() { continue; }
            or_clauses.push("(directors = ? OR directors LIKE ? OR directors LIKE ? OR directors LIKE ? OR actors LIKE ? OR actors LIKE ?)");
            params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name)); params.push(format!("%\"name\":\"{}\"%", name)); params.push(format!("%\"name\": \"{}\"%", name));
        }
        if !or_clauses.is_empty() { sql.push_str(&format!(" AND ({})", or_clauses.join(" OR "))); }
    }

    for raw_id in tag_filters {
        let name = raw_id.replace("tag_", "").trim().to_string();
        if name.is_empty() { continue; }
        sql.push_str(" AND (tags = ? OR tags LIKE ? OR tags LIKE ? OR tags LIKE ?)");
        params.push(name.clone()); params.push(format!("{} / %", name)); params.push(format!("% / {}", name)); params.push(format!("% / {} / %", name));
    }

    let total: i64 = if is_search {
        0
    } else {
        let from_idx = sql.find(" FROM items").unwrap_or(0);
        let count_sql = format!("SELECT COUNT(*){}", &sql[from_idx..]);
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
    sql.push_str(&format!("{} LIMIT {} OFFSET {}", order_clause, limit, start));
    
    let mut stmt = match conn.prepare(&sql) { Ok(s) => s, Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response() };
    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
    let mut items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(param_refs), |row| {
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
        let play_count: i32 = row.get(25).unwrap_or(0);
        let is_favorite: i32 = row.get(26).unwrap_or(0);
        let last_played_str: String = row.get(27).unwrap_or_default();
        let _played_pct = if total_ticks > 0 { (playback_ticks * 100 / total_ticks) as i32 } else { 0 };
        let date_str = format_iso8601(final_date_created);
        
        Ok(json!({
            "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "ServerId": "rust-em-server",
            "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" },
            "IsFolder": false, "MediaType": "Video", "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0),
            "Overview": row.get::<_, String>(4).unwrap_or_default(), 
            "CommunityRating": row.get::<_, f64>(8).unwrap_or(0.0), "RunTimeTicks": total_ticks, "Size": size,
            "DateCreated": date_str.clone(), "PrimaryImageAspectRatio": 0.6666666666666666,
            "Container": "mp4", "CanDelete": false, "CanDownload": false, "LocationType": "FileSystem", "Status": "Released",
            "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) },
            "BackdropImageTags": if has_fanart { json!([backdrop_tag]) } else { json!([]) },
            "Path": decode_url(&row.get::<_, String>(6).unwrap_or_default()),
            "MediaSources": [{ "Id": id.to_string(), "Protocol": "Http", "SupportsDirectPlay": true, "Container": "mp4", "Size": size, "Type": "Default", "Name": "Direct Play", "RunTimeTicks": total_ticks }],
            "UserData": {
                "PlaybackPositionTicks": playback_ticks,
                "PlayCount": play_count,
                "IsFavorite": is_favorite > 0,
                "Played": play_count > 0,
                "PlayedPercentage": if total_ticks > 0 { (playback_ticks as f64 / total_ticks as f64) * 100.0 } else { 0.0 },
                "LastPlayedDate": if !last_played_str.is_empty() {
                    if let Ok(ts) = last_played_str.parse::<i64>() { json!(format_iso8601(ts)) } else { json!(null) }
                } else { json!(null) },
                "UnplayedItemCount": 0
            }
        }))
    }).unwrap().filter_map(Result::ok).collect();
    items.extend(search_person_items);
    let final_total = if is_search {
        if search_term_len < 4 { 0 } else { items.len() as i64 }
    } else { total };
    Json(json!({ "Items": items, "TotalRecordCount": final_total })).into_response()
}

pub async fn emby_item_detail(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<String>, Extension(req_user_id): Extension<i64>) -> Response { emby_item_detail_inner(pool, item_id, req_user_id).await }
pub async fn emby_item_detail_with_user(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((_user_id, item_id)): AxumPath<(String, String)>, Extension(req_user_id): Extension<i64>) -> Response { emby_item_detail_inner(pool, item_id, req_user_id).await }

async fn emby_item_detail_inner(pool: Arc<Mutex<Connection>>, item_id: String, req_user_id: i64) -> Response {
    if item_id.starts_with("person_") { return emby_person(AxumPath(item_id)).await.into_response(); } 
    else if item_id.starts_with("studio_") { return emby_studio(AxumPath(item_id)).await.into_response(); }
    let conn = pool.lock().unwrap();
    
    if item_id.starts_with("boxset_") {
        let mut found_set_name = String::new(); let mut names = Vec::new();
        if let Ok(mut stmt_all) = conn.prepare("SELECT DISTINCT set_name FROM items WHERE set_name != ''") {
            if let Ok(mapped) = stmt_all.query_map([], |r| r.get::<_, String>(0)) { for s in mapped.flatten() { names.push(s); } }
        }
        for full_str in names {
            for name in full_str.split(" / ").filter(|s| !s.trim().is_empty()) {
                let mut h = Sha256::new(); h.update(name.as_bytes()); let hash = h.finalize();
                if format!("boxset_{:x}", hash) == item_id { found_set_name = name.to_string(); break; }
            }
            if !found_set_name.is_empty() { break; }
        }
        if found_set_name.is_empty() { return StatusCode::NOT_FOUND.into_response(); }
        
        let is_fav = if let Ok(count) = conn.query_row("SELECT COUNT(1) FROM boxset_favorites WHERE user_id = ?1 AND boxset_id = ?2", rusqlite::params![req_user_id, &item_id], |r| r.get::<_, i64>(0)) { count > 0 } else { false };
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE (set_name = ?1 OR set_name LIKE ?2 OR set_name LIKE ?3 OR set_name LIKE ?4)", [&found_set_name, &format!("{} / %", found_set_name), &format!("% / {}", found_set_name), &format!("% / {} / %", found_set_name)], |r| r.get(0)).unwrap_or(0);
        return Json(json!({ "Name": found_set_name, "Id": item_id, "ServerId": "rust-em-server", "Type": "BoxSet", "IsFolder": true, "MediaType": "Video", "ChildCount": count, "RecursiveItemCount": count, "ImageTags": {"Primary": format!("{}_cover", item_id)}, "BackdropImageTags": [], "Overview": "合集", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": is_fav, "Played": false } })).into_response();
    }

    let Ok(id) = item_id.parse::<i64>() else { return StatusCode::NOT_FOUND.into_response(); };
    let item_opt = conn.query_row(
        "SELECT items.id, items.title, items.type, items.year, items.plot, items.poster_path, items.file_path, items.studios, items.parts, items.runtime, items.actors, items.rating, items.mpaa, items.genres, items.premiered, items.tmdb_id, items.imdb_id, items.size, items.mtime, items.series, items.label, items.set_name, items.tagline, items.tags, items.directors, items.num, items.stills, items.releasedate, items.original_title, items.sort_title, items.country, items.language, items.writers, items.trailer, items.media_streams, items.date_created, items.primary_image_aspect_ratio, items.original_language, items.fanart_path, items.logo_path, items.thumb_path, items.banner_path, items.strm_content, IFNULL(ud.playback_position_ticks, 0), IFNULL(ud.play_count, 0), IFNULL(ud.is_favorite, 0), IFNULL(ud.last_played_date, '') FROM items LEFT JOIN user_data ud ON ud.item_id = CAST(items.id AS TEXT) AND ud.user_id = ? WHERE items.id = ?",
        rusqlite::params![req_user_id, id],
        |row| {
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
            let dir_names: Vec<&str> = directors_str.split(" / ").filter(|s| !s.is_empty()).collect();
            let director_str = dir_names.join(", ");
            if !director_str.is_empty() { people.push(json!({ "Name": director_str, "Type": "Director", "Id": format!("person_{}", director_str), "PrimaryImageTag": "" })); }
            
            for a in actors.iter() {
                people.push(json!({
                    "Name": a["name"].as_str().unwrap_or_default(), "Role": a["role"].as_str().unwrap_or_default(),
                    "Type": "Actor", "Id": format!("person_{}", a["name"].as_str().unwrap_or_default()), "PrimaryImageTag": ""
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
                        "Id": format!("{}_cd{}", id, i), "ItemId": id.to_string(), "Path": decode_url(&real_path), "DirectStreamUrl": stream_url,
                        "Protocol": "Http", "Container": "mp4", "Name": title.clone(), "Type": "Default", "VideoType": "VideoFile", "IsRemote": true,
                        "Bitrate": est_bitrate, "RunTimeTicks": run_time_ticks, "Size": size, "DefaultAudioStreamIndex": 1, "DefaultSubtitleStreamIndex": -1,
                        "SupportsDirectPlay": true, "SupportsDirectStream": true, "SupportsProbing": true, "SupportsTranscoding": false,
                        "AddApiKeyToDirectStreamUrl": false, "RequiresClosing": false, "RequiresOpening": false, "ProbeEngine": "rust-native", "MediaStreams": media_streams.clone()
                    })
                }).collect()
            };

            Ok(json!({
                "Id": id.to_string(), "ServerId": "rust-em-server", "Name": title.clone(),
                "OriginalTitle": json!(original_title), "SortName": json!(sort_title.clone()), "ForcedSortName": json!(sort_title.clone()),
                "Type": if typ == "movies" { "Movie" } else { "Series" },
                "MediaType": "Video", "IsFolder": false, "Overview": plot, "ProductionYear": year_str.parse::<i32>().unwrap_or(0), "PremiereDate": premiered, "ReleaseDate": releasedate,
                "DateCreated": format_iso8601(date_created_ts), "CommunityRating": rating, "OfficialRating": mpaa, "RunTimeTicks": run_time_ticks, "Size": size, "Bitrate": est_bitrate,
                "Genres": genres_vec, "GenreItems": genre_items, "Tags": tags_vec, "TagItems": tag_items,
                "Taglines": if tagline.is_empty() { json!([]) } else { json!([tagline]) }, "Studios": studios, "People": people,
                "ProviderIds": json!({ "Tmdb": tmdb_id, "Imdb": imdb_id, "num": num }),
                "ImageTags": image_tags, "BackdropImageTags": backdrop_tags, "PrimaryImageAspectRatio": if aspect_ratio > 0.0 { json!(aspect_ratio) } else { json!(0.6666666666666666) },
                "ProductionLocations": country.split(" / ").filter(|s| !s.is_empty()).collect::<Vec<_>>(), "OriginalLanguage": json!(if !original_language.is_empty() { original_language } else { language }),
                "SetName": json!(set_name), "SeriesName": json!(if typ == "tvshows" { series } else { "".to_string() }),
                "Path": decode_url(&real_path), "Container": "mp4", "MediaSourceCount": media_sources.len(), "MediaSources": media_sources,
                "ProbeEngine": "rust-native", "LocationType": "FileSystem", "CanDelete": false, "CanDownload": false, "CanDeleteFromFolders": [], "HideMissingActorImages": true,
                "UserData": { 
                    "ItemId": id.to_string(), "Key": id.to_string(), 
                    "PlaybackPositionTicks": playback_ticks, "PlayCount": play_count, 
                    "IsFavorite": is_favorite > 0, "Played": play_count > 0, 
                    "PlayedPercentage": if run_time_ticks > 0 { playback_ticks * 100 / run_time_ticks } else { 0 },
                    "LastPlayedDate": if !last_played_str.is_empty() {
                        if let Ok(ts) = last_played_str.parse::<i64>() { json!(format_iso8601(ts)) } else { json!(null) }
                    } else { json!(null) }
                }
            }))
        }
    ).ok();
    
    if let Some(item) = item_opt { Json(item).into_response() } else { StatusCode::NOT_FOUND.into_response() }
}

pub async fn emby_item_image(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, image_type)): AxumPath<(String, String)>, Query(query): Query<HashMap<String, String>>) -> Response { emby_item_image_inner(pool, item_id, image_type, 0, query).await }
pub async fn emby_item_image_with_index(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, image_type, index)): AxumPath<(String, String, String)>, Query(query): Query<HashMap<String, String>>) -> Response { emby_item_image_inner(pool, item_id, image_type, index.parse::<usize>().unwrap_or(0), query).await }

async fn emby_item_image_inner(pool: Arc<Mutex<Connection>>, item_id: String, image_type: String, index: usize, query: HashMap<String, String>) -> Response {
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

    if item_id.starts_with("lib_") || item_id.starts_with("genre_") || item_id.starts_with("tag_") || item_id.starts_with("boxset_") {
        let clean_id = item_id.replace("_cover", "");
        let (folder, prefix) = if clean_id.starts_with("lib_") {
            ("library", "lib")
        } else if clean_id.starts_with("genre_") {
            ("genre", "genre")
        } else if clean_id.starts_with("boxset_") {
            ("boxset", "boxset")
        } else {
            ("tag", "tag")
        };
        let raw_name = clean_id.trim_start_matches(&format!("{}_", prefix));
        
        let mut sanitized_name = String::new();
        for c in raw_name.chars() {
            if c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|' {
                sanitized_name.push('_');
            } else {
                sanitized_name.push(c);
            }
        }

        if prefix == "boxset" {
            let dir = std::path::PathBuf::from("data/cache/covers").join(folder);
            if let Ok(entries) = std::fs::read_dir(&dir) {
                let match_prefix = format!("boxset_{}_", sanitized_name.trim());
                for entry in entries.flatten() {
                    let fname = entry.file_name().to_string_lossy().to_string();
                    if fname.starts_with(&match_prefix) && fname.ends_with(".jpg") {
                        if let Ok(bytes) = tokio::fs::read(entry.path()).await {
                            return (StatusCode::OK, [(CONTENT_TYPE, "image/jpeg")], bytes).into_response();
                        }
                    }
                }
            }
        } else {
            let path = std::path::PathBuf::from("data/cache/covers").join(folder).join(format!("{}_{}.jpg", prefix, sanitized_name.trim()));
            if path.exists() {
                if let Ok(mut bytes) = tokio::fs::read(&path).await {
                    let req_w = query.get("maxWidth").or_else(|| query.get("width")).and_then(|s| s.parse::<u32>().ok());
                    let req_h = query.get("maxHeight").or_else(|| query.get("height")).and_then(|s| s.parse::<u32>().ok());
                    
                    if let (Some(w), Some(h)) = (req_w, req_h) {
                        if w > 0 && h > 0 {
                            if let Ok(img) = image::load_from_memory(&bytes) {
                                let resized = img.resize_exact(w, h, image::imageops::FilterType::Triangle);
                                let mut out = std::io::Cursor::new(Vec::new());
                                let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 80);
                                if encoder.encode_image(&resized).is_ok() {
                                    bytes = out.into_inner();
                                }
                            }
                        }
                    }
                    return (StatusCode::OK, [(CONTENT_TYPE, "image/jpeg")], bytes).into_response();
                }
            }
        }
        
        if !item_id.starts_with("boxset_") {
            let png_bytes: [u8; 67] = [
                0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
                0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
                0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
                0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
                0x42, 0x60, 0x82
            ];
            return (StatusCode::OK, [(CONTENT_TYPE, "image/png")], png_bytes).into_response();
        }
    }

    let path_opt = {
        let conn = pool.lock().unwrap();
        if item_id.starts_with("boxset_") {
            let mut found_set_name = String::new(); let mut names = Vec::new();
            if let Ok(mut stmt_all) = conn.prepare("SELECT DISTINCT set_name FROM items WHERE set_name != ''") {
                if let Ok(mapped) = stmt_all.query_map([], |r| r.get::<_, String>(0)) { for s in mapped.flatten() { names.push(s); } }
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
                    stmt.query_row([id], |row| Ok((row.get::<_, String>(0).unwrap_or_default(), row.get::<_, String>(1).unwrap_or_default()))).map(|(stills_json, fanart_path)| {
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

    if let Some(path_str) = path_opt { if !path_str.is_empty() { return serve_file(&path_str).await; } }
    StatusCode::NOT_FOUND.into_response()
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
        stmt.query_row([item_id], |row| Ok((row.get::<_, String>(0).ok(), row.get::<_, i32>(1).unwrap_or(0), row.get::<_, String>(2).unwrap_or_else(|_| "[]".to_string()), row.get::<_, i64>(3).unwrap_or(0)))).unwrap_or((None, 0, "[]".to_string(), 0))
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
            let path = parts[0].clone(); let ua_clone = ua.clone();
            let client = GLOBAL_CLIENT.get_or_init(|| reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(15)).build().unwrap()).clone();
            tokio::spawn(async move {
                if let Ok(content) = tokio::fs::read_to_string(&path).await {
                    let url = content.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
                    if url.starts_with("http") {
                        if let Some(final_url) = resolve_qmediasync_url(&client, url, &ua_clone).await {
                            let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
                            let mut lock = cache.lock().unwrap();
                            lock.insert(cache_key, (final_url, Instant::now() + Duration::from_secs(300)));
                            println!("🔥 [PREFETCH] 成功预热直链 (ID: {})", item_id);
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
            "Id": format!("{}_cd{}", item_id, i), "PlaySessionId": play_session_id.clone(), "Path": stream_url.clone(), "DirectStreamUrl": stream_url,
            "Protocol": "Http", "Container": ext, "Name": "Direct Play", "Type": "Default", "VideoType": "VideoFile", "IsRemote": true, "RunTimeTicks": run_time_ticks, "Size": size, "Bitrate": 0,
            "DefaultAudioStreamIndex": 1, "DefaultSubtitleStreamIndex": -1, "SupportsDirectPlay": true, "SupportsDirectStream": true, "SupportsProbing": true, "SupportsTranscoding": false,
            "SupportsExternalStream": false, "IsInfiniteStream": false, "RequiresOpening": false, "RequiresClosing": false, "RequiresLooping": false,
            "ReadAtNativeFramerate": false, "IgnoreDts": false, "IgnoreIndex": false, "GenPtsInput": false, "MediaStreams": final_streams.clone()
        })
    }).collect();
    Json(json!({ "MediaSources": media_sources, "PlaySessionId": play_session_id })).into_response()
}

pub async fn emby_stream(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response { emby_stream_inner(pool, item_id, query, headers).await }
pub async fn emby_stream_with_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _ext)): AxumPath<(i64, String)>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response { emby_stream_inner(pool, item_id, query, headers).await }
pub async fn emby_stream_with_source(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id)): AxumPath<(i64, String)>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response { emby_stream_inner(pool, item_id, query, headers).await }
pub async fn emby_stream_with_source_ext(State(pool): State<Arc<Mutex<Connection>>>, AxumPath((item_id, _source_id, _ext)): AxumPath<(i64, String, String)>, Query(query): Query<HashMap<String, String>>, headers: HeaderMap) -> Response { emby_stream_inner(pool, item_id, query, headers).await }

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
                println!("⚡ [CACHE] 命中预热缓存，秒开播放！(ID: {})", item_id);
                return (StatusCode::FOUND, [(LOCATION, cached_url.clone())], "").into_response(); 
            } 
        }
    }

    let (strm_path_opt, item_title) = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts, title FROM items WHERE id = ?1").unwrap();
        let row_res = stmt.query_row([item_id], |row| Ok((row.get::<_, String>(0).ok(), row.get::<_, String>(1).unwrap_or_else(|_| "未知影片".to_string()))));
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
        return StatusCode::BAD_GATEWAY.into_response();
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
                
                let mut runtime_mins: i32 = conn.query_row("SELECT runtime FROM items WHERE id = ?1", [item_id], |r| r.get(0)).unwrap_or(0);
                let _current_ticks: i64 = conn.query_row("SELECT playback_position_ticks FROM user_data WHERE user_id = ?1 AND item_id = ?2", rusqlite::params![req_user_id, item_id.to_string()], |r| r.get(0)).unwrap_or(0);
                
                if let Some(client_ticks) = val["RunTimeTicks"].as_i64() {
                    if client_ticks > 0 {
                        runtime_mins = (client_ticks / 10_000_000 / 60) as i32;
                        let _ = conn.execute("UPDATE items SET runtime = ?1 WHERE id = ?2", rusqlite::params![runtime_mins, item_id]);
                    }
                }
                
                if runtime_mins <= 0 && ticks > 0 {
                    runtime_mins = (ticks / 10_000_000 / 60) as i32 + 30;
                    let _ = conn.execute("UPDATE items SET runtime = ?1 WHERE id = ?2", rusqlite::params![runtime_mins, item_id]);
                    
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
                
                let total_ticks = (runtime_mins as i64) * 60 * 10_000_000;
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

pub async fn emby_similar(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();

    let (genres_str, tags_str): (String, String) = conn.query_row(
        "SELECT genres, tags FROM items WHERE id = ?1",
        [item_id],
        |r| Ok((r.get::<_, String>(0).unwrap_or_default(), r.get::<_, String>(1).unwrap_or_default())),
    ).unwrap_or_default();

    let mut terms: Vec<(String, i64)> = Vec::new();
    for t in tags_str.split(" / ").map(str::trim).filter(|s| !s.is_empty()).take(8) { terms.push((t.to_string(), 2)); }
    for g in genres_str.split(" / ").map(str::trim).filter(|s| !s.is_empty()).take(8) { terms.push((g.to_string(), 1)); }

    if terms.is_empty() {
        let mut stmt = conn.prepare("SELECT id, title, type, year, plot, poster_path, rating FROM items WHERE id != ?1 ORDER BY RANDOM() LIMIT 6").unwrap();
        let items: Vec<Value> = stmt.query_map([item_id], |row| {
            let id: i64 = row.get(0).unwrap_or_default();
            let poster_path: String = row.get(5).unwrap_or_default();
            let has_poster = !poster_path.is_empty();
            let poster_tag = if has_poster {
                let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
                let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
                format!("tag_{}_{}", id, &hash_hex[..8])
            } else { String::new() };
            Ok(json!({ "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" }, "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0), "Overview": row.get::<_, String>(4).unwrap_or_default(), "CommunityRating": row.get::<_, f64>(6).unwrap_or(0.0), "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) }, "BackdropImageTags": [] }))
        }).unwrap().filter_map(Result::ok).collect();
        return Json(json!({ "Items": items, "TotalRecordCount": items.len() }));
    }

    let mut score_expr = String::new();
    let mut where_expr = String::new();
    for (i, (_term, weight)) in terms.iter().enumerate() {
        let p = i + 2;
        if i > 0 {
            score_expr.push_str(" + ");
            where_expr.push_str(" OR ");
        }
        score_expr.push_str(&format!("((CASE WHEN tags LIKE ?{} THEN {} ELSE 0 END) + (CASE WHEN genres LIKE ?{} THEN {} ELSE 0 END))", p, weight * 2, p, weight));
        where_expr.push_str(&format!("genres LIKE ?{} OR tags LIKE ?{}", p, p));
    }

    let sql = format!("SELECT id, title, type, year, plot, poster_path, rating, ({}) AS score FROM items WHERE id != ?1 AND ({}) ORDER BY score DESC, RANDOM() LIMIT 6", score_expr, where_expr);

    let mut params_vec: Vec<rusqlite::types::Value> = Vec::with_capacity(terms.len() + 1);
    params_vec.push(rusqlite::types::Value::Integer(item_id));
    for (term, _) in &terms { params_vec.push(rusqlite::types::Value::Text(format!("%{}%", term))); }

    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(_) => return Json(json!({ "Items": [], "TotalRecordCount": 0 })),
    };

    let items: Vec<Value> = stmt.query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
        let id: i64 = row.get(0).unwrap_or_default();
        let poster_path: String = row.get(5).unwrap_or_default();
        let has_poster = !poster_path.is_empty();
        let poster_tag = if has_poster {
            let mut h = Sha256::new(); h.update(poster_path.as_bytes()); let hash = h.finalize();
            let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            format!("tag_{}_{}", id, &hash_hex[..8])
        } else { String::new() };
        Ok(json!({ "Name": row.get::<_, String>(1).unwrap_or_default(), "Id": id.to_string(), "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" }, "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0), "Overview": row.get::<_, String>(4).unwrap_or_default(), "CommunityRating": row.get::<_, f64>(6).unwrap_or(0.0), "ImageTags": if has_poster { json!({"Primary": poster_tag}) } else { json!({}) }, "BackdropImageTags": [] }))
    }).unwrap().filter_map(Result::ok).collect();

    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_thumbnail_set(AxumPath(item_id): AxumPath<i64>) -> impl IntoResponse { Json(json!({ "Id": item_id.to_string(), "ThumbnailSet": {} })) }

pub async fn emby_virtual_folders(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let mut stmt = conn.prepare("SELECT id, name, type FROM libraries").unwrap();
    let folders: Vec<Value> = stmt.query_map([], |row| {
        let id: i64 = row.get(0).unwrap_or_default(); let name: String = row.get(1).unwrap_or_default(); let lib_type: String = row.get(2).unwrap_or_default();
        Ok(json!({ "Name": name, "Id": format!("lib_{}", id), "Guid": format!("lib_{}", id), "Path": format!("/media/lib_{}", id), "CollectionType": lib_type, "LibraryOptions": { "PathInfos": [], "EnablePhotos": true, "EnableRealtimeMonitor": false, "SaveLocalMetadata": true }, "ItemId": format!("lib_{}", id), "PrimaryImageItemId": "", "RefreshStatus": "Idle" }))
    }).unwrap().filter_map(Result::ok).collect();
    Json(folders)
}

pub async fn emby_item_counts(State(pool): State<Arc<Mutex<Connection>>>) -> impl IntoResponse {
    let conn = pool.lock().unwrap();
    let movie_count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE type='movies'", [], |r| r.get(0)).unwrap_or(0);
    let series_count: i64 = conn.query_row("SELECT COUNT(*) FROM items WHERE type='tvshows'", [], |r| r.get(0)).unwrap_or(0);
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
        let has_poster = !row.get::<_, String>(4).unwrap_or_default().is_empty();
        Ok(json!({ "ItemId": id.to_string(), "Id": id.to_string(), "Name": row.get::<_, String>(1).unwrap_or_default(), "Type": if row.get::<_, String>(2).unwrap_or_default() == "movies" { "Movie" } else { "Series" }, "ProductionYear": row.get::<_, String>(3).unwrap_or_default().parse::<i32>().unwrap_or(0), "ImageTags": if has_poster { json!({"Primary": format!("tag_{}", id)}) } else { json!({}) }, "MatchedTerm": term, "PrimaryImageTag": if has_poster { format!("tag_{}", id) } else { "".to_string() } }))
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

pub async fn emby_item_image_user_only(_: AxumPath<(String, String)>) -> Response { StatusCode::NOT_FOUND.into_response() }

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

pub async fn emby_genres(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Extension(req_user_id): Extension<i64>
) -> impl IntoResponse {
    let _ = headers;
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
            for g_str in mapped.flatten() {
                for g in g_str.split(" / ") { let name = g.trim(); if !name.is_empty() { set.insert(name.to_string()); } }
            }
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    let items: Vec<Value> = names.into_iter().map(|name| {
        json!({ "Name": name, "Id": format!("genre_{}", name), "ServerId": "rust-em-server", "Type": "Genre", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": get_cover_tag("genre", "genre", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_tags(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Extension(req_user_id): Extension<i64>
) -> impl IntoResponse {
    let _ = headers;
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
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for t_str in mapped.flatten() { for t in t_str.split(" / ") { if !t.trim().is_empty() { set.insert(t.trim().to_string()); } } }
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    let items: Vec<Value> = names.into_iter().map(|name| {
        json!({ "Name": name, "Id": format!("tag_{}", name), "ServerId": "rust-em-server", "Type": "Tag", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {"Primary": get_cover_tag("tag", "tag", &name)}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_years(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Extension(req_user_id): Extension<i64>
) -> impl IntoResponse {
    let _ = headers;
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

pub async fn emby_official_ratings(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Extension(req_user_id): Extension<i64>
) -> impl IntoResponse {
    let _ = headers;
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

pub async fn emby_studios(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    Extension(req_user_id): Extension<i64>
) -> impl IntoResponse {
    let _ = headers;
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
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for st_str in mapped.flatten() { for st in st_str.split(" / ") { if !st.trim().is_empty() { set.insert(st.trim().to_string()); } } }
        }
    }
    let mut names: Vec<String> = set.into_iter().collect();
    names.sort();
    let items: Vec<Value> = names.into_iter().map(|name| {
        json!({ "Name": name, "Id": name, "ServerId": "rust-em-server", "Type": "Studio", "IsFolder": false, "MediaType": "Video", "PrimaryImageAspectRatio": 0.6666666666666666, "ImageTags": {}, "BackdropImageTags": [], "ImageBlurHashes": {}, "LocationType": "FileSystem", "UserData": { "PlaybackPositionTicks": 0, "PlayCount": 0, "IsFavorite": false, "Played": false, "PlayedPercentage": 0 } })
    }).collect();
    Json(json!({ "Items": items, "TotalRecordCount": items.len() }))
}

pub async fn emby_items_filters(
    State(pool): State<Arc<Mutex<Connection>>>, 
    Query(query): Query<HashMap<String, String>>,
    Extension(req_user_id): Extension<i64>
) -> impl IntoResponse {
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

pub async fn emby_activity_log() -> impl IntoResponse {
    Json(json!({ "Items": [], "TotalRecordCount": 0 }))
}

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
                for path in paths { scan_directory(pool_clone.clone(), id, lib_type.clone(), path); }
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
