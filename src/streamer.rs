//! 302 播放重定向与预热缓存。
//!
//! 职责：解析 .strm 内的网盘链接 → 跟随 302 拿到真实直链 → 缓存 5 分钟 → 返回 302。
//! 包含：PlaybackInfo、Stream、Download 等播放相关 handler，以及详情页预热逻辑。
//! 依赖：utils.rs（sanitize_media_streams / extract_extension_from_url）。

use axum::{
    extract::{State, Query, Path as AxumPath},
    http::{StatusCode, HeaderMap, header::{LOCATION, USER_AGENT}},
    Json,
    response::{IntoResponse, Response},
};
use std::sync::{Arc, Mutex, OnceLock};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use rusqlite::Connection;
use tokio::io::AsyncReadExt;
use serde_json::{json, Value};

use crate::utils::{sanitize_media_streams, extract_extension_from_url};

pub static STRM_CACHE: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
pub static GLOBAL_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
const MAX_STRM_SIZE: u64 = 16 * 1024;

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
    
    let (parts_json_opt, runtime, media_streams_json, size, item_title) = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts, runtime, media_streams, size, title FROM items WHERE id = ?1").unwrap();
        stmt.query_row([item_id], |row| Ok((
            row.get::<_, String>(0).ok(), 
            row.get::<_, i32>(1).unwrap_or(0), 
            row.get::<_, String>(2).unwrap_or_else(|_| "[]".to_string()), 
            row.get::<_, i64>(3).unwrap_or(0),
            row.get::<_, String>(4).unwrap_or_else(|_| "未知影片".to_string())
        ))).unwrap_or((None, 0, "[]".to_string(), 0, "未知影片".to_string()))
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
    let run_time_ticks = (runtime as i64) * 10_000_000;
    
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
            let title_clone = item_title.clone();
            let client = GLOBAL_CLIENT.get_or_init(|| reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).timeout(Duration::from_secs(15)).build().unwrap()).clone();
            tokio::spawn(async move {
                if let Ok(content) = tokio::fs::read_to_string(&path).await {
                    let url = content.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
                    if url.starts_with("http") {
                        if let Some(final_url) = resolve_qmediasync_url(&client, url, &ua_clone).await {
                            let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
                            let mut lock = cache.lock().unwrap();
                            lock.insert(cache_key, (final_url, Instant::now() + Duration::from_secs(300)));
                            println!("🔥 [PREFETCH] 成功预热直链 (ID: {}, 片名: {})", item_id, title_clone);
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

    let cache_key = format!("{}|{}|{}|{}", item_id, part_idx, ip, ua);
    let cache = STRM_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    { 
        let cache_lock = cache.lock().unwrap(); 
        if let Some((cached_url, expire_at)) = cache_lock.get(&cache_key) { 
            if Instant::now() < *expire_at { 
                let url_preview: String = cached_url.chars().take(30).collect();
                println!("⚡ [CACHE] 秒开播放 (ID: {}, 片名: {}) -> {}", item_id, item_title, url_preview);
                return (StatusCode::FOUND, [(LOCATION, cached_url.clone())], "").into_response(); 
            } 
        }
    }
    
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
    
    let url_preview: String = final_url.chars().take(30).collect();
    println!("▶️ [PLAY] 重新解析直链并返回 302 (ID: {}, 片名: {}) -> {}", item_id, item_title, url_preview);
    
    (StatusCode::FOUND, [(LOCATION, final_url)], "").into_response()
}

pub async fn emby_download(State(pool): State<Arc<Mutex<Connection>>>, AxumPath(item_id): AxumPath<i64>) -> Response {
    let path_opt = {
        let conn = pool.lock().unwrap();
        let mut stmt = conn.prepare("SELECT parts, file_path FROM items WHERE id = ?1").unwrap();
        stmt.query_row([item_id], |row| {
            let parts_json: String = row.get(0).unwrap_or_default();
            let file_path: String = row.get(1).unwrap_or_default();
            let parts: Vec<String> = serde_json::from_str(&parts_json).unwrap_or_default();
            Ok(parts.into_iter().next().unwrap_or(file_path))
        }).ok()
    };
    let Some(strm_path) = path_opt else { return StatusCode::NOT_FOUND.into_response(); };
    let file = match tokio::fs::File::open(&strm_path).await { Ok(f) => f, Err(_) => return StatusCode::NOT_FOUND.into_response() };
    let mut contents = String::new();
    if file.take(MAX_STRM_SIZE).read_to_string(&mut contents).await.is_err() { return StatusCode::INTERNAL_SERVER_ERROR.into_response(); }
    let url = contents.lines().map(str::trim).find(|s| !s.is_empty()).unwrap_or("");
    if url.starts_with("http") { return (StatusCode::FOUND, [(LOCATION, url.to_string())], "").into_response(); }
    StatusCode::NOT_FOUND.into_response()
}
