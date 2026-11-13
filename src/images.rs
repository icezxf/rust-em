//! 图片下发服务。
//!
//! 职责：虚拟 ID（lib_/genre_/tag_/boxset_/person_/studio_）与数字 ID 的图片路径解析、
//! 封面缓存 tag 计算、按 maxWidth/maxHeight 实时缩放、返回 image/jpeg 或 image/png。
//!
//! 与 covers.rs 的分工：covers.rs 负责“生成拼接封面”，images.rs 负责“把图发给客户端”。

use axum::{
    extract::{State, Path as AxumPath, Query},
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

pub fn get_cover_tag(folder: &str, prefix: &str, name: &str) -> String {
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

pub async fn serve_file(path_str: &str) -> Response {
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
                "thumb" => {
                    // 先查 thumb_path，如果为空或者该条目是 Episode，直接回退去读 poster_path
                    let thumb_opt: Option<String> = conn.query_row("SELECT thumb_path FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).ok();
                    let is_ep: bool = conn.query_row("SELECT type FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).map(|t| t == "Episode").unwrap_or(false);
                    if is_ep || thumb_opt.as_deref().unwrap_or("").is_empty() {
                        conn.query_row("SELECT poster_path FROM items WHERE id = ?1", [id], |r| r.get::<_, String>(0)).ok()
                    } else {
                        thumb_opt
                    }
                },
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

pub async fn emby_item_image_user_only(_: AxumPath<(String, String)>) -> Response { StatusCode::NOT_FOUND.into_response() }
