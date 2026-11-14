//! 鉴权与权限。
//!
//! 职责：Token 存储（TOKENS）、日志中间件（logger_middleware）、
//! 请求鉴权中间件（auth）、普通用户媒体库权限查询（get_allowed_libs）。

use axum::{
    extract::Request,
    http::{StatusCode, Method},
    middleware::Next,
    response::Response,
};
use rusqlite::Connection;
use std::sync::{Mutex, OnceLock};
use std::collections::HashMap;

pub static TOKENS: OnceLock<Mutex<HashMap<String, i64>>> = OnceLock::new();

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
    if !path.starts_with("/api") && !path.starts_with("/emby") && !path.starts_with("/Items") && !path.starts_with("/Users") && !path.starts_with("/Videos") && !path.starts_with("/System") && !path.starts_with("/Auth") && !path.starts_with("/Sessions") && !path.starts_with("/Persons") && !path.starts_with("/Shows") && !path.starts_with("/Genres") && !path.starts_with("/Tags") && !path.starts_with("/Years") && !path.starts_with("/Studios") && !path.starts_with("/OfficialRatings") && !path.starts_with("/Search") {
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

pub fn get_allowed_libs(conn: &Connection, user_id: i64) -> Option<Vec<i64>> {
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

