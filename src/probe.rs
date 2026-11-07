use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// 全局节流器：串行 + 固定间隔，默认 1000ms，115 风控安全
pub struct ProbeLimiter {
    last: Mutex<Instant>,
    interval: Duration,
}

impl ProbeLimiter {
    pub fn new(interval_ms: u64) -> Arc<Self> {
        Arc::new(Self { last: Mutex::new(Instant::now() - Duration::from_secs(3600)), interval: Duration::from_millis(interval_ms) })
    }
    pub async fn acquire(&self) {
        let mut last = self.last.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.interval {
            tokio::time::sleep(self.interval - elapsed).await;
        }
        *last = Instant::now();
    }
}

pub fn get_mediainfo_json_path(strm_path: &Path) -> PathBuf {
    let stem = strm_path.file_stem().unwrap_or_default().to_string_lossy();
    strm_path.parent().unwrap_or(Path::new(".")).join(format!("{}-mediainfo.json", stem))
}

/// 读 JSON，返回第一项 MediaSourceInfo（兼容其他程序的数组格式和旧版单对象格式）
pub fn load_cached_source(json_path: &Path) -> Option<Value> {
    let content = std::fs::read_to_string(json_path).ok()?;
    let v: Value = serde_json::from_str(&content).ok()?;
    if let Some(arr) = v.as_array() {
        arr.first().and_then(|item| item.get("MediaSourceInfo")).cloned()
    } else if v.get("MediaSourceInfo").is_some() {
        v.get("MediaSourceInfo").cloned()
    } else {
        Some(v)
    }
}

fn infer_container(content_type: &str, url: &str) -> String {
    let ct = content_type.to_lowercase();
    if ct.contains("mp4") { return "mov,mp4,m4a,3gp,3g2,mj2".into(); }
    if ct.contains("matroska") || ct.contains("mkv") { return "matroska,webm".into(); }
    if ct.contains("mpegts") { return "mpegts".into(); }
    let p = url.split('?').next().unwrap_or(url);
    if let Some(idx) = p.rfind('.') {
        let ext = p[idx+1..].to_lowercase();
        match ext.as_str() {
            "mp4" | "m4v" => return "mov,mp4,m4a,3gp,3g2,mj2".into(),
            "mkv" | "webm" => return "matroska,webm".into(),
            "ts" | "m2ts" => return "mpegts".into(),
            "avi" => return "avi".into(),
            "flv" => return "flv".into(),
            _ => {}
        }
    }
    "mov,mp4,m4a,3gp,3g2,mj2".into()
}

pub async fn probe_strm(
    strm_path: &Path,
    limiter: Arc<ProbeLimiter>,
    client: &reqwest::Client,
) -> Result<Value, String> {
    let json_path = get_mediainfo_json_path(strm_path);
    // 缓存命中且含 MediaStreams 才跳过
    if json_path.exists() {
        if let Some(src) = load_cached_source(&json_path) {
            if src.get("MediaStreams").and_then(|m| m.as_array()).map(|a| !a.is_empty()).unwrap_or(false) {
                return Ok(src);
            }
        }
    }

    let url = tokio::fs::read_to_string(strm_path)
        .await
        .map_err(|e| format!("读strm失败: {}", e))?
        .lines().map(str::trim).find(|s| !s.is_empty())
        .ok_or_else(|| "strm为空".to_string())?.to_string();
    if !url.starts_with("http") { return Err("strm不是http链接".to_string()); }

    // 解析直链（client 已禁用 redirect，手动取 Location）
    limiter.acquire().await;
    let direct_url = {
        let resp = client.get(&url)
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send().await.map_err(|e| format!("请求解析器失败: {}", e))?;
        if resp.status().is_redirection() {
            resp.headers().get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok()).map(|s| s.to_string())
                .ok_or_else(|| "302缺少Location".to_string())?
        } else { url.clone() }
    };

    // HEAD 拿 size / content-type
    limiter.acquire().await;
    let (size, content_type) = {
        let resp = client.head(&direct_url)
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send().await.map_err(|e| format!("HEAD失败: {}", e))?;
        let len = resp.headers().get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
        let ct = resp.headers().get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
        (len, ct)
    };

    // Range 读头部 2MB（留给 mediainfo 解析）
    limiter.acquire().await;
    let _header_bytes = {
        let resp = client.get(&direct_url)
            .header(reqwest::header::RANGE, "bytes=0-2097151")
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send().await.map_err(|e| format!("Range失败: {}", e))?;
        resp.bytes().await.map_err(|e| format!("读body失败: {}", e))?.to_vec()
    };

    let container = infer_container(&content_type, &direct_url);
    // Emby 原生 MediaSourceInfo 结构，Path/DirectStreamUrl 留空由 PlaybackInfo 动态补
    let media_source = json!({
        "Bitrate": 0,
        "Container": container,
        "Formats": [],
        "HasMixedProtocols": false,
        "IsInfiniteStream": false,
        "IsRemote": true,
        "MediaStreams": [],
        "Protocol": "Http",
        "ReadAtNativeFramerate": false,
        "RequiredHttpHeaders": {},
        "RequiresClosing": false,
        "RequiresLooping": false,
        "RequiresOpening": false,
        "RunTimeTicks": 0,
        "Size": size,
        "SupportsDirectPlay": true,
        "SupportsDirectStream": true,
        "SupportsProbing": true,
        "SupportsTranscoding": true,
        "Type": "Default"
    });

    let arr = json!([{ "Chapters": [], "MediaSourceInfo": media_source }]);
    if let Ok(s) = serde_json::to_string_pretty(&arr) {
        let _ = tokio::fs::write(&json_path, s).await;
    }
    Ok(media_source)
}
