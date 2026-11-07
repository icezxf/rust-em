use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct MediaProbeResult {
    pub container: String,
    pub run_time_ticks: i64,
    pub size: u64,
    pub bitrate: u64,
    pub video_streams: Vec<VideoStreamInfo>,
    pub audio_streams: Vec<AudioStreamInfo>,
    pub subtitle_streams: Vec<SubtitleStreamInfo>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct VideoStreamInfo {
    pub index: i32, pub codec: String, pub profile: String, pub level: i32,
    pub width: i32, pub height: i32, pub bitrate: u64, pub frame_rate: f64,
    pub is_interlaced: bool, pub is_anamorphic: bool,
    pub video_range: String, pub color_space: String, pub color_transfer: String,
    pub color_primaries: String, pub pixel_format: String, pub aspect_ratio: String,
    pub ref_frames: i32, pub display_title: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AudioStreamInfo {
    pub index: i32, pub codec: String, pub channels: i32, pub sample_rate: i32,
    pub bitrate: u64, pub language: String, pub display_title: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SubtitleStreamInfo {
    pub index: i32, pub codec: String, pub language: String,
    pub is_default: bool, pub is_forced: bool, pub is_external: bool,
    pub display_title: String,
}

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

pub async fn probe_strm(
    strm_path: &Path,
    limiter: Arc<ProbeLimiter>,
    client: &reqwest::Client,
) -> Result<MediaProbeResult, String> {
    // 1. 缓存命中直接返回
    let json_path = get_mediainfo_json_path(strm_path);
    if json_path.exists() {
        if let Ok(content) = tokio::fs::read_to_string(&json_path).await {
            if let Ok(result) = serde_json::from_str::<MediaProbeResult>(&content) {
                return Ok(result);
            }
        }
    }

    // 2. 读 .strm 里的 URL
    let url = tokio::fs::read_to_string(strm_path)
        .await
        .map_err(|e| format!("读strm失败: {}", e))?
        .lines().map(str::trim).find(|s| !s.is_empty())
        .ok_or_else(|| "strm为空".to_string())?.to_string();
    if !url.starts_with("http") { return Err("strm不是http链接".to_string()); }

    // 3. 解析直链（qmediasync 302 跟随）
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

    // 4. HEAD 拿 Content-Length / Content-Type
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

    // 5. Range 读头部 2MB 存临时文件（留给 mediainfo 解析）
    limiter.acquire().await;
    let _header_bytes = {
        let resp = client.get(&direct_url)
            .header(reqwest::header::RANGE, "bytes=0-2097151")
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send().await.map_err(|e| format!("Range失败: {}", e))?;
        resp.bytes().await.map_err(|e| format!("读body失败: {}", e))?.to_vec()
    };

    // 6. 先用 HEAD 推断的字段组装结果（mediainfo 字段映射下一步填入）
    let container = infer_container(&content_type, &direct_url);
    let result = MediaProbeResult {
        container,
        size,
        run_time_ticks: 0,
        bitrate: 0,
        video_streams: vec![],
        audio_streams: vec![],
        subtitle_streams: vec![],
    };

    // 7. 写 JSON 缓存
    if let Ok(json) = serde_json::to_string_pretty(&result) {
        let _ = tokio::fs::write(&json_path, json).await;
    }
    Ok(result)
}

fn infer_container(content_type: &str, url: &str) -> String {
    let ct = content_type.to_lowercase();
    if ct.contains("mp4") { return "mp4".into(); }
    if ct.contains("matroska") || ct.contains("mkv") { return "mkv".into(); }
    let p = url.split('?').next().unwrap_or(url);
    if let Some(idx) = p.rfind('.') {
        let ext = p[idx+1..].to_lowercase();
        if !ext.is_empty() && ext.len() <= 5 { return ext; }
    }
    "mp4".into()
}
