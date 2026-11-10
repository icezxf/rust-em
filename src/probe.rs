use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use mediainfo::{MediaInfo, StreamKind, InfoKind};

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

    limiter.acquire().await;
    let direct_url = {
        let resp = client.get(&url)
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
            .send().await.map_err(|e| format!("请求解析器失败: {}", e))?;
        if resp.status().is_redirection() {
            resp.headers().get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok()).map(|s| s.to_string())
                .ok_or_else(|| "302缺少Location".to_string())?
        } else {
            let text = resp.text().await.unwrap_or_default();
            if let Some(start) = text.find("href=\"") {
                let rest = &text[start + 6..];
                if let Some(end) = rest.find('\"') {
                    rest[..end].replace("&amp;", "&")
                } else { return Err("HTML中未找到完整的href".into()); }
            } else if text.starts_with("http") { text.trim().to_string() } else { return Err("无法解析直链地址".into()); }
        }
    };

    limiter.acquire().await;
    
    // 恢复 16MB 读取，确保撞到关键帧
    const CHUNK_SIZE: u64 = 16 * 1024 * 1024;
    
    // 1. 请求头部
    let mut resp = client.get(&direct_url)
        .header(reqwest::header::RANGE, format!("bytes=0-{}", CHUNK_SIZE - 1))
        .header(reqwest::header::USER_AGENT, "Mozilla/5.0")
        .send().await.map_err(|e| format!("请求头部失败: {}", e))?;

    let content_type = resp.headers().get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    if content_type.contains("text/html") {
        return Err("网盘返回了错误页面".to_string());
    }

    let mut total_size = 0u64;
    if let Some(cr) = resp.headers().get(reqwest::header::CONTENT_RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(total) = cr.split('/').last() {
            total_size = total.parse::<u64>().unwrap_or(0);
        }
    }
    if total_size == 0 {
        total_size = resp.headers().get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    }

    let mut header_bytes = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        header_bytes.extend_from_slice(&chunk);
        if header_bytes.len() as u64 >= CHUNK_SIZE { break; }
    }

    if header_bytes.len() < 1024 {
        return Err("读取到的视频头数据太小".to_string());
    }

    // 2. 请求尾部
    let mut tail_bytes = Vec::new();
    if total_size > CHUNK_SIZE * 2 {
        let start = total_size - CHUNK_SIZE;
        if let Ok(mut tail_resp) = client.get(&direct_url)
            .header(reqwest::header::RANGE, format!("bytes={}-{}", start, total_size - 1))
            .header(reqwest::header::USER_AGENT, "Mozilla/5.0")
            .send().await 
        {
            while let Ok(Some(chunk)) = tail_resp.chunk().await {
                tail_bytes.extend_from_slice(&chunk);
                if tail_bytes.len() as u64 >= CHUNK_SIZE { break; }
            }
        }
    }

    let container = infer_container(&content_type, &direct_url);
    
    let mut media_streams = Value::Null;
    let mut default_audio_index = -1;
    let mut default_sub_index = -1;

    // 3. 安全地拼接头尾文件，避免空洞
    if let Ok(mut temp_file) = tempfile::NamedTempFile::new() {
        use std::io::Write;
        if temp_file.write_all(&header_bytes).is_ok() {
            if !tail_bytes.is_empty() && total_size > (header_bytes.len() + tail_bytes.len()) as u64 {
                if temp_file.as_file().set_len(total_size).is_ok() {
                    // 使用 as_file_mut 绕开 Seek trait 引入的问题
                    use std::io::Seek;
                    if temp_file.as_file_mut().seek(std::io::SeekFrom::Start(total_size - tail_bytes.len() as u64)).is_ok() {
                        let _ = temp_file.write_all(&tail_bytes);
                    }
                }
            }
            let temp_path = temp_file.path().to_owned();
            if let Ok(Ok(parsed)) = tokio::task::spawn_blocking(move || parse_mediainfo(&temp_path)).await {
                if let Some(ms) = parsed.get("MediaStreams") { media_streams = ms.clone(); }
                if let Some(da) = parsed.get("DefaultAudioStreamIndex") { default_audio_index = da.as_i64().unwrap_or(-1); }
                if let Some(ds) = parsed.get("DefaultSubtitleStreamIndex") { default_sub_index = ds.as_i64().unwrap_or(-1); }
            }
        }
    }

    if media_streams.is_null() || media_streams.as_array().map_or(true, |a| a.is_empty()) {
        media_streams = json!([]);
    }

    let media_source = json!({
        "AddApiKeyToDirectStreamUrl": false, "Bitrate": 0, "Container": container, "Formats": [], "HasMixedProtocols": false, "IsInfiniteStream": false, "IsRemote": true,
        "MediaStreams": media_streams, "Protocol": "Http", "ReadAtNativeFramerate": false, "RequiredHttpHeaders": {}, "RequiresClosing": false, "RequiresLooping": false, "RequiresOpening": false, "RunTimeTicks": 0, "Size": total_size,
        "SupportsDirectPlay": true, "SupportsDirectStream": true, "SupportsProbing": true, "SupportsTranscoding": false, "Type": "Default",
        "DefaultAudioStreamIndex": default_audio_index, "DefaultSubtitleStreamIndex": default_sub_index
    });

    let result = json!([{ "Chapters": [], "MediaSourceInfo": media_source }]);
    if let Ok(s) = serde_json::to_string_pretty(&result) {
        let _ = tokio::fs::write(&json_path, s).await;
    }
    Ok(media_source)
}

fn parse_mediainfo(path: &Path) -> Result<Value, String> {
    let mut mi = MediaInfo::new();
    if !mi.open(path.to_str().unwrap_or_default()) {
        return Err("mediainfo 无法打开文件".to_string());
    }

    let mut streams = Vec::new();
    let mut global_index = 0;
    let mut default_audio: i64 = -1;
    let mut default_sub: i64 = -1;

    // 1. 视频流
    let video_count = mi.count_get(StreamKind::Video, None);
    for i in 0..video_count {
        let get_str = |k| mi.get(StreamKind::Video, i, k, InfoKind::Text);
        let get_i = |k| get_str(k).parse::<i64>().unwrap_or(0);
        let codec = get_str("Format").to_lowercase();
        let profile = get_str("Format_Profile");
        let width = get_i("Width");
        let height = get_i("Height");
        let pixel_format = get_str("ColorSpace");
        let bit_depth = get_i("BitDepth");
        let depth = if bit_depth > 0 { bit_depth } else { 8 };
        
        let p = if width >= 2160 { "2160p" } else if width >= 1080 { "1080p" } else { "720p" };
        let display_title = format!("{} {}", p, codec.to_uppercase());

        let fr_str = get_str("FrameRate");
        let fr_val = if let Some((n, d)) = fr_str.split_once('/') {
            n.parse::<f64>().unwrap_or(0.0) / d.parse::<f64>().unwrap_or(1.0)
        } else {
            fr_str.parse::<f64>().unwrap_or(0.0)
        };

        streams.push(json!({
            "Type": "Video", "Index": global_index, "Codec": codec, "Width": width, "Height": height,
            "Profile": profile, "Level": get_i("Format_Level"), "PixelFormat": pixel_format, "BitDepth": depth,
            "BitRate": get_i("BitRate"), "AverageFrameRate": fr_val, "RealFrameRate": fr_val,
            "DisplayTitle": display_title, "Title": display_title,
            "AspectRatio": "16:9", "IsAnamorphic": false, "IsDefault": true, "IsForced": false, "IsExternal": false, "Language": get_str("Language"),
            "Protocol": "File", "SupportsExternalStream": false, "AttachmentSize": 0
        }));
        global_index += 1;
    }

    // 2. 音频流
    let audio_count = mi.count_get(StreamKind::Audio, None);
    for i in 0..audio_count {
        let get_str = |k| mi.get(StreamKind::Audio, i, k, InfoKind::Text);
        let get_i = |k| get_str(k).parse::<i64>().unwrap_or(0);
        
        let codec = get_str("Format").to_uppercase();
        let ch = get_i("Channels");
        let layout = get_str("ChannelLayout");
        let lang_code = get_str("Language").to_lowercase();
        let lang_str = if lang_code.starts_with("zh") || lang_code == "chi" { "Chinese" } else if lang_code.starts_with("en") || lang_code == "eng" { "English" } else { "Unknown" };
        let lang_iso = if lang_code.starts_with("zh") || lang_code == "chi" { "chi" } else if lang_code.starts_with("en") || lang_code == "eng" { "eng" } else { "und" };
        let title_name = get_str("Title");
        
        let mut display_title = format!("{} {} {}", lang_str, codec, layout);
        if !title_name.is_empty() { display_title = format!("{} {}", display_title, title_name); }

        let is_default = get_i("Default") == 1 || (default_audio == -1 && i == 0);
        if is_default && default_audio == -1 { default_audio = global_index as i64; }

        streams.push(json!({
            "Type": "Audio", "Index": global_index, "Codec": codec.to_lowercase(),
            "Channels": ch, "ChannelLayout": layout, "BitRate": get_i("BitRate"), "SampleRate": get_i("SamplingRate"),
            "DisplayTitle": display_title, "Title": display_title, "DisplayLanguage": lang_str,
            "Language": lang_iso, "IsDefault": is_default, "IsForced": false, "IsExternal": false,
            "Protocol": "File", "SupportsExternalStream": false, "AttachmentSize": 0, "IsAnamorphic": false
        }));
        global_index += 1;
    }

    // 3. 字幕流
    let text_count = mi.count_get(StreamKind::Text, None);
    for i in 0..text_count {
        let get_str = |k| mi.get(StreamKind::Text, i, k, InfoKind::Text);
        let get_i = |k| get_str(k).parse::<i64>().unwrap_or(0);
        
        let codec = get_str("Format").to_uppercase();
        let lang_code = get_str("Language").to_lowercase();
        let lang_str = if lang_code.starts_with("zh") || lang_code == "chi" { "Chinese" } else if lang_code.starts_with("en") || lang_code == "eng" { "English" } else { "Unknown" };
        let lang_iso = if lang_code.starts_with("zh") || lang_code == "chi" { "chi" } else if lang_code.starts_with("en") || lang_code == "eng" { "eng" } else { "und" };
        let title_name = get_str("Title");
        
        let display_title = if title_name.is_empty() { format!("{} ({})", lang_str, codec) } else { format!("{} ({}) {}", lang_str, codec, title_name) };

        let is_default = get_i("Default") == 1;
        if is_default && default_sub == -1 { default_sub = global_index as i64; }

        streams.push(json!({
            "Type": "Subtitle", "Index": global_index, "Codec": codec.to_lowercase(),
            "DisplayTitle": display_title, "Title": display_title, "DisplayLanguage": lang_str,
            "Language": lang_iso, "IsDefault": is_default, "IsForced": get_i("Forced") == 1, "IsExternal": false, "IsExternalUrl": false,
            "Protocol": "File", "SupportsExternalStream": false, "AttachmentSize": 0, "IsAnamorphic": false,
            "DeliveryMethod": "Embed", "SubtitleLocationType": "InternalStream"
        }));
        global_index += 1;
    }

    if default_sub == -1 && text_count > 0 {
        default_sub = (video_count + audio_count) as i64; 
    }

    Ok(json!({
        "MediaStreams": streams,
        "DefaultAudioStreamIndex": default_audio,
        "DefaultSubtitleStreamIndex": default_sub
    }))
}
