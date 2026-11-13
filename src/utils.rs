use serde_json::{json, Value};

pub fn decode_url(input: &str) -> String {
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

pub fn to_hex(s: &str) -> String { s.as_bytes().iter().map(|b| format!("{:02x}", b)).collect() }

pub fn from_hex(s: &str) -> Option<String> {
    if !s.chars().all(|c| c.is_ascii_hexdigit()) { return None; }
    let mut bytes = Vec::new();
    for i in (0..s.len()).step_by(2) {
        if i + 1 < s.len() {
            if let Ok(b) = u8::from_str_radix(&s[i..i+2], 16) { bytes.push(b); } else { return None; }
        }
    }
    String::from_utf8(bytes).ok()
}

pub fn format_iso8601(unix_secs: i64) -> String {
    if let Some(dt) = chrono::DateTime::from_timestamp(unix_secs, 0) { dt.format("%Y-%m-%dT%H:%M:%S.0000000Z").to_string() } 
    else { "2024-01-01T00:00:00.0000000Z".to_string() }
}

pub fn extract_extension_from_url(url: &str) -> String {
    let path_part = url.split('?').next().unwrap_or(url);
    if let Some(idx) = path_part.rfind('.') {
        let ext = &path_part[idx+1..];
        if !ext.is_empty() && ext.len() <= 5 && ext.chars().all(|c| c.is_ascii_alphanumeric()) { return ext.to_lowercase(); }
    }
    "mp4".to_string()
}

pub fn sanitize_media_streams(streams: &mut Value) {
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
