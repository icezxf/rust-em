//! 媒体库扫描与入库。
//!
//! 职责：遍历目录 → 解析 .strm / .nfo → 匹配图片 → 读取 probe 缓存 → 写入 items 表。
//! 依赖：nfo.rs（XML 解析）、probe.rs（媒体探测缓存）。

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{UNIX_EPOCH, SystemTime};
use std::path::{Path, PathBuf};
use rusqlite::Connection;
use serde_json::{json, Value};

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

pub fn scan_directory(pool: Arc<Mutex<Connection>>, lib_id: i64, lib_type: String, dir_path: String) {
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

        let mut final_size = if probe_size > 0 { probe_size } else { size };
        if final_size < 1024 * 1024 { final_size = 0; }
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
