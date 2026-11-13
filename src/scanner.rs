use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::{UNIX_EPOCH, SystemTime};
use std::path::{Path, PathBuf};
use rusqlite::Connection;
use serde_json::{json, Value};
use regex::Regex;

fn get_image_type(stem: &str) -> Option<&'static str> {
    let lower = stem.to_lowercase();
    let base = if let Some((b, suffix)) = lower.rsplit_once('-') { if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) { b } else { &lower } } else { &lower };
    match base { 
        "poster" => Some("poster"), 
        "fanart" | "backdrop" => Some("fanart"), 
        "logo" | "clearlogo" => Some("logo"), 
        "thumb" | "thumbnail" => Some("thumb"), 
        "banner" => Some("banner"), 
        _ => None 
    }
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
    println!("🔍 开始扫描目录: {:?} (类型: {})", dir_path, lib_type);
    let mut count = 0;
    let mut strm_files = Vec::new();
    
    // 1. 收集所有 .strm 文件
    for entry in walkdir::WalkDir::new(&dir_path).into_iter().filter_map(|e| e.ok()) {
        if entry.path().is_file() && entry.path().extension().and_then(|e| e.to_str()).map_or(false, |e| e.eq_ignore_ascii_case("strm")) {
            strm_files.push(entry.path().to_path_buf());
        }
    }
    
    // 2. 按目录和基础文件名分组（合并分段视频）
    let mut grouped: HashMap<(PathBuf, String), Vec<PathBuf>> = HashMap::new();
    for path in strm_files {
        let stem = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
        let base_stem = get_base_stem(&stem);
        let parent = path.parent().unwrap_or(Path::new("")).to_path_buf();
        grouped.entry((parent, base_stem)).or_default().push(path);
    }

    let re_tv = Regex::new(r"(?i)(?:S|Season)\s*(\d{1,2})[\s._-]*E(?:P|pisode)?\s*(\d{1,3})").unwrap();
    let re_ep = Regex::new(r"(?i)(?:E|EP|第)\s*(\d{1,3})\s*集?").unwrap();
    let re_s = Regex::new(r"(?i)(?:Season|S)\s*(\d{1,2})").unwrap();

    for ((parent, base_stem), mut paths) in grouped {
        count += 1;
        paths.sort();
        let first_path = &paths[0];
        
        let is_tv = lib_type == "tvshows";
        let mut s_num = 1;
        let mut e_num = 1;
        let mut series_title = String::new();
        let mut series_dir = parent.clone();

        if is_tv {
            let parent_name = parent.file_name().unwrap_or_default().to_string_lossy().to_lowercase();
            
            // 判断是否在 Season 目录下
            if parent_name.starts_with("season") || parent_name.starts_with("s0") || parent_name.starts_with("s1") {
                if let Some(p_parent) = parent.parent() {
                    series_dir = p_parent.to_path_buf();
                }
                if let Some(caps) = re_s.captures(&parent_name) {
                    s_num = caps.get(1).unwrap().as_str().parse::<i32>().unwrap_or(1);
                }
            }
            
            // 提取集号
            if let Some(caps) = re_tv.captures(&base_stem) {
                s_num = caps.get(1).unwrap().as_str().parse::<i32>().unwrap_or(s_num);
                e_num = caps.get(2).unwrap().as_str().parse::<i32>().unwrap_or(e_num);
            } else if let Some(caps) = re_ep.captures(&base_stem) {
                e_num = caps.get(1).unwrap().as_str().parse::<i32>().unwrap_or(e_num);
            }

            // 提取剧名（根目录名，去掉年份）
            series_title = series_dir.file_name().unwrap_or_default().to_string_lossy().to_string();
            if let Some(idx) = series_title.rfind('(') {
                series_title = series_title[..idx].trim().to_string();
            }
        }

        let metadata = match std::fs::metadata(first_path) { Ok(m) => m, Err(_) => continue };
        let size: i64 = paths.iter().filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len() as i64)).sum();
        let mtime = metadata.modified().unwrap_or(UNIX_EPOCH).duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        let virtual_path = parent.join(format!("{}.strm", base_stem)).to_string_lossy().to_string();
        let mut combined_content = String::new();
        for p in &paths { if let Ok(c) = std::fs::read_to_string(p) { combined_content.push_str(c.trim()); combined_content.push('\n'); } }
        let strm_content = combined_content.trim().to_string();
        let parts_json = serde_json::to_string(&paths.iter().map(|p| p.to_string_lossy().to_string()).collect::<Vec<_>>()).unwrap_or_default();

        // 解析单集/电影的 NFO
        let nfo_candidates = vec![parent.join(format!("{}.nfo", base_stem)), parent.join("movie.nfo")];
        let mut title = base_stem.clone(); let mut plot = String::new(); let mut year = String::new();
        let mut rating = 0.0; let mut mpaa = String::new(); let mut genres = String::new(); let mut tags = String::new();
        let mut directors = String::new(); let mut actors_json = String::new(); let mut runtime = 0; let mut premiered = String::new();
        let mut releasedate = String::new(); let mut tagline = String::new(); let mut studios = String::new(); let mut tmdb_id = String::new();
        let mut imdb_id = String::new(); let mut series = String::new(); let mut label = String::new(); let mut set_name = String::new();
        let mut nfo_poster = String::new(); let mut nfo_fanart = String::new();
        let mut original_title = String::new(); let mut sort_title = String::new(); let mut country = String::new(); let mut language = String::new();
        let mut writers = String::new(); let mut trailer = String::new(); let mut num = String::new();

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
                    
                    // 💡 核心修复：入库前清洗数据，将混在演员表里的导演剔除
                    let director_names: std::collections::HashSet<String> = meta.directors.iter().cloned().collect();
                    let actors_arr: Vec<Value> = meta.actors.into_iter()
                        .filter(|a| {
                            if let Some(name) = &a.name {
                                !director_names.contains(name) // 如果该演员是导演，直接过滤掉
                            } else {
                                false
                            }
                        })
                        .map(|a| json!({ "name": a.name.unwrap_or_default(), "role": a.role.unwrap_or_default(), "thumb": a.thumb.unwrap_or_default() })).collect();
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
                                    if let Ok(secs) = rest[..end].trim().parse::<i32>() { runtime = secs; }
                                }
                            }
                            if runtime <= 0 {
                                if let Some(start) = nfo_text.find("<duration>") {
                                    let rest = &nfo_text[start + 10..];
                                    if let Some(end) = rest.find("</duration>") {
                                        if let Ok(mins) = rest[..end].trim().parse::<i32>() { runtime = mins * 60; }
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        // 本地图片匹配
        let mut final_poster = nfo_poster; let mut final_fanart = nfo_fanart;
        let mut logo = String::new(); let mut thumb = String::new(); let mut banner = String::new();
        
        // 如果是剧集，优先找同名的 .jpg 或 .png 作为单集封面
        if is_tv {
            for ext in &["jpg", "png", "webp"] {
                let ep_img = parent.join(format!("{}.{}", base_stem, ext));
                if ep_img.exists() { final_poster = ep_img.to_string_lossy().to_string(); break; }
            }
        }
        
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
                        match img_type { 
                            "poster" => final_poster = p_str, 
                            "fanart" => final_fanart = p_str, 
                            "logo" => if logo.is_empty() { logo = p_str }, 
                            "thumb" => if thumb.is_empty() { thumb = p_str }, 
                            "banner" => if banner.is_empty() { banner = p_str }, 
                            _ => {} 
                        }
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
        
        // 读取 Probe 探测缓存
        let mediainfo_json_path = crate::probe::get_mediainfo_json_path(first_path);
        let (probe_size, mut media_streams_json_str) = if mediainfo_json_path.exists() {
            if let Some(src) = crate::probe::load_cached_source(&mediainfo_json_path) {
                let s = src.get("Size").and_then(|v| v.as_i64()).unwrap_or(0);
                let ms = src.get("MediaStreams").cloned().unwrap_or(json!([]));
                let ms_str = serde_json::to_string(&ms).unwrap_or_else(|_| "[]".to_string());
                if let Some(rt_ticks) = src.get("RunTimeTicks").and_then(|v| v.as_i64()) {
                    if rt_ticks > 0 { runtime = (rt_ticks / 10_000_000) as i32; }
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

            if is_tv {
                // === 剧集流程 ===
                let tv_nfo_path = series_dir.join("tvshow.nfo");
                let mut tv_title = series_title.clone();
                let mut tv_plot = String::new();
                let mut tv_year = String::new();
                let mut tv_rating = 0.0;
                let mut tv_genres = String::new();
                let mut tv_premiered = String::new();
                let mut tv_studios = String::new();
                let mut tv_tmdb_id = String::new();
                let mut tv_imdb_id = String::new();
                let mut tv_actors_json = "[]".to_string();
                
                if tv_nfo_path.exists() {
                    if let Some(tv_meta) = crate::nfo::read_nfo(&tv_nfo_path) {
                        if let Some(t) = &tv_meta.title { tv_title = t.clone(); }
                        if let Some(p) = &tv_meta.plot { tv_plot = p.clone(); }
                        if let Some(y) = &tv_meta.year { tv_year = y.clone(); }
                        if let Some(r) = tv_meta.rating { tv_rating = r; }
                        if !tv_meta.genres.is_empty() { tv_genres = tv_meta.genres.join(" / "); }
                        if let Some(pr) = &tv_meta.premiered { tv_premiered = pr.clone(); }
                        if !tv_meta.studios.is_empty() { tv_studios = tv_meta.studios.join(" / "); }
                        for uid in tv_meta.unique_ids {
                            if uid.0 == "tmdb" { tv_tmdb_id = uid.1.clone(); }
                            if uid.0 == "imdb" { tv_imdb_id = uid.1.clone(); }
                        }
                        let actors_arr: Vec<Value> = tv_meta.actors.into_iter().filter(|a| a.name.is_some()).map(|a| json!({ "name": a.name.unwrap_or_default(), "role": a.role.unwrap_or_default(), "thumb": a.thumb.unwrap_or_default() })).collect();
                        tv_actors_json = serde_json::to_string(&actors_arr).unwrap_or_default();
                    }
                }
                
                let mut tv_poster = String::new();
                let mut tv_fanart = String::new();
                for ext in &["jpg", "png"] {
                    if tv_poster.is_empty() && series_dir.join(format!("poster.{}", ext)).exists() { tv_poster = series_dir.join(format!("poster.{}", ext)).to_string_lossy().to_string(); }
                    if tv_fanart.is_empty() && series_dir.join(format!("fanart.{}", ext)).exists() { tv_fanart = series_dir.join(format!("fanart.{}", ext)).to_string_lossy().to_string(); }
                }
                let series_path = series_dir.to_string_lossy().to_string();
                let series_virtual_path = format!("{}/series_virtual", series_path);
                
                // 获取或创建 Series
                let series_id = match conn.query_row("SELECT id FROM items WHERE type = 'Series' AND file_path = ?1", [&series_virtual_path], |r| r.get::<_, i64>(0)) {
                    Ok(id) => {
                        let _ = conn.execute(
                            "UPDATE items SET title = ?1, plot = ?2, year = ?3, rating = ?4, genres = ?5, premiered = ?6, studios = ?7, tmdb_id = ?8, imdb_id = ?9, actors = ?10, poster_path = ?11, fanart_path = ?12, updated_at = ?13 WHERE id = ?14", 
                            rusqlite::params![tv_title, tv_plot, tv_year, tv_rating, tv_genres, tv_premiered, tv_studios, tv_tmdb_id, tv_imdb_id, tv_actors_json, tv_poster, tv_fanart, updated_at, id]
                        );
                        id
                    },
                    Err(_) => {
                        let _ = conn.execute(
                            "INSERT INTO items (library_id, title, type, file_path, poster_path, fanart_path, plot, year, rating, genres, premiered, studios, tmdb_id, imdb_id, actors, updated_at) VALUES (?1, ?2, 'Series', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
                            rusqlite::params![lib_id, tv_title, series_virtual_path, tv_poster, tv_fanart, tv_plot, tv_year, tv_rating, tv_genres, tv_premiered, tv_studios, tv_tmdb_id, tv_imdb_id, tv_actors_json, updated_at]
                        );
                        conn.last_insert_rowid()
                    }
                };

                // 解析季 NFO
                let season_nfo_path = parent.join("season.nfo");
                let mut season_title = format!("Season {}", s_num);
                let mut season_rating = 0.0;
                let mut season_premiered = String::new();
                let mut season_plot = String::new();
                
                if season_nfo_path.exists() {
                    if let Some(s_meta) = crate::nfo::read_nfo(&season_nfo_path) {
                        if let Some(t) = &s_meta.title { season_title = t.clone(); }
                        if let Some(r) = s_meta.rating { season_rating = r; }
                        if let Some(pr) = &s_meta.premiered { season_premiered = pr.clone(); }
                        if let Some(p) = &s_meta.plot { season_plot = p.clone(); }
                    }
                }
                
                // 💡 评分兜底：如果剧集没评分，且季有评分，把季的评分补给剧集
                if tv_rating <= 0.0 && season_rating > 0.0 {
                    let _ = conn.execute("UPDATE items SET rating = ?1 WHERE id = ?2", rusqlite::params![season_rating, series_id]);
                }

                let mut season_poster = String::new();
                let s_pad = format!("{:02}", s_num);
                if series_dir.join(format!("season{}-poster.jpg", s_pad)).exists() {
                    season_poster = series_dir.join(format!("season{}-poster.jpg", s_pad)).to_string_lossy().to_string();
                } else if series_dir.join(format!("season{}-poster.jpg", s_num)).exists() {
                    season_poster = series_dir.join(format!("season{}-poster.jpg", s_num)).to_string_lossy().to_string();
                } else if parent.join("poster.jpg").exists() {
                    season_poster = parent.join("poster.jpg").to_string_lossy().to_string();
                }

                let season_path = format!("{}/season_{}", series_path, s_num);
                let season_id = match conn.query_row("SELECT id FROM items WHERE type = 'Season' AND parent_id = ?1 AND season_number = ?2", rusqlite::params![series_id, s_num], |r| r.get::<_, i64>(0)) {
                    Ok(id) => {
                        let _ = conn.execute("UPDATE items SET title = ?1, rating = ?2, premiered = ?3, plot = ?4, poster_path = ?5, updated_at = ?6 WHERE id = ?7", 
                            rusqlite::params![season_title, season_rating, season_premiered, season_plot, season_poster, updated_at, id]);
                        id
                    },
                    Err(_) => {
                        let _ = conn.execute(
                            "INSERT INTO items (library_id, title, type, file_path, parent_id, series_id, season_number, poster_path, rating, premiered, plot, updated_at) VALUES (?1, ?2, 'Season', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                            rusqlite::params![lib_id, season_title, season_path, series_id, series_id, s_num, season_poster, season_rating, season_premiered, season_plot, updated_at]
                        );
                        conn.last_insert_rowid()
                    }
                };

                let mut ep_title = title.clone();
                if ep_title.is_empty() || ep_title.to_lowercase().contains(&format!("s{:02}e{:02}", s_num, e_num)) {
                    ep_title = format!("第 {} 集", e_num);
                }
                
                let _ = conn.execute(
                    "INSERT INTO items (library_id, title, num, file_path, type, plot, year, poster_path, fanart_path, size, mtime, rating, mpaa, genres, tags, directors, actors, runtime, premiered, studios, tmdb_id, imdb_id, series, strm_content, stills, parts, media_streams, updated_at, series_id, season_number, episode_number, parent_id) 
                     VALUES (?1, ?2, ?3, ?4, 'Episode', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31)
                     ON CONFLICT(file_path) DO UPDATE SET title=excluded.title, size=excluded.size, mtime=excluded.mtime, plot=excluded.plot, poster_path=excluded.poster_path, runtime=excluded.runtime, updated_at=excluded.updated_at, type=excluded.type, series_id=excluded.series_id, season_number=excluded.season_number, episode_number=excluded.episode_number, parent_id=excluded.parent_id",
                    rusqlite::params![lib_id, ep_title, e_num.to_string(), virtual_path, plot, year, final_poster, final_fanart, final_size, mtime, rating, mpaa, genres, tags, directors, actors_json, runtime, premiered, studios, tmdb_id, imdb_id, tv_title, strm_content, stills_json, parts_json, media_streams_json_str, updated_at, series_id, s_num, e_num, season_id]
                );
            } else {
                // === 电影流程 ===
                let _ = conn.execute(
                    "INSERT INTO items (library_id, title, num, file_path, type, plot, year, poster_path, fanart_path, logo_path, thumb_path, banner_path, size, mtime, rating, mpaa, genres, tags, directors, actors, runtime, premiered, tagline, studios, tmdb_id, imdb_id, series, label, set_name, strm_content, stills, parts, original_title, sort_title, country, language, writers, trailer, releasedate, media_streams, updated_at) 
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40, ?41)
                     ON CONFLICT(file_path) DO UPDATE SET title=excluded.title, num=excluded.num, size=excluded.size, mtime=excluded.mtime, plot=excluded.plot, year=excluded.year, poster_path=excluded.poster_path, fanart_path=excluded.fanart_path, logo_path=excluded.logo_path, thumb_path=excluded.thumb_path, banner_path=excluded.banner_path, rating=excluded.rating, mpaa=excluded.mpaa, genres=excluded.genres, tags=excluded.tags, directors=excluded.directors, actors=excluded.actors, runtime=excluded.runtime, premiered=excluded.premiered, tagline=excluded.tagline, studios=excluded.studios, tmdb_id=excluded.tmdb_id, imdb_id=excluded.imdb_id, series=excluded.series, label=excluded.label, set_name=excluded.set_name, strm_content=excluded.strm_content, stills=excluded.stills, parts=excluded.parts, original_title=excluded.original_title, sort_title=excluded.sort_title, country=excluded.country, language=excluded.language, writers=excluded.writers, trailer=excluded.trailer, releasedate=excluded.releasedate, media_streams=excluded.media_streams, updated_at=excluded.updated_at, type=excluded.type",
                    rusqlite::params![lib_id, title, num, virtual_path, lib_type, plot, year, final_poster, final_fanart, logo, thumb, banner, final_size, mtime, rating, mpaa, genres, tags, directors, actors_json, runtime, premiered, tagline, studios, tmdb_id, imdb_id, series, label, set_name, strm_content, stills_json, parts_json, original_title, sort_title, country, language, writers, trailer, releasedate, media_streams_json_str, updated_at]
                );
            }
        }
    }
    println!("✅ 扫描完成。共处理 {} 个媒体项。", count);
}
