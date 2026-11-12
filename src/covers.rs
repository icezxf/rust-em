
use image::{GenericImage, GenericImageView};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use rusqlite::Connection;
use std::path::PathBuf;
use sha2::{Digest, Sha256};


fn sanitize_filename(name: &str) -> String {
    let mut sanitized = String::new();
    for c in name.chars() {
        if c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|' {
            sanitized.push('_');
        } else {
            sanitized.push(c);
        }
    }
    sanitized.trim().to_string()
}



pub fn generate_all_covers(pool: Arc<Mutex<Connection>>) {
    let cache_dir = PathBuf::from("data/cache/covers");
    let _ = std::fs::create_dir_all(cache_dir.join("library"));
    let _ = std::fs::create_dir_all(cache_dir.join("genre"));
    let _ = std::fs::create_dir_all(cache_dir.join("tag"));
    let _ = std::fs::create_dir_all(cache_dir.join("boxset"));

    let mut valid_files = std::collections::HashSet::new();
    let mut jobs = Vec::new();

    // 用花括号限制锁的作用域，查完数据立刻释放，绝不影响主服务！
    {
        let conn = match pool.lock() { Ok(c) => c, Err(_) => return };

        // 1. 收集媒体库
        if let Ok(mut stmt) = conn.prepare("SELECT id FROM libraries") {
            if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, i64>(0)) {
                for id in mapped.flatten() {
                    let paths: Vec<String> = {
                        let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE library_id = ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id DESC LIMIT 4").unwrap();
                        p_stmt.query_map([id], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
                    };
                    if !paths.is_empty() {
                        let out_path = cache_dir.join("library").join(format!("lib_{}.jpg", id));
                        let hash_path = out_path.with_extension("hash");
                        valid_files.insert(out_path.to_string_lossy().to_string());
                        valid_files.insert(hash_path.to_string_lossy().to_string());
                        jobs.push((0, paths, out_path, hash_path)); // 0 = 横版
                    }
                }
            }
        }

        // 2. 收集类型
        let mut all_genres = std::collections::HashSet::new();
        if let Ok(mut stmt) = conn.prepare("SELECT genres FROM items WHERE genres != ''") {
            if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                for g_str in mapped.flatten() {
                    for g in g_str.split(" / ") { if !g.trim().is_empty() { all_genres.insert(g.trim().to_string()); } }
                }
            }
        }
        for genre in all_genres {
            let like = format!("%{}%", genre);
            let paths: Vec<String> = {
                let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE genres LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id DESC LIMIT 4").unwrap();
                p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
            };
            if !paths.is_empty() {
                let out_path = cache_dir.join("genre").join(format!("genre_{}.jpg", sanitize_filename(&genre)));
                let hash_path = out_path.with_extension("hash");
                valid_files.insert(out_path.to_string_lossy().to_string());
                valid_files.insert(hash_path.to_string_lossy().to_string());
                jobs.push((1, paths, out_path, hash_path)); // 1 = 竖版
            }
        }

        // 3. 收集标签
        let mut all_tags = std::collections::HashSet::new();
        if let Ok(mut stmt) = conn.prepare("SELECT tags FROM items WHERE tags != ''") {
            if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                for t_str in mapped.flatten() {
                    for t in t_str.split(" / ") { if !t.trim().is_empty() { all_tags.insert(t.trim().to_string()); } }
                }
            }
        }
        for tag in all_tags {
            let like = format!("%{}%", tag);
            let paths: Vec<String> = {
                let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE tags LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id DESC LIMIT 4").unwrap();
                p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
            };
            if !paths.is_empty() {
                let out_path = cache_dir.join("tag").join(format!("tag_{}.jpg", sanitize_filename(&tag)));
                let hash_path = out_path.with_extension("hash");
                valid_files.insert(out_path.to_string_lossy().to_string());
                valid_files.insert(hash_path.to_string_lossy().to_string());
                jobs.push((1, paths, out_path, hash_path)); // 1 = 竖版
            }
        }

        // 4. 收集合集
        let mut all_boxsets = std::collections::HashSet::new();
        if let Ok(mut stmt) = conn.prepare("SELECT set_name FROM items WHERE set_name != ''") {
            if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                for b_str in mapped.flatten() {
                    for b in b_str.split(" / ") { if !b.trim().is_empty() { all_boxsets.insert(b.trim().to_string()); } }
                }
            }
        }
        for boxset in all_boxsets {
            let like = format!("%{}%", boxset);
            let paths: Vec<String> = {
                let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE set_name LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id DESC LIMIT 4").unwrap();
                p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
            };
            if !paths.is_empty() {
                let mut h = Sha256::new(); h.update(boxset.as_bytes()); let hash = h.finalize();
                let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
                let out_path = cache_dir.join("boxset").join(format!("boxset_{}_{}.jpg", hash_hex, sanitize_filename(&boxset)));
                let hash_path = out_path.with_extension("hash");
                valid_files.insert(out_path.to_string_lossy().to_string());
                valid_files.insert(hash_path.to_string_lossy().to_string());
                jobs.push((1, paths, out_path, hash_path)); // 1 = 竖版
            }
        }
    } // 🔒 数据库锁在这里释放，主服务瞬间恢复流畅！

    // ===== 开始后台处理图片 =====
    println!("🎨 [COVERS] 正在处理 {} 个封面任务...", jobs.len());
    let mut generated = 0;
    for (kind, paths, out_path, hash_path) in jobs {
        let need = {
            if !out_path.exists() { true } else {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                for p in &paths { std::hash::Hash::hash(p, &mut hasher); }
                let current_hash = format!("{:x}", std::hash::Hasher::finish(&hasher));
                if let Ok(old_hash) = std::fs::read_to_string(&hash_path) {
                    if old_hash == current_hash { false } else {
                        let _ = std::fs::write(&hash_path, &current_hash); true
                    }
                } else {
                    let _ = std::fs::write(&hash_path, &current_hash); true
                }
            }
        };

        if need {
            if kind == 0 {
                generate_landscape_cover(&paths, &out_path);
            } else {
                generate_portrait_cover(&paths, &out_path);
            }
            generated += 1;
            // 💡 主动休眠 20 毫秒，让出 CPU，绝对不影响主服务看视频、刷列表
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    // 清理幽灵文件
    let mut removed = 0;
    for folder in &["library", "genre", "tag", "boxset"] {
        let dir = cache_dir.join(folder);
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    let path_str = path.to_string_lossy().to_string();
                    if !valid_files.contains(&path_str) {
                        let _ = std::fs::remove_file(&path);
                        removed += 1;
                    }
                }
            }
        }
    }

    println!("✅ [COVERS] 封面生成完毕。新生成 {} 个，清理 {} 个。", generated, removed);
}

fn generate_landscape_cover(paths: &[String], out_path: &std::path::Path) {
    let mut canvas = image::RgbImage::new(800, 450);
    let count = paths.len();
    if count == 0 { return; }
    let cell_w = 800 / (count as u32);
    for (i, path) in paths.iter().enumerate() {
        let x = i as u32 * cell_w;
        let w = if i == count - 1 { 800 - x } else { cell_w };
        draw_to_region(&mut canvas, path, x, 0, w, 450);
    }
    if let Err(e) = canvas.save(out_path) { println!("❌ [COVERS] 保存失败: {}", e); }
}

fn generate_portrait_cover(paths: &[String], out_path: &std::path::Path) {
    let mut canvas = image::RgbImage::new(400, 600);
    let count = paths.len().min(4);
    match count {
        0 => {}
        1 => { draw_to_region(&mut canvas, &paths[0], 0, 0, 400, 600); }
        2 => {
            draw_to_region(&mut canvas, &paths[0], 0, 0, 200, 600);
            draw_to_region(&mut canvas, &paths[1], 200, 0, 200, 600);
        }
        3 => {
            draw_to_region(&mut canvas, &paths[0], 0, 0, 200, 600);
            draw_to_region(&mut canvas, &paths[1], 200, 0, 200, 300);
            draw_to_region(&mut canvas, &paths[2], 200, 300, 200, 300);
        }
        _ => {
            draw_to_region(&mut canvas, &paths[0], 0, 0, 200, 300);
            draw_to_region(&mut canvas, &paths[1], 200, 0, 200, 300);
            draw_to_region(&mut canvas, &paths[2], 0, 300, 200, 300);
            draw_to_region(&mut canvas, &paths[3], 200, 300, 200, 300);
        }
    }
    if let Err(e) = canvas.save(out_path) { println!("❌ [COVERS] 保存失败: {}", e); }
}

fn draw_to_region(canvas: &mut image::RgbImage, path: &str, x: u32, y: u32, w: u32, h: u32) {
    if let Ok(img) = image::open(path) {
        let (img_w, img_h) = img.dimensions();
        let target_ratio = w as f64 / h as f64;
        let img_ratio = img_w as f64 / img_h as f64;

        // 1. 填充纯深色背景（极速，完全不耗 CPU）
        for cy in y..y+h {
            for cx in x..x+w {
                canvas.put_pixel(cx, cy, image::Rgb([22, 25, 31]));
            }
        }

        // 2. 等比计算尺寸
        let (new_w, new_h) = if img_ratio > target_ratio {
            (w, (w as f64 / img_ratio) as u32)
        } else {
            ((h as f64 * img_ratio) as u32, h)
        };

        // 3. 使用 thumbnail（专为缩小优化的极速算法）
        let thumb = img.thumbnail(new_w, new_h).to_rgb8();
        let offset_x = x + (w.saturating_sub(new_w)) / 2;
        let offset_y = y + (h.saturating_sub(new_h)) / 2;
        let _ = canvas.copy_from(&thumb, offset_x, offset_y);
    }
}
