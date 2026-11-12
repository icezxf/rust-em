
use image::{GenericImage, GenericImageView};
use std::sync::{Arc, Mutex};
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

    let conn = match pool.lock() { Ok(c) => c, Err(_) => return };
    println!("🎨 [COVERS] 开始生成媒体库封面...");
    // 1. 生成媒体库封面 (16:9)
    if let Ok(mut stmt) = conn.prepare("SELECT id FROM libraries") {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, i64>(0)) {
            for id in mapped.flatten() {
                let paths: Vec<String> = {
                    let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE library_id = ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 4").unwrap();
                    p_stmt.query_map([id], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
                };
                if !paths.is_empty() {
                    let out_path = cache_dir.join("library").join(format!("lib_{}.jpg", id));
                    generate_landscape_cover(&paths, &out_path);
                }
            }
        }
    }

    println!("🎨 [COVERS] 开始生成类型封面...");
    // 2. 生成类型封面 (2:3)
    let mut all_genres = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare("SELECT genres FROM items WHERE genres != ''") {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for g_str in mapped.flatten() {
                for g in g_str.split(" / ") { if !g.trim().is_empty() { all_genres.insert(g.trim().to_string()); } }
            }
        }
    }
    for genre in all_genres {
        let paths: Vec<String> = {
            let like = format!("%{}%", genre);
            let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE genres LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 4").unwrap();
            p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
        };
        if !paths.is_empty() {
            let out_path = cache_dir.join("genre").join(format!("genre_{}.jpg", genre));
            generate_portrait_cover(&paths, &out_path);
        }
    }

    println!("🎨 [COVERS] 开始生成标签封面...");
    // 3. 生成标签封面 (2:3)
    let mut all_tags = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare("SELECT tags FROM items WHERE tags != ''") {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for t_str in mapped.flatten() {
                for t in t_str.split(" / ") { if !t.trim().is_empty() { all_tags.insert(t.trim().to_string()); } }
            }
        }
    }
    for tag in all_tags {
        let paths: Vec<String> = {
            let like = format!("%{}%", tag);
            let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE tags LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 4").unwrap();
            p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
        };
        if !paths.is_empty() {
            let out_path = cache_dir.join("tag").join(format!("tag_{}.jpg", tag));
            generate_portrait_cover(&paths, &out_path);
        }
    }

    // 4. 生成合集封面 (2:3)
    println!("🎨 [COVERS] 开始生成合集封面...");
    let mut all_boxsets = std::collections::HashSet::new();
    if let Ok(mut stmt) = conn.prepare("SELECT set_name FROM items WHERE set_name != ''") {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, String>(0)) {
            for b_str in mapped.flatten() {
                for b in b_str.split(" / ") { if !b.trim().is_empty() { all_boxsets.insert(b.trim().to_string()); } }
            }
        }
    }
    for boxset in all_boxsets {
        let paths: Vec<String> = {
            let like = format!("%{}%", boxset);
            let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE set_name LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 4").unwrap();
            p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
        };
        if !paths.is_empty() {
            let mut h = Sha256::new(); h.update(boxset.as_bytes()); let hash = h.finalize();
            let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            let out_path = cache_dir.join("boxset").join(format!("boxset_{}_{}.jpg", hash_hex, sanitize_filename(&boxset)));
            generate_portrait_cover(&paths, &out_path);
        }
    }

    println!("✅ [COVERS] 封面生成完毕。");
}







fn generate_landscape_cover(paths: &[String], out_path: &std::path::Path) {
    let mut canvas = image::RgbImage::new(800, 450);
    let count = paths.len();
    if count == 0 { return; }
    
    // 1xN 横向均匀平分
    let cell_w = 800 / (count as u32);
    for (i, path) in paths.iter().enumerate() {
        let x = i as u32 * cell_w;
        // 处理最后一个元素的余数，确保铺满 800
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

        // 【性能暴增 1】：背景不原图模糊，先缩小到 1/8，模糊后再拉伸，肉眼完全看不出区别
        let small_w = (w / 8).max(1);
        let small_h = (h / 8).max(1);
        let bg = img.resize_exact(small_w, small_h, image::imageops::FilterType::Nearest)
                     .blur(3.0)
                     .resize_exact(w, h, image::imageops::FilterType::Triangle);
        let _ = canvas.copy_from(&bg.to_rgb8(), x, y);

        // 计算等比缩放尺寸
        let (new_w, new_h) = if img_ratio > target_ratio {
            (w, (w as f64 / img_ratio) as u32)
        } else {
            ((h as f64 * img_ratio) as u32, h)
        };

        // 【性能暴增 2】：放弃 Lanczos3，改用 CatmullRom（画质依然极佳，但速度快好几倍）
        let thumb = img.resize_exact(new_w, new_h, image::imageops::FilterType::CatmullRom);
        let offset_x = x + (w.saturating_sub(new_w)) / 2;
        let offset_y = y + (h.saturating_sub(new_h)) / 2;
        let _ = canvas.copy_from(&thumb.to_rgb8(), offset_x, offset_y);
    }
}
