
use image::{GenericImage, GenericImageView, imageops::FilterType};
use std::sync::{Arc, Mutex};
use rusqlite::Connection;
use std::path::PathBuf;

pub fn generate_all_covers(pool: Arc<Mutex<Connection>>) {
    let cache_dir = PathBuf::from("data/cache/covers");
    let _ = std::fs::create_dir_all(cache_dir.join("library"));
    let _ = std::fs::create_dir_all(cache_dir.join("genre"));
    let _ = std::fs::create_dir_all(cache_dir.join("tag"));

    let conn = match pool.lock() { Ok(c) => c, Err(_) => return };
    println!("🎨 [COVERS] 开始生成媒体库封面...");
    // 1. 生成媒体库封面 (16:9)
    if let Ok(mut stmt) = conn.prepare("SELECT id FROM libraries") {
        if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, i64>(0)) {
            for id in mapped.flatten() {
                let paths: Vec<String> = {
                    let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE library_id = ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 9").unwrap();
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
            let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE genres LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 9").unwrap();
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
            let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE tags LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY RANDOM() LIMIT 9").unwrap();
            p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
        };
        if !paths.is_empty() {
            let out_path = cache_dir.join("tag").join(format!("tag_{}.jpg", tag));
            generate_portrait_cover(&paths, &out_path);
        }
    }
    println!("✅ [COVERS] 封面生成完毕。");
}




fn generate_landscape_cover(paths: &[String], out_path: &std::path::Path) {
    let mut canvas = image::RgbImage::new(800, 450);
    let count = paths.len().min(4);

    match count {
        0 => {}
        1 => { draw_to_region(&mut canvas, &paths[0], 0, 0, 800, 450); }
        2 => {
            draw_to_region(&mut canvas, &paths[0], 0, 0, 400, 450);
            draw_to_region(&mut canvas, &paths[1], 400, 0, 400, 450);
        }
        3 => {
            draw_to_region(&mut canvas, &paths[0], 0, 0, 400, 450);
            draw_to_region(&mut canvas, &paths[1], 400, 0, 400, 225);
            draw_to_region(&mut canvas, &paths[2], 400, 225, 400, 225);
        }
        _ => {
            draw_to_region(&mut canvas, &paths[0], 0, 0, 400, 225);
            draw_to_region(&mut canvas, &paths[1], 400, 0, 400, 225);
            draw_to_region(&mut canvas, &paths[2], 0, 225, 400, 225);
            draw_to_region(&mut canvas, &paths[3], 400, 225, 400, 225);
        }
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

        // 居中裁剪并填满
        let (crop_w, crop_h) = if img_ratio > target_ratio {
            ((h as f64 * img_ratio) as u32, img_h)
        } else {
            (img_w, (img_w as f64 / target_ratio) as u32)
        };
        let crop_x = (img_w - crop_w) / 2;
        let crop_y = (img_h - crop_h) / 2;

        // 使用 Lanczos3 提升缩放清晰度
        let thumb = img.crop_imm(crop_x, crop_y, crop_w, crop_h).resize_exact(w, h, image::imageops::FilterType::Lanczos3);
        let _ = canvas.copy_from(&thumb.to_rgb8(), x, y);
    }
}
