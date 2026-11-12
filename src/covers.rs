
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
    let count = paths.len();

    if count == 0 { return; }
    if count == 1 {
        draw_to_region(&mut canvas, &paths[0], 0, 0, 800, 450);
        if let Err(e) = canvas.save(out_path) { println!("❌ [COVERS] 保存失败: {}", e); }
        return;
    }

    // 横版 3x3 网格，类似你截图右上角的密集拼接
    let grid_size = 3;
    let cell_w = 800 / grid_size;
    let cell_h = 450 / grid_size;
    
    for i in 0..9 {
        if i >= count { break; }
        let row = i / grid_size;
        let col = i % grid_size;
        draw_to_region(&mut canvas, &paths[i], col * cell_w, row * cell_h, cell_w, cell_h);
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

// 核心：不裁剪，背景模糊填充，前景完整显示
fn draw_to_region(canvas: &mut image::RgbImage, path: &str, x: u32, y: u32, w: u32, h: u32) {
    if let Ok(img) = image::open(path) {
        let (img_w, img_h) = img.dimensions();
        let target_ratio = w as f64 / h as f64;
        let img_ratio = img_w as f64 / img_h as f64;

        // 1. 画背景（强行拉伸铺满，并高斯模糊）
        let bg = img.resize_exact(w, h, image::imageops::FilterType::Gaussian).blur(15.0);
        let _ = canvas.copy_from(&bg.to_rgb8(), x, y);

        // 2. 计算完整缩放后的尺寸 (Contain)
        let (new_w, new_h) = if img_ratio > target_ratio {
            (w, (w as f64 / img_ratio) as u32) // 宽贴边，高留空
        } else {
            ((h as f64 * img_ratio) as u32, h) // 高贴边，宽留空
        };

        // 3. 画前景
        let thumb = img.resize_exact(new_w, new_h, image::imageops::FilterType::Lanczos3);
        let offset_x = x + (w - new_w) / 2;
        let offset_y = y + (h - new_h) / 2;
        let _ = canvas.copy_from(&thumb.to_rgb8(), offset_x, offset_y);
    }
}
