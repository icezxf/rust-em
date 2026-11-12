
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
    println!("✅ [COVERS] 封面生成完毕。");
}

fn generate_landscape_cover(paths: &[String], out_path: &std::path::Path) {
    let mut canvas = image::RgbImage::new(800, 450);
    let count = paths.len();
    let slice_width = 800 / count as u32;
    
    for (i, path) in paths.iter().enumerate() {
        if let Ok(img) = image::open(path) {
            let (w, h) = img.dimensions();
            let target_ratio = slice_width as f64 / 450.0;
            let img_ratio = w as f64 / h as f64;
            
            let (crop_w, crop_h) = if img_ratio > target_ratio {
                ((450.0 * img_ratio) as u32, h)
            } else {
                (w, (w as f64 / target_ratio) as u32)
            };
            let x = (w - crop_w) / 2;
            let y = (h - crop_h) / 2;
            
            let thumb = img.crop_imm(x, y, crop_w, crop_h).resize_exact(slice_width, 450, FilterType::Triangle);
            let _ = canvas.copy_from(&thumb.to_rgb8(), (i as u32) * slice_width, 0);
        }
    }
    let _ = canvas.save(out_path);
}

fn generate_portrait_cover(paths: &[String], out_path: &std::path::Path) {
    let mut canvas = image::RgbImage::new(400, 600);
    let count = paths.len().min(4);
    
    let positions = [(0, 0), (200, 0), (0, 300), (200, 300)];
    for i in 0..4 {
        if count == 0 { break; }
        let path = &paths[i % count];
        if let Ok(img) = image::open(path) {
            let (w, h) = img.dimensions();
            let target_ratio = 200.0 / 300.0;
            let img_ratio = w as f64 / h as f64;
            
            let (crop_w, crop_h) = if img_ratio > target_ratio {
                ((300.0 * img_ratio) as u32, h)
            } else {
                (w, (w as f64 / target_ratio) as u32)
            };
            let x = (w - crop_w) / 2;
            let y = (h - crop_h) / 2;
            
            let thumb = img.crop_imm(x, y, crop_w, crop_h).resize_exact(200, 300, FilterType::Triangle);
            let _ = canvas.copy_from(&thumb.to_rgb8(), positions[i].0, positions[i].1);
        }
    }
    let _ = canvas.save(out_path);
}
