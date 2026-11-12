use image::codecs::jpeg::JpegEncoder;

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




pub async fn generate_all_covers(pool: Arc<Mutex<Connection>>) {
    let cache_dir = PathBuf::from("data/cache/covers");
    let _ = std::fs::create_dir_all(cache_dir.join("library"));
    let _ = std::fs::create_dir_all(cache_dir.join("genre"));
    let _ = std::fs::create_dir_all(cache_dir.join("tag"));
    let _ = std::fs::create_dir_all(cache_dir.join("boxset"));

    let mut jobs = Vec::new();

    // 🔒 用花括号限制锁的作用域，查完数据立刻释放！防止在 await 时死锁
    {
        let conn = match pool.lock() { Ok(c) => c, Err(_) => return };

        // 1. 收集媒体库
        if let Ok(mut stmt) = conn.prepare("SELECT id FROM libraries") {
            if let Ok(mapped) = stmt.query_map([], |r| r.get::<_, i64>(0)) {
                for id in mapped.flatten() {
                    let paths: Vec<String> = {
                        let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE library_id = ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id ASC LIMIT 4").unwrap();
                        p_stmt.query_map([id], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
                    };
                    if !paths.is_empty() {
                        let out_path = cache_dir.join("library").join(format!("lib_{}.jpg", id));
                        jobs.push((0, paths, out_path, format!("library_{}", id)));
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
                let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE genres LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id ASC LIMIT 4").unwrap();
                p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
            };
            if !paths.is_empty() {
                let out_path = cache_dir.join("genre").join(format!("genre_{}.jpg", sanitize_filename(&genre)));
                jobs.push((1, paths, out_path, format!("genre_{}", genre)));
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
                let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE tags LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id ASC LIMIT 4").unwrap();
                p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
            };
            if !paths.is_empty() {
                let out_path = cache_dir.join("tag").join(format!("tag_{}.jpg", sanitize_filename(&tag)));
                jobs.push((1, paths, out_path, format!("tag_{}", tag)));
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
                let mut p_stmt = conn.prepare("SELECT poster_path FROM items WHERE set_name LIKE ?1 AND poster_path != '' AND poster_path NOT LIKE 'http%' ORDER BY id ASC LIMIT 4").unwrap();
                p_stmt.query_map([&like], |r| r.get::<_, String>(0)).unwrap().filter_map(Result::ok).collect()
            };
            if !paths.is_empty() {
                let mut h = Sha256::new(); h.update(boxset.as_bytes()); let hash = h.finalize();
                let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
                let out_path = cache_dir.join("boxset").join(format!("boxset_{}_{}.jpg", hash_hex, sanitize_filename(&boxset)));
                jobs.push((1, paths, out_path, format!("boxset_{}", boxset)));
            }
        }
    } // 🔒 锁在这里释放，主服务瞬间恢复响应

    println!("🎨 [COVERS] 正在检查 {} 个封面任务...", jobs.len());
    let mut generated = 0;
    let mut skipped = 0;

    for (kind, paths, out_path, display_name) in jobs {
        let hash_path = out_path.with_extension("hash");
        
        let need = if !out_path.exists() || !hash_path.exists() {
            true
        } else {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for p in &paths { std::hash::Hash::hash(p, &mut hasher); }
            let current_hash = format!("{:x}", std::hash::Hasher::finish(&hasher));
            
            if let Ok(old_hash) = std::fs::read_to_string(&hash_path) {
                old_hash != current_hash
            } else {
                true
            }
        };

        if need {
            println!("  -> 正在生成: {}", display_name);
            let out_c = out_path.clone();
            let p_c = paths.clone();
            
            // 丢入阻塞线程池处理 CPU 密集型解码和画图，不卡主线程
            let _ = tokio::task::spawn_blocking(move || {
                if kind == 0 {
                    generate_landscape_cover(&p_c, &out_c);
                } else {
                    generate_portrait_cover(&p_c, &out_c);
                }
            }).await;

            generated += 1;
            
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for p in &paths { std::hash::Hash::hash(p, &mut hasher); }
            let current_hash = format!("{:x}", std::hash::Hasher::finish(&hasher));
            let _ = std::fs::write(&hash_path, current_hash);
            
            // 💡 非阻塞休眠 150ms，让出 CPU
            tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
        } else {
            skipped += 1;
        }
    }

    println!("✅ [COVERS] 封面检查完毕。实际生成: {} 个，跳过(未变化): {} 个。", generated, skipped);
}



fn generate_landscape_cover(paths: &[String], out_path: &std::path::Path) {
    let count = paths.len().min(4);
    if count == 0 { return; }

    // 媒体库（横版）：全部横向无缝拼接 1×N (N为实际图片数)
    let cell_w = 400;
    let cell_h = 600;
    let mut canvas = image::RgbImage::new(cell_w * count as u32, cell_h);

    for (i, path) in paths.iter().take(count).enumerate() {
        draw_to_region(&mut canvas, path, i as u32 * cell_w, 0, cell_w, cell_h);
    }
    if let Err(e) = canvas.save(out_path) { println!("❌ [COVERS] 保存失败: {}", e); }
}

fn generate_portrait_cover(paths: &[String], out_path: &std::path::Path) {
    let count = paths.len().min(4);
    if count == 0 { return; }

    // 类型/标签（竖版）：也是无缝拼接
    let cell_w = 400;
    let cell_h = 600;
    
    // 动态决定画布大小，保证一像素的留白都不会有
    let (canvas_w, canvas_h) = match count {
        1 => (cell_w, cell_h),
        2 => (cell_w * 2, cell_h),
        3 => (cell_w * 3, cell_h),
        _ => (cell_w * 2, cell_h * 2), // 4张拼 2x2 完美正方形
    };

    let mut canvas = image::RgbImage::new(canvas_w, canvas_h);

    if count <= 3 {
        for (i, path) in paths.iter().take(count).enumerate() {
            draw_to_region(&mut canvas, path, i as u32 * cell_w, 0, cell_w, cell_h);
        }
    } else {
        draw_to_region(&mut canvas, &paths[0], 0, 0, cell_w, cell_h);
        draw_to_region(&mut canvas, &paths[1], cell_w, 0, cell_w, cell_h);
        draw_to_region(&mut canvas, &paths[2], 0, cell_h, cell_w, cell_h);
        draw_to_region(&mut canvas, &paths[3], cell_w, cell_h, cell_w, cell_h);
    }
    if let Err(e) = canvas.save(out_path) { println!("❌ [COVERS] 保存失败: {}", e); }
}

fn draw_to_region(canvas: &mut image::RgbImage, path: &str, x: u32, y: u32, w: u32, h: u32) {
    if let Ok(img) = image::open(path) {
        // 因为设定的格子 (400x600) 就是标准海报比例 (2:3)
        // 所以直接 resize_exact 不会导致拉伸变形，且绝对填满不留底
        let thumb = img.resize_exact(w, h, image::imageops::FilterType::Triangle).to_rgb8();
        let _ = canvas.copy_from(&thumb, x, y);
    }
}
