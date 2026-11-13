//! 数据库初始化与表结构维护。
//!
//! 职责：建表（users / libraries / items / settings / user_data / ...）、
//! 初始化默认管理员、补齐缺失列、初始化 settings 默认值。

use rusqlite::Connection;

pub fn init_db(conn: &Connection) {
    // 💡 全量建表，有则跳过，无则创建
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS libraries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            path TEXT NOT NULL,
            type TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            library_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            file_path TEXT NOT NULL UNIQUE,
            type TEXT,
            year TEXT,
            plot TEXT,
            poster_path TEXT,
            fanart_path TEXT,
            logo_path TEXT,
            thumb_path TEXT,
            banner_path TEXT,
            size INTEGER DEFAULT 0,
            mtime INTEGER DEFAULT 0,
            rating REAL DEFAULT 0,
            mpaa TEXT,
            genres TEXT,
            tags TEXT,
            directors TEXT,
            actors TEXT,
            runtime INTEGER DEFAULT 0,
            num TEXT,
            premiered TEXT,
            tagline TEXT,
            studios TEXT,
            tmdb_id TEXT,
            imdb_id TEXT,
            series TEXT,
            label TEXT,
            set_name TEXT,
            strm_content TEXT,
            stills TEXT,
            parts TEXT,
            original_title TEXT,
            sort_title TEXT,
            country TEXT,
            language TEXT,
            writers TEXT,
            trailer TEXT,
            releasedate TEXT,
            media_streams TEXT,
            date_created INTEGER DEFAULT 0,
            primary_image_aspect_ratio REAL DEFAULT 0,
            original_language TEXT,
            playback_ticks INTEGER DEFAULT 0,
            play_count INTEGER DEFAULT 0,
            is_favorite INTEGER DEFAULT 0,
            updated_at INTEGER DEFAULT 0,
            series_id INTEGER,
            season_number INTEGER,
            episode_number INTEGER,
            parent_id INTEGER
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS user_library_access (
            user_id INTEGER NOT NULL,
            library_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, library_id)
        );
        CREATE TABLE IF NOT EXISTS user_data (
            user_id INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            is_favorite INTEGER DEFAULT 0,
            play_count INTEGER DEFAULT 0,
            playback_position_ticks INTEGER DEFAULT 0,
            last_played_date TEXT,
            PRIMARY KEY (user_id, item_id)
        );
        CREATE TABLE IF NOT EXISTS boxset_favorites (
            user_id INTEGER NOT NULL,
            boxset_id TEXT NOT NULL,
            PRIMARY KEY (user_id, boxset_id)
        );
        "
    ).expect("初始化数据库表失败");

    // 💡 如果 users 表完全为空，初始化默认 admin/admin (8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918 是 admin 的 sha256)
    let user_count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0)).unwrap_or(0);
    if user_count == 0 {
        let _ = conn.execute(
            "INSERT INTO users (id, username, password) VALUES (1, 'admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918')",
            [],
        );
        println!("✅ [DB] 已初始化默认管理员账号: admin / admin");
    }
}

pub fn ensure_extra_columns(conn: &Connection) {
    // 兼容可能存在的旧库字段缺失（如果是全新库，这些会静默失败或忽略）
    let missing_columns = [
        ("size", "INTEGER DEFAULT 0"), ("mtime", "INTEGER DEFAULT 0"), ("logo_path", "TEXT"),
        ("thumb_path", "TEXT"), ("banner_path", "TEXT"), ("rating", "REAL DEFAULT 0"),
        ("mpaa", "TEXT"), ("genres", "TEXT"), ("tags", "TEXT"), ("directors", "TEXT"),
        ("actors", "TEXT"), ("runtime", "INTEGER DEFAULT 0"), ("num", "TEXT"),
        ("premiered", "TEXT"), ("tagline", "TEXT"), ("studios", "TEXT"), ("tmdb_id", "TEXT"),
        ("imdb_id", "TEXT"), ("series", "TEXT"), ("label", "TEXT"), ("set_name", "TEXT"),
        ("strm_content", "TEXT"), ("stills", "TEXT"), ("parts", "TEXT"),
        ("original_title", "TEXT"), ("sort_title", "TEXT"), ("country", "TEXT"),
        ("language", "TEXT"), ("writers", "TEXT"), ("trailer", "TEXT"),
        ("releasedate", "TEXT"), ("media_streams", "TEXT"), ("date_created", "INTEGER DEFAULT 0"),
        ("primary_image_aspect_ratio", "REAL DEFAULT 0"), ("original_language", "TEXT"),
        ("playback_ticks", "INTEGER DEFAULT 0"), ("updated_at", "INTEGER DEFAULT 0"),
        ("play_count", "INTEGER DEFAULT 0"), ("is_favorite", "INTEGER DEFAULT 0"),
        ("series_id", "INTEGER"), ("season_number", "INTEGER"), ("episode_number", "INTEGER"), ("parent_id", "INTEGER"),
    ];
    
    for (col_name, col_type) in missing_columns {
        let check_sql = format!("SELECT {} FROM items LIMIT 1", col_name);
        if conn.prepare(&check_sql).is_err() {
            let _ = conn.execute(&format!("ALTER TABLE items ADD COLUMN {} {}", col_name, col_type), []);
        }
    }
}

pub fn init_settings_table(conn: &Connection) {
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)", []);
    let _ = conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('prefetch_playback', 'false')", []);
}
