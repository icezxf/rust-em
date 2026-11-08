use rusqlite::Connection;
pub fn init_db(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT);
        CREATE TABLE IF NOT EXISTS libraries (id INTEGER PRIMARY KEY, name TEXT, path TEXT, type TEXT);
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY, library_id INTEGER, title TEXT, file_path TEXT UNIQUE,
            type TEXT, plot TEXT, year TEXT, poster_path TEXT, fanart_path TEXT,
            logo_path TEXT, thumb_path TEXT, banner_path TEXT, size INTEGER DEFAULT 0, mtime INTEGER DEFAULT 0,
            rating REAL DEFAULT 0, mpaa TEXT, genres TEXT, tags TEXT, directors TEXT, actors TEXT,
            runtime INTEGER DEFAULT 0, num TEXT, premiered TEXT, tagline TEXT, studios TEXT, tmdb_id TEXT, imdb_id TEXT,
            series TEXT, label TEXT, set_name TEXT, strm_content TEXT, stills TEXT,
            parts TEXT -- 新增：存储多个片段路径的JSON数组
        );
        INSERT OR IGNORE INTO users (username, password) VALUES ('admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918');"
    ).unwrap();
    let missing_columns = [
        ("size", "INTEGER DEFAULT 0"), ("mtime", "INTEGER DEFAULT 0"), ("logo_path", "TEXT"), ("thumb_path", "TEXT"), ("banner_path", "TEXT"), ("rating", "REAL DEFAULT 0"), ("mpaa", "TEXT"), ("genres", "TEXT"), ("tags", "TEXT"), ("directors", "TEXT"), ("actors", "TEXT"), ("runtime", "INTEGER DEFAULT 0"), ("num", "TEXT"), ("premiered", "TEXT"), ("tagline", "TEXT"), ("studios", "TEXT"), ("tmdb_id", "TEXT"), ("imdb_id", "TEXT"), ("series", "TEXT"), ("label", "TEXT"), ("set_name", "TEXT"), ("strm_content", "TEXT"), ("stills", "TEXT"), ("parts", "TEXT"),
    ];
    for (col_name, col_type) in missing_columns {
        let check_sql = format!("SELECT {} FROM items LIMIT 1", col_name);
        if conn.prepare(&check_sql).is_err() {
            let _ = conn.execute(&format!("ALTER TABLE items ADD COLUMN {} {}", col_name, col_type), []);
        }
    }
}

pub fn ensure_extra_columns(conn: &rusqlite::Connection) {
    let _ = conn.execute("ALTER TABLE items ADD COLUMN original_title TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN sort_title TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN country TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN language TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN writers TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN trailer TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN releasedate TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN media_streams TEXT", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN date_created INTEGER DEFAULT 0", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN primary_image_aspect_ratio REAL DEFAULT 0", []);
    let _ = conn.execute("ALTER TABLE items ADD COLUMN original_language TEXT", []);
}

pub fn init_settings_table(conn: &rusqlite::Connection) {
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)", []);
    let _ = conn.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('prefetch_playback', 'false')", []);
}
