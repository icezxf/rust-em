use rusqlite::Connection;
pub fn init_db(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT);
        CREATE TABLE IF NOT EXISTS libraries (id INTEGER PRIMARY KEY, name TEXT, path TEXT, type TEXT);
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY, library_id INTEGER, title TEXT, file_path TEXT UNIQUE,
            type TEXT, plot TEXT, year TEXT, poster_path TEXT, fanart_path TEXT,
            logo_path TEXT, thumb_path TEXT, banner_path TEXT, size INTEGER DEFAULT 0, mtime INTEGER DEFAULT 0,
            rating REAL DEFAULT 0, mpaa TEXT, genres TEXT, directors TEXT, actors TEXT,
            runtime INTEGER DEFAULT 0, num TEXT, premiered TEXT, tagline TEXT, studios TEXT, tmdb_id TEXT, imdb_id TEXT,
            series TEXT, label TEXT, set_name TEXT
        );
        INSERT OR IGNORE INTO users (username, password) VALUES ('admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918');"
    ).unwrap();
    let missing_columns = [
        ("size", "INTEGER DEFAULT 0"), ("mtime", "INTEGER DEFAULT 0"), ("logo_path", "TEXT"), ("thumb_path", "TEXT"), ("banner_path", "TEXT"), ("rating", "REAL DEFAULT 0"), ("mpaa", "TEXT"), ("genres", "TEXT"), ("directors", "TEXT"), ("actors", "TEXT"), ("runtime", "INTEGER DEFAULT 0"), ("num", "TEXT"), ("premiered", "TEXT"), ("tagline", "TEXT"), ("studios", "TEXT"), ("tmdb_id", "TEXT"), ("imdb_id", "TEXT"), ("series", "TEXT"), ("label", "TEXT"), ("set_name", "TEXT"),
    ];
    for (col_name, col_type) in missing_columns {
        let check_sql = format!("SELECT {} FROM items LIMIT 1", col_name);
        if conn.prepare(&check_sql).is_err() {
            let _ = conn.execute(&format!("ALTER TABLE items ADD COLUMN {} {}", col_name, col_type), []);
        }
    }
}
