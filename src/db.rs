use rusqlite::Connection;

pub fn init_db(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            password TEXT
        );
        CREATE TABLE IF NOT EXISTS libraries (
            id INTEGER PRIMARY KEY,
            name TEXT,
            path TEXT,
            type TEXT
        );
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY,
            library_id INTEGER,
            title TEXT,
            file_path TEXT UNIQUE,
            type TEXT,
            plot TEXT,
            year TEXT,
            poster_path TEXT,
            fanart_path TEXT,
            logo_path TEXT,        -- 新增
            thumb_path TEXT,       -- 新增
            banner_path TEXT,      -- 新增
            size INTEGER DEFAULT 0,
            mtime INTEGER DEFAULT 0
        );
        INSERT OR IGNORE INTO users (username, password) VALUES ('admin', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918');"
    ).unwrap();
}
