mod db;
mod api;
mod nfo;

use axum::{routing::{get, post}, Router, middleware};
use tower_http::{services::ServeDir, cors::CorsLayer};
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    // 确保数据目录存在（避免挂载卷覆盖导致的报错）
    std::fs::create_dir_all("/app/data").ok();
    
    // 初始化 SQLite 数据库，存放在持久化目录
    let conn = rusqlite::Connection::open("/app/data/rust_em.db").unwrap();
    db::init_db(&conn);
    let db_pool = Arc::new(Mutex::new(conn));

    // 1. 需要鉴权的 API 路由
    let protected_routes = Router::new()
        .route("/libraries", post(api::add_library).get(api::get_libraries))
        .route("/items", get(api::get_items))
        // 【关键修改】：/browse 改回 GET 请求，避开反向代理吃 POST Body 的问题
        .route("/browse", get(api::browse_dir)) 
        .route("/user/update", post(api::update_user))
        .layer(middleware::from_fn(api::auth))
        .with_state(db_pool.clone());

    // 2. 不需要鉴权的公开 API 路由（如登录）
    let public_routes = Router::new()
        .route("/login", post(api::login))
        .with_state(db_pool.clone());

    // 3. 合并 API 路由
    let api_routes = Router::new()
        .merge(public_routes)
        .merge(protected_routes);

    // 4. 组装最终的 App（包含 API 和前端静态文件）
    let app = Router::new()
        .nest("/api", api_routes)
        // Emby 兼容接口的占位
        .route("/System/Info", get(api::emby_info))
        .route("/Users/{userId}/Items", get(api::emby_items))
        // 托管前端静态文件（将 public 目录映射到根路径 /）
        .fallback_service(ServeDir::new("public"))
        // 允许跨域请求，防止浏览器报错
        .layer(CorsLayer::permissive());

    // 启动服务监听 8096 端口
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8096").await.unwrap();
    println!("🚀 rust-em running on 8096");
    axum::serve(listener, app).await.unwrap();
}