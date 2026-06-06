use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use contextpool::{
    admin, auth, db,
    http::{self, AppState},
    pool, proxy, settings, stats,
};
use serde_json::Value;
use tower::ServiceExt;

fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://contextpool:contextpool@127.0.0.1:45432/contextpool".to_string()
    })
}

async fn reset_db(pool: &sqlx::PgPool) {
    sqlx::query(
        r#"
        TRUNCATE TABLE
            context7_request_logs,
            context7_minute_stats,
            admin_sessions,
            admin_users,
            relay_tokens,
            api_keys,
            settings
        RESTART IDENTITY CASCADE
        "#,
    )
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn admin_stats_and_relay_routes_work() {
    let pool = db::connect(&database_url()).await.unwrap();
    db::migrate(&pool).await.unwrap();
    reset_db(&pool).await;

    let manager = pool::Manager::new(pool::Config {
        failure_threshold: 2,
        cooldown: std::time::Duration::from_secs(30),
        flush_interval: std::time::Duration::from_millis(50),
        flush_batch_size: 64,
    });
    let settings_service = settings::Service::new(pool.clone());
    let stats_service = stats::Service::new(
        pool.clone(),
        stats::Config {
            flush_interval: std::time::Duration::from_millis(50),
            batch_size: 64,
            queue_size: 128,
        },
    );
    let auth_service = auth::Service::new(pool.clone());
    auth_service.bootstrap().await.unwrap();
    let admin_service = admin::Service::new(
        pool.clone(),
        manager.clone(),
        settings_service.clone(),
        stats_service.clone(),
    );
    admin_service.bootstrap().await.unwrap();
    let proxy_service =
        proxy::Service::new(manager, settings_service, stats_service.clone(), None).unwrap();

    let dist = tempfile::tempdir().unwrap();
    std::fs::write(dist.path().join("index.html"), "<html>admin</html>").unwrap();

    let app = http::router(AppState {
        auth: auth_service,
        admin: admin_service,
        proxy: proxy_service,
        frontend_dist: dist.path().to_path_buf(),
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/admin/auth/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["setup_required"], true);
    assert_eq!(body["authenticated"], false);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/admin/auth/setup")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"password123"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = json_body(response).await;
    let admin_token = body["token"].as_str().unwrap().to_string();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/admin/keys")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::from(
                    r#"{"name":"primary","api_key":"ctx7sk-secret","enabled":true}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let key = json_body(response).await;
    assert_eq!(key["api_key"], "ctx7sk-secret");
    assert_eq!(key["masked_api_key"], "ctx7...cret");

    stats_service.record(stats::Event {
        api_key_id: 1,
        api_key_name: "primary".to_string(),
        method: "GET".to_string(),
        path: "/relay/context7/v2/context".to_string(),
        status_code: 401,
        latency_ms: 12,
        error: "Unauthorized".to_string(),
        started_at: Utc.with_ymd_and_hms(2026, 4, 26, 9, 10, 0).unwrap(),
        finished_at: Utc.with_ymd_and_hms(2026, 4, 26, 9, 10, 0).unwrap(),
        ..stats::Event::default()
    });
    stats_service.flush().await.unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/admin/stats/context7/summary")
                .header(header::AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let summary = json_body(response).await;
    assert_eq!(summary["total_requests"], 1);
    assert_eq!(summary["failed_requests"], 1);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/admin/relay-token")
                .header(header::AUTHORIZATION, format!("Bearer {admin_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let relay = json_body(response).await;
    let relay_token = relay["token"].as_str().unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/relay/context7/v2/context")
                .header(header::AUTHORIZATION, format!("Bearer {relay_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        String::from_utf8(to_bytes(response.into_body(), 1024).await.unwrap().to_vec())
            .unwrap()
            .contains("admin")
    );
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}
