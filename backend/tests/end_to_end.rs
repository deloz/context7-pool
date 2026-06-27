use axum::{
    Router,
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
    settings_service
        .update_context7("http://127.0.0.1:1/api")
        .await
        .unwrap();
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

    let (status, first_relay) = admin_json(
        app.clone(),
        Method::POST,
        "/api/admin/relay-tokens",
        &admin_token,
        Some(r#"{"name":"mcp-client-a"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let first_relay_id = first_relay["id"].as_i64().unwrap();
    let first_relay_token = first_relay["token"].as_str().unwrap().to_string();

    let (status, second_relay) = admin_json(
        app.clone(),
        Method::POST,
        "/api/admin/relay-tokens",
        &admin_token,
        Some(r#"{"name":"mcp-client-b"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let second_relay_id = second_relay["id"].as_i64().unwrap();
    let second_relay_token = second_relay["token"].as_str().unwrap().to_string();

    let (status, relay_page) = admin_json(
        app.clone(),
        Method::GET,
        "/api/admin/relay-tokens?page=1&page_size=10",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(relay_page["total"], 2);
    assert_eq!(relay_page["page"], 1);
    assert_eq!(relay_page["page_size"], 10);
    assert_eq!(relay_page["items"].as_array().unwrap().len(), 2);

    let (status, relay_page) = admin_json(
        app.clone(),
        Method::GET,
        "/api/admin/relay-tokens?page=1&page_size=1",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(relay_page["total"], 2);
    assert_eq!(relay_page["page"], 1);
    assert_eq!(relay_page["page_size"], 1);
    assert_eq!(relay_page["items"].as_array().unwrap().len(), 1);

    assert_eq!(
        relay_status(app.clone(), &first_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        relay_status(app.clone(), &second_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (status, rotated_relay) = admin_json(
        app.clone(),
        Method::POST,
        &format!("/api/admin/relay-tokens/{first_relay_id}/rotate"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rotated_relay_id = rotated_relay["id"].as_i64().unwrap();
    let rotated_relay_token = rotated_relay["token"].as_str().unwrap().to_string();
    assert_ne!(rotated_relay_id, first_relay_id);
    assert_eq!(
        relay_status(app.clone(), &first_relay_token).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        relay_status(app.clone(), &rotated_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        relay_status(app.clone(), &second_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (status, relay_page) = admin_json(
        app.clone(),
        Method::GET,
        "/api/admin/relay-tokens?page=1&page_size=10",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(relay_page["total"], 2);
    assert!(
        !relay_page["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == first_relay_id)
    );

    let status = admin_status(
        app.clone(),
        Method::DELETE,
        &format!("/api/admin/relay-tokens/{second_relay_id}"),
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        relay_status(app.clone(), &second_relay_token).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        relay_status(app.clone(), &rotated_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (status, updated_relay) = admin_json(
        app.clone(),
        Method::PATCH,
        &format!("/api/admin/relay-tokens/{rotated_relay_id}"),
        &admin_token,
        Some(r#"{"name":"renamed-client-a"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(updated_relay["name"], "renamed-client-a");

    let (status, relay_page) = admin_json(
        app.clone(),
        Method::GET,
        "/api/admin/relay-tokens?page=1&page_size=10",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(relay_page["total"], 1);
    assert_eq!(relay_page["items"][0]["id"], rotated_relay_id);
    assert_eq!(relay_page["items"][0]["name"], "renamed-client-a");

    let (status, legacy_relay) = admin_json(
        app.clone(),
        Method::POST,
        "/api/admin/relay-token",
        &admin_token,
        Some(r#"{"name":"legacy-compatible"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let legacy_relay_token = legacy_relay["token"].as_str().unwrap().to_string();
    assert_eq!(
        relay_status(app.clone(), &legacy_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        relay_status(app.clone(), &rotated_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

    let status = admin_status(
        app.clone(),
        Method::DELETE,
        "/api/admin/relay-token",
        &admin_token,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        relay_status(app.clone(), &legacy_relay_token).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        relay_status(app.clone(), &rotated_relay_token).await,
        StatusCode::SERVICE_UNAVAILABLE
    );

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

async fn admin_json(
    app: Router,
    method: Method,
    uri: &str,
    admin_token: &str,
    body: Option<&str>,
) -> (StatusCode, Value) {
    let response = admin_request(app, method, uri, admin_token, body).await;
    let status = response.status();
    (status, json_body(response).await)
}

async fn admin_status(
    app: Router,
    method: Method,
    uri: &str,
    admin_token: &str,
    body: Option<&str>,
) -> StatusCode {
    admin_request(app, method, uri, admin_token, body)
        .await
        .status()
}

async fn admin_request(
    app: Router,
    method: Method,
    uri: &str,
    admin_token: &str,
    body: Option<&str>,
) -> axum::response::Response {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {admin_token}"));
    if body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    app.oneshot(
        builder
            .body(Body::from(body.unwrap_or_default().to_string()))
            .unwrap(),
    )
    .await
    .unwrap()
}

async fn relay_status(app: Router, relay_token: &str) -> StatusCode {
    app.oneshot(
        Request::builder()
            .method(Method::GET)
            .uri("/relay/context7/v2/context")
            .header(header::AUTHORIZATION, format!("Bearer {relay_token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap()
    .status()
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}
