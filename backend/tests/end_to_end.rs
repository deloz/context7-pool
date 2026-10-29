use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use contextpool::{
    admin, auth, db,
    entity::{api_key, minute_stat, request_log},
    http::{self, AppState},
    pool, proxy, settings, stats,
};
use sea_orm::{
    ActiveModelTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, Set,
};
use serde_json::Value;
use tower::ServiceExt;

static DATABASE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://contextpool:contextpool@127.0.0.1:45432/contextpool".to_string()
    })
}

async fn reset_db(db: &DatabaseConnection) {
    db.execute_unprepared(
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
    .await
    .unwrap();
}

#[tokio::test]
async fn admin_stats_and_relay_routes_work() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let database = db::connect(&database_url()).await.unwrap();
    db::migrate(&database).await.unwrap();
    reset_db(&database).await;

    let manager = pool::Manager::new(pool::Config {
        failure_threshold: 2,
        cooldown: std::time::Duration::from_secs(30),
        flush_interval: std::time::Duration::from_millis(50),
        flush_batch_size: 64,
    });
    let settings_service = settings::Service::new(database.clone());
    let stats_service = stats::Service::new(
        database.clone(),
        stats::Config {
            flush_interval: std::time::Duration::from_millis(50),
            batch_size: 64,
            queue_size: 128,
        },
    );
    let auth_service = auth::Service::new(database.clone());
    auth_service.bootstrap().await.unwrap();
    let admin_service = admin::Service::new(
        database.clone(),
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

#[tokio::test]
async fn minute_upsert_merges_accumulated_counters() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let database = db::connect(&database_url()).await.unwrap();
    db::migrate(&database).await.unwrap();
    reset_db(&database).await;
    let service = test_stats_service(&database);

    service.record(test_event(1, 200, 10, 10));
    service.record(test_event(1, 500, 80, 10));
    service.flush().await.unwrap();

    let rows = minute_stat::Entity::find().all(&database).await.unwrap();
    assert_eq!(rows.len(), 1);
    let first = &rows[0];
    assert_eq!(first.total_requests, 2);
    assert_eq!(first.success_requests, 1);
    assert_eq!(first.failed_requests, 1);
    assert_eq!(first.status_2xx, 1);
    assert_eq!(first.status_5xx, 1);
    assert_eq!(first.total_latency_ms, 90);
    assert_eq!(first.max_latency_ms, 80);
    assert_eq!(first.last_status_code, 500);

    service.record(test_event(1, 429, 120, 10));
    service.flush().await.unwrap();

    let rows = minute_stat::Entity::find().all(&database).await.unwrap();
    assert_eq!(rows.len(), 1);
    let merged = &rows[0];
    assert_eq!(merged.total_requests, 3);
    assert_eq!(merged.success_requests, 1);
    assert_eq!(merged.failed_requests, 2);
    assert_eq!(merged.status_4xx, 1);
    assert_eq!(merged.total_latency_ms, 210);
    assert_eq!(merged.max_latency_ms, 120);
    assert_eq!(merged.last_status_code, 429);

    let summary = service.summary().await.unwrap();
    assert_eq!(summary.total_requests, 3);
    assert_eq!(summary.success_requests, 1);
    assert_eq!(summary.failed_requests, 2);
    assert_eq!(summary.status_2xx, 1);
    assert_eq!(summary.status_4xx, 1);
    assert_eq!(summary.status_5xx, 1);
    assert_eq!(summary.total_latency_ms, 210);
    assert_eq!(summary.max_latency_ms, 120);
}

#[tokio::test]
async fn log_filters_and_pagination_are_exact() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let database = db::connect(&database_url()).await.unwrap();
    db::migrate(&database).await.unwrap();
    reset_db(&database).await;
    let service = test_stats_service(&database);

    for (id, status, latency, minute) in [
        (1, 200, 10, 1),
        (1, 401, 20, 2),
        (1, 500, 30, 3),
        (2, 401, 40, 4),
        (2, 503, 50, 5),
    ] {
        service.record(test_event(id, status, latency, minute));
    }
    service.flush().await.unwrap();

    let first_page = service
        .list_logs(stats::LogQuery {
            page: Some(1),
            page_size: Some(2),
            api_key_id: None,
            success: None,
            status_code: None,
        })
        .await
        .unwrap();
    assert_eq!(first_page.total, 5);
    assert_eq!(first_page.items.len(), 2);

    let last_page = service
        .list_logs(stats::LogQuery {
            page: Some(3),
            page_size: Some(2),
            api_key_id: None,
            success: None,
            status_code: None,
        })
        .await
        .unwrap();
    assert_eq!(last_page.total, 5);
    assert_eq!(last_page.items.len(), 1);

    let by_key = service
        .list_logs(stats::LogQuery {
            page: Some(1),
            page_size: Some(10),
            api_key_id: Some(1),
            success: None,
            status_code: None,
        })
        .await
        .unwrap();
    assert_eq!(by_key.total, 3);

    let by_success = service
        .list_logs(stats::LogQuery {
            page: Some(1),
            page_size: Some(10),
            api_key_id: None,
            success: Some(false),
            status_code: None,
        })
        .await
        .unwrap();
    assert_eq!(by_success.total, 4);

    let by_status = service
        .list_logs(stats::LogQuery {
            page: Some(1),
            page_size: Some(10),
            api_key_id: None,
            success: None,
            status_code: Some(401),
        })
        .await
        .unwrap();
    assert_eq!(by_status.total, 2);
    assert!(by_status.items.iter().all(|item| item.status_code == 401));

    let log_count = request_log::Entity::find().count(&database).await.unwrap();
    assert_eq!(log_count, 5);
}

#[tokio::test]
async fn migration_baselines_legacy_schema_without_data_loss() {
    let _guard = DATABASE_TEST_LOCK.lock().await;
    let database = db::connect(&database_url()).await.unwrap();
    db::migrate(&database).await.unwrap();
    drop_schema(&database).await;

    for statement in LEGACY_SCHEMA.split(';') {
        let statement = statement.trim();
        if !statement.is_empty() {
            database.execute_unprepared(statement).await.unwrap();
        }
    }

    let inserted = api_key::ActiveModel {
        name: Set("legacy-key".to_string()),
        api_key: Set("legacy-secret".to_string()),
        enabled: Set(true),
        health_status: Set("healthy".to_string()),
        failure_streak: Set(0),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
        ..Default::default()
    }
    .insert(&database)
    .await
    .unwrap();

    db::migrate(&database).await.unwrap();

    let keys = api_key::Entity::find().all(&database).await.unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].id, inserted.id);
    assert_eq!(keys[0].name, "legacy-key");

    let statement = sea_orm::Statement::from_string(
        sea_orm::DbBackend::Postgres,
        "SELECT COUNT(*)::BIGINT AS count FROM seaql_migrations \
         WHERE version = 'm20260606_000001_init'",
    );
    let row = database.query_one_raw(statement).await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "count").unwrap(), 1);

    reset_db(&database).await;
}

fn test_stats_service(database: &DatabaseConnection) -> stats::Service {
    stats::Service::new(
        database.clone(),
        stats::Config {
            flush_interval: std::time::Duration::from_millis(50),
            batch_size: 64,
            queue_size: 128,
        },
    )
}

fn test_event(api_key_id: i64, status_code: i32, latency_ms: i64, minute: u32) -> stats::Event {
    let at = Utc
        .with_ymd_and_hms(2026, 4, 26, 9, minute, 0)
        .single()
        .unwrap();
    stats::Event {
        api_key_id,
        api_key_name: format!("key-{api_key_id}"),
        method: "GET".to_string(),
        path: "/relay/context7/v2/context".to_string(),
        status_code,
        latency_ms,
        error: if status_code >= 400 {
            "request failed".to_string()
        } else {
            String::new()
        },
        started_at: at,
        finished_at: at,
        ..stats::Event::default()
    }
}

async fn drop_schema(database: &DatabaseConnection) {
    database
        .execute_unprepared(
            r#"
            DROP TABLE IF EXISTS
                context7_request_logs,
                context7_minute_stats,
                request_logs,
                minute_stats,
                admin_sessions,
                admin_users,
                relay_tokens,
                api_keys,
                settings,
                seaql_migrations
            CASCADE
            "#,
        )
        .await
        .unwrap();
}

const LEGACY_SCHEMA: &str = r#"
CREATE TABLE api_keys (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    api_key TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    health_status TEXT NOT NULL DEFAULT 'healthy',
    failure_streak INTEGER NOT NULL DEFAULT 0,
    cooldown_until TIMESTAMPTZ NULL,
    last_error TEXT NULL,
    last_status_code INTEGER NULL,
    last_success_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE admin_users (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT admin_users_username_key UNIQUE (username)
);
CREATE TABLE admin_sessions (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    token_hash TEXT NOT NULL,
    admin_user_id BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NULL,
    CONSTRAINT admin_sessions_token_hash_key UNIQUE (token_hash)
);
CREATE TABLE relay_tokens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL DEFAULT 'context7-relay',
    token_hash TEXT NOT NULL,
    token TEXT NULL,
    masked_token TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NULL,
    revoked_at TIMESTAMPTZ NULL,
    CONSTRAINT relay_tokens_token_hash_key UNIQUE (token_hash)
);
CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE context7_request_logs (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    api_key_id BIGINT NOT NULL,
    api_key_name TEXT NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    query TEXT NOT NULL DEFAULT '',
    status_code INTEGER NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    latency_ms BIGINT NOT NULL DEFAULT 0,
    error TEXT NULL,
    client_ip TEXT NULL,
    user_agent TEXT NULL,
    client_source TEXT NULL,
    client_ide TEXT NULL,
    client_version TEXT NULL,
    transport TEXT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX context7_request_logs_started_at_idx
    ON context7_request_logs (started_at);
CREATE INDEX context7_request_logs_api_key_id_started_at_idx
    ON context7_request_logs (api_key_id, started_at);
CREATE INDEX context7_request_logs_success_started_at_idx
    ON context7_request_logs (success, started_at);
CREATE INDEX context7_request_logs_status_code_started_at_idx
    ON context7_request_logs (status_code, started_at);
CREATE TABLE context7_minute_stats (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    api_key_id BIGINT NOT NULL,
    api_key_name TEXT NOT NULL,
    minute_at TIMESTAMPTZ NOT NULL,
    total_requests INTEGER NOT NULL DEFAULT 0,
    success_requests INTEGER NOT NULL DEFAULT 0,
    failed_requests INTEGER NOT NULL DEFAULT 0,
    status_2xx INTEGER NOT NULL DEFAULT 0,
    status_4xx INTEGER NOT NULL DEFAULT 0,
    status_5xx INTEGER NOT NULL DEFAULT 0,
    network_errors INTEGER NOT NULL DEFAULT 0,
    total_latency_ms BIGINT NOT NULL DEFAULT 0,
    max_latency_ms BIGINT NOT NULL DEFAULT 0,
    last_status_code INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT context7_minute_stats_api_key_id_minute_at_key UNIQUE (api_key_id, minute_at)
);
CREATE INDEX context7_minute_stats_minute_at_idx
    ON context7_minute_stats (minute_at);
"#;

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
