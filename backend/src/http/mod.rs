use std::{path::PathBuf, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, delete, get, patch, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs;

use crate::{
    admin,
    auth::{self, bearer_token},
    error::{AppError, AppResult},
    models::{Context7Config, CreateKeyInput, UpdateKeyInput},
    proxy, stats,
};

#[derive(Clone)]
pub struct AppState {
    pub auth: auth::Service,
    pub admin: admin::Service,
    pub proxy: proxy::Service,
    pub frontend_dist: PathBuf,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/admin/auth/status", get(auth_status))
        .route("/api/admin/auth/setup", post(auth_setup))
        .route("/api/admin/auth/login", post(auth_login))
        .route("/api/admin/auth/logout", post(auth_logout))
        .route(
            "/api/admin/auth/change-password",
            post(auth_change_password),
        )
        .route("/api/admin/meta", get(admin_meta))
        .route("/api/admin/stats/context7/summary", get(stats_summary))
        .route("/api/admin/stats/context7/minutes", get(stats_minutes))
        .route("/api/admin/stats/context7/logs", get(stats_logs))
        .route("/api/admin/settings/context7", get(get_context7_settings))
        .route(
            "/api/admin/settings/context7",
            patch(update_context7_settings),
        )
        .route("/api/admin/keys", get(list_keys))
        .route("/api/admin/keys", post(create_key))
        .route("/api/admin/keys/{id}", get(get_key))
        .route("/api/admin/keys/{id}", patch(update_key))
        .route("/api/admin/keys/{id}", delete(delete_key))
        .route("/api/admin/keys/{id}/reset-health", post(reset_key_health))
        .route("/api/admin/relay-tokens", get(list_relay_tokens))
        .route("/api/admin/relay-tokens", post(create_relay_token))
        .route("/api/admin/relay-tokens/{id}", patch(update_relay_token))
        .route("/api/admin/relay-tokens/{id}", delete(delete_relay_token))
        .route(
            "/api/admin/relay-tokens/{id}/rotate",
            post(rotate_relay_token),
        )
        .route("/api/admin/relay-token", get(get_relay_token))
        .route("/api/admin/relay-token", post(generate_relay_token))
        .route("/api/admin/relay-token", delete(revoke_relay_token))
        .route("/relay/context7/v2/libs/search", get(context7_proxy))
        .route("/relay/context7/v2/context", get(context7_proxy))
        .route("/admin", get(serve_admin))
        .route("/admin/{*path}", get(serve_admin))
        .fallback(any(generic_proxy))
        .with_state(Arc::new(state))
}

async fn healthz() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

async fn auth_status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<Json<crate::models::AuthStatus>> {
    let token = bearer_token(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    );
    Ok(Json(state.auth.status(token.as_deref()).await?))
}

#[derive(Deserialize)]
struct UsernamePasswordInput {
    username: String,
    password: String,
}

async fn auth_setup(
    State(state): State<Arc<AppState>>,
    Json(input): Json<UsernamePasswordInput>,
) -> AppResult<(StatusCode, Json<crate::models::TokenResponse>)> {
    Ok((
        StatusCode::CREATED,
        Json(state.auth.setup(&input.username, &input.password).await?),
    ))
}

async fn auth_login(
    State(state): State<Arc<AppState>>,
    Json(input): Json<UsernamePasswordInput>,
) -> AppResult<Json<crate::models::TokenResponse>> {
    Ok(Json(
        state.auth.login(&input.username, &input.password).await?,
    ))
}

async fn auth_logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<StatusCode> {
    let token = bearer_token(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    );
    require_admin(&state, &headers).await?;
    state.auth.logout(token.as_deref()).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ChangePasswordInput {
    old_password: String,
    new_password: String,
}

async fn auth_change_password(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<ChangePasswordInput>,
) -> AppResult<StatusCode> {
    let identity = require_admin(&state, &headers).await?;
    state
        .auth
        .change_password(&identity, &input.old_password, &input.new_password)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_meta(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<Json<crate::models::RuntimeMeta>> {
    require_admin(&state, &headers).await?;
    Ok(Json(state.admin.meta().await))
}

async fn stats_summary(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<Json<stats::Summary>> {
    require_admin(&state, &headers).await?;
    Ok(Json(state.admin.stats_summary().await?))
}

#[derive(Deserialize)]
struct MinuteParams {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    api_key_id: Option<i64>,
}

#[derive(Serialize)]
struct ItemsResponse<T> {
    items: Vec<T>,
}

async fn stats_minutes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<MinuteParams>,
) -> AppResult<Json<ItemsResponse<stats::MinuteItem>>> {
    require_admin(&state, &headers).await?;
    if matches!(params.api_key_id, Some(value) if value <= 0) {
        return Err(AppError::BadRequest("invalid api_key_id".to_string()));
    }
    let items = state
        .admin
        .stats_minutes(stats::MinuteQuery {
            from: params.from,
            to: params.to,
            api_key_id: params.api_key_id,
        })
        .await?;
    Ok(Json(ItemsResponse { items }))
}

#[derive(Deserialize)]
struct LogParams {
    page: Option<i64>,
    page_size: Option<i64>,
    api_key_id: Option<i64>,
    success: Option<bool>,
    status_code: Option<i32>,
}

async fn stats_logs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<LogParams>,
) -> AppResult<Json<stats::LogPage>> {
    require_admin(&state, &headers).await?;
    if matches!(params.api_key_id, Some(value) if value <= 0) {
        return Err(AppError::BadRequest("invalid api_key_id".to_string()));
    }
    if matches!(params.status_code, Some(value) if value < 0) {
        return Err(AppError::BadRequest("invalid status_code".to_string()));
    }
    Ok(Json(
        state
            .admin
            .stats_logs(stats::LogQuery {
                page: params.page,
                page_size: params.page_size,
                api_key_id: params.api_key_id,
                success: params.success,
                status_code: params.status_code,
            })
            .await?,
    ))
}

async fn get_context7_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<Json<Context7Config>> {
    require_admin(&state, &headers).await?;
    Ok(Json(state.admin.settings().get_context7().await))
}

async fn update_context7_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<Context7Config>,
) -> AppResult<Json<Context7Config>> {
    require_admin(&state, &headers).await?;
    Ok(Json(
        state
            .admin
            .settings()
            .update_context7(&input.api_base_url)
            .await?,
    ))
}

async fn list_keys(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<Json<admin::KeyListResponse>> {
    require_admin(&state, &headers).await?;
    Ok(Json(admin::KeyListResponse {
        items: state.admin.list_keys().await?,
    }))
}

async fn create_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<CreateKeyInput>,
) -> AppResult<(StatusCode, Json<crate::models::KeyView>)> {
    require_admin(&state, &headers).await?;
    if input.name.trim().is_empty() || input.api_key.trim().is_empty() {
        return Err(AppError::BadRequest(
            "name and api_key are required".to_string(),
        ));
    }
    Ok((
        StatusCode::CREATED,
        Json(state.admin.create_key(input).await?),
    ))
}

async fn get_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<Json<crate::models::KeyView>> {
    require_admin(&state, &headers).await?;
    Ok(Json(state.admin.get_key(id).await?))
}

async fn update_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(input): Json<UpdateKeyInput>,
) -> AppResult<Json<crate::models::KeyView>> {
    require_admin(&state, &headers).await?;
    if input
        .name
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(AppError::BadRequest("name cannot be empty".to_string()));
    }
    if input
        .api_key
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(AppError::BadRequest("api_key cannot be empty".to_string()));
    }
    Ok(Json(state.admin.update_key(id, input).await?))
}

async fn delete_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<StatusCode> {
    require_admin(&state, &headers).await?;
    state.admin.delete_key(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn reset_key_health(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<Json<crate::models::KeyView>> {
    require_admin(&state, &headers).await?;
    Ok(Json(state.admin.reset_health(id).await?))
}

async fn get_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<Json<crate::models::RelayTokenView>> {
    require_admin(&state, &headers).await?;
    Ok(Json(state.auth.get_relay_token().await?))
}

#[derive(Deserialize)]
struct RelayTokenParams {
    page: Option<i64>,
    page_size: Option<i64>,
}

async fn list_relay_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<RelayTokenParams>,
) -> AppResult<Json<crate::models::RelayTokenPage>> {
    require_admin(&state, &headers).await?;
    let (page, page_size) = relay_token_pagination(params)?;
    Ok(Json(state.auth.list_relay_tokens(page, page_size).await?))
}

async fn create_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<crate::models::CreateRelayTokenInput>,
) -> AppResult<(StatusCode, Json<crate::models::RelayTokenResponse>)> {
    require_admin(&state, &headers).await?;
    Ok((
        StatusCode::CREATED,
        Json(state.auth.create_relay_token(&input.name).await?),
    ))
}

async fn update_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(input): Json<crate::models::UpdateRelayTokenInput>,
) -> AppResult<Json<crate::models::RelayTokenItem>> {
    require_admin(&state, &headers).await?;
    validate_positive_id(id)?;
    if input.name.trim().is_empty() {
        return Err(AppError::BadRequest("name cannot be empty".to_string()));
    }
    Ok(Json(state.auth.update_relay_token(id, &input.name).await?))
}

async fn rotate_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<Json<crate::models::RelayTokenResponse>> {
    require_admin(&state, &headers).await?;
    validate_positive_id(id)?;
    Ok(Json(state.auth.rotate_relay_token(id).await?))
}

async fn delete_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<StatusCode> {
    require_admin(&state, &headers).await?;
    validate_positive_id(id)?;
    state.auth.delete_relay_token(id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn generate_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Option<Json<crate::models::CreateRelayTokenInput>>,
) -> AppResult<(StatusCode, Json<crate::models::RelayTokenResponse>)> {
    require_admin(&state, &headers).await?;
    let name = body
        .as_ref()
        .map(|payload| payload.name.as_str())
        .unwrap_or("");
    Ok((
        StatusCode::CREATED,
        Json(state.auth.generate_relay_token(name).await?),
    ))
}

async fn revoke_relay_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> AppResult<StatusCode> {
    require_admin(&state, &headers).await?;
    state.auth.revoke_relay_token().await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn context7_proxy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Request,
) -> AppResult<Response> {
    let token = bearer_token(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
    .ok_or(AppError::Unauthorized)?;
    state.auth.validate_relay_token(&token).await?;
    let client_ip = proxy::context7_client_ip(&headers, None);
    state.proxy.handle_context7(request, client_ip).await
}

async fn generic_proxy(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    request: Request,
) -> AppResult<Response> {
    let path = request.uri().path();
    if path.starts_with("/api/") || path.starts_with("/relay/context7") {
        return Err(AppError::NotFound);
    }
    let token = bearer_token(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
    .ok_or(AppError::Unauthorized)?;
    state.auth.validate_relay_token(&token).await?;
    state.proxy.handle_generic(request).await
}

async fn serve_admin(
    State(state): State<Arc<AppState>>,
    uri: axum::http::Uri,
) -> AppResult<Response> {
    let index = state.frontend_dist.join("index.html");
    if !index.exists() {
        return Err(AppError::ServiceUnavailable(
            "frontend assets not found, run npm install && npm run build in frontend/".to_string(),
        ));
    }

    let requested = uri
        .path()
        .trim_start_matches("/admin")
        .trim_start_matches('/');
    let candidate = if requested.is_empty() {
        index.clone()
    } else {
        let clean = requested
            .split('/')
            .filter(|part| !part.is_empty() && *part != "." && *part != "..")
            .collect::<Vec<_>>()
            .join("/");
        state.frontend_dist.join(clean)
    };
    let path = if candidate.is_file() {
        candidate
    } else {
        index
    };
    let body = fs::read(&path)
        .await
        .map_err(|err| AppError::Internal(format!("read admin asset: {err}")))?;
    let content_type = content_type_for(&path);
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap())
}

async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> AppResult<crate::models::AdminIdentity> {
    let token = bearer_token(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
    .ok_or(AppError::Unauthorized)?;
    state.auth.validate_admin_token(&token).await
}

fn relay_token_pagination(params: RelayTokenParams) -> AppResult<(i64, i64)> {
    let page = params.page.unwrap_or(1);
    let page_size = params.page_size.unwrap_or(10);
    if page < 1 {
        return Err(AppError::BadRequest("page must be at least 1".to_string()));
    }
    if !(1..=100).contains(&page_size) {
        return Err(AppError::BadRequest(
            "page_size must be between 1 and 100".to_string(),
        ));
    }
    Ok((page, page_size))
}

fn validate_positive_id(id: i64) -> AppResult<()> {
    if id <= 0 {
        return Err(AppError::BadRequest("id must be positive".to_string()));
    }
    Ok(())
}

fn content_type_for(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("")
    {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "json" => "application/json",
        _ => "application/octet-stream",
    }
}
