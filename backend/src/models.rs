use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Clone, Debug, FromRow)]
pub struct ApiKeyRow {
    pub id: i64,
    pub name: String,
    pub api_key: String,
    pub enabled: bool,
    pub health_status: String,
    pub failure_streak: i32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_status_code: Option<i32>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
pub struct AdminUserRow {
    pub id: i64,
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug, FromRow)]
pub struct AdminSessionRow {
    pub id: i64,
    pub admin_user_id: i64,
    pub expires_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
pub struct RelayTokenRow {
    pub id: i64,
    pub name: String,
    pub token_hash: String,
    pub token: Option<String>,
    pub masked_token: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeMeta {
    pub total_key_count: usize,
    pub available_key_count: usize,
    pub cooling_key_count: usize,
    pub snapshot_updated_at: DateTime<Utc>,
    pub failure_threshold: i32,
    pub cooldown_seconds: u64,
    pub snapshot_version: u64,
    pub upstream_configured: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct KeyView {
    pub id: i64,
    pub name: String,
    pub api_key: String,
    pub masked_api_key: String,
    pub enabled: bool,
    pub health_status: String,
    pub failure_streak: i32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_status_code: Option<i32>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateKeyInput {
    pub name: String,
    pub api_key: String,
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateKeyInput {
    pub name: Option<String>,
    pub api_key: Option<String>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Context7Config {
    pub api_base_url: String,
}

#[derive(Debug, Serialize)]
pub struct AuthStatus {
    pub setup_required: bool,
    pub authenticated: bool,
}

#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub token: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct AdminIdentity {
    pub admin_user_id: i64,
    pub session_id: i64,
    pub username: String,
}

#[derive(Debug, Serialize)]
pub struct RelayTokenView {
    pub configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub masked_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct RelayTokenResponse {
    pub id: i64,
    pub name: String,
    pub token: String,
    pub masked_token: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct RelayTokenItem {
    pub id: i64,
    pub name: String,
    pub token: Option<String>,
    pub masked_token: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct RelayTokenPage {
    pub items: Vec<RelayTokenItem>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateRelayTokenInput {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateRelayTokenInput {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct ProxyKey {
    pub id: i64,
    pub name: String,
    pub api_key: String,
}
