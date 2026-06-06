use std::sync::Arc;

use chrono::Utc;
use sqlx::PgPool;
use tokio::sync::RwLock;
use url::Url;

use crate::{
    error::{AppError, AppResult},
    models::Context7Config,
};

pub const CONTEXT7_API_BASE_URL_KEY: &str = "context7_api_base_url";
pub const DEFAULT_CONTEXT7_API_BASE_URL: &str = "https://context7.com/api";

#[derive(Clone)]
pub struct Service {
    pool: PgPool,
    snapshot: Arc<RwLock<Snapshot>>,
}

#[derive(Clone)]
struct Snapshot {
    context7: Context7Config,
    target: Option<Url>,
}

impl Service {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            snapshot: Arc::new(RwLock::new(Snapshot {
                context7: Context7Config {
                    api_base_url: DEFAULT_CONTEXT7_API_BASE_URL.to_string(),
                },
                target: Url::parse(DEFAULT_CONTEXT7_API_BASE_URL).ok(),
            })),
        }
    }

    pub async fn bootstrap(&self) -> AppResult<()> {
        let now = Utc::now();
        sqlx::query!(
            r#"
            INSERT INTO settings (key, value, created_at, updated_at)
            VALUES ($1, $2, $3, $3)
            ON CONFLICT (key) DO NOTHING
            "#,
            CONTEXT7_API_BASE_URL_KEY,
            DEFAULT_CONTEXT7_API_BASE_URL,
            now
        )
        .execute(&self.pool)
        .await?;

        let value = sqlx::query_scalar!(
            r#"SELECT value FROM settings WHERE key = $1"#,
            CONTEXT7_API_BASE_URL_KEY
        )
        .fetch_one(&self.pool)
        .await?;

        let parsed = validate_api_base_url(&value)?;
        self.store_snapshot(parsed.to_string(), Some(parsed)).await;
        Ok(())
    }

    pub async fn get_context7(&self) -> Context7Config {
        self.snapshot.read().await.context7.clone()
    }

    pub async fn current_context7_target(&self) -> Option<Url> {
        self.snapshot.read().await.target.clone()
    }

    pub async fn context7_configured(&self) -> bool {
        self.current_context7_target().await.is_some()
    }

    pub async fn update_context7(&self, api_base_url: &str) -> AppResult<Context7Config> {
        let parsed = validate_api_base_url(api_base_url)?;
        let value = parsed.to_string();
        let now = Utc::now();
        let result = sqlx::query!(
            r#"
            UPDATE settings
            SET value = $2, updated_at = $3
            WHERE key = $1
            "#,
            CONTEXT7_API_BASE_URL_KEY,
            value,
            now
        )
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AppError::NotFound);
        }

        self.store_snapshot(value, Some(parsed)).await;
        Ok(self.get_context7().await)
    }

    async fn store_snapshot(&self, raw: String, target: Option<Url>) {
        *self.snapshot.write().await = Snapshot {
            context7: Context7Config { api_base_url: raw },
            target,
        };
    }
}

pub fn validate_api_base_url(raw: &str) -> AppResult<Url> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest("api_base_url is required".to_string()));
    }

    let mut parsed = Url::parse(trimmed)
        .map_err(|_| AppError::BadRequest("api_base_url must be a valid URL".to_string()))?;
    if parsed.scheme().is_empty() || parsed.host_str().is_none() {
        return Err(AppError::BadRequest(
            "api_base_url must include scheme and host".to_string(),
        ));
    }

    let path = parsed.path().trim_end_matches('/').to_string();
    parsed.set_path(&path);
    Ok(parsed)
}
