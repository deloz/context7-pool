use std::sync::Arc;

use chrono::Utc;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use tokio::sync::RwLock;
use url::Url;

use crate::{
    entity::setting,
    error::{AppError, AppResult},
    models::Context7Config,
};

pub const CONTEXT7_API_BASE_URL_KEY: &str = "context7_api_base_url";
pub const DEFAULT_CONTEXT7_API_BASE_URL: &str = "https://context7.com/api";

#[derive(Clone)]
pub struct Service {
    db: DatabaseConnection,
    snapshot: Arc<RwLock<Snapshot>>,
}

#[derive(Clone)]
struct Snapshot {
    context7: Context7Config,
    target: Option<Url>,
}

impl Service {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
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
        setting::Entity::insert(setting::ActiveModel {
            key: Set(CONTEXT7_API_BASE_URL_KEY.to_string()),
            value: Set(DEFAULT_CONTEXT7_API_BASE_URL.to_string()),
            created_at: Set(now),
            updated_at: Set(now),
        })
        .on_conflict(
            OnConflict::column(setting::Column::Key)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(&self.db)
        .await?;

        let stored = setting::Entity::find_by_id(CONTEXT7_API_BASE_URL_KEY)
            .one(&self.db)
            .await?
            .ok_or_else(|| AppError::Internal("context7 api base url missing".to_string()))?;

        let parsed = validate_api_base_url(&stored.value)?;
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
        let result = setting::Entity::update_many()
            .col_expr(setting::Column::Value, value.clone().into())
            .col_expr(setting::Column::UpdatedAt, now.into())
            .filter(setting::Column::Key.eq(CONTEXT7_API_BASE_URL_KEY))
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
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
