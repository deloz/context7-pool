use chrono::Utc;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, Set,
};
use serde::Serialize;

use crate::{
    entity::api_key,
    error::{AppError, AppResult},
    models::{CreateKeyInput, KeyView, RuntimeMeta, UpdateKeyInput},
    pool::{Manager, PersistedState, Record},
    settings, stats,
};

#[derive(Clone)]
pub struct Service {
    db: DatabaseConnection,
    manager: Manager,
    settings: settings::Service,
    stats: stats::Service,
}

#[derive(Serialize)]
pub struct KeyListResponse {
    pub items: Vec<KeyView>,
}

impl Service {
    pub fn new(
        db: DatabaseConnection,
        manager: Manager,
        settings: settings::Service,
        stats: stats::Service,
    ) -> Self {
        Self {
            db,
            manager,
            settings,
            stats,
        }
    }

    pub async fn bootstrap(&self) -> AppResult<()> {
        self.settings.bootstrap().await?;
        let records = self.load_records().await?;
        self.manager.load(records, Utc::now()).await;
        Ok(())
    }

    pub async fn list_keys(&self) -> AppResult<Vec<KeyView>> {
        let items = api_key::Entity::find()
            .order_by_asc(api_key::Column::Id)
            .all(&self.db)
            .await?;

        let mut views = Vec::with_capacity(items.len());
        for item in items {
            let record = self.manager.overlay(item.into(), Utc::now()).await;
            views.push(view_from_record(record));
        }
        Ok(views)
    }

    pub async fn create_key(&self, input: CreateKeyInput) -> AppResult<KeyView> {
        let name = trim_required(&input.name, "name")?;
        let api_key = trim_required(&input.api_key, "api_key")?;
        let enabled = input.enabled.unwrap_or(true);
        let now = Utc::now();

        let item = api_key::ActiveModel {
            name: Set(name),
            api_key: Set(api_key),
            enabled: Set(enabled),
            health_status: Set("healthy".to_string()),
            failure_streak: Set(0),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&self.db)
        .await?;

        let record: Record = item.into();
        self.manager.put_record(record.clone(), Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn get_key(&self, id: i64) -> AppResult<KeyView> {
        let item = self.get_key_model(id).await?;
        let record = self.manager.overlay(item.into(), Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn update_key(&self, id: i64, input: UpdateKeyInput) -> AppResult<KeyView> {
        if id <= 0 {
            return Err(AppError::BadRequest("invalid id".to_string()));
        }
        let current = self.get_key_model(id).await?;
        let name = match input.name {
            Some(value) => trim_required(&value, "name")?,
            None => current.name,
        };
        let api_key = match input.api_key {
            Some(value) => trim_required(&value, "api_key")?,
            None => current.api_key,
        };
        let enabled = input.enabled.unwrap_or(current.enabled);
        let now = Utc::now();

        let active = api_key::ActiveModel {
            id: Set(id),
            name: Set(name),
            api_key: Set(api_key),
            enabled: Set(enabled),
            updated_at: Set(now),
            ..Default::default()
        };
        let item = active.update(&self.db).await?;

        let record: Record = item.into();
        self.manager.put_record(record.clone(), Utc::now()).await;
        let record = self.manager.overlay(record, Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn delete_key(&self, id: i64) -> AppResult<()> {
        let result = api_key::Entity::delete_many()
            .filter(api_key::Column::Id.eq(id))
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
            return Err(AppError::NotFound);
        }
        self.manager.delete_record(id, Utc::now()).await;
        Ok(())
    }

    pub async fn reset_health(&self, id: i64) -> AppResult<KeyView> {
        if id <= 0 {
            return Err(AppError::BadRequest("invalid id".to_string()));
        }

        let active = api_key::ActiveModel {
            id: Set(id),
            health_status: Set("healthy".to_string()),
            failure_streak: Set(0),
            cooldown_until: Set(None),
            last_error: Set(None),
            last_status_code: Set(None),
            updated_at: Set(Utc::now()),
            ..Default::default()
        };
        let item = active
            .update(&self.db)
            .await
            .map_err(crate::error::not_found_from_db)?;

        self.manager.reset_health(id, Utc::now()).await;
        let record = self.manager.overlay(item.into(), Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn meta(&self) -> RuntimeMeta {
        self.manager
            .meta(Utc::now(), self.settings.context7_configured().await)
            .await
    }

    pub async fn flush_runtime_states(&self, states: Vec<PersistedState>) -> anyhow::Result<()> {
        for state in states {
            let last_success_at = match state.last_success_at {
                Some(value) => Expr::value(value),
                None => Expr::col(api_key::Column::LastSuccessAt),
            };
            let result = api_key::Entity::update_many()
                .col_expr(
                    api_key::Column::HealthStatus,
                    state.health_status.clone().into(),
                )
                .col_expr(api_key::Column::FailureStreak, state.failure_streak.into())
                .col_expr(api_key::Column::CooldownUntil, state.cooldown_until.into())
                .col_expr(api_key::Column::LastError, state.last_error.clone().into())
                .col_expr(
                    api_key::Column::LastStatusCode,
                    state.last_status_code.into(),
                )
                .col_expr(api_key::Column::LastSuccessAt, last_success_at)
                .col_expr(api_key::Column::UpdatedAt, Utc::now().into())
                .filter(api_key::Column::Id.eq(state.id))
                .exec(&self.db)
                .await?;
            if result.rows_affected == 0 {
                tracing::warn!("skip flushing missing api key state id={}", state.id);
            }
        }
        Ok(())
    }

    pub async fn stats_summary(&self) -> AppResult<stats::Summary> {
        self.stats.summary().await
    }

    pub async fn stats_minutes(
        &self,
        query: stats::MinuteQuery,
    ) -> AppResult<Vec<stats::MinuteItem>> {
        self.stats.list_minutes(query).await
    }

    pub async fn stats_logs(&self, query: stats::LogQuery) -> AppResult<stats::LogPage> {
        self.stats.list_logs(query).await
    }

    pub fn settings(&self) -> settings::Service {
        self.settings.clone()
    }

    async fn get_key_model(&self, id: i64) -> AppResult<api_key::Model> {
        if id <= 0 {
            return Err(AppError::BadRequest("invalid id".to_string()));
        }
        api_key::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or(AppError::NotFound)
    }

    async fn load_records(&self) -> AppResult<Vec<Record>> {
        let rows = api_key::Entity::find()
            .order_by_asc(api_key::Column::Id)
            .all(&self.db)
            .await?;
        Ok(rows.into_iter().map(Record::from).collect())
    }
}

fn view_from_record(record: Record) -> KeyView {
    KeyView {
        id: record.id,
        name: record.name,
        masked_api_key: mask_api_key(&record.api_key),
        api_key: record.api_key,
        enabled: record.enabled,
        health_status: record.health_status,
        failure_streak: record.failure_streak,
        cooldown_until: record.cooldown_until,
        last_error: record.last_error,
        last_status_code: record.last_status_code,
        last_success_at: record.last_success_at,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

fn mask_api_key(value: &str) -> String {
    if value.len() <= 8 {
        "********".to_string()
    } else {
        format!("{}...{}", &value[..4], &value[value.len() - 4..])
    }
}

fn trim_required(value: &str, field: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadRequest(format!("{field} cannot be empty")));
    }
    Ok(trimmed.to_string())
}
