use chrono::Utc;
use serde::Serialize;
use sqlx::PgPool;

use crate::{
    error::{AppError, AppResult, not_found_from_sqlx},
    models::{ApiKeyRow, CreateKeyInput, KeyView, RuntimeMeta, UpdateKeyInput},
    pool::{Manager, PersistedState, Record},
    settings, stats,
};

#[derive(Clone)]
pub struct Service {
    pool: PgPool,
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
        pool: PgPool,
        manager: Manager,
        settings: settings::Service,
        stats: stats::Service,
    ) -> Self {
        Self {
            pool,
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
        let items = sqlx::query_as!(
            ApiKeyRow,
            r#"
            SELECT id, name, api_key, enabled, health_status, failure_streak,
                   cooldown_until, last_error, last_status_code, last_success_at,
                   created_at, updated_at
            FROM api_keys
            ORDER BY id ASC
            "#
        )
        .fetch_all(&self.pool)
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

        let item = sqlx::query_as!(
            ApiKeyRow,
            r#"
            INSERT INTO api_keys
                (name, api_key, enabled, health_status, failure_streak, created_at, updated_at)
            VALUES ($1, $2, $3, 'healthy', 0, $4, $4)
            RETURNING id, name, api_key, enabled, health_status, failure_streak,
                      cooldown_until, last_error, last_status_code, last_success_at,
                      created_at, updated_at
            "#,
            name,
            api_key,
            enabled,
            now
        )
        .fetch_one(&self.pool)
        .await?;

        let record: Record = item.into();
        self.manager.put_record(record.clone(), Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn get_key(&self, id: i64) -> AppResult<KeyView> {
        let item = self.get_key_row(id).await?;
        let record = self.manager.overlay(item.into(), Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn update_key(&self, id: i64, input: UpdateKeyInput) -> AppResult<KeyView> {
        if id <= 0 {
            return Err(AppError::BadRequest("invalid id".to_string()));
        }
        let current = self.get_key_row(id).await?;
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

        let item = sqlx::query_as!(
            ApiKeyRow,
            r#"
            UPDATE api_keys
            SET name = $2, api_key = $3, enabled = $4, updated_at = $5
            WHERE id = $1
            RETURNING id, name, api_key, enabled, health_status, failure_streak,
                      cooldown_until, last_error, last_status_code, last_success_at,
                      created_at, updated_at
            "#,
            id,
            name,
            api_key,
            enabled,
            now
        )
        .fetch_one(&self.pool)
        .await
        .map_err(not_found_from_sqlx)?;

        let record: Record = item.into();
        self.manager.put_record(record.clone(), Utc::now()).await;
        let record = self.manager.overlay(record, Utc::now()).await;
        Ok(view_from_record(record))
    }

    pub async fn delete_key(&self, id: i64) -> AppResult<()> {
        let result = sqlx::query!("DELETE FROM api_keys WHERE id = $1", id)
            .execute(&self.pool)
            .await?;
        if result.rows_affected() == 0 {
            return Err(AppError::NotFound);
        }
        self.manager.delete_record(id, Utc::now()).await;
        Ok(())
    }

    pub async fn reset_health(&self, id: i64) -> AppResult<KeyView> {
        let item = sqlx::query_as!(
            ApiKeyRow,
            r#"
            UPDATE api_keys
            SET health_status = 'healthy',
                failure_streak = 0,
                cooldown_until = NULL,
                last_error = NULL,
                last_status_code = NULL,
                updated_at = $2
            WHERE id = $1
            RETURNING id, name, api_key, enabled, health_status, failure_streak,
                      cooldown_until, last_error, last_status_code, last_success_at,
                      created_at, updated_at
            "#,
            id,
            Utc::now()
        )
        .fetch_one(&self.pool)
        .await
        .map_err(not_found_from_sqlx)?;

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
            let result = sqlx::query!(
                r#"
                UPDATE api_keys
                SET health_status = $2,
                    failure_streak = $3,
                    cooldown_until = $4,
                    last_error = $5,
                    last_status_code = $6,
                    last_success_at = COALESCE($7, last_success_at),
                    updated_at = $8
                WHERE id = $1
                "#,
                state.id,
                state.health_status,
                state.failure_streak,
                state.cooldown_until,
                state.last_error,
                state.last_status_code,
                state.last_success_at,
                Utc::now()
            )
            .execute(&self.pool)
            .await?;
            if result.rows_affected() == 0 {
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

    async fn get_key_row(&self, id: i64) -> AppResult<ApiKeyRow> {
        if id <= 0 {
            return Err(AppError::BadRequest("invalid id".to_string()));
        }
        sqlx::query_as!(
            ApiKeyRow,
            r#"
            SELECT id, name, api_key, enabled, health_status, failure_streak,
                   cooldown_until, last_error, last_status_code, last_success_at,
                   created_at, updated_at
            FROM api_keys
            WHERE id = $1
            "#,
            id
        )
        .fetch_one(&self.pool)
        .await
        .map_err(not_found_from_sqlx)
    }

    async fn load_records(&self) -> AppResult<Vec<Record>> {
        let rows = sqlx::query_as!(
            ApiKeyRow,
            r#"
            SELECT id, name, api_key, enabled, health_status, failure_streak,
                   cooldown_until, last_error, last_status_code, last_success_at,
                   created_at, updated_at
            FROM api_keys
            ORDER BY id ASC
            "#
        )
        .fetch_all(&self.pool)
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
