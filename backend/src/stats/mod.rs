use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Timelike, Utc};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use sea_orm::sea_query::{Expr, Func, OnConflict};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    entity::{minute_stat, request_log},
    error::AppResult,
};

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_BATCH_SIZE: usize = 128;
const DEFAULT_QUEUE_SIZE: usize = 2048;
const MAX_STORED_TEXT_LENGTH: usize = 2048;
const DEFAULT_LOG_PAGE_SIZE: i64 = 20;
const MAX_LOG_PAGE_SIZE: i64 = 100;

#[derive(Clone, Debug)]
pub struct Config {
    pub flush_interval: Duration,
    pub batch_size: usize,
    pub queue_size: usize,
}

#[derive(Clone)]
pub struct Service {
    db: DatabaseConnection,
    flush_interval: Duration,
    batch_size: usize,
    queue_tx: tokio::sync::mpsc::Sender<Event>,
    queue_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<Event>>>,
    pending: Arc<Mutex<Vec<Event>>>,
}

#[derive(Clone, Debug)]
pub struct Event {
    pub api_key_id: i64,
    pub api_key_name: String,
    pub method: String,
    pub path: String,
    pub query: String,
    pub status_code: i32,
    pub success: bool,
    pub latency_ms: i64,
    pub error: String,
    pub client_ip: String,
    pub user_agent: String,
    pub client_source: String,
    pub client_ide: String,
    pub client_version: String,
    pub transport: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Default)]
pub struct Summary {
    pub total_requests: i64,
    pub success_requests: i64,
    pub failed_requests: i64,
    pub success_rate: f64,
    pub average_latency_ms: f64,
    pub last_request_at: Option<DateTime<Utc>>,
    pub last_status_code: i32,
    pub last_error: Option<String>,
    pub network_errors: i64,
    pub status_2xx: i64,
    pub status_4xx: i64,
    pub status_5xx: i64,
    pub total_latency_ms: i64,
    pub max_latency_ms: i64,
}

#[derive(Debug, Deserialize)]
pub struct MinuteQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub api_key_id: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct MinuteItem {
    pub api_key_id: i64,
    pub api_key_name: String,
    pub minute_at: DateTime<Utc>,
    pub total_requests: i64,
    pub success_requests: i64,
    pub failed_requests: i64,
    pub status_2xx: i64,
    pub status_4xx: i64,
    pub status_5xx: i64,
    pub network_errors: i64,
    pub total_latency_ms: i64,
    pub max_latency_ms: i64,
    pub last_status_code: i32,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub success_rate: f64,
    pub average_latency_ms: f64,
}

#[derive(Debug, Deserialize)]
pub struct LogQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
    pub api_key_id: Option<i64>,
    pub success: Option<bool>,
    pub status_code: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct LogPage {
    pub items: Vec<LogItem>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Serialize)]
pub struct LogItem {
    pub id: i64,
    pub api_key_id: i64,
    pub api_key_name: String,
    pub method: String,
    pub path: String,
    pub query: String,
    pub status_code: i32,
    pub success: bool,
    pub latency_ms: i64,
    pub error: Option<String>,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub client_source: Option<String>,
    pub client_ide: Option<String>,
    pub client_version: Option<String>,
    pub transport: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct MinuteAggregate {
    api_key_id: i64,
    api_key_name: String,
    minute_at: DateTime<Utc>,
    total_requests: i32,
    success_requests: i32,
    failed_requests: i32,
    status_2xx: i32,
    status_4xx: i32,
    status_5xx: i32,
    network_errors: i32,
    total_latency_ms: i64,
    max_latency_ms: i64,
    last_status_code: i32,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
    last_finished_at: DateTime<Utc>,
}

impl Service {
    pub fn new(db: DatabaseConnection, cfg: Config) -> Self {
        let flush_interval = if cfg.flush_interval.is_zero() {
            DEFAULT_FLUSH_INTERVAL
        } else {
            cfg.flush_interval
        };
        let batch_size = if cfg.batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            cfg.batch_size
        };
        let queue_size = if cfg.queue_size == 0 {
            DEFAULT_QUEUE_SIZE
        } else {
            cfg.queue_size
        };
        let (queue_tx, queue_rx) = tokio::sync::mpsc::channel(queue_size);
        Self {
            db,
            flush_interval,
            batch_size,
            queue_tx,
            queue_rx: Arc::new(Mutex::new(queue_rx)),
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn record(&self, event: Event) {
        if event.api_key_id <= 0 {
            return;
        }
        if self.queue_tx.try_send(normalize_event(event)).is_err() {
            tracing::warn!("context7 stats queue full, dropping event");
        }
    }

    pub async fn run(&self) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        loop {
            tokio::select! {
                event = recv_one(&self.queue_rx) => {
                    let Some(event) = event else {
                        return;
                    };
                    self.add_pending(event).await;
                    if self.pending.lock().await.len() >= self.batch_size
                        && let Err(err) = self.flush().await
                    {
                        tracing::warn!("flush context7 stats: {err}");
                    }
                }
                _ = ticker.tick() => {
                    if let Err(err) = self.flush().await {
                        tracing::warn!("flush context7 stats: {err}");
                    }
                }
            }
        }
    }

    pub async fn flush(&self) -> AppResult<()> {
        self.drain_queue().await;
        let batch = {
            let pending = self.pending.lock().await;
            if pending.is_empty() {
                return Ok(());
            }
            pending.clone()
        };

        self.write_batch(&batch).await?;

        let mut pending = self.pending.lock().await;
        if pending.len() <= batch.len() {
            pending.clear();
        } else {
            pending.drain(..batch.len());
        }
        Ok(())
    }

    pub async fn summary(&self) -> AppResult<Summary> {
        let totals = minute_stat::Entity::find()
            .select_only()
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::TotalRequests)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "total_requests",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::SuccessRequests)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "success_requests",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::FailedRequests)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "failed_requests",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::NetworkErrors)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "network_errors",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::Status2xx)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "status_2xx",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::Status4xx)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "status_4xx",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::Status5xx)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "status_5xx",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::sum(Expr::col(minute_stat::Column::TotalLatencyMs)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "total_latency_ms",
            )
            .column_as(
                Expr::FunctionCall(Func::cast_as(
                    Func::coalesce([
                        Func::max(Expr::col(minute_stat::Column::MaxLatencyMs)).into(),
                        Expr::value(0),
                    ]),
                    "BIGINT",
                )),
                "max_latency_ms",
            )
            .into_tuple::<(i64, i64, i64, i64, i64, i64, i64, i64, i64)>()
            .one(&self.db)
            .await?;

        let (
            total_requests,
            success_requests,
            failed_requests,
            network_errors,
            status_2xx,
            status_4xx,
            status_5xx,
            total_latency_ms,
            max_latency_ms,
        ) = totals.unwrap_or_default();

        let mut summary = Summary {
            total_requests,
            success_requests,
            failed_requests,
            network_errors,
            status_2xx,
            status_4xx,
            status_5xx,
            total_latency_ms,
            max_latency_ms,
            ..Summary::default()
        };
        if summary.total_requests > 0 {
            summary.success_rate = ratio_decimal(summary.success_requests, summary.total_requests);
            summary.average_latency_ms =
                ratio_decimal(summary.total_latency_ms, summary.total_requests);
        }

        if let Some(last) = request_log::Entity::find()
            .order_by_desc(request_log::Column::FinishedAt)
            .order_by_desc(request_log::Column::Id)
            .one(&self.db)
            .await?
        {
            summary.last_request_at = Some(last.finished_at);
            summary.last_status_code = last.status_code;
            summary.last_error = last.error;
        }

        Ok(summary)
    }

    pub async fn list_minutes(&self, params: MinuteQuery) -> AppResult<Vec<MinuteItem>> {
        let mut condition = Condition::all();
        if let Some(from) = params.from {
            condition = condition.add(minute_stat::Column::MinuteAt.gte(from));
        }
        if let Some(to) = params.to {
            condition = condition.add(minute_stat::Column::MinuteAt.lte(to));
        }
        if let Some(api_key_id) = params.api_key_id {
            condition = condition.add(minute_stat::Column::ApiKeyId.eq(api_key_id));
        }

        let rows = minute_stat::Entity::find()
            .filter(condition)
            .order_by_asc(minute_stat::Column::MinuteAt)
            .order_by_asc(minute_stat::Column::ApiKeyId)
            .all(&self.db)
            .await?;

        Ok(rows.into_iter().map(minute_item).collect())
    }

    pub async fn list_logs(&self, params: LogQuery) -> AppResult<LogPage> {
        let page = std::cmp::max(params.page.unwrap_or(1), 1);
        let page_size = params
            .page_size
            .unwrap_or(DEFAULT_LOG_PAGE_SIZE)
            .clamp(1, MAX_LOG_PAGE_SIZE);

        let condition = log_filters(&params);
        let total = request_log::Entity::find()
            .filter(condition.clone())
            .count(&self.db)
            .await? as i64;

        let rows = request_log::Entity::find()
            .filter(condition)
            .order_by_desc(request_log::Column::StartedAt)
            .order_by_desc(request_log::Column::Id)
            .offset(((page - 1) * page_size) as u64)
            .limit(page_size as u64)
            .all(&self.db)
            .await?;

        Ok(LogPage {
            items: rows.into_iter().map(log_item).collect(),
            total,
            page,
            page_size,
        })
    }

    async fn add_pending(&self, event: Event) {
        self.pending.lock().await.push(normalize_event(event));
    }

    async fn drain_queue(&self) {
        let mut queue_rx = self.queue_rx.lock().await;
        while let Ok(event) = queue_rx.try_recv() {
            drop(queue_rx);
            self.add_pending(event).await;
            queue_rx = self.queue_rx.lock().await;
        }
    }

    async fn write_batch(&self, batch: &[Event]) -> AppResult<()> {
        let tx = self.db.begin().await?;
        for event in batch {
            request_log::ActiveModel {
                api_key_id: Set(event.api_key_id),
                api_key_name: Set(event.api_key_name.clone()),
                method: Set(event.method.clone()),
                path: Set(event.path.clone()),
                query: Set(event.query.clone()),
                status_code: Set(event.status_code),
                success: Set(event.success),
                latency_ms: Set(event.latency_ms),
                error: Set(optional_string(&event.error)),
                client_ip: Set(optional_string(&event.client_ip)),
                user_agent: Set(optional_string(&event.user_agent)),
                client_source: Set(optional_string(&event.client_source)),
                client_ide: Set(optional_string(&event.client_ide)),
                client_version: Set(optional_string(&event.client_version)),
                transport: Set(optional_string(&event.transport)),
                started_at: Set(event.started_at),
                finished_at: Set(event.finished_at),
                ..Default::default()
            }
            .insert(&tx)
            .await?;
        }

        for item in aggregate_events(batch) {
            let on_conflict =
                OnConflict::columns([minute_stat::Column::ApiKeyId, minute_stat::Column::MinuteAt])
                    .update_columns([minute_stat::Column::ApiKeyName])
                    .values([
                        (
                            minute_stat::Column::TotalRequests,
                            accumulated("total_requests"),
                        ),
                        (
                            minute_stat::Column::SuccessRequests,
                            accumulated("success_requests"),
                        ),
                        (
                            minute_stat::Column::FailedRequests,
                            accumulated("failed_requests"),
                        ),
                        (minute_stat::Column::Status2xx, accumulated("status_2xx")),
                        (minute_stat::Column::Status4xx, accumulated("status_4xx")),
                        (minute_stat::Column::Status5xx, accumulated("status_5xx")),
                        (
                            minute_stat::Column::NetworkErrors,
                            accumulated("network_errors"),
                        ),
                        (
                            minute_stat::Column::TotalLatencyMs,
                            accumulated("total_latency_ms"),
                        ),
                        (
                            minute_stat::Column::MaxLatencyMs,
                            Func::greatest([
                                Expr::col((minute_stat::Entity, minute_stat::Column::MaxLatencyMs)),
                                Expr::cust("EXCLUDED.max_latency_ms"),
                            ])
                            .into(),
                        ),
                        (
                            minute_stat::Column::LastStatusCode,
                            Expr::cust("EXCLUDED.last_status_code"),
                        ),
                        (
                            minute_stat::Column::LastError,
                            Expr::cust("EXCLUDED.last_error"),
                        ),
                        (
                            minute_stat::Column::UpdatedAt,
                            Expr::cust("EXCLUDED.updated_at"),
                        ),
                    ])
                    .to_owned();

            minute_stat::Entity::insert(minute_stat::ActiveModel {
                api_key_id: Set(item.api_key_id),
                api_key_name: Set(item.api_key_name),
                minute_at: Set(item.minute_at),
                total_requests: Set(item.total_requests),
                success_requests: Set(item.success_requests),
                failed_requests: Set(item.failed_requests),
                status_2xx: Set(item.status_2xx),
                status_4xx: Set(item.status_4xx),
                status_5xx: Set(item.status_5xx),
                network_errors: Set(item.network_errors),
                total_latency_ms: Set(item.total_latency_ms),
                max_latency_ms: Set(item.max_latency_ms),
                last_status_code: Set(item.last_status_code),
                last_error: Set(item.last_error),
                updated_at: Set(item.updated_at),
                ..Default::default()
            })
            .on_conflict(on_conflict)
            .exec(&tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

/// `COALESCE(current, 0) + COALESCE(EXCLUDED.<column>, 0)`, matching the
/// accumulated counters that the pre-SeaORM SQL maintained in `DO UPDATE`.
fn accumulated(column: &str) -> Expr {
    Expr::cust(format!(
        "COALESCE(context7_minute_stats.{column}, 0) + COALESCE(EXCLUDED.{column}, 0)"
    ))
}

async fn recv_one(queue_rx: &Mutex<tokio::sync::mpsc::Receiver<Event>>) -> Option<Event> {
    queue_rx.lock().await.recv().await
}

fn log_filters(params: &LogQuery) -> Condition {
    let mut condition = Condition::all();
    if let Some(api_key_id) = params.api_key_id {
        condition = condition.add(request_log::Column::ApiKeyId.eq(api_key_id));
    }
    if let Some(success) = params.success {
        condition = condition.add(request_log::Column::Success.eq(success));
    }
    if let Some(status_code) = params.status_code {
        condition = condition.add(request_log::Column::StatusCode.eq(status_code));
    }
    condition
}

fn minute_item(row: minute_stat::Model) -> MinuteItem {
    let total_requests = i64::from(row.total_requests);
    let success_requests = i64::from(row.success_requests);
    MinuteItem {
        api_key_id: row.api_key_id,
        api_key_name: row.api_key_name,
        minute_at: row.minute_at,
        total_requests,
        success_requests,
        failed_requests: i64::from(row.failed_requests),
        status_2xx: i64::from(row.status_2xx),
        status_4xx: i64::from(row.status_4xx),
        status_5xx: i64::from(row.status_5xx),
        network_errors: i64::from(row.network_errors),
        total_latency_ms: row.total_latency_ms,
        max_latency_ms: row.max_latency_ms,
        last_status_code: row.last_status_code,
        last_error: row.last_error,
        updated_at: row.updated_at,
        success_rate: ratio_decimal(success_requests, total_requests),
        average_latency_ms: ratio_decimal(row.total_latency_ms, total_requests),
    }
}

fn log_item(row: request_log::Model) -> LogItem {
    LogItem {
        id: row.id,
        api_key_id: row.api_key_id,
        api_key_name: row.api_key_name,
        method: row.method,
        path: row.path,
        query: row.query,
        status_code: row.status_code,
        success: row.success,
        latency_ms: row.latency_ms,
        error: row.error,
        client_ip: row.client_ip,
        user_agent: row.user_agent,
        client_source: row.client_source,
        client_ide: row.client_ide,
        client_version: row.client_version,
        transport: row.transport,
        started_at: row.started_at,
        finished_at: row.finished_at,
    }
}

fn normalize_event(mut event: Event) -> Event {
    let now = Utc::now();
    if event.finished_at.timestamp() == 0 {
        event.finished_at = now;
    }
    if event.started_at.timestamp() == 0 {
        event.started_at = event.finished_at;
    }
    if event.latency_ms <= 0 {
        event.latency_ms =
            std::cmp::Ord::max((event.finished_at - event.started_at).num_milliseconds(), 0);
    }
    event.api_key_name = event.api_key_name.trim().to_string();
    event.method = event.method.trim().to_string();
    event.path = event.path.trim().to_string();
    event.query = limit_string(event.query);
    event.error = limit_string(event.error.trim().to_string());
    event.client_ip = limit_string(event.client_ip.trim().to_string());
    event.user_agent = limit_string(event.user_agent.trim().to_string());
    event.client_source = limit_string(event.client_source.trim().to_string());
    event.client_ide = limit_string(event.client_ide.trim().to_string());
    event.client_version = limit_string(event.client_version.trim().to_string());
    event.transport = limit_string(event.transport.trim().to_string());
    event.success = event.status_code >= 200 && event.status_code < 300;
    event
}

fn aggregate_events(batch: &[Event]) -> Vec<MinuteAggregate> {
    let mut aggregates: HashMap<(i64, DateTime<Utc>), MinuteAggregate> = HashMap::new();
    for event in batch {
        let minute_at = event
            .started_at
            .with_second(0)
            .and_then(|value| value.with_nanosecond(0))
            .unwrap_or(event.started_at);
        let key = (event.api_key_id, minute_at);
        let item = aggregates.entry(key).or_insert_with(|| MinuteAggregate {
            api_key_id: event.api_key_id,
            api_key_name: event.api_key_name.clone(),
            minute_at,
            total_requests: 0,
            success_requests: 0,
            failed_requests: 0,
            status_2xx: 0,
            status_4xx: 0,
            status_5xx: 0,
            network_errors: 0,
            total_latency_ms: 0,
            max_latency_ms: 0,
            last_status_code: 0,
            last_error: None,
            updated_at: event.finished_at,
            last_finished_at: DateTime::<Utc>::UNIX_EPOCH,
        });

        item.total_requests += 1;
        if event.success {
            item.success_requests += 1;
        } else {
            item.failed_requests += 1;
        }
        match event.status_code {
            0 => item.network_errors += 1,
            200..=299 => item.status_2xx += 1,
            400..=499 => item.status_4xx += 1,
            500..=599 => item.status_5xx += 1,
            _ => {}
        }
        item.total_latency_ms += event.latency_ms;
        item.max_latency_ms = std::cmp::Ord::max(item.max_latency_ms, event.latency_ms);
        if event.finished_at >= item.last_finished_at {
            item.api_key_name = event.api_key_name.clone();
            item.last_status_code = event.status_code;
            item.last_error = optional_string(&event.error);
            item.updated_at = event.finished_at;
            item.last_finished_at = event.finished_at;
        }
    }
    aggregates.into_values().collect()
}

fn optional_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn limit_string(value: String) -> String {
    if value.len() <= MAX_STORED_TEXT_LENGTH {
        value
    } else {
        value[..MAX_STORED_TEXT_LENGTH].to_string()
    }
}

fn ratio_decimal(numerator: i64, denominator: i64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    (Decimal::from(numerator) / Decimal::from(denominator))
        .to_f64()
        .unwrap_or(0.0)
}

impl Default for Event {
    fn default() -> Self {
        Self {
            api_key_id: 0,
            api_key_name: String::new(),
            method: String::new(),
            path: String::new(),
            query: String::new(),
            status_code: 0,
            success: false,
            latency_ms: 0,
            error: String::new(),
            client_ip: String::new(),
            user_agent: String::new(),
            client_source: String::new(),
            client_ide: String::new(),
            client_version: String::new(),
            transport: String::new(),
            started_at: DateTime::<Utc>::UNIX_EPOCH,
            finished_at: DateTime::<Utc>::UNIX_EPOCH,
        }
    }
}
