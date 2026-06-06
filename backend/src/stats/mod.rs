use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Timelike, Utc};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, QueryBuilder, Row};
use tokio::sync::Mutex;

use crate::error::AppResult;

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
    pool: PgPool,
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
    pub fn new(pool: PgPool, cfg: Config) -> Self {
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
            pool,
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
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(total_requests), 0)::BIGINT AS total_requests,
                COALESCE(SUM(success_requests), 0)::BIGINT AS success_requests,
                COALESCE(SUM(failed_requests), 0)::BIGINT AS failed_requests,
                COALESCE(SUM(network_errors), 0)::BIGINT AS network_errors,
                COALESCE(SUM(status_2xx), 0)::BIGINT AS status_2xx,
                COALESCE(SUM(status_4xx), 0)::BIGINT AS status_4xx,
                COALESCE(SUM(status_5xx), 0)::BIGINT AS status_5xx,
                COALESCE(SUM(total_latency_ms), 0)::BIGINT AS total_latency_ms,
                COALESCE(MAX(max_latency_ms), 0)::BIGINT AS max_latency_ms
            FROM context7_minute_stats
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let mut summary = Summary {
            total_requests: row.try_get("total_requests")?,
            success_requests: row.try_get("success_requests")?,
            failed_requests: row.try_get("failed_requests")?,
            network_errors: row.try_get("network_errors")?,
            status_2xx: row.try_get("status_2xx")?,
            status_4xx: row.try_get("status_4xx")?,
            status_5xx: row.try_get("status_5xx")?,
            total_latency_ms: row.try_get("total_latency_ms")?,
            max_latency_ms: row.try_get("max_latency_ms")?,
            ..Summary::default()
        };
        if summary.total_requests > 0 {
            summary.success_rate = ratio_decimal(summary.success_requests, summary.total_requests);
            summary.average_latency_ms =
                ratio_decimal(summary.total_latency_ms, summary.total_requests);
        }

        if let Some(last) = sqlx::query(
            r#"
            SELECT finished_at, status_code, error
            FROM context7_request_logs
            ORDER BY finished_at DESC, id DESC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?
        {
            summary.last_request_at = Some(last.try_get("finished_at")?);
            summary.last_status_code = last.try_get("status_code")?;
            summary.last_error = last.try_get("error")?;
        }

        Ok(summary)
    }

    pub async fn list_minutes(&self, params: MinuteQuery) -> AppResult<Vec<MinuteItem>> {
        let mut builder = QueryBuilder::new(
            r#"
            SELECT api_key_id, api_key_name, minute_at, total_requests, success_requests,
                   failed_requests, status_2xx, status_4xx, status_5xx, network_errors,
                   total_latency_ms, max_latency_ms, last_status_code, last_error, updated_at
            FROM context7_minute_stats
            WHERE TRUE
            "#,
        );
        if let Some(from) = params.from {
            builder.push(" AND minute_at >= ").push_bind(from);
        }
        if let Some(to) = params.to {
            builder.push(" AND minute_at <= ").push_bind(to);
        }
        if let Some(api_key_id) = params.api_key_id {
            builder.push(" AND api_key_id = ").push_bind(api_key_id);
        }
        builder.push(" ORDER BY minute_at ASC, api_key_id ASC");

        let rows = builder.build().fetch_all(&self.pool).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            let total_requests = i64::from(row.try_get::<i32, _>("total_requests")?);
            let success_requests = i64::from(row.try_get::<i32, _>("success_requests")?);
            let total_latency_ms = row.try_get::<i64, _>("total_latency_ms")?;
            items.push(MinuteItem {
                api_key_id: row.try_get("api_key_id")?,
                api_key_name: row.try_get("api_key_name")?,
                minute_at: row.try_get("minute_at")?,
                total_requests,
                success_requests,
                failed_requests: i64::from(row.try_get::<i32, _>("failed_requests")?),
                status_2xx: i64::from(row.try_get::<i32, _>("status_2xx")?),
                status_4xx: i64::from(row.try_get::<i32, _>("status_4xx")?),
                status_5xx: i64::from(row.try_get::<i32, _>("status_5xx")?),
                network_errors: i64::from(row.try_get::<i32, _>("network_errors")?),
                total_latency_ms,
                max_latency_ms: row.try_get("max_latency_ms")?,
                last_status_code: row.try_get("last_status_code")?,
                last_error: row.try_get("last_error")?,
                updated_at: row.try_get("updated_at")?,
                success_rate: ratio_decimal(success_requests, total_requests),
                average_latency_ms: ratio_decimal(total_latency_ms, total_requests),
            });
        }
        Ok(items)
    }

    pub async fn list_logs(&self, params: LogQuery) -> AppResult<LogPage> {
        let page = params.page.unwrap_or(1).max(1);
        let page_size = params
            .page_size
            .unwrap_or(DEFAULT_LOG_PAGE_SIZE)
            .clamp(1, MAX_LOG_PAGE_SIZE);

        let mut count_builder = QueryBuilder::new(
            "SELECT COUNT(*)::BIGINT AS total FROM context7_request_logs WHERE TRUE",
        );
        push_log_filters(&mut count_builder, &params);
        let total: i64 = count_builder
            .build()
            .fetch_one(&self.pool)
            .await?
            .try_get("total")?;

        let mut builder = QueryBuilder::new(
            r#"
            SELECT id, api_key_id, api_key_name, method, path, query, status_code, success,
                   latency_ms, error, client_ip, user_agent, client_source, client_ide,
                   client_version, transport, started_at, finished_at
            FROM context7_request_logs
            WHERE TRUE
            "#,
        );
        push_log_filters(&mut builder, &params);
        builder
            .push(" ORDER BY started_at DESC, id DESC LIMIT ")
            .push_bind(page_size)
            .push(" OFFSET ")
            .push_bind((page - 1) * page_size);

        let rows = builder.build().fetch_all(&self.pool).await?;
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(LogItem {
                id: row.try_get("id")?,
                api_key_id: row.try_get("api_key_id")?,
                api_key_name: row.try_get("api_key_name")?,
                method: row.try_get("method")?,
                path: row.try_get("path")?,
                query: row.try_get("query")?,
                status_code: row.try_get("status_code")?,
                success: row.try_get("success")?,
                latency_ms: row.try_get("latency_ms")?,
                error: row.try_get("error")?,
                client_ip: row.try_get("client_ip")?,
                user_agent: row.try_get("user_agent")?,
                client_source: row.try_get("client_source")?,
                client_ide: row.try_get("client_ide")?,
                client_version: row.try_get("client_version")?,
                transport: row.try_get("transport")?,
                started_at: row.try_get("started_at")?,
                finished_at: row.try_get("finished_at")?,
            });
        }

        Ok(LogPage {
            items,
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
        let mut tx = self.pool.begin().await?;
        for event in batch {
            let error = optional_string(&event.error);
            let client_ip = optional_string(&event.client_ip);
            let user_agent = optional_string(&event.user_agent);
            let client_source = optional_string(&event.client_source);
            let client_ide = optional_string(&event.client_ide);
            let client_version = optional_string(&event.client_version);
            let transport = optional_string(&event.transport);
            sqlx::query!(
                r#"
                INSERT INTO context7_request_logs
                    (api_key_id, api_key_name, method, path, query, status_code, success,
                     latency_ms, error, client_ip, user_agent, client_source, client_ide,
                     client_version, transport, started_at, finished_at)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7,
                     $8, $9, $10, $11, $12, $13,
                     $14, $15, $16, $17)
                "#,
                event.api_key_id,
                event.api_key_name,
                event.method,
                event.path,
                event.query,
                event.status_code,
                event.success,
                event.latency_ms,
                error,
                client_ip,
                user_agent,
                client_source,
                client_ide,
                client_version,
                transport,
                event.started_at,
                event.finished_at
            )
            .execute(&mut *tx)
            .await?;
        }

        for item in aggregate_events(batch) {
            sqlx::query!(
                r#"
                INSERT INTO context7_minute_stats
                    (api_key_id, api_key_name, minute_at, total_requests, success_requests,
                     failed_requests, status_2xx, status_4xx, status_5xx, network_errors,
                     total_latency_ms, max_latency_ms, last_status_code, last_error, updated_at)
                VALUES
                    ($1, $2, $3, $4, $5,
                     $6, $7, $8, $9, $10,
                     $11, $12, $13, $14, $15)
                ON CONFLICT (api_key_id, minute_at) DO UPDATE
                SET api_key_name = EXCLUDED.api_key_name,
                    total_requests = COALESCE(context7_minute_stats.total_requests, 0) + COALESCE(EXCLUDED.total_requests, 0),
                    success_requests = COALESCE(context7_minute_stats.success_requests, 0) + COALESCE(EXCLUDED.success_requests, 0),
                    failed_requests = COALESCE(context7_minute_stats.failed_requests, 0) + COALESCE(EXCLUDED.failed_requests, 0),
                    status_2xx = COALESCE(context7_minute_stats.status_2xx, 0) + COALESCE(EXCLUDED.status_2xx, 0),
                    status_4xx = COALESCE(context7_minute_stats.status_4xx, 0) + COALESCE(EXCLUDED.status_4xx, 0),
                    status_5xx = COALESCE(context7_minute_stats.status_5xx, 0) + COALESCE(EXCLUDED.status_5xx, 0),
                    network_errors = COALESCE(context7_minute_stats.network_errors, 0) + COALESCE(EXCLUDED.network_errors, 0),
                    total_latency_ms = COALESCE(context7_minute_stats.total_latency_ms, 0) + COALESCE(EXCLUDED.total_latency_ms, 0),
                    max_latency_ms = GREATEST(context7_minute_stats.max_latency_ms, EXCLUDED.max_latency_ms),
                    last_status_code = EXCLUDED.last_status_code,
                    last_error = EXCLUDED.last_error,
                    updated_at = EXCLUDED.updated_at
                "#,
                item.api_key_id,
                item.api_key_name,
                item.minute_at,
                item.total_requests,
                item.success_requests,
                item.failed_requests,
                item.status_2xx,
                item.status_4xx,
                item.status_5xx,
                item.network_errors,
                item.total_latency_ms,
                item.max_latency_ms,
                item.last_status_code,
                item.last_error,
                item.updated_at
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

async fn recv_one(queue_rx: &Mutex<tokio::sync::mpsc::Receiver<Event>>) -> Option<Event> {
    queue_rx.lock().await.recv().await
}

fn push_log_filters<'a>(builder: &mut QueryBuilder<'a, sqlx::Postgres>, params: &'a LogQuery) {
    if let Some(api_key_id) = params.api_key_id {
        builder.push(" AND api_key_id = ").push_bind(api_key_id);
    }
    if let Some(success) = params.success {
        builder.push(" AND success = ").push_bind(success);
    }
    if let Some(status_code) = params.status_code {
        builder.push(" AND status_code = ").push_bind(status_code);
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
        event.latency_ms = (event.finished_at - event.started_at)
            .num_milliseconds()
            .max(0);
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
        item.max_latency_ms = item.max_latency_ms.max(event.latency_ms);
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
