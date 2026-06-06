use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, RwLock, mpsc};

use crate::models::{ApiKeyRow, ProxyKey, RuntimeMeta};

#[derive(Clone, Debug)]
pub struct Record {
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

impl From<ApiKeyRow> for Record {
    fn from(value: ApiKeyRow) -> Self {
        Self {
            id: value.id,
            name: value.name,
            api_key: value.api_key,
            enabled: value.enabled,
            health_status: value.health_status,
            failure_streak: value.failure_streak,
            cooldown_until: value.cooldown_until,
            last_error: value.last_error,
            last_status_code: value.last_status_code,
            last_success_at: value.last_success_at,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PersistedState {
    pub id: i64,
    pub health_status: String,
    pub failure_streak: i32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_status_code: Option<i32>,
    pub last_success_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub failure_threshold: i32,
    pub cooldown: Duration,
    pub flush_interval: Duration,
    pub flush_batch_size: usize,
}

#[derive(Clone, Debug)]
struct RuntimeState {
    health_status: String,
    failure_streak: i32,
    cooldown_until: Option<DateTime<Utc>>,
    last_error: Option<String>,
    last_status_code: Option<i32>,
    last_success_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
struct Snapshot {
    version: u64,
    updated_at: DateTime<Utc>,
    total_keys: usize,
    cooling_keys: usize,
    available_keys: Vec<ProxyKey>,
}

#[derive(Debug)]
struct Inner {
    keys: HashMap<i64, Record>,
    states: HashMap<i64, RuntimeState>,
    dirty: HashMap<i64, PersistedState>,
}

#[derive(Clone)]
pub struct Manager {
    failure_threshold: i32,
    cooldown: Duration,
    flush_interval: Duration,
    flush_batch_size: usize,
    cursor: Arc<AtomicU64>,
    snapshot: Arc<RwLock<Snapshot>>,
    inner: Arc<Mutex<Inner>>,
    flush_tx: mpsc::Sender<()>,
    flush_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

#[derive(Debug, thiserror::Error)]
#[error("no available api key")]
pub struct NoAvailableKey;

impl Manager {
    pub fn new(cfg: Config) -> Self {
        let now = Utc::now();
        let (flush_tx, flush_rx) = mpsc::channel(1);
        Self {
            failure_threshold: cfg.failure_threshold,
            cooldown: cfg.cooldown,
            flush_interval: cfg.flush_interval,
            flush_batch_size: cfg.flush_batch_size,
            cursor: Arc::new(AtomicU64::new(0)),
            snapshot: Arc::new(RwLock::new(Snapshot {
                version: 0,
                updated_at: now,
                total_keys: 0,
                cooling_keys: 0,
                available_keys: Vec::new(),
            })),
            inner: Arc::new(Mutex::new(Inner {
                keys: HashMap::new(),
                states: HashMap::new(),
                dirty: HashMap::new(),
            })),
            flush_tx,
            flush_rx: Arc::new(Mutex::new(flush_rx)),
        }
    }

    pub async fn load(&self, records: Vec<Record>, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().await;
        inner.keys.clear();
        inner.states.clear();
        inner.dirty.clear();
        for record in records {
            inner.states.insert(
                record.id,
                RuntimeState {
                    health_status: normalize_health_status(
                        &record.health_status,
                        record.failure_streak,
                        record.cooldown_until,
                    ),
                    failure_streak: record.failure_streak,
                    cooldown_until: record.cooldown_until,
                    last_error: record.last_error.clone(),
                    last_status_code: record.last_status_code,
                    last_success_at: record.last_success_at,
                },
            );
            inner.keys.insert(record.id, record);
        }
        self.rebuild_snapshot_locked(&inner, now).await;
    }

    pub async fn put_record(&self, record: Record, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().await;
        inner
            .states
            .entry(record.id)
            .or_insert_with(|| RuntimeState {
                health_status: normalize_health_status(
                    &record.health_status,
                    record.failure_streak,
                    record.cooldown_until,
                ),
                failure_streak: record.failure_streak,
                cooldown_until: record.cooldown_until,
                last_error: record.last_error.clone(),
                last_status_code: record.last_status_code,
                last_success_at: record.last_success_at,
            });
        inner.keys.insert(record.id, record);
        self.rebuild_snapshot_locked(&inner, now).await;
    }

    pub async fn delete_record(&self, id: i64, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().await;
        inner.keys.remove(&id);
        inner.states.remove(&id);
        inner.dirty.remove(&id);
        self.rebuild_snapshot_locked(&inner, now).await;
    }

    pub async fn reset_health(&self, id: i64, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().await;
        let Some(record) = inner.keys.get(&id) else {
            return;
        };
        let last_success_at = record.last_success_at;
        inner.states.insert(
            id,
            RuntimeState {
                health_status: "healthy".to_string(),
                failure_streak: 0,
                cooldown_until: None,
                last_error: None,
                last_status_code: None,
                last_success_at,
            },
        );
        self.mark_dirty_locked(&mut inner, id);
        self.rebuild_snapshot_locked(&inner, now).await;
    }

    pub async fn select(&self) -> Result<ProxyKey, NoAvailableKey> {
        self.select_excluding(&HashSet::new(), Utc::now()).await
    }

    pub async fn select_excluding(
        &self,
        excluded: &HashSet<i64>,
        now: DateTime<Utc>,
    ) -> Result<ProxyKey, NoAvailableKey> {
        self.refresh(now).await;

        let snapshot = self.snapshot.read().await.clone();
        if snapshot.available_keys.is_empty() {
            return Err(NoAvailableKey);
        }

        let len = snapshot.available_keys.len();
        let start = (self.cursor.fetch_add(1, Ordering::SeqCst) as usize) % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            let key = snapshot.available_keys[index].clone();
            if !excluded.contains(&key.id) {
                return Ok(key);
            }
        }

        Err(NoAvailableKey)
    }

    pub async fn report_success(&self, id: i64, status_code: i32, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().await;
        let Some(record) = inner.keys.get(&id) else {
            return;
        };
        let enabled = record.enabled;
        let Some(state) = inner.states.get_mut(&id) else {
            return;
        };
        let was_cooling = state.cooldown_until.is_some_and(|until| until > now);
        state.health_status = "healthy".to_string();
        state.failure_streak = 0;
        state.cooldown_until = None;
        state.last_error = None;
        state.last_status_code = Some(status_code);
        state.last_success_at = Some(now);
        self.mark_dirty_locked(&mut inner, id);
        if enabled && was_cooling {
            self.rebuild_snapshot_locked(&inner, now).await;
        }
    }

    pub async fn report_failure(
        &self,
        id: i64,
        status_code: i32,
        reason: String,
        now: DateTime<Utc>,
    ) {
        let mut inner = self.inner.lock().await;
        let Some(record) = inner.keys.get(&id) else {
            return;
        };
        if !record.enabled {
            return;
        }
        let Some(state) = inner.states.get_mut(&id) else {
            return;
        };

        state.failure_streak += 1;
        state.last_error = if reason.trim().is_empty() {
            None
        } else {
            Some(reason)
        };
        if status_code > 0 {
            state.last_status_code = Some(status_code);
        }

        if state.failure_streak >= self.failure_threshold {
            state.health_status = "cooling".to_string();
            state.cooldown_until =
                Some(now + chrono::Duration::from_std(self.cooldown).unwrap_or_default());
        } else {
            state.health_status = "degraded".to_string();
        }

        let cooling = state.cooldown_until.is_some();
        self.mark_dirty_locked(&mut inner, id);
        if cooling {
            self.rebuild_snapshot_locked(&inner, now).await;
        }
    }

    pub async fn meta(&self, now: DateTime<Utc>, upstream_configured: bool) -> RuntimeMeta {
        self.refresh(now).await;
        let snapshot = self.snapshot.read().await.clone();
        RuntimeMeta {
            total_key_count: snapshot.total_keys,
            available_key_count: snapshot.available_keys.len(),
            cooling_key_count: snapshot.cooling_keys,
            snapshot_updated_at: snapshot.updated_at,
            failure_threshold: self.failure_threshold,
            cooldown_seconds: self.cooldown.as_secs(),
            snapshot_version: snapshot.version,
            upstream_configured,
        }
    }

    pub async fn overlay(&self, mut record: Record, now: DateTime<Utc>) -> Record {
        self.refresh(now).await;
        let inner = self.inner.lock().await;
        if let Some(state) = inner.states.get(&record.id) {
            record.health_status = state.health_status.clone();
            record.failure_streak = state.failure_streak;
            record.cooldown_until = state.cooldown_until;
            record.last_error = state.last_error.clone();
            record.last_status_code = state.last_status_code;
            record.last_success_at = state.last_success_at;
        }
        record
    }

    pub async fn refresh(&self, now: DateTime<Utc>) {
        let mut inner = self.inner.lock().await;
        if self.expire_cooldowns_locked(&mut inner, now) {
            self.rebuild_snapshot_locked(&inner, now).await;
        }
    }

    pub async fn run<F, Fut>(&self, mut flush: F)
    where
        F: FnMut(Vec<PersistedState>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let mut ticker = tokio::time::interval(self.flush_interval);
        let mut flush_rx = self.flush_rx.lock().await;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let _ = self.flush_once(&mut flush).await;
                }
                Some(_) = flush_rx.recv() => {
                    let _ = self.flush_once(&mut flush).await;
                }
            }
        }
    }

    pub async fn flush_once<F, Fut>(&self, flush: &mut F) -> anyhow::Result<()>
    where
        F: FnMut(Vec<PersistedState>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<()>>,
    {
        let now = Utc::now();
        let batch = {
            let mut inner = self.inner.lock().await;
            if self.expire_cooldowns_locked(&mut inner, now) {
                self.rebuild_snapshot_locked(&inner, now).await;
            }
            let batch = inner.dirty.values().cloned().collect::<Vec<_>>();
            inner.dirty.clear();
            batch
        };

        if batch.is_empty() {
            return Ok(());
        }

        if let Err(err) = flush(batch.clone()).await {
            let mut inner = self.inner.lock().await;
            for item in batch {
                inner.dirty.insert(item.id, item);
            }
            let _ = self.flush_tx.try_send(());
            return Err(err);
        }
        Ok(())
    }

    async fn rebuild_snapshot_locked(&self, inner: &Inner, now: DateTime<Utc>) {
        let mut available = Vec::new();
        let mut cooling_keys = 0;
        for record in inner.keys.values() {
            if !record.enabled {
                continue;
            }
            let Some(state) = inner.states.get(&record.id) else {
                continue;
            };
            if state.cooldown_until.is_some_and(|until| until > now) {
                cooling_keys += 1;
                continue;
            }
            available.push(ProxyKey {
                id: record.id,
                name: record.name.clone(),
                api_key: record.api_key.clone(),
            });
        }
        available.sort_by_key(|item| item.id);

        let mut snapshot = self.snapshot.write().await;
        let version = snapshot.version + 1;
        *snapshot = Snapshot {
            version,
            updated_at: now,
            total_keys: inner.keys.len(),
            cooling_keys,
            available_keys: available,
        };
    }

    fn expire_cooldowns_locked(&self, inner: &mut Inner, now: DateTime<Utc>) -> bool {
        let mut changed = false;
        let ids = inner.keys.keys().copied().collect::<Vec<_>>();
        for id in ids {
            let Some(record) = inner.keys.get(&id) else {
                continue;
            };
            if !record.enabled {
                continue;
            }
            let Some(state) = inner.states.get_mut(&id) else {
                continue;
            };
            if state.cooldown_until.is_none_or(|until| until > now) {
                continue;
            }
            state.cooldown_until = None;
            state.failure_streak = 0;
            state.health_status = "healthy".to_string();
            self.mark_dirty_locked(inner, id);
            changed = true;
        }
        changed
    }

    fn mark_dirty_locked(&self, inner: &mut Inner, id: i64) {
        let Some(state) = inner.states.get(&id) else {
            return;
        };
        inner.dirty.insert(
            id,
            PersistedState {
                id,
                health_status: state.health_status.clone(),
                failure_streak: state.failure_streak,
                cooldown_until: state.cooldown_until,
                last_error: state.last_error.clone(),
                last_status_code: state.last_status_code,
                last_success_at: state.last_success_at,
            },
        );
        if inner.dirty.len() >= self.flush_batch_size {
            let _ = self.flush_tx.try_send(());
        }
    }
}

fn normalize_health_status(
    current: &str,
    failure_streak: i32,
    cooldown_until: Option<DateTime<Utc>>,
) -> String {
    if cooldown_until.is_some() {
        "cooling".to_string()
    } else if failure_streak > 0 {
        "degraded".to_string()
    } else if !current.trim().is_empty() {
        current.to_string()
    } else {
        "healthy".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config {
            failure_threshold: 2,
            cooldown: Duration::from_secs(30),
            flush_interval: Duration::from_secs(1),
            flush_batch_size: 64,
        }
    }

    fn record(id: i64, enabled: bool) -> Record {
        let now = Utc::now();
        Record {
            id,
            name: format!("key-{id}"),
            api_key: format!("token-{id}"),
            enabled,
            health_status: "healthy".to_string(),
            failure_streak: 0,
            cooldown_until: None,
            last_error: None,
            last_status_code: None,
            last_success_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn round_robin_skips_disabled_and_cooling() {
        let now = Utc::now();
        let manager = Manager::new(cfg());
        let mut cooling = record(3, true);
        cooling.cooldown_until = Some(now + chrono::Duration::seconds(10));
        manager
            .load(
                vec![record(1, true), record(2, false), cooling, record(4, true)],
                now,
            )
            .await;

        assert_eq!(manager.select().await.unwrap().id, 1);
        assert_eq!(manager.select().await.unwrap().id, 4);
        assert_eq!(manager.select().await.unwrap().id, 1);
    }

    #[tokio::test]
    async fn failure_cooling_and_recovery() {
        let now = Utc::now();
        let manager = Manager::new(cfg());
        manager
            .load(vec![record(1, true), record(2, true)], now)
            .await;

        manager
            .report_failure(1, 429, "too many requests".to_string(), now)
            .await;
        let degraded = manager.overlay(record(1, true), now).await;
        assert_eq!(degraded.health_status, "degraded");
        assert_eq!(degraded.failure_streak, 1);

        manager
            .report_failure(1, 429, "too many requests".to_string(), now)
            .await;
        let cooling = manager.overlay(record(1, true), now).await;
        assert_eq!(cooling.health_status, "cooling");
        assert_eq!(cooling.failure_streak, 2);
        assert!(cooling.cooldown_until.is_some());

        assert_eq!(manager.select().await.unwrap().id, 2);

        manager.report_success(1, 200, now).await;
        let recovered = manager.overlay(record(1, true), now).await;
        assert_eq!(recovered.health_status, "healthy");
        assert_eq!(recovered.failure_streak, 0);
        assert!(recovered.cooldown_until.is_none());
    }

    #[tokio::test]
    async fn select_excluding_skips_attempted_keys() {
        let now = Utc::now();
        let manager = Manager::new(cfg());
        manager
            .load(vec![record(1, true), record(2, true)], now)
            .await;

        let selected = manager
            .select_excluding(&HashSet::from([1]), now)
            .await
            .unwrap();
        assert_eq!(selected.id, 2);
    }
}
