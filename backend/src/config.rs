use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

const DEFAULT_HTTP_ADDR: &str = ":42421";
const DEFAULT_DATABASE_URL: &str = "postgres://contextpool:contextpool@127.0.0.1:45432/contextpool";
const DEFAULT_FAILURE_THRESHOLD: i32 = 3;
const DEFAULT_COOLDOWN_SECONDS: u64 = 30;
const DEFAULT_FLUSH_BATCH_SIZE: usize = 64;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_FRONTEND_DIST: &str = "../frontend/dist";

#[derive(Clone, Debug)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub database_url: String,
    pub upstream_base_url: Option<String>,
    pub failure_threshold: i32,
    pub cooldown: Duration,
    pub flush_batch_size: usize,
    pub flush_interval: Duration,
    pub shutdown_timeout: Duration,
    pub frontend_dist: PathBuf,
}

impl Config {
    pub fn load() -> Result<Self> {
        Ok(Self {
            http_addr: parse_addr(&getenv("CONTEXTPOOL_HTTP_ADDR", DEFAULT_HTTP_ADDR))?,
            database_url: database_url(),
            upstream_base_url: non_empty_env("CONTEXTPOOL_UPSTREAM_BASE_URL"),
            failure_threshold: getenv_i32(
                "CONTEXTPOOL_FAILURE_THRESHOLD",
                DEFAULT_FAILURE_THRESHOLD,
            ),
            cooldown: Duration::from_secs(getenv_u64(
                "CONTEXTPOOL_COOLDOWN_SECONDS",
                DEFAULT_COOLDOWN_SECONDS,
            )),
            flush_batch_size: getenv_usize(
                "CONTEXTPOOL_FLUSH_BATCH_SIZE",
                DEFAULT_FLUSH_BATCH_SIZE,
            ),
            flush_interval: getenv_duration("CONTEXTPOOL_FLUSH_INTERVAL", DEFAULT_FLUSH_INTERVAL),
            shutdown_timeout: getenv_duration(
                "CONTEXTPOOL_SHUTDOWN_TIMEOUT",
                DEFAULT_SHUTDOWN_TIMEOUT,
            ),
            frontend_dist: resolve_frontend_dist(&getenv(
                "CONTEXTPOOL_FRONTEND_DIST",
                DEFAULT_FRONTEND_DIST,
            )),
        })
    }

    pub fn has_upstream(&self) -> bool {
        self.upstream_base_url
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    }
}

fn database_url() -> String {
    non_empty_env("CONTEXTPOOL_DATABASE_URL")
        .or_else(|| non_empty_env("DATABASE_URL"))
        .unwrap_or_else(|| DEFAULT_DATABASE_URL.to_string())
}

fn getenv(key: &str, fallback: &str) -> String {
    non_empty_env(key).unwrap_or_else(|| fallback.to_string())
}

fn non_empty_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn getenv_i32(key: &str, fallback: i32) -> i32 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<i32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn getenv_u64(key: &str, fallback: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn getenv_usize(key: &str, fallback: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn getenv_duration(key: &str, fallback: Duration) -> Duration {
    env::var(key)
        .ok()
        .and_then(|value| parse_duration(value.trim()))
        .filter(|value| !value.is_zero())
        .unwrap_or(fallback)
}

fn parse_duration(raw: &str) -> Option<Duration> {
    if let Some(ms) = raw.strip_suffix("ms") {
        return ms.parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(sec) = raw.strip_suffix('s') {
        return sec.parse::<u64>().ok().map(Duration::from_secs);
    }
    raw.parse::<u64>().ok().map(Duration::from_secs)
}

fn parse_addr(raw: &str) -> Result<SocketAddr> {
    let normalized = if raw.starts_with(':') {
        format!("0.0.0.0{raw}")
    } else {
        raw.to_string()
    };
    normalized
        .parse::<SocketAddr>()
        .with_context(|| format!("parse CONTEXTPOOL_HTTP_ADDR={raw:?}"))
}

fn resolve_frontend_dist(raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        return path;
    }

    if raw != DEFAULT_FRONTEND_DIST {
        return env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| PathBuf::from(raw));
    }

    let cwd = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let candidates = if cwd.file_name().is_some_and(|name| name == "backend") {
        vec![cwd.join("../frontend/dist"), cwd.join("frontend/dist")]
    } else {
        vec![cwd.join("frontend/dist"), cwd.join("../frontend/dist")]
    };

    candidates
        .iter()
        .find(|candidate| candidate.exists())
        .cloned()
        .unwrap_or_else(|| candidates[0].clone())
}
