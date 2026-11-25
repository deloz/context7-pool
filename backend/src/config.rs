use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    time::Duration,
};

use anyhow::{Context, Result, bail};

const DEFAULT_HTTP_ADDR: &str = ":42421";
const DEFAULT_DATABASE_URL: &str = "postgres://contextpool:contextpool@127.0.0.1:45432/contextpool";
const DEFAULT_FAILURE_THRESHOLD: i32 = 3;
const DEFAULT_COOLDOWN_SECONDS: u64 = 30;
const DEFAULT_FLUSH_BATCH_SIZE: usize = 64;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_FRONTEND_DIST: &str = "../frontend/dist";
const DEFAULT_UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const DEFAULT_UPSTREAM_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_UPSTREAM_TOTAL_TIMEOUT: Duration = Duration::from_secs(25);
const DEFAULT_UPSTREAM_MAX_ATTEMPTS: usize = 3;
const DEFAULT_UPSTREAM_PROBE_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub database_url: String,
    pub upstream_base_url: Option<String>,
    pub upstream_connect_timeout: Duration,
    pub upstream_request_timeout: Duration,
    pub upstream_total_timeout: Duration,
    pub upstream_max_attempts: usize,
    pub upstream_proxy_url: Option<String>,
    pub upstream_resolve_addrs: HashMap<String, Vec<IpAddr>>,
    pub upstream_discovery_domains: HashMap<String, Vec<String>>,
    pub upstream_probe_interval: Option<Duration>,
    pub failure_threshold: i32,
    pub cooldown: Duration,
    pub flush_batch_size: usize,
    pub flush_interval: Duration,
    pub shutdown_timeout: Duration,
    pub frontend_dist: PathBuf,
}

impl Config {
    pub fn load() -> Result<Self> {
        let upstream_resolve_addrs = non_empty_env("CONTEXTPOOL_UPSTREAM_RESOLVE_ADDRS")
            .map(|raw| parse_resolve_addrs(&raw))
            .transpose()?
            .unwrap_or_default();
        let upstream_discovery_domains = non_empty_env("CONTEXTPOOL_UPSTREAM_DISCOVERY_DOMAINS")
            .map(|raw| parse_discovery_domains(&raw))
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            http_addr: parse_addr(&getenv("CONTEXTPOOL_HTTP_ADDR", DEFAULT_HTTP_ADDR))?,
            database_url: database_url(),
            upstream_base_url: non_empty_env("CONTEXTPOOL_UPSTREAM_BASE_URL"),
            upstream_connect_timeout: getenv_duration(
                "CONTEXTPOOL_UPSTREAM_CONNECT_TIMEOUT",
                DEFAULT_UPSTREAM_CONNECT_TIMEOUT,
            ),
            upstream_request_timeout: getenv_duration(
                "CONTEXTPOOL_UPSTREAM_REQUEST_TIMEOUT",
                DEFAULT_UPSTREAM_REQUEST_TIMEOUT,
            ),
            upstream_total_timeout: getenv_duration(
                "CONTEXTPOOL_UPSTREAM_TOTAL_TIMEOUT",
                DEFAULT_UPSTREAM_TOTAL_TIMEOUT,
            ),
            upstream_max_attempts: getenv_usize(
                "CONTEXTPOOL_UPSTREAM_MAX_ATTEMPTS",
                DEFAULT_UPSTREAM_MAX_ATTEMPTS,
            ),
            upstream_proxy_url: non_empty_env("CONTEXTPOOL_UPSTREAM_PROXY"),
            upstream_resolve_addrs,
            upstream_discovery_domains,
            upstream_probe_interval: getenv_optional_duration(
                "CONTEXTPOOL_UPSTREAM_PROBE_INTERVAL",
                DEFAULT_UPSTREAM_PROBE_INTERVAL,
            ),
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

fn getenv_optional_duration(key: &str, fallback: Duration) -> Option<Duration> {
    let Some(raw) = non_empty_env(key) else {
        return Some(fallback);
    };
    let Some(parsed) = parse_duration(&raw) else {
        return Some(fallback);
    };
    (!parsed.is_zero()).then_some(parsed)
}

fn parse_duration(raw: &str) -> Option<Duration> {
    let raw = raw.trim();
    if let Some(ms) = raw.strip_suffix("ms") {
        return ms.trim().parse::<u64>().ok().map(Duration::from_millis);
    }
    if let Some(min) = raw.strip_suffix('m') {
        return min
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|value| value.checked_mul(60))
            .map(Duration::from_secs);
    }
    if let Some(sec) = raw.strip_suffix('s') {
        return sec.trim().parse::<u64>().ok().map(Duration::from_secs);
    }
    raw.parse::<u64>().ok().map(Duration::from_secs)
}

fn parse_resolve_addrs(raw: &str) -> Result<HashMap<String, Vec<IpAddr>>> {
    let mut map: HashMap<String, Vec<IpAddr>> = HashMap::new();
    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((host, ips_raw)) = entry.split_once('=') else {
            bail!(
                "invalid CONTEXTPOOL_UPSTREAM_RESOLVE_ADDRS entry {entry:?}: expected host=ip1,ip2"
            );
        };
        let host = normalize_host(host)?;
        let list = map.entry(host.clone()).or_default();
        let mut added = 0usize;
        for ip_raw in ips_raw.split(',') {
            let ip_raw = ip_raw.trim();
            if ip_raw.is_empty() {
                continue;
            }
            let ip = IpAddr::from_str(ip_raw).with_context(|| {
                format!("invalid IP {ip_raw:?} in CONTEXTPOOL_UPSTREAM_RESOLVE_ADDRS for {host}")
            })?;
            if !list.contains(&ip) {
                list.push(ip);
            }
            added += 1;
        }
        if added == 0 {
            bail!("missing IP addresses in CONTEXTPOOL_UPSTREAM_RESOLVE_ADDRS for {host}");
        }
    }
    Ok(map)
}

fn parse_discovery_domains(raw: &str) -> Result<HashMap<String, Vec<String>>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((host, aliases_raw)) = entry.split_once('=') else {
            bail!(
                "invalid CONTEXTPOOL_UPSTREAM_DISCOVERY_DOMAINS entry {entry:?}: expected host=alias1,alias2"
            );
        };
        let host = normalize_host(host)?;
        let list = map.entry(host.clone()).or_default();
        let mut added = 0usize;
        for alias_raw in aliases_raw.split(',') {
            let alias_raw = alias_raw.trim();
            if alias_raw.is_empty() {
                continue;
            }
            let alias = normalize_host(alias_raw)
                .with_context(|| format!("invalid discovery domain {alias_raw:?} for {host}"))?;
            if !list.contains(&alias) {
                list.push(alias);
            }
            added += 1;
        }
        if added == 0 {
            bail!("missing discovery domains in CONTEXTPOOL_UPSTREAM_DISCOVERY_DOMAINS for {host}");
        }
    }
    Ok(map)
}

fn normalize_host(raw: &str) -> Result<String> {
    let host = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty()
        || host.contains("://")
        || host.contains('/')
        || host.chars().any(char::is_whitespace)
    {
        bail!("invalid host {raw:?}");
    }
    Ok(host)
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[test]
    fn parse_duration_supports_ms_s_m_and_plain_seconds() {
        assert_eq!(parse_duration("100ms"), Some(Duration::from_millis(100)));
        assert_eq!(parse_duration("15s"), Some(Duration::from_secs(15)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("30"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("0s"), Some(Duration::ZERO));
        assert_eq!(parse_duration("abc"), None);
    }

    #[test]
    fn parse_resolve_addrs_validates_and_deduplicates() {
        let parsed = parse_resolve_addrs(
            " Context7.com = 216.198.79.1, 64.29.17.1, 216.198.79.1 ; api.example.com=1.2.3.4 ",
        )
        .unwrap();
        assert_eq!(
            parsed.get("context7.com"),
            Some(&vec![
                IpAddr::V4(Ipv4Addr::new(216, 198, 79, 1)),
                IpAddr::V4(Ipv4Addr::new(64, 29, 17, 1)),
            ])
        );
        assert_eq!(
            parsed.get("api.example.com"),
            Some(&vec![IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4))])
        );
    }

    #[test]
    fn parse_resolve_addrs_rejects_invalid_entries() {
        assert!(parse_resolve_addrs("context7.com").is_err());
        assert!(parse_resolve_addrs("context7.com=").is_err());
        assert!(parse_resolve_addrs("context7.com=not-an-ip").is_err());
        assert!(parse_resolve_addrs("https://context7.com=1.2.3.4").is_err());
    }

    #[test]
    fn parse_discovery_domains_validates_and_normalizes() {
        let parsed = parse_discovery_domains(
            "Context7.com=cname.vercel-dns-017.com, cname.vercel-dns.com., cname.vercel-dns.com",
        )
        .unwrap();
        assert_eq!(
            parsed.get("context7.com"),
            Some(&vec![
                "cname.vercel-dns-017.com".to_string(),
                "cname.vercel-dns.com".to_string(),
            ])
        );
        assert!(parse_discovery_domains("context7.com=").is_err());
        assert!(parse_discovery_domains("context7.com=https://bad.example").is_err());
    }
}
