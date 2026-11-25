use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use reqwest::{
    Client,
    dns::{Addrs, Name, Resolve, Resolving},
    redirect::Policy,
};

const DEFAULT_PROBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_PROBE_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// Dynamic DNS discovery, passive-safe active health probing, and round-robin
/// multi-IP failover resolver for upstream hosts.
#[derive(Clone, Debug)]
pub struct UpstreamResolver {
    seeds: HashMap<String, Vec<IpAddr>>,
    discovery_domains: HashMap<String, Vec<String>>,
    active: Arc<RwLock<HashMap<String, Vec<IpAddr>>>>,
    cursors: Arc<HashMap<String, AtomicUsize>>,
    probe_interval: Option<Duration>,
    probe_connect_timeout: Duration,
    probe_request_timeout: Duration,
}

impl UpstreamResolver {
    pub fn new(
        seeds: HashMap<String, Vec<IpAddr>>,
        discovery_domains: HashMap<String, Vec<String>>,
        probe_interval: Option<Duration>,
    ) -> Self {
        let mut normalized_seeds = HashMap::new();
        for (host, ips) in seeds {
            let key = normalize_host(&host);
            if !key.is_empty() && !ips.is_empty() {
                normalized_seeds.insert(key, dedup_ips(ips));
            }
        }

        let mut normalized_discovery = HashMap::new();
        for (host, aliases) in discovery_domains {
            let key = normalize_host(&host);
            let cleaned: Vec<String> = aliases
                .into_iter()
                .map(|alias| normalize_host(&alias))
                .filter(|alias| !alias.is_empty())
                .collect();
            if !key.is_empty() && !cleaned.is_empty() {
                normalized_discovery.insert(key, cleaned);
            }
        }

        let mut hosts = HashSet::new();
        hosts.extend(normalized_seeds.keys().cloned());
        hosts.extend(normalized_discovery.keys().cloned());

        let mut cursors = HashMap::with_capacity(hosts.len());
        let mut initial_active = HashMap::new();
        for host in hosts {
            cursors.insert(host.clone(), AtomicUsize::new(0));
            if let Some(ips) = normalized_seeds.get(&host) {
                initial_active.insert(host, ips.clone());
            }
        }

        Self {
            seeds: normalized_seeds,
            discovery_domains: normalized_discovery,
            active: Arc::new(RwLock::new(initial_active)),
            cursors: Arc::new(cursors),
            probe_interval,
            probe_connect_timeout: DEFAULT_PROBE_CONNECT_TIMEOUT,
            probe_request_timeout: DEFAULT_PROBE_REQUEST_TIMEOUT,
        }
    }

    pub fn is_active(&self) -> bool {
        !self.cursors.is_empty()
    }

    pub fn managed_hosts(&self) -> Vec<String> {
        let mut hosts: Vec<String> = self.cursors.keys().cloned().collect();
        hosts.sort();
        hosts
    }

    pub fn active_addrs(&self, host: &str) -> Vec<IpAddr> {
        let key = normalize_host(host);
        self.active
            .read()
            .ok()
            .and_then(|guard| guard.get(&key).cloned())
            .unwrap_or_default()
    }

    /// Run an initial discovery + probe pass bounded by `max_wait` so startup
    /// never blocks if DNS or upstream probes are slow.
    pub async fn warmup(&self, max_wait: Duration) {
        if !self.is_active() {
            return;
        }
        if tokio::time::timeout(max_wait, self.probe_once())
            .await
            .is_err()
        {
            tracing::warn!(
                timeout_ms = max_wait.as_millis(),
                "upstream resolver warmup timed out, keeping seed/fallback addresses"
            );
        }
    }

    /// Periodically re-discover and probe candidate upstream IPs until aborted.
    pub async fn run(&self) {
        let Some(interval) = self.probe_interval else {
            return;
        };
        if !self.is_active() {
            return;
        }
        let start = tokio::time::Instant::now() + interval;
        let mut ticker = tokio::time::interval_at(start, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            self.probe_once().await;
        }
    }

    /// Discover candidate IPs from seeds + discovery domains + target DNS and
    /// promote reachable IPs ordered by lowest RTT.
    pub async fn probe_once(&self) {
        for host in self.managed_hosts() {
            let candidates = self.gather_candidates(&host).await;
            if candidates.is_empty() {
                tracing::warn!(host = %host, "upstream discovery yielded no candidate IPs");
                continue;
            }

            let probes = candidates.iter().copied().map(|ip| {
                let host = host.clone();
                let connect_timeout = self.probe_connect_timeout;
                let request_timeout = self.probe_request_timeout;
                async move {
                    (
                        ip,
                        probe_https_head(&host, ip, connect_timeout, request_timeout).await,
                    )
                }
            });
            let results = join_all(probes).await;
            let ranked = rank_probe_results(&results);

            let mut guard = match self.active.write() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            if !ranked.is_empty() {
                let changed = guard.get(&host) != Some(&ranked);
                guard.insert(host.clone(), ranked.clone());
                if changed {
                    tracing::info!(
                        host = %host,
                        healthy = ?ranked,
                        total_candidates = candidates.len(),
                        "upstream resolver updated healthy IPs"
                    );
                }
            } else if let Some(existing) = guard.get(&host)
                && !existing.is_empty()
            {
                tracing::warn!(
                    host = %host,
                    retained = ?existing,
                    "all upstream IP probes failed; retaining previous healthy IP list"
                );
            } else {
                guard.insert(host.clone(), candidates.clone());
                tracing::warn!(
                    host = %host,
                    fallback = ?candidates,
                    "all upstream IP probes failed with empty baseline; using full candidate list"
                );
            }
        }
    }

    async fn gather_candidates(&self, host: &str) -> Vec<IpAddr> {
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();

        if let Some(seeds) = self.seeds.get(host) {
            for &ip in seeds {
                if seen.insert(ip) {
                    candidates.push(ip);
                }
            }
        }

        let mut lookup_names = Vec::new();
        if let Some(aliases) = self.discovery_domains.get(host) {
            lookup_names.extend(aliases.iter().cloned());
        }
        lookup_names.push(host.to_string());

        for name in lookup_names {
            match tokio::net::lookup_host((name.as_str(), 443)).await {
                Ok(addrs) => {
                    for addr in addrs {
                        let ip = addr.ip();
                        if seen.insert(ip) {
                            candidates.push(ip);
                        }
                    }
                }
                Err(err) => {
                    tracing::debug!(host = %host, lookup = %name, error = %err, "candidate DNS lookup failed");
                }
            }
        }

        candidates
    }
}

impl Resolve for UpstreamResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = normalize_host(name.as_str());
        if let Some(cursor) = self.cursors.get(&host)
            && let Ok(guard) = self.active.read()
            && let Some(ips) = guard.get(&host)
            && !ips.is_empty()
        {
            let rotated = rotate_addrs(ips, cursor.fetch_add(1, Ordering::Relaxed));
            let addrs: Addrs = Box::new(rotated.into_iter().map(|ip| SocketAddr::new(ip, 0)));
            return Box::pin(std::future::ready(Ok(addrs)));
        }

        Box::pin(async move {
            let resolved: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let addrs: Addrs = Box::new(resolved.into_iter());
            Ok(addrs)
        })
    }
}

/// Lightweight keyless `HEAD https://<host>/` probe pinned to `ip:443`.
/// Validates TCP reachability, TLS SNI + certificate verification, and non-5xx
/// HTTP routing without consuming upstream API quota.
async fn probe_https_head(
    host: &str,
    ip: IpAddr,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Option<Duration> {
    let client = Client::builder()
        .resolve(host, SocketAddr::new(ip, 443))
        .redirect(Policy::none())
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .tcp_user_timeout(Some(request_timeout))
        .build()
        .ok()?;
    let url = format!("https://{host}/");
    let started = Instant::now();
    let response = client.head(url).send().await.ok()?;
    if response.status().is_server_error() {
        return None;
    }
    Some(started.elapsed())
}

fn rank_probe_results(results: &[(IpAddr, Option<Duration>)]) -> Vec<IpAddr> {
    let mut healthy: Vec<(IpAddr, Duration)> = results
        .iter()
        .filter_map(|(ip, rtt)| rtt.map(|dur| (*ip, dur)))
        .collect();
    healthy.sort_by_key(|(_, rtt)| *rtt);
    healthy.into_iter().map(|(ip, _)| ip).collect()
}

fn rotate_addrs(ips: &[IpAddr], cursor: usize) -> Vec<IpAddr> {
    if ips.is_empty() {
        return Vec::new();
    }
    let start = cursor % ips.len();
    let mut rotated = Vec::with_capacity(ips.len());
    rotated.extend_from_slice(&ips[start..]);
    rotated.extend_from_slice(&ips[..start]);
    rotated
}

fn dedup_ips(ips: Vec<IpAddr>) -> Vec<IpAddr> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(ips.len());
    for ip in ips {
        if seen.insert(ip) {
            out.push(ip);
        }
    }
    out
}

fn normalize_host(raw: &str) -> String {
    raw.trim().trim_end_matches('.').to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    #[test]
    fn rank_probe_results_filters_failures_and_sorts_by_rtt() {
        let ip1 = IpAddr::V4(Ipv4Addr::new(76, 76, 21, 21));
        let ip2 = IpAddr::V4(Ipv4Addr::new(216, 198, 79, 1));
        let ip3 = IpAddr::V4(Ipv4Addr::new(64, 29, 17, 1));
        let ranked = rank_probe_results(&[
            (ip1, None),
            (ip3, Some(Duration::from_millis(280))),
            (ip2, Some(Duration::from_millis(190))),
        ]);
        assert_eq!(ranked, vec![ip2, ip3]);
    }

    #[tokio::test]
    async fn resolve_rotates_active_ips_case_insensitively() {
        let ip1 = IpAddr::V4(Ipv4Addr::new(216, 198, 79, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(64, 29, 17, 1));
        let ip3 = IpAddr::V4(Ipv4Addr::new(66, 33, 60, 194));
        let mut seeds = HashMap::new();
        seeds.insert("Context7.COM".to_string(), vec![ip1, ip2, ip3]);

        let resolver = UpstreamResolver::new(seeds, HashMap::new(), None);
        assert!(resolver.is_active());
        assert_eq!(resolver.active_addrs("context7.com"), vec![ip1, ip2, ip3]);

        let first: Vec<IpAddr> = resolver
            .resolve("context7.com".parse().unwrap())
            .await
            .unwrap()
            .map(|addr| addr.ip())
            .collect();
        let second: Vec<IpAddr> = resolver
            .resolve("CONTEXT7.COM".parse().unwrap())
            .await
            .unwrap()
            .map(|addr| addr.ip())
            .collect();
        let third: Vec<IpAddr> = resolver
            .resolve("context7.com.".parse().unwrap())
            .await
            .unwrap()
            .map(|addr| addr.ip())
            .collect();

        assert_eq!(first, vec![ip1, ip2, ip3]);
        assert_eq!(second, vec![ip2, ip3, ip1]);
        assert_eq!(third, vec![ip3, ip1, ip2]);
    }
}
