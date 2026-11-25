pub mod resolver;

use std::{
    collections::{HashMap, HashSet},
    error::Error as StdError,
    net::IpAddr,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, Response, StatusCode, Uri},
};
use bytes::Bytes;
use chrono::Utc;
use reqwest::Client;
use serde_json::Value;
use tokio::time::Instant;
use url::Url;

use crate::{
    error::{AppError, AppResult},
    models::ProxyKey,
    pool::Manager,
    proxy::resolver::UpstreamResolver,
    settings,
    stats::{Event, Service as StatsService},
};

const MAX_RETRY_BODY_BYTES: usize = 16 << 20;
const MAX_FAILURE_BODY_BYTES: usize = 64 << 10;
const RELAY_PREFIX: &str = "/relay/context7";
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(4);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_TOTAL_TIMEOUT: Duration = Duration::from_secs(25);
const DEFAULT_MAX_ATTEMPTS: usize = 3;

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub total_timeout: Duration,
    pub max_attempts: usize,
    pub proxy_url: Option<String>,
    pub resolve_addrs: HashMap<String, Vec<IpAddr>>,
    pub discovery_domains: HashMap<String, Vec<String>>,
    pub probe_interval: Option<Duration>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            total_timeout: DEFAULT_TOTAL_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            proxy_url: None,
            resolve_addrs: HashMap::new(),
            discovery_domains: HashMap::new(),
            probe_interval: None,
        }
    }
}

#[derive(Clone)]
pub struct Service {
    manager: Manager,
    settings: settings::Service,
    stats: StatsService,
    upstream: Option<Url>,
    client: Client,
    resolver: Option<Arc<UpstreamResolver>>,
    request_timeout: Duration,
    total_timeout: Duration,
    max_attempts: usize,
}

#[derive(Clone)]
struct StatsTemplate {
    method: String,
    path: String,
    query: String,
    client_ip: String,
    user_agent: String,
    client_source: String,
    client_ide: String,
    client_version: String,
    transport: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamDisposition {
    Success,
    Passthrough,
    RetryPenalizeKey,
    RetryTransient,
}

impl Service {
    pub fn new(
        manager: Manager,
        settings: settings::Service,
        stats: StatsService,
        upstream: Option<String>,
    ) -> AppResult<Self> {
        Self::with_config(manager, settings, stats, upstream, ProxyConfig::default())
    }

    pub fn with_config(
        manager: Manager,
        settings: settings::Service,
        stats: StatsService,
        upstream: Option<String>,
        cfg: ProxyConfig,
    ) -> AppResult<Self> {
        let upstream = upstream
            .filter(|value| !value.trim().is_empty())
            .map(|value| Url::parse(value.trim()))
            .transpose()
            .map_err(|_| AppError::BadRequest("parse upstream url".to_string()))?;

        let connect_timeout = non_zero_duration(cfg.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        let request_timeout = non_zero_duration(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        let total_timeout = non_zero_duration(cfg.total_timeout, DEFAULT_TOTAL_TIMEOUT);
        let max_attempts = cfg.max_attempts.max(1);

        let resolver = Arc::new(UpstreamResolver::new(
            cfg.resolve_addrs,
            cfg.discovery_domains,
            cfg.probe_interval,
        ));
        let active_resolver = resolver.is_active().then_some(resolver);

        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(128)
            .pool_idle_timeout(Duration::from_secs(60))
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .tcp_user_timeout(Some(request_timeout))
            .http2_keep_alive_interval(Some(Duration::from_secs(20)))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true);

        if let Some(resolver) = active_resolver.as_ref() {
            builder = builder.dns_resolver(resolver.clone());
        }
        if let Some(proxy_url) = cfg
            .proxy_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let proxy = reqwest::Proxy::all(proxy_url)
                .map_err(|err| AppError::BadRequest(format!("parse upstream proxy: {err}")))?;
            builder = builder.proxy(proxy);
        }

        let client = builder
            .build()
            .map_err(|err| AppError::Internal(format!("build proxy client: {err}")))?;

        Ok(Self {
            manager,
            settings,
            stats,
            upstream,
            client,
            resolver: active_resolver,
            request_timeout,
            total_timeout,
            max_attempts,
        })
    }

    pub fn resolver(&self) -> Option<Arc<UpstreamResolver>> {
        self.resolver.clone()
    }

    pub async fn handle_context7(
        &self,
        request: Request<Body>,
        client_ip: String,
    ) -> AppResult<Response<Body>> {
        let target = self
            .settings
            .current_context7_target()
            .await
            .ok_or_else(|| {
                AppError::ServiceUnavailable("context7 relay is not configured".to_string())
            })?;

        let relay_path = request
            .uri()
            .path()
            .strip_prefix(RELAY_PREFIX)
            .unwrap_or(request.uri().path())
            .to_string();
        if !is_allowed_context7_path(&relay_path) {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap());
        }

        let template = stats_template(&request, client_ip);
        self.serve_proxy_request(
            request,
            move |req| {
                let mut resolved = target.clone();
                resolved.set_path(&join_url_path(target.path(), &relay_path));
                resolved.set_query(join_raw_query(target.query(), req.uri().query()).as_deref());
                Ok(resolved)
            },
            Some(template),
        )
        .await
    }

    pub async fn handle_generic(&self, request: Request<Body>) -> AppResult<Response<Body>> {
        let upstream = self.upstream.clone().ok_or_else(|| {
            AppError::ServiceUnavailable("upstream is not configured".to_string())
        })?;
        self.serve_proxy_request(
            request,
            move |req| {
                let mut target = upstream.clone();
                target.set_path(&join_url_path(upstream.path(), req.uri().path()));
                target.set_query(join_raw_query(upstream.query(), req.uri().query()).as_deref());
                Ok(target)
            },
            None,
        )
        .await
    }

    async fn serve_proxy_request<F>(
        &self,
        request: Request<Body>,
        resolve_target: F,
        stats_template: Option<StatsTemplate>,
    ) -> AppResult<Response<Body>>
    where
        F: Fn(&Request<Body>) -> AppResult<Url>,
    {
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, MAX_RETRY_BODY_BYTES + 1)
            .await
            .map_err(|_| AppError::BadRequest("read request body failed".to_string()))?;
        if body.len() > MAX_RETRY_BODY_BYTES {
            return Err(AppError::PayloadTooLarge);
        }
        let request = Request::from_parts(parts, Body::empty());

        let deadline = Instant::now() + self.total_timeout;
        let mut penalized_keys = HashSet::new();
        let mut tried_keys = HashSet::new();
        let mut attempts = 0usize;
        let mut last_timed_out = false;

        loop {
            if attempts >= self.max_attempts {
                break;
            }
            let now_instant = Instant::now();
            if now_instant >= deadline {
                last_timed_out = true;
                break;
            }
            let remaining = deadline.saturating_duration_since(now_instant);
            let attempt_timeout = remaining.min(self.request_timeout);

            let key = match self.manager.select_excluding(&tried_keys, Utc::now()).await {
                Ok(key) => key,
                Err(_) if penalized_keys.len() < tried_keys.len() => {
                    // All remaining keys were tried on transient errors; allow
                    // retrying healthy keys (e.g. single-key pool across multi-IPs).
                    tried_keys = penalized_keys.clone();
                    match self.manager.select_excluding(&tried_keys, Utc::now()).await {
                        Ok(key) => key,
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            };
            tried_keys.insert(key.id);
            attempts += 1;

            let target = resolve_target(&request)
                .map_err(|_| AppError::BadGateway("build upstream request failed".to_string()))?;
            let started_at = Utc::now();

            let upstream_request =
                match build_upstream_request(&request, &target, &key, body.clone()) {
                    Ok(request) => request,
                    Err(err) => {
                        let finished_at = Utc::now();
                        let reason = err.to_string();
                        self.record_attempt(
                            &stats_template,
                            &key,
                            0,
                            reason.clone(),
                            started_at,
                            finished_at,
                        );
                        tracing::warn!(
                            attempt = attempts,
                            api_key_id = key.id,
                            error = %reason,
                            "build upstream request failed"
                        );
                        last_timed_out = false;
                        continue;
                    }
                };

            let upstream_response =
                match tokio::time::timeout(attempt_timeout, self.client.execute(upstream_request))
                    .await
                {
                    Ok(Ok(response)) => response,
                    Ok(Err(err)) => {
                        let finished_at = Utc::now();
                        let latency_ms = (finished_at - started_at).num_milliseconds().max(0);
                        let timed_out = err.is_timeout();
                        let reason = format_reqwest_error(&err);
                        self.record_attempt(
                            &stats_template,
                            &key,
                            0,
                            reason.clone(),
                            started_at,
                            finished_at,
                        );
                        tracing::warn!(
                            attempt = attempts,
                            api_key_id = key.id,
                            latency_ms,
                            timed_out,
                            error = %reason,
                            "upstream transport failed"
                        );
                        last_timed_out = timed_out;
                        continue;
                    }
                    Err(_) => {
                        let finished_at = Utc::now();
                        let latency_ms = (finished_at - started_at).num_milliseconds().max(0);
                        let reason = format!(
                            "upstream attempt timed out after {}ms",
                            attempt_timeout.as_millis()
                        );
                        self.record_attempt(
                            &stats_template,
                            &key,
                            0,
                            reason.clone(),
                            started_at,
                            finished_at,
                        );
                        tracing::warn!(
                            attempt = attempts,
                            api_key_id = key.id,
                            latency_ms,
                            error = %reason,
                            "upstream attempt deadline exceeded"
                        );
                        last_timed_out = true;
                        continue;
                    }
                };

            let status = upstream_response.status();
            let status_code = status.as_u16() as i32;
            match classify_upstream_status(status) {
                UpstreamDisposition::Success => {
                    let read_budget = deadline
                        .saturating_duration_since(Instant::now())
                        .min(self.request_timeout);
                    match read_upstream_response(upstream_response, read_budget).await {
                        Ok(response) => {
                            let finished_at = Utc::now();
                            self.manager
                                .report_success(key.id, status_code, finished_at)
                                .await;
                            self.record_attempt(
                                &stats_template,
                                &key,
                                status_code,
                                String::new(),
                                started_at,
                                finished_at,
                            );
                            return Ok(response);
                        }
                        Err(err) => {
                            let finished_at = Utc::now();
                            let latency_ms = (finished_at - started_at).num_milliseconds().max(0);
                            last_timed_out = err.timed_out;
                            self.record_attempt(
                                &stats_template,
                                &key,
                                0,
                                err.message.clone(),
                                started_at,
                                finished_at,
                            );
                            tracing::warn!(
                                attempt = attempts,
                                api_key_id = key.id,
                                status = status_code,
                                latency_ms,
                                error = %err.message,
                                "read upstream response body failed"
                            );
                            continue;
                        }
                    }
                }
                UpstreamDisposition::Passthrough => {
                    let read_budget = deadline
                        .saturating_duration_since(Instant::now())
                        .min(self.request_timeout);
                    let headers = filtered_response_headers(upstream_response.headers());
                    let body_bytes =
                        match tokio::time::timeout(read_budget, upstream_response.bytes()).await {
                            Ok(Ok(bytes)) => bytes,
                            Ok(Err(err)) => {
                                let finished_at = Utc::now();
                                let reason = format_reqwest_error(&err);
                                last_timed_out = err.is_timeout();
                                self.record_attempt(
                                    &stats_template,
                                    &key,
                                    0,
                                    reason,
                                    started_at,
                                    finished_at,
                                );
                                continue;
                            }
                            Err(_) => {
                                let finished_at = Utc::now();
                                last_timed_out = true;
                                self.record_attempt(
                                    &stats_template,
                                    &key,
                                    0,
                                    "upstream response body timed out".to_string(),
                                    started_at,
                                    finished_at,
                                );
                                continue;
                            }
                        };
                    let finished_at = Utc::now();
                    let reason = failure_reason_from_bytes(status, &body_bytes);
                    self.record_attempt(
                        &stats_template,
                        &key,
                        status_code,
                        reason,
                        started_at,
                        finished_at,
                    );
                    return build_axum_response(status, &headers, body_bytes);
                }
                UpstreamDisposition::RetryPenalizeKey => {
                    let reason = failure_reason(upstream_response).await;
                    let finished_at = Utc::now();
                    let latency_ms = (finished_at - started_at).num_milliseconds().max(0);
                    penalized_keys.insert(key.id);
                    self.manager
                        .report_failure(key.id, status_code, reason.clone(), finished_at)
                        .await;
                    self.record_attempt(
                        &stats_template,
                        &key,
                        status_code,
                        reason.clone(),
                        started_at,
                        finished_at,
                    );
                    tracing::warn!(
                        attempt = attempts,
                        api_key_id = key.id,
                        status = status_code,
                        latency_ms,
                        error = %reason,
                        "upstream rejected api key, rotating"
                    );
                    last_timed_out = false;
                }
                UpstreamDisposition::RetryTransient => {
                    let reason = failure_reason(upstream_response).await;
                    let finished_at = Utc::now();
                    let latency_ms = (finished_at - started_at).num_milliseconds().max(0);
                    self.record_attempt(
                        &stats_template,
                        &key,
                        status_code,
                        reason.clone(),
                        started_at,
                        finished_at,
                    );
                    tracing::warn!(
                        attempt = attempts,
                        api_key_id = key.id,
                        status = status_code,
                        latency_ms,
                        error = %reason,
                        "upstream transient status, retrying"
                    );
                    last_timed_out = status == StatusCode::REQUEST_TIMEOUT
                        || status == StatusCode::GATEWAY_TIMEOUT;
                }
            }
        }

        if attempts == 0 {
            Err(AppError::ServiceUnavailable(
                "no available api key".to_string(),
            ))
        } else if last_timed_out {
            Err(AppError::GatewayTimeout(
                "upstream request timed out".to_string(),
            ))
        } else {
            Err(AppError::ServiceUnavailable(
                "all upstream keys failed".to_string(),
            ))
        }
    }

    fn record_attempt(
        &self,
        template: &Option<StatsTemplate>,
        key: &ProxyKey,
        status_code: i32,
        reason: String,
        started_at: chrono::DateTime<Utc>,
        finished_at: chrono::DateTime<Utc>,
    ) {
        let Some(template) = template else {
            return;
        };
        let latency_ms = (finished_at - started_at).num_milliseconds().max(0);
        self.stats.record(Event {
            api_key_id: key.id,
            api_key_name: key.name.clone(),
            method: template.method.clone(),
            path: template.path.clone(),
            query: template.query.clone(),
            status_code,
            success: (200..300).contains(&status_code),
            latency_ms,
            error: reason,
            client_ip: template.client_ip.clone(),
            user_agent: template.user_agent.clone(),
            client_source: template.client_source.clone(),
            client_ide: template.client_ide.clone(),
            client_version: template.client_version.clone(),
            transport: template.transport.clone(),
            started_at,
            finished_at,
        });
    }
}

fn stats_template(request: &Request<Body>, client_ip: String) -> StatsTemplate {
    let headers = request.headers();
    StatsTemplate {
        method: request.method().to_string(),
        path: request.uri().path().to_string(),
        query: request.uri().query().unwrap_or("").to_string(),
        client_ip,
        user_agent: header_value(headers, "User-Agent"),
        client_source: header_value(headers, "X-Context7-Source"),
        client_ide: header_value(headers, "X-Context7-Client-IDE"),
        client_version: header_value(headers, "X-Context7-Client-Version"),
        transport: header_value(headers, "X-Context7-Transport"),
    }
}

fn non_zero_duration(value: Duration, fallback: Duration) -> Duration {
    if value.is_zero() { fallback } else { value }
}

fn is_allowed_context7_path(relay_path: &str) -> bool {
    let Some(rest) = relay_path.strip_prefix("/v2/") else {
        return false;
    };
    if rest.is_empty() || rest.contains('\\') {
        return false;
    }
    rest.split('/')
        .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn classify_upstream_status(status: StatusCode) -> UpstreamDisposition {
    if status.is_success() {
        return UpstreamDisposition::Success;
    }
    match status.as_u16() {
        401 | 402 | 403 | 429 => UpstreamDisposition::RetryPenalizeKey,
        408 | 500..=599 => UpstreamDisposition::RetryTransient,
        _ => UpstreamDisposition::Passthrough,
    }
}

struct BodyReadError {
    message: String,
    timed_out: bool,
}

async fn read_upstream_response(
    response: reqwest::Response,
    timeout: Duration,
) -> Result<Response<Body>, BodyReadError> {
    let status = response.status();
    let headers = filtered_response_headers(response.headers());
    let body = match tokio::time::timeout(timeout, response.bytes()).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            return Err(BodyReadError {
                message: format_reqwest_error(&err),
                timed_out: err.is_timeout(),
            });
        }
        Err(_) => {
            return Err(BodyReadError {
                message: format!(
                    "read upstream response body timed out after {}ms",
                    timeout.as_millis()
                ),
                timed_out: true,
            });
        }
    };
    build_axum_response(status, &headers, body).map_err(|err| BodyReadError {
        message: err.to_string(),
        timed_out: false,
    })
}

fn filtered_response_headers(source: &HeaderMap) -> HeaderMap {
    let mut headers = clone_header(source);
    remove_hop_by_hop_headers(&mut headers);
    headers
}

fn build_axum_response(
    status: StatusCode,
    headers: &HeaderMap,
    body: Bytes,
) -> AppResult<Response<Body>> {
    let mut builder = Response::builder().status(status);
    for (key, value) in headers.iter() {
        builder = builder.header(key, value);
    }
    builder
        .body(Body::from(body))
        .map_err(|err| AppError::Internal(format!("build response: {err}")))
}

fn build_upstream_request(
    inbound: &Request<Body>,
    target: &Url,
    key: &ProxyKey,
    body: Bytes,
) -> anyhow::Result<reqwest::Request> {
    let method = reqwest::Method::from_bytes(inbound.method().as_str().as_bytes())?;
    let mut builder = reqwest::Request::new(method, target.as_str().parse()?);
    *builder.headers_mut() = clone_header(inbound.headers());
    remove_hop_by_hop_headers(builder.headers_mut());
    remove_inbound_proxy_headers(builder.headers_mut());
    builder.headers_mut().remove("Authorization");
    builder
        .headers_mut()
        .insert("Authorization", format!("Bearer {}", key.api_key).parse()?);
    *builder.body_mut() = Some(reqwest::Body::from(body));
    Ok(builder)
}

async fn failure_reason(response: reqwest::Response) -> String {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    failure_reason_from_bytes(status, &body)
}

fn failure_reason_from_bytes(status: StatusCode, body: &[u8]) -> String {
    let body = if body.len() > MAX_FAILURE_BODY_BYTES {
        &body[..MAX_FAILURE_BODY_BYTES]
    } else {
        body
    };
    if let Some(message) = json_error_message(body) {
        return message;
    }
    status
        .canonical_reason()
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()))
}

fn format_reqwest_error(err: &reqwest::Error) -> String {
    let category = if err.is_timeout() {
        "upstream timeout"
    } else if err.is_connect() {
        "upstream connect error"
    } else if err.is_request() {
        "upstream request error"
    } else if err.is_body() || err.is_decode() {
        "upstream body error"
    } else {
        "upstream transport error"
    };

    let mut details = Vec::new();
    let mut source: Option<&(dyn StdError + 'static)> = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !text.is_empty() && !details.iter().any(|existing| existing == &text) {
            details.push(text);
        }
        source = cause.source();
    }

    if details.is_empty() {
        format!("{category}: {err}")
    } else {
        format!("{category}: {}", details.join(": "))
    }
}

fn json_error_message(body: &[u8]) -> Option<String> {
    let payload = serde_json::from_slice::<Value>(body).ok()?;
    for key in ["error", "message"] {
        if let Some(message) = payload_message(payload.get(key)?) {
            return Some(message);
        }
    }
    None
}

fn payload_message(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Object(map) => {
            for key in ["message", "error"] {
                if let Some(message) = map.get(key).and_then(payload_message) {
                    return Some(message);
                }
            }
            None
        }
        _ => None,
    }
}

fn join_url_path(base_path: &str, relay_path: &str) -> String {
    let base = base_path.trim_end_matches('/');
    if relay_path.is_empty() {
        return if base.is_empty() {
            "/".to_string()
        } else {
            base.to_string()
        };
    }
    let relay_path = if relay_path.starts_with('/') {
        relay_path.to_string()
    } else {
        format!("/{relay_path}")
    };
    if base.is_empty() {
        relay_path
    } else {
        format!("{base}{relay_path}")
    }
}

fn join_raw_query(base_query: Option<&str>, request_query: Option<&str>) -> Option<String> {
    match (
        base_query.filter(|value| !value.is_empty()),
        request_query.filter(|value| !value.is_empty()),
    ) {
        (None, None) => None,
        (Some(base), None) => Some(base.to_string()),
        (None, Some(request)) => Some(request.to_string()),
        (Some(base), Some(request)) => Some(format!("{base}&{request}")),
    }
}

fn clone_header(source: &HeaderMap) -> HeaderMap {
    let mut target = HeaderMap::new();
    for (key, value) in source {
        target.append(key, value.clone());
    }
    target
}

fn remove_hop_by_hop_headers(headers: &mut HeaderMap) {
    let connection_values = headers
        .get_all("Connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(',').map(str::trim).collect::<Vec<_>>())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for value in connection_values {
        headers.remove(value.as_str());
    }
    for key in [
        "Connection",
        "Proxy-Connection",
        "Keep-Alive",
        "Proxy-Authenticate",
        "Proxy-Authorization",
        "Te",
        "Trailer",
        "Transfer-Encoding",
        "Upgrade",
    ] {
        headers.remove(key);
    }
}

fn remove_inbound_proxy_headers(headers: &mut HeaderMap) {
    for key in [
        "Host",
        "Content-Length",
        "Forwarded",
        "X-Forwarded-For",
        "X-Forwarded-Host",
        "X-Forwarded-Proto",
        "X-Forwarded-Port",
        "X-Real-Ip",
        "Remote-Host",
    ] {
        headers.remove(key);
    }
}

fn header_value(headers: &HeaderMap, key: &str) -> String {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string()
}

pub fn context7_client_ip(headers: &HeaderMap, fallback: Option<String>) -> String {
    header_value(headers, "Mcp-Client-Ip")
        .trim()
        .to_string()
        .if_empty_else(|| fallback.unwrap_or_default())
}

trait EmptyStringExt {
    fn if_empty_else<F: FnOnce() -> String>(self, fallback: F) -> String;
}

impl EmptyStringExt for String {
    fn if_empty_else<F: FnOnce() -> String>(self, fallback: F) -> String {
        if self.trim().is_empty() {
            fallback()
        } else {
            self
        }
    }
}

#[allow(dead_code)]
fn _keep_imports(_: (&Method, &Uri)) {}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::get,
    };
    use sea_orm::DatabaseConnection;
    use tokio::net::TcpListener;

    use super::*;
    use crate::pool::{Config as PoolConfig, Record};

    fn make_key(id: i64) -> Record {
        let now = Utc::now();
        Record {
            id,
            name: format!("key-{id}"),
            api_key: format!("ctx7sk-test-{id}"),
            enabled: true,
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

    async fn setup_proxy_service(
        base_url: &str,
        key_count: i64,
        cfg: ProxyConfig,
    ) -> (Service, Manager) {
        let db = DatabaseConnection::default();
        let manager = Manager::new(PoolConfig {
            failure_threshold: 2,
            cooldown: Duration::from_secs(30),
            flush_interval: Duration::from_secs(60),
            flush_batch_size: 64,
        });
        let records = (1..=key_count).map(make_key).collect();
        manager.load(records, Utc::now()).await;

        let settings = settings::Service::new(db.clone());
        settings.set_test_target(base_url).await;
        let stats = StatsService::new(
            db,
            crate::stats::Config {
                flush_interval: Duration::from_secs(60),
                batch_size: 64,
                queue_size: 128,
            },
        );
        let service = Service::with_config(manager.clone(), settings, stats, None, cfg).unwrap();
        (service, manager)
    }

    async fn spawn_mock_server(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}/api")
    }

    #[test]
    fn context7_path_validation_allows_v2_and_blocks_traversal_and_v1() {
        assert!(is_allowed_context7_path("/v2/libs/search"));
        assert!(is_allowed_context7_path("/v2/context"));
        assert!(is_allowed_context7_path("/v2/mcp/experimental-tools"));
        assert!(!is_allowed_context7_path("/v2/"));
        assert!(!is_allowed_context7_path("/v2"));
        assert!(!is_allowed_context7_path("/v2/../admin"));
        assert!(!is_allowed_context7_path("/v2//context"));
        assert!(!is_allowed_context7_path("/v1/search"));
        assert!(!is_allowed_context7_path("/"));
    }

    #[tokio::test]
    async fn passthrough_301_and_404_without_retries_or_key_penalties() {
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/api/v2/context",
                get({
                    let calls = calls.clone();
                    move || {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::MOVED_PERMANENTLY,
                                [(axum::http::header::CONTENT_TYPE, "application/json")],
                                r#"{"error":"library_redirected","redirectUrl":"/new/lib"}"#,
                            )
                        }
                    }
                }),
            )
            .route(
                "/api/v2/libs/search",
                get({
                    let calls = calls.clone();
                    move || {
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            (
                                StatusCode::NOT_FOUND,
                                [(axum::http::header::CONTENT_TYPE, "application/json")],
                                r#"{"error":"Library not found"}"#,
                            )
                        }
                    }
                }),
            );
        let base_url = spawn_mock_server(app).await;
        let (service, manager) = setup_proxy_service(&base_url, 3, ProxyConfig::default()).await;

        let req_301 = Request::builder()
            .method(Method::GET)
            .uri("/relay/context7/v2/context?libraryId=/old/id&query=test")
            .body(Body::empty())
            .unwrap();
        let resp_301 = service
            .handle_context7(req_301, "1.2.3.4".to_string())
            .await
            .unwrap();
        assert_eq!(resp_301.status(), StatusCode::MOVED_PERMANENTLY);
        let body_301 = to_bytes(resp_301.into_body(), 1024).await.unwrap();
        assert!(String::from_utf8_lossy(&body_301).contains("library_redirected"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let req_404 = Request::builder()
            .method(Method::GET)
            .uri("/relay/context7/v2/libs/search?libraryName=nope&query=x")
            .body(Body::empty())
            .unwrap();
        let resp_404 = service
            .handle_context7(req_404, "1.2.3.4".to_string())
            .await
            .unwrap();
        assert_eq!(resp_404.status(), StatusCode::NOT_FOUND);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        for id in 1..=3 {
            let key = manager.overlay(make_key(id), Utc::now()).await;
            assert_eq!(key.health_status, "healthy");
            assert_eq!(key.failure_streak, 0);
        }
    }

    #[tokio::test]
    async fn retries_429_with_penalty_and_502_without_penalty_and_strips_inbound_headers() {
        #[derive(Clone)]
        struct MockState {
            attempts: Arc<AtomicUsize>,
        }

        async fn handler(
            State(state): State<MockState>,
            headers: HeaderMap,
        ) -> (StatusCode, String) {
            assert!(headers.get("x-forwarded-for").is_none());
            assert!(headers.get("remote-host").is_none());
            let host = headers
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            assert_ne!(host, "context7.beihemote.com");

            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let n = state.attempts.fetch_add(1, Ordering::SeqCst);
            match n {
                0 => (
                    StatusCode::BAD_GATEWAY,
                    r#"{"error":"bad gateway"}"#.to_string(),
                ),
                1 => (
                    StatusCode::TOO_MANY_REQUESTS,
                    r#"{"error":"rate limited"}"#.to_string(),
                ),
                _ => (StatusCode::OK, format!(r#"{{"auth":"{auth}"}}"#)),
            }
        }

        let state = MockState {
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route("/api/v2/mcp/experimental-tools", get(handler))
            .with_state(state.clone());
        let base_url = spawn_mock_server(app).await;
        let (service, manager) = setup_proxy_service(&base_url, 3, ProxyConfig::default()).await;

        let req = Request::builder()
            .method(Method::GET)
            .uri("/relay/context7/v2/mcp/experimental-tools")
            .header("Host", "context7.beihemote.com")
            .header("X-Forwarded-For", "1.2.3.4")
            .header("REMOTE-HOST", "1.2.3.4")
            .body(Body::empty())
            .unwrap();
        let resp = service
            .handle_context7(req, "1.2.3.4".to_string())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.attempts.load(Ordering::SeqCst), 3);

        let key1 = manager.overlay(make_key(1), Utc::now()).await;
        let key2 = manager.overlay(make_key(2), Utc::now()).await;
        let key3 = manager.overlay(make_key(3), Utc::now()).await;
        assert_eq!(key1.failure_streak, 0, "502 should not penalize key-1");
        assert_eq!(key2.failure_streak, 1, "429 should penalize key-2");
        assert_eq!(key3.health_status, "healthy");
    }

    #[tokio::test]
    async fn slow_upstream_returns_504_within_total_timeout_budget() {
        let app = Router::new().route(
            "/api/v2/context",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                (StatusCode::OK, "too late")
            }),
        );
        let base_url = spawn_mock_server(app).await;
        let cfg = ProxyConfig {
            connect_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_millis(80),
            total_timeout: Duration::from_millis(200),
            max_attempts: 3,
            ..ProxyConfig::default()
        };
        let (service, manager) = setup_proxy_service(&base_url, 1, cfg).await;

        let req = Request::builder()
            .method(Method::GET)
            .uri("/relay/context7/v2/context?libraryId=/a/b&query=c")
            .body(Body::empty())
            .unwrap();
        let err = service
            .handle_context7(req, "1.2.3.4".to_string())
            .await
            .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::GATEWAY_TIMEOUT);

        let key1 = manager.overlay(make_key(1), Utc::now()).await;
        assert_eq!(key1.health_status, "healthy");
        assert_eq!(key1.failure_streak, 0);
    }
}
