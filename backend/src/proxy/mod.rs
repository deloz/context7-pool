use std::collections::HashSet;

use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, Response, StatusCode, Uri},
};
use bytes::Bytes;
use chrono::Utc;
use reqwest::Client;
use serde_json::Value;
use url::Url;

use crate::{
    error::{AppError, AppResult},
    models::ProxyKey,
    pool::Manager,
    settings,
    stats::{Event, Service as StatsService},
};

const MAX_RETRY_BODY_BYTES: usize = 16 << 20;
const MAX_FAILURE_BODY_BYTES: usize = 64 << 10;
const RELAY_PREFIX: &str = "/relay/context7";

#[derive(Clone)]
pub struct Service {
    manager: Manager,
    settings: settings::Service,
    stats: StatsService,
    upstream: Option<Url>,
    client: Client,
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

impl Service {
    pub fn new(
        manager: Manager,
        settings: settings::Service,
        stats: StatsService,
        upstream: Option<String>,
    ) -> AppResult<Self> {
        let upstream = upstream
            .filter(|value| !value.trim().is_empty())
            .map(|value| Url::parse(value.trim()))
            .transpose()
            .map_err(|_| AppError::BadRequest("parse upstream url".to_string()))?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(128)
            .build()
            .map_err(|err| AppError::Internal(format!("build proxy client: {err}")))?;
        Ok(Self {
            manager,
            settings,
            stats,
            upstream,
            client,
        })
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
        if relay_path != "/v2/libs/search" && relay_path != "/v2/context" {
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

        let mut excluded = HashSet::new();
        loop {
            let key = match self.manager.select_excluding(&excluded, Utc::now()).await {
                Ok(key) => key,
                Err(_) => {
                    let message = if excluded.is_empty() {
                        "no available api key"
                    } else {
                        "all upstream keys failed"
                    };
                    return Err(AppError::ServiceUnavailable(message.to_string()));
                }
            };
            excluded.insert(key.id);

            let target = resolve_target(&request)
                .map_err(|_| AppError::BadGateway("build upstream request failed".to_string()))?;
            let started_at = Utc::now();

            let upstream_request =
                match build_upstream_request(&request, &target, &key, body.clone()) {
                    Ok(request) => request,
                    Err(err) => {
                        let finished_at = Utc::now();
                        self.manager
                            .report_failure(key.id, 0, err.to_string(), finished_at)
                            .await;
                        self.record_attempt(
                            &stats_template,
                            &key,
                            0,
                            err.to_string(),
                            started_at,
                            finished_at,
                        );
                        continue;
                    }
                };

            let upstream_response = self.client.execute(upstream_request).await;
            let finished_at = Utc::now();
            let upstream_response = match upstream_response {
                Ok(response) => response,
                Err(err) => {
                    self.manager
                        .report_failure(key.id, 0, err.to_string(), finished_at)
                        .await;
                    self.record_attempt(
                        &stats_template,
                        &key,
                        0,
                        err.to_string(),
                        started_at,
                        finished_at,
                    );
                    continue;
                }
            };

            let status = upstream_response.status();
            if status.is_success() {
                let response = upstream_to_axum(upstream_response).await?;
                self.manager
                    .report_success(key.id, status.as_u16() as i32, finished_at)
                    .await;
                self.record_attempt(
                    &stats_template,
                    &key,
                    status.as_u16() as i32,
                    String::new(),
                    started_at,
                    finished_at,
                );
                return Ok(response);
            }

            let reason = failure_reason(upstream_response).await;
            self.manager
                .report_failure(key.id, status.as_u16() as i32, reason.clone(), finished_at)
                .await;
            self.record_attempt(
                &stats_template,
                &key,
                status.as_u16() as i32,
                reason,
                started_at,
                finished_at,
            );
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
    builder.headers_mut().remove("Authorization");
    builder
        .headers_mut()
        .insert("Authorization", format!("Bearer {}", key.api_key).parse()?);
    *builder.body_mut() = Some(reqwest::Body::from(body));
    Ok(builder)
}

async fn upstream_to_axum(response: reqwest::Response) -> AppResult<Response<Body>> {
    let status = response.status();
    let mut headers = clone_header(response.headers());
    remove_hop_by_hop_headers(&mut headers);
    let body = response
        .bytes()
        .await
        .map_err(|err| AppError::BadGateway(format!("read upstream response failed: {err}")))?;
    let mut builder = Response::builder().status(status);
    for (key, value) in headers.iter() {
        builder = builder.header(key, value);
    }
    builder
        .body(Body::from(body))
        .map_err(|err| AppError::Internal(format!("build response: {err}")))
}

async fn failure_reason(response: reqwest::Response) -> String {
    let status = response.status();
    let body = response.bytes().await.unwrap_or_default();
    let body = if body.len() > MAX_FAILURE_BODY_BYTES {
        &body[..MAX_FAILURE_BODY_BYTES]
    } else {
        &body
    };
    if let Some(message) = json_error_message(body) {
        return message;
    }
    status
        .canonical_reason()
        .map(ToString::to_string)
        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()))
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
