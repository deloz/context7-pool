use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{DateTime, Duration, Utc};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, RwLock};

use crate::{
    error::{AppError, AppResult},
    models::{
        AdminIdentity, AdminSessionRow, AdminUserRow, RelayTokenResponse, RelayTokenRow,
        RelayTokenView, TokenResponse,
    },
};

const ADMIN_SESSION_TTL: Duration = Duration::hours(24);
const ADMIN_SESSION_TOUCH_INTERVAL_SECONDS: i64 = 60;
const RELAY_TOKEN_TOUCH_INTERVAL_SECONDS: i64 = 60;
const DEFAULT_RELAY_TOKEN_NAME: &str = "context7-relay";
const ADMIN_TOKEN_PREFIX: &str = "cpa_";
const RELAY_TOKEN_PREFIX: &str = "cpr_";

#[derive(Clone)]
pub struct Service {
    pool: PgPool,
    relay_snapshot: Arc<RwLock<RelayTokenSnapshot>>,
    relay_touch_unix: Arc<AtomicI64>,
    admin_touch: Arc<Mutex<HashMap<i64, i64>>>,
}

#[derive(Clone, Debug, Default)]
struct RelayTokenSnapshot {
    id: i64,
    token_hash: String,
}

impl Service {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            relay_snapshot: Arc::new(RwLock::new(RelayTokenSnapshot::default())),
            relay_touch_unix: Arc::new(AtomicI64::new(0)),
            admin_touch: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn bootstrap(&self) -> AppResult<()> {
        self.refresh_relay_token_snapshot().await
    }

    pub async fn status(&self, token: Option<&str>) -> AppResult<crate::models::AuthStatus> {
        let setup_required = self.setup_required().await?;
        let mut authenticated = false;
        if let Some(token) = token.filter(|value| !value.trim().is_empty()) {
            match self.validate_admin_token(token).await {
                Ok(_) => authenticated = true,
                Err(AppError::Unauthorized) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(crate::models::AuthStatus {
            setup_required,
            authenticated,
        })
    }

    pub async fn setup(&self, username: &str, password: &str) -> AppResult<TokenResponse> {
        let username = normalize_username(username)?;
        validate_password(password)?;
        let password_hash = hash(password, DEFAULT_COST)?;
        let (token, token_hash) = generate_token(ADMIN_TOKEN_PREFIX)?;
        let now = Utc::now();
        let expires_at = now + ADMIN_SESSION_TTL;

        let mut tx = self.pool.begin().await?;
        sqlx::query!("LOCK TABLE admin_users IN EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await?;
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM admin_users")
            .fetch_one(&mut *tx)
            .await?
            .unwrap_or(0);
        if count > 0 {
            return Err(AppError::SetupAlreadyComplete);
        }

        let user_id = sqlx::query_scalar!(
            r#"
            INSERT INTO admin_users (username, password_hash, created_at, updated_at)
            VALUES ($1, $2, $3, $3)
            RETURNING id
            "#,
            username,
            password_hash,
            now
        )
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query!(
            r#"
            INSERT INTO admin_sessions (admin_user_id, token_hash, expires_at, created_at, last_used_at)
            VALUES ($1, $2, $3, $4, $4)
            "#,
            user_id,
            token_hash,
            expires_at,
            now
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(TokenResponse { token, expires_at })
    }

    pub async fn login(&self, username: &str, password: &str) -> AppResult<TokenResponse> {
        let username = username.trim();
        if username.is_empty() || password.is_empty() {
            return Err(AppError::InvalidCredentials);
        }

        let user = sqlx::query_as!(
            AdminUserRow,
            r#"SELECT id, username, password_hash FROM admin_users WHERE username = $1"#,
            username
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AppError::InvalidCredentials)?;

        if !verify(password, &user.password_hash)? {
            return Err(AppError::InvalidCredentials);
        }

        let (token, token_hash) = generate_token(ADMIN_TOKEN_PREFIX)?;
        let now = Utc::now();
        let expires_at = now + ADMIN_SESSION_TTL;
        sqlx::query!(
            r#"
            INSERT INTO admin_sessions (admin_user_id, token_hash, expires_at, created_at, last_used_at)
            VALUES ($1, $2, $3, $4, $4)
            "#,
            user.id,
            token_hash,
            expires_at,
            now
        )
        .execute(&self.pool)
        .await?;

        Ok(TokenResponse { token, expires_at })
    }

    pub async fn validate_admin_token(&self, token: &str) -> AppResult<AdminIdentity> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AppError::Unauthorized);
        }
        let token_hash = hash_token(token);
        let now = Utc::now();

        let row = sqlx::query(
            r#"
            SELECT s.id AS session_id,
                   s.admin_user_id,
                   s.expires_at,
                   s.last_used_at,
                   u.username
            FROM admin_sessions s
            JOIN admin_users u ON u.id = s.admin_user_id
            WHERE s.token_hash = $1 AND s.expires_at > $2
            LIMIT 1
            "#,
        )
        .bind(&token_hash)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AppError::Unauthorized)?;

        let session = AdminSessionRow {
            id: row.try_get("session_id")?,
            admin_user_id: row.try_get("admin_user_id")?,
            expires_at: row.try_get("expires_at")?,
            last_used_at: row.try_get("last_used_at")?,
        };
        let username: String = row.try_get("username")?;

        if self.should_touch_admin_session(&session, now).await {
            sqlx::query!(
                "UPDATE admin_sessions SET last_used_at = $2 WHERE id = $1",
                session.id,
                now
            )
            .execute(&self.pool)
            .await?;
        }

        Ok(AdminIdentity {
            admin_user_id: session.admin_user_id,
            session_id: session.id,
            username,
        })
    }

    pub async fn logout(&self, token: Option<&str>) -> AppResult<()> {
        let Some(token) = token.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(());
        };
        let token_hash = hash_token(token);
        sqlx::query!(
            "DELETE FROM admin_sessions WHERE token_hash = $1",
            token_hash
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn change_password(
        &self,
        identity: &AdminIdentity,
        old_password: &str,
        new_password: &str,
    ) -> AppResult<()> {
        validate_password(new_password)?;
        if old_password.trim().is_empty() {
            return Err(AppError::InvalidCredentials);
        }

        let mut tx = self.pool.begin().await?;
        let user = sqlx::query_as!(
            AdminUserRow,
            r#"SELECT id, username, password_hash FROM admin_users WHERE id = $1"#,
            identity.admin_user_id
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::Unauthorized)?;

        if !verify(old_password, &user.password_hash)? {
            return Err(AppError::InvalidCredentials);
        }

        let password_hash = hash(new_password, DEFAULT_COST)?;
        let now = Utc::now();
        sqlx::query!(
            "UPDATE admin_users SET password_hash = $2, updated_at = $3 WHERE id = $1",
            user.id,
            password_hash,
            now
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "DELETE FROM admin_sessions WHERE admin_user_id = $1",
            user.id
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn get_relay_token(&self) -> AppResult<RelayTokenView> {
        let Some(item) = self.current_relay_token().await? else {
            return Ok(RelayTokenView {
                configured: false,
                name: None,
                token: None,
                masked_token: None,
                created_at: None,
                last_used_at: None,
            });
        };

        Ok(RelayTokenView {
            configured: true,
            name: Some(item.name),
            token: item.token,
            masked_token: Some(item.masked_token),
            created_at: Some(item.created_at),
            last_used_at: item.last_used_at,
        })
    }

    pub async fn generate_relay_token(&self, name: &str) -> AppResult<RelayTokenResponse> {
        let name = name.trim();
        let name = if name.is_empty() {
            DEFAULT_RELAY_TOKEN_NAME
        } else {
            name
        };
        let (token, token_hash) = generate_token(RELAY_TOKEN_PREFIX)?;
        let masked_token = mask_token(&token);
        let now = Utc::now();

        let mut tx = self.pool.begin().await?;
        sqlx::query!(
            "UPDATE relay_tokens SET revoked_at = $1 WHERE revoked_at IS NULL",
            now
        )
        .execute(&mut *tx)
        .await?;
        let row = sqlx::query_as!(
            RelayTokenRow,
            r#"
            INSERT INTO relay_tokens (name, token_hash, token, masked_token, created_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, name, token_hash, token, masked_token, created_at, last_used_at
            "#,
            name,
            token_hash,
            token,
            masked_token,
            now
        )
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        *self.relay_snapshot.write().await = RelayTokenSnapshot {
            id: row.id,
            token_hash: row.token_hash,
        };
        self.relay_touch_unix.store(0, Ordering::SeqCst);

        Ok(RelayTokenResponse {
            token: row.token.unwrap_or_default(),
            masked_token: row.masked_token,
            created_at: row.created_at,
        })
    }

    pub async fn revoke_relay_token(&self) -> AppResult<()> {
        let now = Utc::now();
        sqlx::query!(
            "UPDATE relay_tokens SET revoked_at = $1 WHERE revoked_at IS NULL",
            now
        )
        .execute(&self.pool)
        .await?;
        *self.relay_snapshot.write().await = RelayTokenSnapshot::default();
        self.relay_touch_unix.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub async fn validate_relay_token(&self, token: &str) -> AppResult<()> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AppError::Unauthorized);
        }
        let snapshot = self.relay_snapshot.read().await.clone();
        if snapshot.id <= 0 || snapshot.token_hash.is_empty() {
            return Err(AppError::Unauthorized);
        }
        let incoming = hash_token(token);
        if incoming
            .as_bytes()
            .ct_eq(snapshot.token_hash.as_bytes())
            .unwrap_u8()
            != 1
        {
            return Err(AppError::Unauthorized);
        }

        let now = Utc::now();
        if self.should_touch_relay_token(now) {
            let result = sqlx::query!(
                "UPDATE relay_tokens SET last_used_at = $2 WHERE id = $1 AND revoked_at IS NULL",
                snapshot.id,
                now
            )
            .execute(&self.pool)
            .await?;
            if result.rows_affected() == 0 {
                *self.relay_snapshot.write().await = RelayTokenSnapshot::default();
                return Err(AppError::Unauthorized);
            }
        }
        Ok(())
    }

    async fn setup_required(&self) -> AppResult<bool> {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM admin_users")
            .fetch_one(&self.pool)
            .await?
            .unwrap_or(0);
        Ok(count == 0)
    }

    async fn current_relay_token(&self) -> AppResult<Option<RelayTokenRow>> {
        Ok(sqlx::query_as!(
            RelayTokenRow,
            r#"
            SELECT id, name, token_hash, token, masked_token, created_at, last_used_at
            FROM relay_tokens
            WHERE revoked_at IS NULL
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            "#
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn refresh_relay_token_snapshot(&self) -> AppResult<()> {
        if let Some(item) = self.current_relay_token().await? {
            *self.relay_snapshot.write().await = RelayTokenSnapshot {
                id: item.id,
                token_hash: item.token_hash,
            };
        } else {
            *self.relay_snapshot.write().await = RelayTokenSnapshot::default();
        }
        self.relay_touch_unix.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn should_touch_admin_session(
        &self,
        session: &AdminSessionRow,
        now: DateTime<Utc>,
    ) -> bool {
        if let Some(last_used_at) = session.last_used_at
            && (now - last_used_at).num_seconds() < ADMIN_SESSION_TOUCH_INTERVAL_SECONDS
        {
            return false;
        }

        let mut touches = self.admin_touch.lock().await;
        let previous = touches.entry(session.id).or_insert_with(|| {
            session
                .last_used_at
                .map(|value| value.timestamp())
                .unwrap_or(0)
        });
        let current = now.timestamp();
        if *previous != 0 && current - *previous < ADMIN_SESSION_TOUCH_INTERVAL_SECONDS {
            return false;
        }
        *previous = current;
        true
    }

    fn should_touch_relay_token(&self, now: DateTime<Utc>) -> bool {
        let previous = self.relay_touch_unix.load(Ordering::SeqCst);
        let current = now.timestamp();
        if previous != 0 && current - previous < RELAY_TOKEN_TOUCH_INTERVAL_SECONDS {
            return false;
        }
        self.relay_touch_unix
            .compare_exchange(previous, current, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
}

pub fn bearer_token(header: Option<&str>) -> Option<String> {
    let parts = header?.split_whitespace().collect::<Vec<_>>();
    if parts.len() == 2 && parts[0].eq_ignore_ascii_case("Bearer") && !parts[1].trim().is_empty() {
        Some(parts[1].to_string())
    } else {
        None
    }
}

fn normalize_username(raw: &str) -> AppResult<String> {
    let username = raw.trim();
    if username.is_empty() {
        return Err(AppError::BadRequest("username is required".to_string()));
    }
    Ok(username.to_string())
}

fn validate_password(password: &str) -> AppResult<()> {
    if password.chars().count() < 8 {
        return Err(AppError::BadRequest(
            "password must be at least 8 characters".to_string(),
        ));
    }
    Ok(())
}

fn generate_token(prefix: &str) -> AppResult<(String, String)> {
    let mut raw = [0_u8; 32];
    OsRng.fill_bytes(&mut raw);
    let token = format!("{prefix}{}", URL_SAFE_NO_PAD.encode(raw));
    let token_hash = hash_token(&token);
    Ok((token, token_hash))
}

fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    hex::encode(digest)
}

fn mask_token(token: &str) -> String {
    if token.len() <= 12 {
        "********".to_string()
    } else {
        format!("{}...{}", &token[..6], &token[token.len() - 4..])
    }
}
