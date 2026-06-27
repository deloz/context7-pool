use std::{collections::HashMap, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{DateTime, Duration, Utc};
use rand::{RngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;

use crate::{
    error::{AppError, AppResult},
    models::{
        AdminIdentity, AdminSessionRow, AdminUserRow, RelayTokenItem, RelayTokenPage,
        RelayTokenResponse, RelayTokenRow, RelayTokenView, TokenResponse,
    },
};

const ADMIN_SESSION_TTL: Duration = Duration::hours(24);
const ADMIN_SESSION_TOUCH_INTERVAL_SECONDS: i64 = 60;
const DEFAULT_RELAY_TOKEN_NAME: &str = "context7-relay";
const ADMIN_TOKEN_PREFIX: &str = "cpa_";
const RELAY_TOKEN_PREFIX: &str = "cpr_";

#[derive(Clone)]
pub struct Service {
    pool: PgPool,
    admin_touch: Arc<Mutex<HashMap<i64, i64>>>,
}

impl Service {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            admin_touch: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn bootstrap(&self) -> AppResult<()> {
        Ok(())
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
                id: None,
                name: None,
                token: None,
                masked_token: None,
                created_at: None,
                last_used_at: None,
            });
        };

        Ok(RelayTokenView {
            configured: true,
            id: Some(item.id),
            name: Some(item.name),
            token: item.token,
            masked_token: Some(item.masked_token),
            created_at: Some(item.created_at),
            last_used_at: item.last_used_at,
        })
    }

    pub async fn list_relay_tokens(&self, page: i64, page_size: i64) -> AppResult<RelayTokenPage> {
        let total =
            sqlx::query_scalar!("SELECT COUNT(*) FROM relay_tokens WHERE revoked_at IS NULL")
                .fetch_one(&self.pool)
                .await?
                .unwrap_or(0);
        let offset = page
            .checked_sub(1)
            .and_then(|value| value.checked_mul(page_size))
            .ok_or_else(|| AppError::BadRequest("page is too large".to_string()))?;
        let rows = sqlx::query_as!(
            RelayTokenRow,
            r#"
            SELECT id, name, token_hash, token, masked_token, created_at, last_used_at
            FROM relay_tokens
            WHERE revoked_at IS NULL
            ORDER BY created_at DESC, id DESC
            LIMIT $1 OFFSET $2
            "#,
            page_size,
            offset
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(RelayTokenPage {
            items: rows.into_iter().map(relay_token_item).collect(),
            total,
            page,
            page_size,
        })
    }

    pub async fn generate_relay_token(&self, name: &str) -> AppResult<RelayTokenResponse> {
        self.create_relay_token(name).await
    }

    pub async fn create_relay_token(&self, name: &str) -> AppResult<RelayTokenResponse> {
        let name = normalize_relay_token_name(name);
        let (token, token_hash) = generate_token(RELAY_TOKEN_PREFIX)?;
        let masked_token = mask_token(&token);
        let now = Utc::now();

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
        .fetch_one(&self.pool)
        .await?;

        Ok(relay_token_response(row))
    }

    pub async fn update_relay_token(&self, id: i64, name: &str) -> AppResult<RelayTokenItem> {
        let name = normalize_relay_token_name(name);
        let row = sqlx::query_as!(
            RelayTokenRow,
            r#"
            UPDATE relay_tokens
            SET name = $2
            WHERE id = $1 AND revoked_at IS NULL
            RETURNING id, name, token_hash, token, masked_token, created_at, last_used_at
            "#,
            id,
            name
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AppError::NotFound)?;

        Ok(relay_token_item(row))
    }

    pub async fn rotate_relay_token(&self, id: i64) -> AppResult<RelayTokenResponse> {
        let mut tx = self.pool.begin().await?;
        let current = sqlx::query_as!(
            RelayTokenRow,
            r#"
            SELECT id, name, token_hash, token, masked_token, created_at, last_used_at
            FROM relay_tokens
            WHERE id = $1 AND revoked_at IS NULL
            "#,
            id
        )
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::NotFound)?;

        let now = Utc::now();
        let result = sqlx::query!(
            "UPDATE relay_tokens SET revoked_at = $2 WHERE id = $1 AND revoked_at IS NULL",
            id,
            now
        )
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AppError::NotFound);
        }

        let (token, token_hash) = generate_token(RELAY_TOKEN_PREFIX)?;
        let masked_token = mask_token(&token);
        let row = sqlx::query_as!(
            RelayTokenRow,
            r#"
            INSERT INTO relay_tokens (name, token_hash, token, masked_token, created_at)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, name, token_hash, token, masked_token, created_at, last_used_at
            "#,
            current.name,
            token_hash,
            token,
            masked_token,
            now
        )
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(relay_token_response(row))
    }

    pub async fn delete_relay_token(&self, id: i64) -> AppResult<()> {
        let now = Utc::now();
        let result = sqlx::query!(
            "UPDATE relay_tokens SET revoked_at = $2 WHERE id = $1 AND revoked_at IS NULL",
            id,
            now
        )
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AppError::NotFound);
        }
        Ok(())
    }

    pub async fn revoke_relay_token(&self) -> AppResult<()> {
        let now = Utc::now();
        sqlx::query!(
            r#"
            UPDATE relay_tokens
            SET revoked_at = $1
            WHERE id = (
                SELECT id
                FROM relay_tokens
                WHERE revoked_at IS NULL
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            )
            "#,
            now
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn validate_relay_token(&self, token: &str) -> AppResult<()> {
        let token = token.trim();
        if token.is_empty() {
            return Err(AppError::Unauthorized);
        }
        let token_hash = hash_token(token);
        let now = Utc::now();
        let row = sqlx::query!(
            r#"
            SELECT id
            FROM relay_tokens
            WHERE token_hash = $1 AND revoked_at IS NULL
            LIMIT 1
            "#,
            token_hash
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AppError::Unauthorized)?;

        let result = sqlx::query!(
            "UPDATE relay_tokens SET last_used_at = $2 WHERE id = $1 AND revoked_at IS NULL",
            row.id,
            now
        )
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(AppError::Unauthorized);
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

fn normalize_relay_token_name(raw: &str) -> String {
    let name = raw.trim();
    if name.is_empty() {
        DEFAULT_RELAY_TOKEN_NAME.to_string()
    } else {
        name.to_string()
    }
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

fn relay_token_item(row: RelayTokenRow) -> RelayTokenItem {
    RelayTokenItem {
        id: row.id,
        name: row.name,
        token: row.token,
        masked_token: row.masked_token,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
    }
}

fn relay_token_response(row: RelayTokenRow) -> RelayTokenResponse {
    RelayTokenResponse {
        id: row.id,
        name: row.name,
        token: row.token.unwrap_or_default(),
        masked_token: row.masked_token,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
    }
}
