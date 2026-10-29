use std::{collections::HashMap, sync::Arc};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{DateTime, Duration, Utc};
use rand::{TryRngCore, rngs::OsRng};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, TransactionTrait,
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    entity::{admin_session, admin_user, relay_token},
    error::{AppError, AppResult},
    models::{
        AdminIdentity, RelayTokenItem, RelayTokenPage, RelayTokenResponse, RelayTokenView,
        TokenResponse,
    },
};

const ADMIN_SESSION_TTL: Duration = Duration::hours(24);
const ADMIN_SESSION_TOUCH_INTERVAL_SECONDS: i64 = 60;
const DEFAULT_RELAY_TOKEN_NAME: &str = "context7-relay";
const ADMIN_TOKEN_PREFIX: &str = "cpa_";
const RELAY_TOKEN_PREFIX: &str = "cpr_";

#[derive(Clone)]
pub struct Service {
    db: DatabaseConnection,
    admin_touch: Arc<Mutex<HashMap<i64, i64>>>,
}

impl Service {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
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

        let tx = self.db.begin().await?;
        tx.execute_unprepared("LOCK TABLE admin_users IN EXCLUSIVE MODE")
            .await?;

        let count = admin_user::Entity::find().count(&tx).await?;
        if count > 0 {
            return Err(AppError::SetupAlreadyComplete);
        }

        let user = admin_user::ActiveModel {
            username: Set(username),
            password_hash: Set(password_hash),
            created_at: Set(now),
            updated_at: Set(now),
            ..Default::default()
        }
        .insert(&tx)
        .await?;

        admin_session::ActiveModel {
            admin_user_id: Set(user.id),
            token_hash: Set(token_hash),
            expires_at: Set(expires_at),
            created_at: Set(now),
            last_used_at: Set(Some(now)),
            ..Default::default()
        }
        .insert(&tx)
        .await?;

        tx.commit().await?;
        Ok(TokenResponse { token, expires_at })
    }

    pub async fn login(&self, username: &str, password: &str) -> AppResult<TokenResponse> {
        let username = username.trim();
        if username.is_empty() || password.is_empty() {
            return Err(AppError::InvalidCredentials);
        }

        let user = admin_user::Entity::find()
            .filter(admin_user::Column::Username.eq(username))
            .one(&self.db)
            .await?
            .ok_or(AppError::InvalidCredentials)?;

        if !verify(password, &user.password_hash)? {
            return Err(AppError::InvalidCredentials);
        }

        let (token, token_hash) = generate_token(ADMIN_TOKEN_PREFIX)?;
        let now = Utc::now();
        let expires_at = now + ADMIN_SESSION_TTL;
        admin_session::ActiveModel {
            admin_user_id: Set(user.id),
            token_hash: Set(token_hash),
            expires_at: Set(expires_at),
            created_at: Set(now),
            last_used_at: Set(Some(now)),
            ..Default::default()
        }
        .insert(&self.db)
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

        let session = admin_session::Entity::find()
            .find_also_related(admin_user::Entity)
            .filter(admin_session::Column::TokenHash.eq(token_hash))
            .filter(admin_session::Column::ExpiresAt.gt(now))
            .one(&self.db)
            .await?
            .and_then(|(session, user)| Some((session, user?)))
            .ok_or(AppError::Unauthorized)?;

        let (session, user) = session;
        let touch = SessionTouch::new(&session, now);
        if self.should_touch_admin_session(touch, now).await {
            admin_session::Entity::update_many()
                .col_expr(admin_session::Column::LastUsedAt, now.into())
                .filter(admin_session::Column::Id.eq(session.id))
                .exec(&self.db)
                .await?;
        }

        Ok(AdminIdentity {
            admin_user_id: user.id,
            session_id: session.id,
            username: user.username,
        })
    }

    pub async fn logout(&self, token: Option<&str>) -> AppResult<()> {
        let Some(token) = token.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(());
        };
        let token_hash = hash_token(token);
        admin_session::Entity::delete_many()
            .filter(admin_session::Column::TokenHash.eq(token_hash))
            .exec(&self.db)
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

        let tx = self.db.begin().await?;
        let user = admin_user::Entity::find_by_id(identity.admin_user_id)
            .one(&tx)
            .await?
            .ok_or(AppError::Unauthorized)?;

        if !verify(old_password, &user.password_hash)? {
            return Err(AppError::InvalidCredentials);
        }

        let password_hash = hash(new_password, DEFAULT_COST)?;
        let now = Utc::now();
        admin_user::ActiveModel {
            id: Set(user.id),
            password_hash: Set(password_hash),
            updated_at: Set(now),
            ..Default::default()
        }
        .update(&tx)
        .await?;
        admin_session::Entity::delete_many()
            .filter(admin_session::Column::AdminUserId.eq(user.id))
            .exec(&tx)
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
        let total = relay_token::Entity::find()
            .filter(relay_token::Column::RevokedAt.is_null())
            .count(&self.db)
            .await?;
        let offset = page
            .checked_sub(1)
            .and_then(|value| value.checked_mul(page_size))
            .ok_or_else(|| AppError::BadRequest("page is too large".to_string()))?;
        let rows = relay_token::Entity::find()
            .filter(relay_token::Column::RevokedAt.is_null())
            .order_by_desc(relay_token::Column::CreatedAt)
            .order_by_desc(relay_token::Column::Id)
            .offset(offset as u64)
            .limit(page_size as u64)
            .all(&self.db)
            .await?;

        Ok(RelayTokenPage {
            items: rows.into_iter().map(relay_token_item).collect(),
            total: total as i64,
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

        let row = relay_token::ActiveModel {
            name: Set(name),
            token_hash: Set(token_hash),
            token: Set(Some(token)),
            masked_token: Set(masked_token),
            created_at: Set(now),
            last_used_at: Set(None),
            revoked_at: Set(None),
            ..Default::default()
        }
        .insert(&self.db)
        .await?;

        Ok(relay_token_response(row))
    }

    pub async fn update_relay_token(&self, id: i64, name: &str) -> AppResult<RelayTokenItem> {
        let name = normalize_relay_token_name(name);
        let result = relay_token::Entity::update_many()
            .col_expr(relay_token::Column::Name, name.into())
            .filter(relay_token::Column::Id.eq(id))
            .filter(relay_token::Column::RevokedAt.is_null())
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
            return Err(AppError::NotFound);
        }
        let row = relay_token::Entity::find_by_id(id)
            .one(&self.db)
            .await?
            .ok_or(AppError::NotFound)?;

        Ok(relay_token_item(row))
    }

    pub async fn rotate_relay_token(&self, id: i64) -> AppResult<RelayTokenResponse> {
        let tx = self.db.begin().await?;
        let current = relay_token::Entity::find_by_id(id)
            .filter(relay_token::Column::RevokedAt.is_null())
            .one(&tx)
            .await?
            .ok_or(AppError::NotFound)?;

        let now = Utc::now();
        let result = relay_token::Entity::update_many()
            .col_expr(relay_token::Column::RevokedAt, now.into())
            .filter(relay_token::Column::Id.eq(id))
            .filter(relay_token::Column::RevokedAt.is_null())
            .exec(&tx)
            .await?;
        if result.rows_affected == 0 {
            return Err(AppError::NotFound);
        }

        let (token, token_hash) = generate_token(RELAY_TOKEN_PREFIX)?;
        let masked_token = mask_token(&token);
        let row = relay_token::ActiveModel {
            name: Set(current.name),
            token_hash: Set(token_hash),
            token: Set(Some(token)),
            masked_token: Set(masked_token),
            created_at: Set(now),
            last_used_at: Set(None),
            revoked_at: Set(None),
            ..Default::default()
        }
        .insert(&tx)
        .await?;
        tx.commit().await?;

        Ok(relay_token_response(row))
    }

    pub async fn delete_relay_token(&self, id: i64) -> AppResult<()> {
        let now = Utc::now();
        let result = relay_token::Entity::update_many()
            .col_expr(relay_token::Column::RevokedAt, now.into())
            .filter(relay_token::Column::Id.eq(id))
            .filter(relay_token::Column::RevokedAt.is_null())
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
            return Err(AppError::NotFound);
        }
        Ok(())
    }

    pub async fn revoke_relay_token(&self) -> AppResult<()> {
        let now = Utc::now();
        let latest = relay_token::Entity::find()
            .filter(relay_token::Column::RevokedAt.is_null())
            .order_by_desc(relay_token::Column::CreatedAt)
            .order_by_desc(relay_token::Column::Id)
            .one(&self.db)
            .await?;
        let Some(latest) = latest else {
            return Ok(());
        };

        relay_token::Entity::update_many()
            .col_expr(relay_token::Column::RevokedAt, now.into())
            .filter(relay_token::Column::Id.eq(latest.id))
            .filter(relay_token::Column::RevokedAt.is_null())
            .exec(&self.db)
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
        let row = relay_token::Entity::find()
            .filter(relay_token::Column::TokenHash.eq(token_hash))
            .filter(relay_token::Column::RevokedAt.is_null())
            .one(&self.db)
            .await?
            .ok_or(AppError::Unauthorized)?;

        let result = relay_token::Entity::update_many()
            .col_expr(relay_token::Column::LastUsedAt, now.into())
            .filter(relay_token::Column::Id.eq(row.id))
            .filter(relay_token::Column::RevokedAt.is_null())
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0 {
            return Err(AppError::Unauthorized);
        }
        Ok(())
    }

    async fn setup_required(&self) -> AppResult<bool> {
        let count = admin_user::Entity::find().count(&self.db).await?;
        Ok(count == 0)
    }

    async fn current_relay_token(&self) -> AppResult<Option<relay_token::Model>> {
        Ok(relay_token::Entity::find()
            .filter(relay_token::Column::RevokedAt.is_null())
            .order_by_desc(relay_token::Column::CreatedAt)
            .order_by_desc(relay_token::Column::Id)
            .one(&self.db)
            .await?)
    }

    async fn should_touch_admin_session(&self, session: SessionTouch, now: DateTime<Utc>) -> bool {
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
    OsRng
        .try_fill_bytes(&mut raw)
        .map_err(|err| AppError::Internal(format!("generate token: {err}")))?;
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

fn relay_token_item(row: relay_token::Model) -> RelayTokenItem {
    RelayTokenItem {
        id: row.id,
        name: row.name,
        token: row.token,
        masked_token: row.masked_token,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
    }
}

fn relay_token_response(row: relay_token::Model) -> RelayTokenResponse {
    RelayTokenResponse {
        id: row.id,
        name: row.name,
        token: row.token.unwrap_or_default(),
        masked_token: row.masked_token,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
    }
}

struct SessionTouch {
    id: i64,
    last_used_at: Option<DateTime<Utc>>,
}

impl SessionTouch {
    fn new(session: &admin_session::Model, _now: DateTime<Utc>) -> Self {
        Self {
            id: session.id,
            last_used_at: session.last_used_at,
        }
    }
}
