use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("setup already completed")]
    SetupAlreadyComplete,
    #[error("resource not found")]
    NotFound,
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    ServiceUnavailable(String),
    #[error("{0}")]
    BadGateway(String),
    #[error("request body too large for retry")]
    PayloadTooLarge,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Bcrypt(#[from] bcrypt::BcryptError),
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error("{0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::Unauthorized | Self::InvalidCredentials => StatusCode::UNAUTHORIZED,
            Self::SetupAlreadyComplete => StatusCode::CONFLICT,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::BadGateway(_) => StatusCode::BAD_GATEWAY,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Sqlx(sqlx::Error::RowNotFound) => StatusCode::NOT_FOUND,
            Self::Sqlx(_) | Self::Bcrypt(_) | Self::Anyhow(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::InvalidCredentials => "invalid credentials".to_string(),
            Self::Unauthorized => "unauthorized".to_string(),
            Self::SetupAlreadyComplete => "setup already completed".to_string(),
            Self::Sqlx(sqlx::Error::RowNotFound) => "resource not found".to_string(),
            other => other.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let message = self.message();
        (status, Json(ErrorBody { error: &message })).into_response()
    }
}

pub fn not_found_from_sqlx(err: sqlx::Error) -> AppError {
    match err {
        sqlx::Error::RowNotFound => AppError::NotFound,
        other => AppError::Sqlx(other),
    }
}
