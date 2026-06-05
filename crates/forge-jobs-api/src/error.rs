//! Handler errors that map cleanly to both HTTP and Tauri IPC.
//!
//! Same tagged-enum shape the Tauri plugin already uses on the
//! frontend side, so callers branch on `kind` (`validation` /
//! `not_found` / `rate_limited` / `internal` / `storage`) instead of
//! parsing strings. Axum's `IntoResponse` impl picks the status code.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use forge_jobs::StorageError;
use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Error {
    /// Operator-supplied input was wrong. 400.
    Validation { field: String, msg: String },
    /// Resource not found. 404.
    NotFound { msg: String },
    /// Conflict (dedupe, concurrent modification, busy lock). 409.
    Conflict { msg: String },
    /// Rate-limited upstream — operator should back off. 429.
    RateLimited { retry_after_secs: u32 },
    /// Anything else from the storage layer. 500.
    Storage { msg: String },
    /// Catch-all bug / panic-equivalent. 500.
    Internal { msg: String },
}

impl Error {
    #[must_use]
    pub fn validation(field: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            msg: msg.into(),
        }
    }

    #[must_use]
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound { msg: msg.into() }
    }

    #[must_use]
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal { msg: msg.into() }
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound(msg) => Self::NotFound { msg },
            StorageError::InvalidInput(msg) => Self::Validation {
                field: "input".into(),
                msg,
            },
            StorageError::Conflict(msg) => Self::Conflict { msg },
            // Transient lock / pool-timeout: a retryable 409, not a 500.
            // A scraper/HPA seeing 500 would treat a momentary busy
            // writer as a hard failure.
            other if other.is_transient_conflict() => Self::Conflict {
                msg: other.to_string(),
            },
            other => Self::Storage {
                msg: other.to_string(),
            },
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Validation {
            field: "json".into(),
            msg: e.to_string(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation { field, msg } => write!(f, "validation({field}): {msg}"),
            Self::NotFound { msg } => write!(f, "not_found: {msg}"),
            Self::Conflict { msg } => write!(f, "conflict: {msg}"),
            Self::RateLimited { retry_after_secs } => {
                write!(f, "rate_limited (retry in {retry_after_secs}s)")
            }
            Self::Storage { msg } => write!(f, "storage: {msg}"),
            Self::Internal { msg } => write!(f, "internal: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Validation { .. } => StatusCode::BAD_REQUEST,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Conflict { .. } => StatusCode::CONFLICT,
            Self::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::Storage { .. } | Self::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = axum::Json(&self);
        (status, body).into_response()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
