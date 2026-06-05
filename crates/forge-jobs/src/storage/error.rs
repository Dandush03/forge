//! Backend-agnostic error type for the storage traits.
//!
//! Each backend's underlying error (sqlx, redis, postgres, …) folds
//! into a `StorageError` variant via `From` impls in that backend's
//! module. The runtime never inspects which backend produced the
//! error — it just propagates or logs.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StorageError {
    #[error("schema migration failed at version {version}: {message}")]
    Migration { version: u32, message: String },

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    /// Generic backend-side failure. The string is whatever the
    /// driver gave us (`sqlx::Error::to_string()`, `redis::Error`, …).
    /// Use `is_transient_conflict` to decide if a retry is worth it.
    #[error("backend: {0}")]
    Backend(String),

    #[error(transparent)]
    Paths(#[from] super::paths::PathsError),
}

impl StorageError {
    /// True when this error looks like a transient MVCC / lock
    /// conflict that the caller should retry. Liberal substring
    /// matching — false positives just cost a harmless retry.
    #[must_use]
    pub fn is_transient_conflict(&self) -> bool {
        let msg = self.to_string().to_lowercase();
        msg.contains("conflict")
            || msg.contains("database is locked")
            || msg.contains("sqlite_busy")
            || msg.contains("deadlock detected")
            || msg.contains("could not serialize access")
            || msg.contains("write conflict")
            || msg.contains("transaction was aborted")
            // sqlx pool-acquire timeout under a backed-up single writer:
            // the work isn't lost, the writer queue is just saturated —
            // retrying once it drains is the right move.
            || msg.contains("pool timed out")
            || msg.contains("timed out while waiting")
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
