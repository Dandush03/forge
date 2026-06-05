//! Path resolution surface for the queue subsystem.
//!
//! The queue needs two directories at boot:
//!  - a **config dir** to look for `queue_database.toml` (Rails-style
//!    XDG fallback when no repo-local config is found)
//!  - a **data dir** to put the embedded `SQLite` file in when the
//!    config doesn't pin an explicit path
//!
//! Embedding a single concrete paths resolver here would lock the
//! crate to one specific host layout (the Tauri app's `XDG_*`-aware
//! dirs in our case). Instead the trait is injected at the call
//! sites that need it, so a different consumer — a CLI tool, a
//! deployed service, an embedded test — can supply its own resolver
//! (env-var, hard-coded path, tempfile) without pulling in the
//! desktop app's `tech-admin-paths` crate.

use std::path::PathBuf;

use thiserror::Error;

/// Resolver for the two filesystem locations the queue subsystem
/// needs.
///
/// Implementors carry whatever upstream paths library makes sense
/// for their host — the queue crate itself stays paths-library
/// agnostic.
pub trait QueuePaths: Send + Sync + std::fmt::Debug {
    /// Directory where `queue_database.toml` lives when no
    /// repo-local copy is found by the bounded CWD walk. Must exist
    /// (or be creatable) since callers may also write into it.
    ///
    /// # Errors
    ///
    /// Implementor-specific: missing required env vars, permission
    /// failures on creation, etc.
    fn config_dir(&self) -> Result<PathBuf, PathsError>;

    /// Default directory for the embedded `SQLite` file. Overridden
    /// by `queue_database.toml`'s `database.path` when set.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::config_dir`].
    fn data_dir(&self) -> Result<PathBuf, PathsError>;
}

/// Crate-owned error type for `QueuePaths` resolution failures.
///
/// Implementors stringify their own paths-library errors into this
/// so the queue crate's public surface doesn't leak the consumer's
/// specific error type. The queue crate only surfaces the message
/// back to the operator.
#[derive(Debug, Error)]
#[error("paths: {0}")]
pub struct PathsError(pub String);

impl PathsError {
    /// Build a `PathsError` from anything `Display`-able.
    #[must_use]
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}
