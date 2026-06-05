//! HTTP API surface for the queue. Mirrors the
//! `tauri-plugin-queue` command surface — the Tauri plugin will be
//! refactored to thin wrappers around the same handlers in a
//! follow-up. See `Cargo.toml` for the design rationale.
//!
//! Public surface:
//! - [`dto`] — request/response shapes shared across the Tauri and
//!   HTTP transports.
//! - [`handlers`] — pure async fns over `&Storage`. Test these
//!   directly against an in-memory `SQLite`, no router or container
//!   needed.
//! - [`router`] — Axum router that exposes the handlers as JSON
//!   endpoints. Mount via `Router::merge(jobs_api::router())`.
//! - [`Error`] — handler errors that map to HTTP status codes via
//!   [`axum::response::IntoResponse`].

pub mod dto;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod router;

pub use error::Error;
