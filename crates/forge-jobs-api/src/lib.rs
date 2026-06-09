//! HTTP API surface for the queue. Mirrors the
//! `tauri-plugin-queue` command surface — the Tauri plugin will be
//! refactored to thin wrappers around the same handlers in a
//! follow-up. See `Cargo.toml` for the design rationale.
//!
//! Public surface:
//! - [`dto`] — request/response shapes shared across the Tauri and
//!   HTTP transports. Kept as a `pub mod` because callers tend to
//!   import many DTOs at once and the namespace is the discoverable
//!   contract.
//! - [`handlers`] — pure async fns over `&Storage`. Test these
//!   directly against an in-memory `SQLite`, no router or container
//!   needed.
//! - [`build_router`] — Axum router that exposes the handlers as JSON
//!   endpoints. Mount via `Router::merge(forge_jobs_api::build_router(storage))`.
//! - [`metrics_render`] — Prometheus text rendering helper.
//! - [`Error`] — handler errors that map to HTTP status codes via
//!   [`axum::response::IntoResponse`].

pub mod dto;
pub mod handlers;

// Small modules — surface is just the named items, demoted to
// `pub(crate)` so the SemVer surface is the re-exports below.
#[allow(unreachable_pub)]
pub(crate) mod error;
#[allow(unreachable_pub)]
pub(crate) mod metrics;
#[allow(unreachable_pub)]
pub(crate) mod router;
#[allow(unreachable_pub)]
pub(crate) mod series;

pub use error::Error;
pub use metrics::render as metrics_render;
pub use router::build as build_router;
