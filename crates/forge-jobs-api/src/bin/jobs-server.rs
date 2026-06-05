//! `jobs-server` — standalone HTTP binary for the queue API.
//!
//! Boots the same Tokio runtime + storage layer as the Tauri host,
//! but serves the queue API over HTTP instead of through `invoke()`.
//! Selects the storage backend at boot:
//!
//! - `QUEUE_BACKEND=sqlite` (default): opens the local-first `SQLite`
//!   queue at `paths::data_dir()/queue.sqlite`. Same file the Tauri
//!   app uses — running both side-by-side talks to the same queue
//!   (don't, unless you mean to).
//! - `QUEUE_BACKEND=postgres`: requires `--features postgres` and
//!   `DATABASE_URL=postgres://...`. The deploy shape for the
//!   multi-replica future.
//!
//! Bind: `127.0.0.1:$JOBS_API_PORT` by default (port 8787). Override
//! the address with `JOBS_API_BIND=0.0.0.0:8787` to expose externally.
//! No auth yet — keep loopback-bound until a future auth-token gate
//! lands.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use forge_jobs::Storage;
use forge_jobs::storage::sqlite::SqliteStorage;
use forge_jobs::storage::{PathsError, QueuePaths};

/// Env-backed [`QueuePaths`] for the server CLI. Same fallbacks as
/// `jobs-db` — the queue crate stays paths-library agnostic.
#[derive(Debug)]
struct EnvQueuePaths;

impl QueuePaths for EnvQueuePaths {
    fn config_dir(&self) -> Result<std::path::PathBuf, PathsError> {
        Ok(std::env::var_os("JOBS_CONFIG_DIR").map_or_else(
            || std::path::PathBuf::from("./jobs/config"),
            std::path::PathBuf::from,
        ))
    }

    fn data_dir(&self) -> Result<std::path::PathBuf, PathsError> {
        Ok(std::env::var_os("JOBS_DATA_DIR").map_or_else(
            || std::path::PathBuf::from("./jobs/data"),
            std::path::PathBuf::from,
        ))
    }
}
use forge_jobs_api::router;

#[allow(
    clippy::expect_used,
    reason = "startup; before tracing is meaningfully attached we can't propagate"
)]
fn main() {
    init_tracing();

    let workers = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(NonZeroUsize::get)
        })
        .unwrap_or(1);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("tokio runtime");

    tracing::info!(
        pid = std::process::id(),
        version = env!("CARGO_PKG_VERSION"),
        tokio_workers = workers,
        "jobs-server starting"
    );

    runtime.block_on(async_main());
}

async fn async_main() {
    let backend = std::env::var("QUEUE_BACKEND").unwrap_or_else(|_| "sqlite".into());
    let storage = match backend.as_str() {
        "sqlite" => {
            tracing::info!("opening sqlite storage");
            let paths = EnvQueuePaths;
            match SqliteStorage::open_default(&paths).await {
                Ok(s) => Storage::from_one(Arc::new(s)),
                Err(e) => fatal_exit("sqlite open", &e),
            }
        }
        #[cfg(feature = "postgres")]
        "postgres" => open_postgres().await,
        other => {
            tracing::error!(
                backend = %other,
                "unsupported QUEUE_BACKEND (compiled features may be missing)"
            );
            std::process::exit(1);
        }
    };

    match storage.jobs.describe().await {
        Ok(info) => tracing::info!(
            backend = %info.backend,
            fields = ?info.fields,
            "storage open"
        ),
        Err(e) => tracing::warn!(?e, "storage describe failed; banner suppressed"),
    }

    let bind = std::env::var("JOBS_API_BIND").unwrap_or_else(|_| {
        let port = std::env::var("JOBS_API_PORT").unwrap_or_else(|_| "8787".into());
        format!("127.0.0.1:{port}")
    });
    let addr: SocketAddr = match bind.parse() {
        Ok(a) => a,
        Err(e) => fatal_exit(&format!("invalid JOBS_API_BIND `{bind}`"), &e),
    };

    let app = router::build(Arc::new(storage));
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => fatal_exit(&format!("bind {addr}"), &e),
    };
    tracing::info!(%addr, "jobs-server listening");

    if let Err(e) = axum::serve(listener, app).await {
        fatal_exit("axum serve", &e);
    }
}

#[cfg(feature = "postgres")]
async fn open_postgres() -> Storage {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        tracing::error!("DATABASE_URL required for QUEUE_BACKEND=postgres");
        std::process::exit(1);
    });
    let max = std::env::var("DATABASE_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(30);
    tracing::info!(url = %redact_url(&url), max_connections = max, "opening postgres storage");
    match forge_jobs::storage::postgres::PostgresStorage::open(&url, max).await {
        Ok(pg) => Storage::from_one(Arc::new(pg)),
        Err(e) => fatal_exit("postgres open", &e),
    }
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "info,\
             forge_jobs=debug,\
             forge_jobs_api=debug,\
             tower_http=debug,\
             hyper=warn,\
             sqlx=warn",
        )
    });
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(true).with_level(true))
        .try_init();
}

/// Log and exit on a fatal startup error. Diverges (`-> !`-ish via
/// `process::exit`) so the caller's match arm can use it as a
/// no-return tail expression in a non-`!` arm.
fn fatal_exit<E: std::fmt::Display>(stage: &str, e: &E) -> ! {
    tracing::error!(stage, error = %e, "fatal startup error");
    std::process::exit(1);
}

/// Strip credentials from a Postgres URL before logging. Naive but
/// good enough for `postgres://user:pass@host/db` shape.
#[cfg(feature = "postgres")]
fn redact_url(url: &str) -> String {
    if let Some(at) = url.rfind('@')
        && let Some(scheme_end) = url.find("://")
    {
        let scheme = &url[..=scheme_end + 2];
        let host_and_db = &url[at..];
        return format!("{scheme}<redacted>{host_and_db}");
    }
    url.to_owned()
}
