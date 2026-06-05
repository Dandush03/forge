//! SQLite-backed implementation of the storage traits.
//!
//! Single-file embedded DB at `paths::data_dir()/queue.sqlite`.
//! WAL mode + two separate sqlx pools:
//!
//! - `write_pool` with `max_connections = 1` — every mutating op
//!   acquires here. The pool semaphore queues concurrent writers in
//!   Rust, so `SQLite` itself never sees lock contention. The retry
//!   loops in `finalize` and `JobCtx::enqueue` (`busy_timeout`-style
//!   safety nets) effectively become dead code under normal load.
//! - `read_pool` with `max_connections = 8` — every read-only op.
//!   WAL gives us snapshot reads that aren't blocked by the writer.
//!
//! Adapter swap path: the four storage traits (`storage.rs`) are
//! adapter-agnostic — a `PostgresStorage` would use a single
//! `max_connections = N` pool because Postgres handles concurrent
//! writes natively; a `RedisStorage` would use a multiplexed
//! connection. The pool split is a SQLite-local implementation
//! detail.
//!
//! The four traits are implemented across submodules — one per
//! concern — so each module stays under ~200 lines and tests can
//! mock one trait without dragging in the others.

mod cron;
mod jobs;
mod notify;
mod procs;
mod queue_config;
mod rate_limit;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use sqlx::ConnectOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use super::db_timing::DbRecorder;
use super::error::{Result, StorageError};

/// Single-connection write pool — see the `finish` doc comment for why
/// `SQLite` caps writers at 1.
pub(super) const WRITE_POOL_MAX: u32 = 1;
/// Multi-connection read pool — WAL gives snapshot reads that don't
/// block on the writer.
pub(super) const READ_POOL_MAX: u32 = 8;

/// SQLite-backed `Storage` implementation.
///
/// Construct via `SqliteStorage::open_default` (file at the standard
/// app data dir) or `SqliteStorage::open_in_memory` (tests).
///
/// `Clone` is cheap — both pools are `Arc`-backed.
#[derive(Clone)]
pub struct SqliteStorage {
    /// Single-connection pool that all writes funnel through. The
    /// sqlx pool semaphore is the actual queueing layer — workers
    /// wait here instead of fighting `SQLite`'s `busy_handler`.
    write_pool: SqlitePool,
    /// Multi-connection pool for SELECT-only operations. WAL mode
    /// lets these run without ever blocking on the writer.
    read_pool: SqlitePool,
    notify: Arc<notify::NotifyHub>,
    /// Process-wide monotonic ULID generator. `ulid::Ulid::new()`
    /// alone isn't FIFO-safe — back-to-back ULIDs minted in the same
    /// millisecond have *random* tails, so `id ASC` can disagree
    /// with insertion order at sub-ms resolution. The `Generator`
    /// increments the previous ULID's tail when the timestamp ties,
    /// giving strict monotonic generation per process. Wrapped in a
    /// tokio Mutex because enqueue is `async`. Lock hold is
    /// microseconds; never on the SQL path.
    ulid_gen: Arc<tokio::sync::Mutex<ulid::Generator>>,
    /// Per-call latency buffer. Each `JobQueue` method opens with an
    /// `OpTimer` that records the elapsed ms here on drop. The metrics
    /// roller drains it once per tick.
    pub(super) db_recorder: Arc<DbRecorder>,
    /// Path the on-disk database was opened from, used by the
    /// DB-health snapshot to stat the `-wal` sidecar. `None` for
    /// in-memory storages (tests) — they have no file to stat.
    pub(super) db_path: Option<PathBuf>,
}

impl std::fmt::Debug for SqliteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStorage").finish_non_exhaustive()
    }
}

impl SqliteStorage {
    /// Open / create the queue DB at the standard app data dir.
    /// Runs pending migrations idempotently.
    pub async fn open_default(paths: &dyn super::QueuePaths) -> Result<Self> {
        let dir = paths.data_dir()?;
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("queue.sqlite");
        Self::open_file(&path).await
    }

    /// Open at an explicit file path. Used by tests with `tempfile`.
    pub async fn open_file(path: &Path) -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            // Generous lock timeout — peak backfill has cleanup_aged,
            // bulk enqueue batches, and worker finalizes all racing
            // for the writer. 30s absorbs a slow cleanup (~1-2s
            // historically, now index-backed) and a 500-row
            // enqueue_bulk tx (~50-250ms) without surfacing as
            // "database is locked" errors. Tighten only if a profile
            // shows we're routinely waiting > 1s.
            .busy_timeout(Duration::from_secs(30))
            // foreign_keys = ON would be the default for a real schema
            // but we keep them off because queue_process.current_job
            // logically points at sync_queue.id but we don't want a
            // job delete to cascade-fail a worker row mid-flight.
            .foreign_keys(false)
            // INFO-level sqlx noise from compile-time-checked queries
            // is too chatty in our trace setup; turn it down.
            .log_statements(tracing::log::LevelFilter::Debug)
            .log_slow_statements(tracing::log::LevelFilter::Warn, Duration::from_millis(500));
        Self::finish(opts, Some(path.to_path_buf())).await
    }

    /// In-memory backend. Each call returns an independent DB —
    /// useful for parallel tests.
    ///
    /// Implementation detail: `shared_cache(true)` + `in_memory(true)`
    /// isn't reliable in sqlx 0.8 — pool connections still see
    /// independent schemas in practice. We pin `max_connections=1`
    /// instead, so the test pool re-uses a single connection that
    /// sees the migration's tables. Tests are fast enough that the
    /// single-writer constraint isn't a concern. `read_pool` aliases
    /// the same single-conn pool so reads and writes share the
    /// schema.
    pub async fn open_in_memory() -> Result<Self> {
        let opts = SqliteConnectOptions::new()
            .in_memory(true)
            .journal_mode(SqliteJournalMode::Memory)
            .foreign_keys(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(opts)
            .await
            .map_err(map_sqlx_err)?;

        sqlx::migrate!("src/storage/sqlite/migrations")
            .run(&pool)
            .await
            .map_err(|e| StorageError::Migration {
                version: 0,
                message: e.to_string(),
            })?;

        Ok(Self {
            write_pool: pool.clone(),
            read_pool: pool,
            notify: Arc::new(notify::NotifyHub::default()),
            ulid_gen: Arc::new(tokio::sync::Mutex::new(ulid::Generator::new())),
            db_recorder: Arc::new(DbRecorder::default()),
            db_path: None,
        })
    }

    async fn finish(opts: SqliteConnectOptions, db_path: Option<PathBuf>) -> Result<Self> {
        // Single-connection write pool. The sqlx pool semaphore
        // becomes the queueing layer; SQLite never sees concurrent
        // writers, so SQLITE_BUSY can't surface to callers.
        // acquire_timeout(30s) gives a generous ceiling for the
        // tail latency of a 500-row enqueue_bulk + a couple of
        // claim/finalize ops queued ahead.
        let write_pool = SqlitePoolOptions::new()
            .max_connections(WRITE_POOL_MAX)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(30))
            .connect_with(opts.clone())
            .await
            .map_err(map_sqlx_err)?;

        // Run migrations once against the writer (the only mutating
        // path). The read pool below sees the migrated schema when
        // its connections open.
        sqlx::migrate!("src/storage/sqlite/migrations")
            .run(&write_pool)
            .await
            .map_err(|e| StorageError::Migration {
                version: 0,
                message: e.to_string(),
            })?;

        // Multi-connection read pool. WAL gives us snapshot reads
        // that don't block on the writer.
        let read_pool = SqlitePoolOptions::new()
            .max_connections(READ_POOL_MAX)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await
            .map_err(map_sqlx_err)?;

        Ok(Self {
            write_pool,
            read_pool,
            notify: Arc::new(notify::NotifyHub::default()),
            ulid_gen: Arc::new(tokio::sync::Mutex::new(ulid::Generator::new())),
            db_recorder: Arc::new(DbRecorder::default()),
            db_path,
        })
    }

    /// Generate the next monotonic ULID for an enqueue. Locks the
    /// generator just long enough to mint one ULID — microseconds —
    /// then drops. The fallback to `Ulid::new()` only fires if the
    /// generator's random-tail budget overflows (you'd need >2^80
    /// ULIDs in one millisecond, which is not a thing).
    pub(crate) async fn next_ulid(&self) -> ulid::Ulid {
        let mut generator = self.ulid_gen.lock().await;
        generator.generate().unwrap_or_else(|_| ulid::Ulid::new())
    }

    /// Single-writer pool. Backend-specific — *not* on the storage
    /// trait. Used by the submodule impls; exposed for tests + ops
    /// one-off scripts.
    #[must_use]
    pub const fn write_pool(&self) -> &SqlitePool {
        &self.write_pool
    }

    /// Read pool. WAL snapshot reads that don't block on writers.
    #[must_use]
    pub const fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }
}

/// Convert any `sqlx::Error` into our backend-agnostic
/// `StorageError`. Centralized so every impl site is one `map_err`.
fn map_sqlx_err(e: sqlx::Error) -> StorageError {
    use sqlx::Error as E;
    match e {
        E::RowNotFound => StorageError::NotFound("row not found".into()),
        E::Database(db) => {
            // SQLite distinct errors we care about: busy / locked /
            // constraint. Everything else is generic Backend.
            let code = db.code().unwrap_or_default();
            // SQLite error codes: SQLITE_BUSY=5, SQLITE_LOCKED=6
            if code == "5" || code == "6" || code == "517" {
                StorageError::Conflict(db.message().to_owned())
            } else if code == "1555" || code == "2067" {
                // UNIQUE constraint violation (primary key / unique index)
                StorageError::Conflict(db.message().to_owned())
            } else {
                StorageError::Backend(format!("sqlite [{code}]: {db}"))
            }
        }
        other => StorageError::Backend(other.to_string()),
    }
}
