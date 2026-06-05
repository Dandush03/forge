//! Adapter selection + connection parameters for the queue store.
//!
//! Loaded once at startup from `<config_dir>/queue_database.toml`,
//! where the config dir is supplied by the consumer's
//! [`super::QueuePaths`] impl. Missing file → `SQLite` at the
//! standard data dir (today's behavior, so existing installs need
//! no migration). Same crate (`toml`) the secrets store uses — no
//! new YAML dep.
//!
//! Example `queue_database.toml` for the embedded `SQLite` default (file is
//! optional — omit entirely for the standard path):
//!
//! ```toml
//! adapter = "sqlite"
//! # path = "/custom/path/to/queue.sqlite"
//! ```
//!
//! Example for Postgres (production). One of `password` /
//! `password_env` must be set; the latter is preferred so secrets
//! never live in the config file:
//!
//! ```toml
//! adapter = "postgres"
//! host = "db.internal"
//! port = 5432
//! database = "tech_admin"
//! username = "tech_admin"
//! password_env = "TECH_ADMIN_DB_PASSWORD"
//! max_connections = 30
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;

use super::Storage;
use super::error::{Result, StorageError};
use super::paths::QueuePaths;
use super::sqlite::SqliteStorage;

/// Adapter-tagged config; `adapter = "sqlite"` / `"postgres"` selects
/// which struct the rest of the keys deserialize into.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "adapter", rename_all = "lowercase")]
#[non_exhaustive]
pub enum DatabaseConfig {
    Sqlite(SqliteConfig),
    Postgres(PostgresConfig),
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self::Sqlite(SqliteConfig::default())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct SqliteConfig {
    /// On-disk path to the queue `SQLite` file. `None` → standard
    /// `<paths.data_dir()>/queue.sqlite` resolved at open time via
    /// the consumer's [`QueuePaths`] impl.
    #[serde(default)]
    pub path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde(default = "default_pg_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    /// Literal password. Kept for dev convenience; production should
    /// use [`password_env`](Self::password_env) instead so the secret
    /// never lives in the config file.
    #[serde(default)]
    pub password: Option<String>,
    /// Name of the env var to read at startup for the password.
    #[serde(default)]
    pub password_env: Option<String>,
    #[serde(default = "default_pg_max_conn")]
    pub max_connections: u32,
}

const fn default_pg_port() -> u16 {
    5432
}

const fn default_pg_max_conn() -> u32 {
    // Mirrors `PostgresStorage::open`'s historical default — enough
    // headroom for ~6 workers + UI + cron + reaper without queueing at
    // the sqlx pool semaphore.
    30
}

/// How many parent directories to inspect when searching for a
/// repo-local `queue_database.toml`. 4 covers `cargo tauri dev` (CWD =
/// `src-tauri/`, workspace root is one up) plus a couple of levels
/// of headroom for `cargo run` inside a deeper subdir; high enough
/// to be useful, low enough not to walk into the user's `$HOME`.
const MAX_REPO_WALK_DEPTH: usize = 4;

/// Walk up from CWD looking for `queue_database.toml`. Returns the first
/// match. Bounded depth so we don't pick up unrelated files higher
/// in the filesystem.
fn find_repo_local_config() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let mut current = cwd.as_path();
    for _ in 0..MAX_REPO_WALK_DEPTH {
        let candidate = current.join("queue_database.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        current = current.parent()?;
    }
    None
}

impl DatabaseConfig {
    /// Load the active config.
    ///
    /// Lookup order, Rails-style:
    /// 1. The first `queue_database.toml` found walking up from CWD (up to
    ///    `MAX_REPO_WALK_DEPTH` levels). `cargo tauri dev` chdirs to
    ///    `src-tauri/`, so a committed `queue_database.toml` at the
    ///    workspace root is one level up — covered by the walk.
    /// 2. `<paths.config_dir()>/queue_database.toml` — XDG-ish path
    ///    for installed binaries, supplied by the consumer's
    ///    [`QueuePaths`] impl.
    /// 3. The `SQLite` default (no config file present anywhere) so a
    ///    fresh install boots without ceremony.
    ///
    /// The resolved path (or "default — no file found") is logged at
    /// INFO so the choice is visible in the boot banner.
    ///
    /// # Errors
    ///
    /// Surfaces filesystem errors other than `NotFound`, malformed
    /// TOML, and the underlying `paths` resolution failure.
    pub fn load(paths: &dyn QueuePaths) -> Result<Self> {
        if let Some(repo_path) = find_repo_local_config() {
            tracing::info!(
                path = %repo_path.display(),
                "queue_database.toml: loading repo-local config",
            );
            return Self::load_from(&repo_path);
        }
        let xdg_path = paths.config_dir()?.join("queue_database.toml");
        if xdg_path.is_file() {
            tracing::info!(
                path = %xdg_path.display(),
                "queue_database.toml: loading XDG config",
            );
        } else {
            tracing::info!(
                "queue_database.toml: no config file found at CWD or XDG; \
                 using SQLite default",
            );
        }
        Self::load_from(&xdg_path)
    }

    /// Load from an explicit path; same `NotFound` → default behavior.
    /// Exposed for tests so they can point at a `tempfile`.
    ///
    /// # Errors
    ///
    /// Filesystem errors other than `NotFound`, and TOML parse
    /// failures.
    pub fn load_from(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => toml::from_str(&s).map_err(|e| {
                StorageError::InvalidInput(format!(
                    "parsing database config {}: {e}",
                    path.display()
                ))
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(StorageError::InvalidInput(format!(
                "reading database config {}: {e}",
                path.display()
            ))),
        }
    }

    /// Build a [`Storage`] bundle from this config. Runs schema
    /// migrations idempotently as part of open.
    ///
    /// # Errors
    ///
    /// Backend-specific connect / migrate failures, plus a config
    /// error when `adapter = "postgres"` is selected on a host built
    /// without the `postgres` feature.
    pub async fn open_storage(&self, paths: &dyn QueuePaths) -> Result<Storage> {
        match self {
            Self::Sqlite(cfg) => open_sqlite(cfg, paths).await,
            Self::Postgres(cfg) => open_postgres(cfg).await,
        }
    }

    /// Stable backend label for boot logging.
    #[must_use]
    pub const fn adapter_name(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "sqlite",
            Self::Postgres(_) => "postgres",
        }
    }

    // ── Rails-style admin verbs (used by the `jobs-db` CLI) ────────

    /// Create the database, idempotently.
    ///
    /// - `SQLite`: ensures the parent directory exists and opens the
    ///   file (sqlx auto-creates it on first connect; migrations run
    ///   as part of open). No-op when the file already exists.
    /// - Postgres: connects to the maintenance `postgres` database
    ///   and issues `CREATE DATABASE`. Skips silently if the database
    ///   already exists. Migrations are NOT run — call
    ///   [`Self::migrate`] separately, or just let the host's normal
    ///   open path do it on first boot.
    ///
    /// # Errors
    ///
    /// Filesystem errors on `SQLite`; auth / privilege / network
    /// errors on Postgres. Identifier validation: the configured
    /// `database` name must match `[A-Za-z_][A-Za-z0-9_]{0,62}` so
    /// it's safe to interpolate into DDL (which can't parameterize).
    pub async fn create_database(&self, paths: &dyn QueuePaths) -> Result<()> {
        match self {
            Self::Sqlite(cfg) => create_sqlite(cfg, paths).await,
            Self::Postgres(cfg) => create_postgres(cfg).await,
        }
    }

    /// Drop the database. Destructive — the caller (CLI) gates this
    /// behind a `--force` flag.
    ///
    /// - `SQLite`: deletes the file (and the `-wal` / `-shm` sidecars).
    ///   No-op when the file doesn't exist.
    /// - Postgres: connects to the maintenance `postgres` database
    ///   and issues `DROP DATABASE`. No-op when the database doesn't
    ///   exist.
    ///
    /// # Errors
    ///
    /// Filesystem permissions on `SQLite`; auth / privilege / "other
    /// session connected" on Postgres.
    pub async fn drop_database(&self, paths: &dyn QueuePaths) -> Result<()> {
        match self {
            Self::Sqlite(cfg) => drop_sqlite(cfg, paths).await,
            Self::Postgres(cfg) => drop_postgres(cfg).await,
        }
    }

    /// Run pending migrations against an already-created database.
    /// Idempotent — already-applied versions are skipped. Equivalent
    /// to opening the storage, which the host does at boot anyway.
    ///
    /// # Errors
    ///
    /// Surfaces backend-specific connect + migrate failures.
    pub async fn migrate(&self, paths: &dyn QueuePaths) -> Result<()> {
        // `open_storage` runs migrations as part of open. Drop the
        // returned bundle immediately — the Arcs go out of scope and
        // the pool closes.
        let _storage = self.open_storage(paths).await?;
        Ok(())
    }

    /// Quick liveness check: open the storage and run a trivial
    /// statement. Used by the CLI's `status` subcommand.
    ///
    /// # Errors
    ///
    /// Same as [`Self::open_storage`].
    pub async fn ping(&self, paths: &dyn QueuePaths) -> Result<()> {
        let storage = self.open_storage(paths).await?;
        // `describe()` already does a real round-trip
        // (`SELECT sqlite_version()` / `SHOW server_version`).
        storage.jobs.describe().await?;
        Ok(())
    }
}

/// Default on-disk path for the embedded `SQLite` queue. Used by both
/// the host's open path and the CLI's create / drop verbs so the
/// "no `path` set" code path resolves to one place.
fn default_sqlite_path(paths: &dyn QueuePaths) -> Result<PathBuf> {
    Ok(paths.data_dir()?.join("queue.sqlite"))
}

async fn create_sqlite(cfg: &SqliteConfig, paths: &dyn QueuePaths) -> Result<()> {
    let path = match cfg.path.clone() {
        Some(p) => p,
        None => default_sqlite_path(paths)?,
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Open creates the file + runs migrations. Drop the storage
    // immediately so the pool releases its handle on the new file.
    let _ = SqliteStorage::open_file(&path).await?;
    tracing::info!(path = %path.display(), "sqlite database created (or already existed)");
    Ok(())
}

// Filesystem-only; no `.await` needed. Keeps signatures simple at
// the call site (`drop_database` is async + matches on both arms).
#[allow(
    clippy::unused_async,
    reason = "matches the postgres twin's signature so `drop_database` can `.await` both arms uniformly"
)]
async fn drop_sqlite(cfg: &SqliteConfig, paths: &dyn QueuePaths) -> Result<()> {
    let path = match cfg.path.clone() {
        Some(p) => p,
        None => default_sqlite_path(paths)?,
    };
    // Delete the main file + WAL sidecars. `remove_file` is a no-op
    // for missing files via `NotFound` swallow.
    for suffix in ["", "-wal", "-shm"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        match std::fs::remove_file(std::path::Path::new(&p)) {
            Ok(()) => tracing::info!(path = ?p, "sqlite: removed"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(feature = "postgres")]
async fn create_postgres(cfg: &PostgresConfig) -> Result<()> {
    validate_pg_identifier(&cfg.database)?;
    let opts = cfg.maintenance_connect_options()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| StorageError::Backend(format!("postgres maintenance connect: {e}")))?;
    let exists: Option<sqlx::postgres::PgRow> =
        sqlx::query("SELECT 1 AS x FROM pg_database WHERE datname = $1")
            .bind(&cfg.database)
            .fetch_optional(&pool)
            .await
            .map_err(|e| StorageError::Backend(format!("pg_database lookup: {e}")))?;
    if exists.is_some() {
        tracing::info!(database = %cfg.database, "postgres database already exists; skipping CREATE");
        return Ok(());
    }
    // Identifier validation above (`validate_pg_identifier`) is what
    // makes interpolation safe here — DDL can't bind a parameter for
    // the database name.
    let sql = format!("CREATE DATABASE \"{}\"", cfg.database);
    sqlx::query(&sql)
        .execute(&pool)
        .await
        .map_err(|e| StorageError::Backend(format!("CREATE DATABASE: {e}")))?;
    tracing::info!(database = %cfg.database, "postgres database created");
    Ok(())
}

#[cfg(feature = "postgres")]
async fn drop_postgres(cfg: &PostgresConfig) -> Result<()> {
    validate_pg_identifier(&cfg.database)?;
    let opts = cfg.maintenance_connect_options()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| StorageError::Backend(format!("postgres maintenance connect: {e}")))?;
    // `WITH (FORCE)` (PG13+) terminates other sessions instead of
    // failing with "database is being accessed by other users".
    // Matches Rails' `db:drop` behavior under load.
    let sql = format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", cfg.database);
    sqlx::query(&sql)
        .execute(&pool)
        .await
        .map_err(|e| StorageError::Backend(format!("DROP DATABASE: {e}")))?;
    tracing::info!(database = %cfg.database, "postgres database dropped");
    Ok(())
}

#[cfg(not(feature = "postgres"))]
#[allow(clippy::unused_async)]
async fn create_postgres(_cfg: &PostgresConfig) -> Result<()> {
    Err(StorageError::InvalidInput(
        "create on postgres requires --features postgres".into(),
    ))
}

#[cfg(not(feature = "postgres"))]
#[allow(clippy::unused_async)]
async fn drop_postgres(_cfg: &PostgresConfig) -> Result<()> {
    Err(StorageError::InvalidInput(
        "drop on postgres requires --features postgres".into(),
    ))
}

/// Postgres unquoted identifiers: leading `[A-Za-z_]`, then
/// `[A-Za-z0-9_]`, max 63 chars (NAMEDATALEN−1). Rejecting anything
/// outside this set lets us safely interpolate the database name
/// into DDL — sqlx can't parameterize identifiers.
#[cfg(feature = "postgres")]
fn validate_pg_identifier(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 63 {
        return Err(StorageError::InvalidInput(format!(
            "database name `{name}` must be 1..=63 chars"
        )));
    }
    let mut chars = name.chars();
    // `is_empty` is rejected above, so `next()` is `Some`; using a
    // `let else` keeps the unreachable branch out of the panic-prone
    // `.expect()` family (which clippy flags as production-unsafe).
    let Some(first) = chars.next() else {
        return Err(StorageError::InvalidInput(format!(
            "database name `{name}` must be 1..=63 chars"
        )));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(StorageError::InvalidInput(format!(
            "database name `{name}` must start with a letter or underscore"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(StorageError::InvalidInput(format!(
                "database name `{name}` contains invalid character `{c}` \
                 (allowed: letters, digits, underscore)"
            )));
        }
    }
    Ok(())
}

async fn open_sqlite(cfg: &SqliteConfig, paths: &dyn QueuePaths) -> Result<Storage> {
    let inner = if let Some(p) = cfg.path.as_deref() {
        SqliteStorage::open_file(p).await?
    } else {
        SqliteStorage::open_default(paths).await?
    };
    Ok(Storage::from_one(Arc::new(inner)))
}

#[cfg(feature = "postgres")]
async fn open_postgres(cfg: &PostgresConfig) -> Result<Storage> {
    let opts = cfg.pg_connect_options()?;
    let inner =
        super::postgres::PostgresStorage::open_with_options(opts, cfg.max_connections).await?;
    Ok(Storage::from_one(Arc::new(inner)))
}

#[cfg(not(feature = "postgres"))]
#[allow(
    clippy::unused_async,
    reason = "async signature matches the `feature = postgres` variant so callers can `.await` it uniformly"
)]
async fn open_postgres(_cfg: &PostgresConfig) -> Result<Storage> {
    Err(StorageError::InvalidInput(
        "queue_database.toml requests adapter = \"postgres\" but this build was compiled \
         without the `postgres` feature. Rebuild with `--features postgres` (or set \
         `adapter = \"sqlite\"`)."
            .into(),
    ))
}

impl PostgresConfig {
    /// Resolve the password from either the literal `password` or
    /// `password_env`. Errors when both are missing — Postgres
    /// requires authentication.
    #[cfg_attr(
        not(feature = "postgres"),
        allow(
            dead_code,
            reason = "only called by `open_postgres` under the postgres feature; \
                      kept in scope so the config still parses + the unit tests \
                      can validate the resolution logic without the feature on"
        )
    )]
    fn resolve_password(&self) -> Result<String> {
        if let Some(p) = &self.password {
            return Ok(p.clone());
        }
        if let Some(env_name) = &self.password_env {
            return std::env::var(env_name).map_err(|_| {
                // Common confusion: `password_env` takes the NAME of
                // an env var to read, not the password itself. When
                // the value looks unlike a typical env-var name
                // (lowercase, no underscores), flag it as a likely
                // field-name mix-up — Daniel hit this exact case the
                // first time the schema shipped.
                let likely_field_mixup =
                    env_name.chars().all(|c| c.is_ascii_lowercase()) && !env_name.contains('_');
                let hint = if likely_field_mixup {
                    format!(
                        " — `password_env` expects the *name* of an env var, not the \
                         password itself. If `{env_name}` is your literal password, \
                         use `password = \"{env_name}\"` instead."
                    )
                } else {
                    String::new()
                };
                StorageError::InvalidInput(format!(
                    "queue_database.toml: password_env `{env_name}` is not set{hint}"
                ))
            });
        }
        Err(StorageError::InvalidInput(
            "queue_database.toml: postgres adapter requires either `password` (literal) \
             or `password_env` (name of an env var holding the password)"
                .into(),
        ))
    }

    /// Connect options targeting the configured database. Used by
    /// the storage open path and by the `jobs-db` CLI's `migrate` /
    /// `status` verbs.
    ///
    /// # Errors
    ///
    /// Surfaces password resolution failure (missing literal +
    /// missing / unset `password_env`).
    #[cfg(feature = "postgres")]
    pub fn pg_connect_options(&self) -> Result<sqlx::postgres::PgConnectOptions> {
        let password = self.resolve_password()?;
        Ok(sqlx::postgres::PgConnectOptions::new()
            .host(&self.host)
            .port(self.port)
            .database(&self.database)
            .username(&self.username)
            .password(&password))
    }

    /// Connect options targeting the cluster's maintenance database
    /// (`postgres`). Used by `jobs-db create` / `drop` to issue DDL
    /// against the server without depending on the target DB
    /// existing.
    ///
    /// # Errors
    ///
    /// Same as [`Self::pg_connect_options`].
    #[cfg(feature = "postgres")]
    pub fn maintenance_connect_options(&self) -> Result<sqlx::postgres::PgConnectOptions> {
        Ok(self.pg_connect_options()?.database("postgres"))
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::panic,
        reason = "unit tests crash loudly on setup failure"
    )]
    use super::*;

    #[test]
    fn default_is_sqlite_with_no_explicit_path() {
        let cfg = DatabaseConfig::default();
        match cfg {
            DatabaseConfig::Sqlite(s) => assert!(s.path.is_none()),
            DatabaseConfig::Postgres(_) => panic!("expected sqlite default"),
        }
    }

    #[test]
    fn parses_minimal_sqlite_toml() {
        let parsed: DatabaseConfig = toml::from_str(r#"adapter = "sqlite""#).unwrap();
        match parsed {
            DatabaseConfig::Sqlite(s) => assert!(s.path.is_none()),
            DatabaseConfig::Postgres(_) => panic!("expected sqlite"),
        }
    }

    #[test]
    fn parses_sqlite_with_custom_path() {
        let parsed: DatabaseConfig = toml::from_str(
            r#"
            adapter = "sqlite"
            path = "/var/lib/tech-admin/queue.sqlite"
            "#,
        )
        .unwrap();
        match parsed {
            DatabaseConfig::Sqlite(s) => assert_eq!(
                s.path,
                Some(PathBuf::from("/var/lib/tech-admin/queue.sqlite")),
            ),
            DatabaseConfig::Postgres(_) => panic!("expected sqlite"),
        }
    }

    #[test]
    fn parses_postgres_with_defaults() {
        let parsed: DatabaseConfig = toml::from_str(
            r#"
            adapter = "postgres"
            host = "db.internal"
            database = "tech_admin"
            username = "tech_admin"
            password_env = "TECH_ADMIN_DB_PASSWORD"
            "#,
        )
        .unwrap();
        match parsed {
            DatabaseConfig::Postgres(p) => {
                assert_eq!(p.host, "db.internal");
                assert_eq!(p.port, 5432, "default port applies");
                assert_eq!(p.max_connections, 30, "default cap applies");
                assert_eq!(p.password_env.as_deref(), Some("TECH_ADMIN_DB_PASSWORD"));
            }
            DatabaseConfig::Sqlite(_) => panic!("expected postgres"),
        }
    }

    #[test]
    fn resolve_password_errors_when_neither_is_set() {
        let cfg = PostgresConfig {
            host: "x".into(),
            port: 5432,
            database: "x".into(),
            username: "x".into(),
            password: None,
            password_env: None,
            max_connections: 30,
        };
        assert!(cfg.resolve_password().is_err());
    }

    #[test]
    fn resolve_password_prefers_literal_over_env() {
        let cfg = PostgresConfig {
            host: "x".into(),
            port: 5432,
            database: "x".into(),
            username: "x".into(),
            password: Some("hunter2".into()),
            // password_env is ignored when password is set.
            password_env: Some("DEFINITELY_NOT_SET_FOR_TEST_999".into()),
            max_connections: 30,
        };
        assert_eq!(cfg.resolve_password().unwrap(), "hunter2");
    }

    #[test]
    fn resolve_password_errors_when_env_var_missing() {
        let cfg = PostgresConfig {
            host: "x".into(),
            port: 5432,
            database: "x".into(),
            username: "x".into(),
            password: None,
            password_env: Some("TECH_ADMIN_DEFINITELY_NOT_SET_8a3f".into()),
            max_connections: 30,
        };
        assert!(cfg.resolve_password().is_err());
    }

    #[test]
    fn load_from_missing_path_returns_default() {
        // A path that definitely doesn't exist.
        let cfg =
            DatabaseConfig::load_from(Path::new("/nonexistent/tech-admin/queue_database.toml"))
                .unwrap();
        match cfg {
            DatabaseConfig::Sqlite(s) => assert!(s.path.is_none()),
            DatabaseConfig::Postgres(_) => panic!("expected sqlite default"),
        }
    }
}
