//! `jobs-db` — Rails-style admin helper for the queue store.
//!
//! Reads `queue_database.toml` via [`DatabaseConfig::load`] (same lookup as
//! the Tauri host) and runs the requested subcommand against the
//! configured adapter.
//!
//! Subcommands (idempotent unless noted):
//!
//! - `create` — create the database. `SQLite`: ensure parent dir +
//!   open (sqlx auto-creates the file). Postgres: connect to the
//!   maintenance `postgres` database and issue `CREATE DATABASE`.
//!   Skips if already exists.
//! - `migrate` — run pending migrations (idempotent). Equivalent to
//!   opening the storage, which the host does at boot anyway.
//! - `status` — report adapter + reachability. Returns a non-zero
//!   exit code on failure so CI can gate on it.
//! - `drop` — **destructive.** `SQLite`: delete the file + `-wal` /
//!   `-shm` sidecars. Postgres: `DROP DATABASE IF EXISTS … WITH
//!   (FORCE)`. Requires `--force` to avoid muscle-memory disasters.
//! - `reset` — drop + create + migrate. Requires `--force`.
//!
//! Invocation (with the cargo alias at `.cargo/config.toml`):
//!
//! ```text
//! cargo db create
//! cargo db status
//! cargo db reset --force
//! ```
//!
//! Without the alias:
//!
//! ```text
//! cargo run -p forge-jobs --bin jobs-db --features postgres -- create
//! ```

// CLI binary: stdout/stderr ARE the user-facing surface. The workspace
// lints flag them in production code (use `tracing` instead) — that
// rule's for the host + library code, not a CLI helper.
#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "CLI output goes to stdout/stderr by design"
)]

use std::path::PathBuf;
use std::process::ExitCode;

use forge_jobs::storage::{DatabaseConfig, PathsError, QueuePaths};

/// Env-backed [`QueuePaths`] for the CLI. The `forge-jobs` crate
/// doesn't bundle a default paths resolver (it stays paths-library
/// agnostic so it can be reused by downstream consumers); the CLI
/// satisfies the trait by reading `JOBS_CONFIG_DIR` /
/// `JOBS_DATA_DIR` env vars and falling back to CWD-relative
/// `./jobs/{config,data}` when unset.
#[derive(Debug)]
struct EnvQueuePaths;

impl QueuePaths for EnvQueuePaths {
    fn config_dir(&self) -> Result<PathBuf, PathsError> {
        Ok(std::env::var_os("JOBS_CONFIG_DIR")
            .map_or_else(|| PathBuf::from("./jobs/config"), PathBuf::from))
    }

    fn data_dir(&self) -> Result<PathBuf, PathsError> {
        Ok(std::env::var_os("JOBS_DATA_DIR")
            .map_or_else(|| PathBuf::from("./jobs/data"), PathBuf::from))
    }
}

fn main() -> ExitCode {
    init_tracing();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((cmd, flags)) = args.split_first().map(|(c, r)| (c.as_str(), r)) else {
        print_usage();
        return ExitCode::from(2);
    };
    let force = flags.iter().any(|f| f == "--force" || f == "-f");

    let paths = EnvQueuePaths;
    let cfg = match DatabaseConfig::load(&paths) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("jobs-db: failed to load queue_database.toml: {e}");
            return ExitCode::FAILURE;
        }
    };
    tracing::info!(adapter = %cfg.adapter_name(), command = %cmd, "jobs-db");

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("jobs-db: failed to build tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(async move {
        match cmd {
            "create" => report(cfg.create_database(&paths).await, "create"),
            "drop" => {
                if !force {
                    eprintln!("jobs-db: refusing to drop without --force");
                    return ExitCode::from(2);
                }
                report(cfg.drop_database(&paths).await, "drop")
            }
            "migrate" => report(cfg.migrate(&paths).await, "migrate"),
            "status" => match cfg.ping(&paths).await {
                Ok(()) => {
                    println!("ok: adapter={}", cfg.adapter_name());
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("status: {e}");
                    ExitCode::FAILURE
                }
            },
            "reset" => {
                if !force {
                    eprintln!("jobs-db: refusing to reset without --force");
                    return ExitCode::from(2);
                }
                if let Err(e) = cfg.drop_database(&paths).await {
                    eprintln!("reset: drop failed: {e}");
                    return ExitCode::FAILURE;
                }
                if let Err(e) = cfg.create_database(&paths).await {
                    eprintln!("reset: create failed: {e}");
                    return ExitCode::FAILURE;
                }
                report(cfg.migrate(&paths).await, "reset: migrate")
            }
            "help" | "--help" | "-h" => {
                print_usage();
                ExitCode::SUCCESS
            }
            other => {
                eprintln!("jobs-db: unknown command `{other}`");
                print_usage();
                ExitCode::from(2)
            }
        }
    })
}

fn report(res: Result<(), forge_jobs::StorageError>, label: &str) -> ExitCode {
    match res {
        Ok(()) => {
            println!("{label}: ok");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{label}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn print_usage() {
    eprintln!(
        "usage: jobs-db <command> [--force]

commands:
  create    create the database (idempotent)
  migrate   run pending migrations
  status    report adapter + reachability (exit non-zero on failure)
  drop      destructive: drop the database (requires --force)
  reset     destructive: drop + create + migrate (requires --force)
  help      print this message

the config is read from queue_database.toml via the same lookup the host
uses: CWD walk (up to 4 levels) then XDG config dir."
    );
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
