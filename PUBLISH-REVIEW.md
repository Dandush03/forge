# forge workspace — pre-publish review

Three parallel deep reviews (forge-jobs, forge-jobs-api, forge-jobs-ui +
forge-charts) + a manifest audit. Findings sorted by what blocks
`cargo publish` and what's real-world risk.

## TL;DR

**Don't publish yet.** Five mechanical manifest blockers + one real
correctness issue (`NOTIFY` identifier injection) + two SemVer-window
choices that get cheaper to make now than after 0.1 is on crates.io.
A focused fix-pass is ~2 hours and gets us to a publish-ready state I'd
sign off on.

---

## 🔴 Publish blockers (cargo publish will reject or actively misbehave)

### B1 — `forge-jobs-api` manifest is incomplete
[crates/forge-jobs-api/Cargo.toml](crates/forge-jobs-api/Cargo.toml)

- `publish = false` (line 5) — `cargo publish` refuses on this alone
- **Missing** `description`, `license`, `repository`, `keywords`,
  `categories`, `readme` — crates.io requires the first three
- The other three crates have them; only api was missed

**Fix:** mirror the metadata block from `forge-jobs` (description tuned
for the api crate, license `"MIT OR Apache-2.0"`, repository
`https://github.com/dandush03/forge`, readme `README.md`, sensible
keywords/categories), and flip `publish = true`.

### B2 — `forge-jobs-ui` is `publish = false`
[crates/forge-jobs-ui/Cargo.toml:8](crates/forge-jobs-ui/Cargo.toml)

Has all the other metadata; just need to flip the flag.

### B3 — Path-deps missing `version` field
- [crates/forge-jobs-api/Cargo.toml:46](crates/forge-jobs-api/Cargo.toml) — `forge-jobs = { path = "../forge-jobs" }`
- [crates/forge-jobs-ui/Cargo.toml:23](crates/forge-jobs-ui/Cargo.toml) — `forge-charts = { path = "../forge-charts" }`

`cargo publish` rejects path-only deps for a published crate. The
in-workspace path stays; the published artifact uses the version.

**Fix:** add `version = "0.1"` next to `path = "..."` on both. Cargo
uses the path during local dev, the version when consumers pull from
crates.io.

### B4 — Wrong `repository` URLs
- `forge-jobs/Cargo.toml:7` → `dandush03/tech-admin`
- `forge-jobs-ui/Cargo.toml:11` → `dandush03/tech-admin`
- `forge-charts/Cargo.toml:7` → `dandush03/forge-charts` (vs the workspace `dandush03/forge`)

Consumers landing on crates.io click → wrong repo / 404. Decide one
canonical URL — recommended `https://github.com/dandush03/forge` for
all four, since they share a workspace.

### B5 — README example binds `0.0.0.0` with no auth warning
[crates/forge-jobs-api/README.md:62](crates/forge-jobs-api/README.md)

The library README's copy-paste example does
`bind("0.0.0.0:8080")` with no auth. Routes include
`POST /queue/:name/backoff` (mutates DB). A consumer copies this and
exposes an unauthenticated mutation endpoint to the internet.

The `jobs-server` binary's source actually defaults to `127.0.0.1`
correctly — the README is the problem.

**Fix:** add a bold "⚠️ Security" section to both the README and the
`router()` doc-comment: "Routes are unauthenticated. Mount behind your
own auth middleware or bind to loopback." Change the example to
`bind("127.0.0.1:8080")`.

---

## 🟠 Real bugs / vulnerabilities

### V1 — `NOTIFY` identifier injection (Postgres only) [HIGH]
[crates/forge-jobs/src/storage/postgres.rs:985](crates/forge-jobs/src/storage/postgres.rs#L985)

```rust
format!("NOTIFY \"q_{queue}\"")
```

If a downstream caller ever lets a user-supplied queue name through —
say, a multi-tenant UI that lets ops define new queues — a `"` in the
name closes the identifier and the rest becomes appended SQL. SQLite
isn't vulnerable (uses in-process `Notify`, not SQL).

**Fix:** validate queue names through the existing
`validate_pg_identifier` regex (`database_config.rs:407`) at
`enqueue` and `ensure_queue` entry, or escape `"` → `""` inline.

### V2 — CSS-context injection via color picker [LOW, defense-in-depth]
[crates/forge-charts/src/chart.rs:185-188](crates/forge-charts/src/chart.rs#L185-L188)

The legend's `<input type="color">` value flows verbatim into inline
`style="--charts-series-{cc}: {hex}"`. Native picker emits
`#rrggbb`, but a synthetic input event from a malicious extension
could inject `red; background-image: url(evil)`. Same-origin
impact (it's the consumer's own UI), low severity.

**Fix:** validate the value matches `^#[0-9a-fA-F]{3,6}$` before
applying. `soften_hex` already does the pattern match — reuse it at
the assignment site.

---

## 🟡 API regret risks (SemVer ratchet — cheap now, expensive later)

### R1 — Public enums missing `#[non_exhaustive]`
Adding a variant after publish is a SemVer-major break.

**Candidates** in forge-jobs (severity = how likely they are to grow):
- `JobOutcome` (already grew once this month with `Dead`) — HIGH
- `JobStatus` (likely to grow with e.g. `Cancelled`) — HIGH
- `StorageError` (any new backend or new failure mode adds a variant) — HIGH
- `FinalizeOutcome` — MEDIUM
- `TimelineEventType` — MEDIUM
- `DatabaseConfig` (a new adapter = new variant) — MEDIUM
- `EnqueueOutcome`, `RateLimitOutcome`, `AcquireOutcome` — LOW (shape is settled)

**In forge-jobs-api:**
- `Error` (handler errors, will grow with new failure modes) — HIGH

**Fix:** annotate the top candidates with `#[non_exhaustive]`. Adds
zero runtime cost; consumers using `match` now get a "must add `_ =>`"
warning instead of a future compile break.

### R2 — `Storage` struct fields locked in public ABI
[crates/forge-jobs/src/storage.rs:547](crates/forge-jobs/src/storage.rs#L547)

```rust
pub struct Storage {
    pub jobs: Arc<dyn JobQueue>,
    pub procs: Arc<dyn ProcessRegistry>,
    // ...
}
```

Adding a 6th trait Arc later is a major break.

**Fix:** mark `#[non_exhaustive]` on the struct, or convert the fields
to private + add `pub fn jobs(&self) -> &Arc<dyn JobQueue>` accessors.

### R3 — `pub mod` over-exposure in ui + charts

- `forge-jobs-ui/src/lib.rs:45-58` — every panel sub-module (`overview`,
  `timeline`, `cron`, `inspector`, etc.) is `pub`. External consumers
  can `use forge_jobs_ui::overview::SomePrivateHelper;` and now you
  can't refactor `overview.rs` without a breaking change.
- `forge-charts/src/lib.rs:73-79` — `axis`, `path`, `scale` (internal
  math) are `pub`.

**Fix:** demote internal sub-modules to `pub(crate)`. Keep only the
documented entry points public (`ipc`, `queue_root` in ui; the
top-level types in charts). The re-exports at lib.rs already surface
what consumers actually need.

---

## 🟢 Polish (not blocking)

### P1 — README claims don't match published reality
- `forge-jobs/README.md:30` — install snippet references
  `dandush03/forge` (correct future state) but `Cargo.toml` repository
  points at `tech-admin` (see B4)
- `forge-jobs/README.md` "What it doesn't give you" section links to
  `../forge-jobs-api/` and `../forge-jobs-ui/` — relative links break
  on crates.io. Either remove or rewrite to absolute URLs once
  published.
- `forge-jobs-api/README.md:30-33` and `forge-jobs/src/lib.rs:2-3`
  reference "Same bodies as the Tauri plugin's IPC commands" — the
  Tauri plugin (`tauri-plugin-queue`) lives in the tech-admin repo,
  not in this workspace. Reword to "shared with the in-process IPC
  binding" or land the plugin here.

### P2 — Defense-in-depth on the HTTP surface
- No `DefaultBodyLimit` on the router — Axum default is 2 MiB; set
  `DefaultBodyLimit::max(64 * 1024)` so future enqueue routes don't
  silently inherit a 2 MiB default.
- No path-param validation regex on `/queue/:name/backoff`. Storage
  layer is parameterized so SQLi is moot, but a clean 400 beats a
  silent 0-rows-updated.
- `tower-http`'s `cors` feature is declared but unused — drop it or
  decide on a default `CorsLayer`.

### P3 — Error message detail leak
`StorageError::Backend` is built from `sqlx::Error::to_string()` which
in some paths echoes SQL fragments + column names. Flows out via
`Error::Storage { msg }` → HTTP response body. On the HTTP boundary,
sanitize to a generic "internal storage error" for clients; keep the
detail in `tracing::error!` for the operator.

### P4 — `password_env` name leak in StorageError
[crates/forge-jobs/src/storage/database_config.rs:497-507](crates/forge-jobs/src/storage/database_config.rs#L497-L507)

The "did you mean a literal password?" hint embeds the env-var name
in the error message. The *name* (not value) is exposed if this error
ever flows through HTTP. Low impact; sanitize for HTTP.

### P5 — `PostgresConfig` derives `Debug` with the password field
[crates/forge-jobs/src/storage/database_config.rs:62-79](crates/forge-jobs/src/storage/database_config.rs#L62-L79)

`#[derive(Debug)]` dumps the literal password if anyone `?`-logs the
struct. No current site does, but `tracing::error!(?cfg, …)` would.

**Fix:** custom `Debug` impl that redacts `password` and shows only
the env-var name for `password_env`.

### P6 — `cargo-deny` not installed
Couldn't run the license / advisory check. Worth a one-time install
and clean run before publishing:

```
cargo install cargo-deny
cargo deny check
```

### P7 — `ulid_gen` failure silently falls back
`sqlite/jobs.rs:70`, `postgres.rs:167`. If `Generator::generate()`
returns `MonotonicError::Overflow` (≥ 2¹²⁸ ULIDs in the same ms —
implausible) FIFO ordering breaks invisibly. A `tracing::warn!` on
the fallback path costs nothing.

---

## ✅ What we already got right

The reviewers flagged these as **considered and intentional / correct**:

- `Mutex<HashMap>` poison handling in `running_jobs` registry
- SQLite refill clamp (H1 from prior review)
- Heartbeat cancel-signalled latch (M3 from prior review)
- License files present (M2 from prior review)
- All `format!`-built SQL outside the NOTIFY one only interpolates
  compile-time constants — not user data
- Migrations are additive; the one `DROP TABLE` is the standard
  SQLite 12-step rebuild
- No `unwrap`/`expect`/`panic!` outside test modules in non-test code
- No `inner_html` / `dangerous_inner_html` in any UI render path
- No SVG `<text>` elements with user-supplied strings — labels render
  as HTML `<div>` overlays so Leptos's default escaping covers them
- `window_event_listener` cleanup is correct (paired `on_cleanup`)
- Effect-loop hazards reviewed — no read-then-write-same-signal loops

---

## Recommended fix order

| Tier | Items | Why now |
|---|---|---|
| **A — fix before push** | B1, B2, B3, B4, B5, V1 | `cargo publish` will fail without A1-A4; V1 + B5 are real risks |
| **B — fix in same pass** | R1 (top 4 enums), R2, R3 | SemVer ratchet — adding `#[non_exhaustive]` later is a major break |
| **C — fix before crates.io public link** | P1, P3, P5 | Visible to first-impression readers |
| **D — punch list for 0.1.1** | V2, P2, P4, P6, P7 | Real but not embarrassing |

**Tier A + B is the publish-blocker set.** Roughly 2 hours of focused
work; I'd ship the whole bundle as one "pre-publish polish" commit
in `forge/`.
