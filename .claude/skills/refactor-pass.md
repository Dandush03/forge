# refactor-pass

Codifies the project's bar for a deliberate refactor sweep — both
line-level hygiene **and** design: SOLID and the layered architecture,
not just clean lines. Apply when the user asks for a "refactor pass,"
"clean up," "go file by file and fix the checklist," or otherwise asks
to bring code up to the standard below. Read `README.md` and the
workspace `Cargo.toml` `[workspace.lints]` first — forge has no
`AGENTS.md`; anything in those overrides what's here.

## Scope

This is a **pure-Rust Cargo workspace** with four crates:

- `forge-jobs` — the job-queue runtime + storage trait with SQLite
  (default) and Postgres (optional, `--features postgres`) adapters. This
  is the product; it carries the multi-replica correctness discipline in
  `.claude/skills/correctness-review.md`.
- `forge-jobs-api` — a thin Axum HTTP / in-process IPC adapter over the
  storage trait. Validate, delegate, map. No domain logic.
- `forge-jobs-ui` — a Leptos CSR panel, host-agnostic via the `QueueIpc`
  trait. Targets `wasm32-unknown-unknown`.
- `forge-charts` — a standalone pure-Rust + SVG chart library for Leptos
  CSR. Project-agnostic; no dependency on the jobs crates.

The checklist below applies to every crate. Skip checklist items whose
preconditions don't exist in the repo and flag the skip; don't invent
files to satisfy a rule from a different stack.

## Checklist (per file or logical unit)

### Compile cleanly
- `cargo check --workspace --all-targets` is green.
- `cargo clippy --workspace --all-targets -- -D warnings` is green.
  Workspace lints (`Cargo.toml [workspace.lints]`) include
  `unwrap_used = deny`, `panic = deny`, `expect_used = warn`, `todo =
  warn`, `dbg_macro = deny`, `print_stdout`/`print_stderr = warn`,
  `unsafe_code = deny`, `unreachable_pub = warn`, plus pedantic +
  nursery.
- `cargo fmt --all -- --check` is clean.
- `cargo test --workspace` is green. The wasm crates
  (`forge-jobs-ui`, `forge-charts`) also `cargo clippy --target
  wasm32-unknown-unknown` clean — a native-only check misses
  `web-sys`/wasm-bindgen breakage.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` is clean.
  **Zero warnings, whether or not you introduced them** — a refactor pass
  that walks a file leaves it warning-free, including pre-existing
  intra-doc-link breaks (ambiguous `fn`-vs-`mod` links want `[`name()`]`
  / `[`mod@name`]`; public docs must not link private items).

### Dead code
- Remove unused functions, types, modules, fields, and imports.
- `#[allow(dead_code)]` is banned. If something is genuinely intended for
  later, leave a `// TODO(owner):` with context, or remove it.
- `unreachable_pub` warnings: drop the `pub` if nothing outside the
  module needs it. The crates use `pub(crate) mod` with inner `pub` items
  as module-local API docs (see `forge-jobs-ui/src/lib.rs`); full crate
  `pub` only when a symbol genuinely crosses the crate boundary and is
  re-exported.

### Panic / error discipline
- No `.unwrap()` or `.expect()` in production paths. Tests and the
  reference-server `main` startup are the only exceptions.
- No `panic!`, `todo!()`, `unimplemented!()`, `dbg!()` in committed code.
- Each `thiserror` variant has a `#[error("...")]` message a caller could
  plausibly see (or that maps cleanly to a status code / IPC error in
  `forge-jobs-api`).
- `forge-jobs` surfaces typed errors via `thiserror`; `anyhow` only at the
  edges (the reference binary / app glue), never across a library
  boundary.

### Tracing, not prints
- No `println!` / `eprintln!` in non-test, non-binary code (`print_stdout`
  / `print_stderr` are warn-level lints).
- In `forge-jobs-ui` (wasm), diagnostics go through
  `web_sys::console`, not stray prints; user-facing failures surface in
  the panel, not only the console.
- Sensitive values (connection strings, job payloads that may carry
  secrets) are not logged at their full value. Don't log a payload blob
  at `INFO` on every dispatch.

### Readability / shape
- Functions: soft target ~20 lines of executable code. A struct
  constructor, a `match` with many short arms, or a Leptos `view!` can be
  longer; a function doing five unrelated things must be split.
- Cyclomatic complexity stays low — prefer flat early-return over nested
  `if let`s; use `let … else`, `?`, and helper functions.
- Names spelled out: no `cfg`, `mgr`, `q`, `idx` for new code unless the
  scope is < 5 lines. Existing short names can stay if changing them
  produces churn without value.

### Docs
- Public items in `forge-jobs` / `forge-charts` (the crates.io-published
  surface): at least one `///` line explaining *what* and (where
  relevant) *when not to use it*. No multi-paragraph docstrings unless the
  API is genuinely subtle.
- Inline `//` comments only when the *why* is non-obvious. Do not restate
  the code. (The existing code leans on "why" comments heavily — match
  that density, don't add narration.)

### Tests
- Every `pub` function in `forge-jobs` has at least one test that
  exercises it.
- Every `Err` variant is produced by at least one test.
- Storage behavior is tested against **both** adapters where it matters —
  a fix to a SQL predicate in `storage::sqlite` wants a parity test (or a
  note) for `storage::postgres`.
- No `std::thread::sleep` for timing; use `tokio::time::pause()` /
  `advance()` for time-dependent runtime code.
- No live network/DB in unit tests; SQLite tests use a `tempfile` DB,
  Postgres integration tests are gated (e.g. behind an env var /
  testcontainers) and don't run in the default `cargo test`.

### Layering
- `forge-jobs` does not depend on `axum`, `leptos`, or any HTTP/UI crate.
  The runtime is storage-trait-driven.
- `forge-jobs-api` handlers do exactly three things: validate, delegate
  to the storage trait, map back to a DTO / IPC result. No queue logic
  here.
- `forge-jobs-ui` reaches the backend only through the `QueueIpc` trait —
  never a direct dependency on `forge-jobs-api`'s transport. The HTTP
  `QueueIpc` impl is behind the `http` feature so the base panel stays
  transport-agnostic.
- `forge-charts` depends on neither the jobs crates nor any app — it's a
  reusable library. A jobs-specific assumption creeping into it is a
  finding.

### SOLID & architecture (not just clean lines)
The checks above are line-level (unwrap, prints, naming) and the Layering
block is *cross-crate* dependency direction. Neither catches a **design**
break — the right code in the wrong place, a fat trait, a branch that
should have been an extension point. A line-clean file can still be a
design finding. So per file *and* per logical unit, evaluate the
architecture and each SOLID principle as it applies to Rust:

- **S — Single Responsibility.** A module must not outgrow its name; a
  file named for one concern that has accreted a second is a finding even
  if every line is clean. A storage adapter file is storage; runtime
  scheduling logic that leaks into it gets extracted, not "tidied in
  place." A `forge-jobs-api` handler is a shell, not a brain: validate →
  delegate → map; branching queue logic in a handler is a finding. One
  source of truth per rule — the throttle-decay predicate must not be
  reimplemented divergently in the two adapters; both encode the same
  contract.
- **O — Open/Closed.** Extend by *adding* (a new module, enum variant,
  trait impl), not by editing a stable core. A new storage backend is a
  new adapter implementing the trait, not `if backend == …` branches
  sprayed across the runtime. A change that bolts a special case onto a
  core dispatch function instead of factoring an extension point is a
  finding.
- **L — Liskov.** Every trait impl honors the trait's contract — same
  pre/postconditions, no surprise panics where the trait implies a total
  operation. The SQLite and Postgres storage impls must be substitutable:
  a caller of the storage trait must not be able to tell which it got
  (modulo documented capability gaps).
- **I — Interface Segregation.** Small, purpose-specific traits over fat
  ones; a caller (or a test stub) must not depend on methods it never
  calls. `QueueIpc` and the storage trait should expose cohesive method
  sets; a trait that forces unrelated methods together, or that exists
  only so one caller can reach one method, is a finding — split it.
- **D — Dependency Inversion.** High-level logic depends on abstractions
  injected at the edge. The runtime takes the storage trait; the UI takes
  `QueueIpc`; neither hard-codes a concrete transport or DB. If you reach
  for a concrete IO type inside the runtime core, invert it behind the
  trait the edge supplies.

A correctness-review pass hunts races / load / logic-bugs; it will *not*
flag a design break. That blind spot is why this section exists — judge
the architecture, not just the lines.

### Unsafe
- `unsafe_code` is `deny` workspace-wide. There should be no new `unsafe`;
  if a genuine need arises it's an explicit, justified exception with a
  comment proving soundness, not a quiet `#[allow]`.

## Workflow

1. **Order.** Walk a crate top-down using
   `tree -I 'target|*.lock|*.png|*.svg' crates/<crate>`. Track progress in
   `.claude/REFACTOR_LOG.md` (create it on the first sweep).
2. **Per file.**
   - Read it fully.
   - Apply the checklist — including the **SOLID & architecture** pass,
     not only the line-level items. Ask explicitly: does this file do one
     thing its name names (S); does it extend by adding rather than
     editing a stable core (O); do its trait impls honor their contracts,
     including SQLite⇄Postgres substitutability (L); are its traits narrow
     (I); does it depend on injected abstractions rather than a concrete
     DB/transport (D)?
   - Keep edits minimal — do not refactor adjacent unrelated code. But a
     design break **is** in scope: extract it into the correct module,
     don't tidy it in place.
   - Mark the row in `REFACTOR_LOG.md` as `done`, `clean`, or `skip`.
3. **Commit cadence.** Per logical change, not necessarily per file. If
   file A's edit forces an edit in file B (rename, type change), commit
   them together. Commit subject follows the repo's convention:
   `refactor(<crate>): <area>`. Each commit must compile and pass
   `fmt + clippy + test`.
4. **Commit only on the user's go.** This skill authorizes commits when
   the user explicitly asked for a refactor sweep; it does not grant
   standing authority for other work. Branch off `main` first if you're on
   it. Never use `--no-verify`; if a hook fails, fix it and re-stage.
5. **Adapter parity is special.** If the refactor touches a SQL predicate
   or a storage-trait method, check the *other* adapter for the same
   change and either apply it or note why it differs. A divergence
   introduced by a one-sided refactor is the worst kind of regression
   here — silent until production flips to Postgres.
6. **Tests can land separately.** If `forge-jobs` is missing tests for
   `pub` items, file a TODO with the items rather than blocking the
   refactor commit. New behavior added during the refactor must ship with
   its test.

## Anti-patterns to watch for in this codebase

- `.unwrap()` / `.expect()` in a worker loop or storage path (lint-denied,
  but watch for `#[allow]` smuggling it back).
- A SQL predicate fixed in one adapter (`storage::sqlite`) and not the
  other (`storage::postgres`) — divergent behavior across backends.
- `format!()`-ing a value into SQL instead of `.bind(...)`; interpolating
  an unescaped queue/kind identifier into a `NOTIFY` channel.
- `chrono::Utc::now()` used where the DB's `now()` is the authority (or
  vice-versa) on the throttle / schedule / retry timing columns.
- `std::thread::sleep` or a real wall-clock wait in a test — use
  `tokio::time` pause/advance.
- A `forge-jobs-api` handler that branches on queue logic instead of
  delegating to the storage trait (a brain, not a shell).
- `forge-jobs-ui` reaching past `QueueIpc` for a concrete transport, or
  `forge-charts` growing a jobs-specific assumption.
- Native `window.confirm()` / `window.prompt()` / `alert()` in
  `forge-jobs-ui` — these silently no-op in the Tauri webview; confirm
  in-DOM instead (see `confirm.rs`).
- A reactive timer / poller created only inside a Leptos `Effect`'s
  deferred first run when it must run at mount — establish it
  synchronously (see the poll-timer pattern in `queue_root.rs`).
- `map(f).unwrap_or(default)` → `map_or(default, f)`.
- `Duration::from_millis(N * 1000)` → `Duration::from_secs(N)`.
- `n % 2 == 0` → `n.is_multiple_of(2)`.
- Adding `pub` where `pub(crate)` would do (`unreachable_pub`).
- A library-crate function returning `anyhow::Error` across its public
  boundary instead of a typed `thiserror`.
- A public item's docs linking a private item, or an ambiguous
  `fn`-vs-`mod` intra-doc link — both fail `cargo doc -D warnings`.

## When to stop

End the pass when every row in `REFACTOR_LOG.md` is filled and the full
workspace passes `fmt + clippy + test + doc` (the doc gate is
`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, zero
warnings whether or not you introduced them), and the wasm crates pass
`clippy --target wasm32-unknown-unknown`. Summarize remaining TODOs
(missing tests, **any SOLID/architecture finding too large to fix in this
pass** — e.g. a module split that needs its own PR, any adapter-parity gap
you flagged) in the final response — do not silently leave them off the
log.
