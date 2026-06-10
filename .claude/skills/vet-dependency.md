# vet-dependency

The checklist for adding a **new direct dependency** to the workspace.
Apply whenever you're about to `cargo add` something, or the user asks
"can we use <crate>," "what should we pull in for X," or "is <crate> OK
to depend on." Read `README.md` and the root `Cargo.toml` first — forge
has no `AGENTS.md`; the dependency conventions live in the manifests.

The bar: *prefer std and existing transitive deps; a new direct dep needs
a one-line justification; keep the surface narrow.* `forge-jobs` and
`forge-charts` **publish to crates.io**, so every dependency is part of
their public cost — the default answer to "should we add this?" is *can
we not?*

The current direct stack is small and deliberate:

- **Runtime / async:** `tokio`, `tokio-util`, `async-trait`.
- **Storage:** `sqlx` (SQLite default; Postgres optional behind the
  `postgres` feature), `uuid` (postgres feature).
- **HTTP transport (`forge-jobs-api`):** `axum`.
- **Serde / data:** `serde`, `serde_json`, `chrono`.
- **Errors / logging:** `thiserror` (libraries), `anyhow` (edges/binaries),
  `tracing`, `tracing-subscriber`.
- **UI / wasm (`forge-jobs-ui`, `forge-charts`):** `leptos` (CSR),
  `wasm-bindgen`, `web-sys`, plus the panel's `fetch` client behind the
  `http` feature.
- **Testing:** `tempfile`, `tokio` test-util.

A new dep that **overlaps** any of these is the high bar: it needs a real
justification, not a quiet `cargo add`.

## Steps

1. **Is it already in the tree?** Before adding anything, check whether a
   crate you already depend on covers it:
   - `cargo tree -i <crate>` / `cargo tree | grep <crate>` — is it
     already a transitive dep you could promote, or does a dep already
     expose this (e.g. a `tokio`/`sqlx`/`axum` feature)?
   - Can std do it? Timers, hashing, simple parsing often don't need a
     crate.
   - Does a crate already in `[workspace.dependencies]` cover it with one
     of its features?
   If yes, **stop** — no new direct dep.

2. **Is it forbidden / redundant?** Reject by default:
   - A **second async runtime** (`async-std`, `smol`) — we are
     `tokio`-only.
   - A **second HTTP/server stack** next to `axum`, or a second SQL layer
     next to `sqlx`.
   - Anything duplicating an existing capability — another date lib next
     to `chrono`, another error lib next to `thiserror`/`anyhow`, another
     JSON lib next to `serde_json`, another async-trait shim.
   - Anything that pulls native TLS / `openssl` by default when `rustls`
     would do, or that drags a heavy transitive tree into the
     **published** `forge-jobs` / `forge-charts` crates.
   - A **non-wasm-compatible** dep added to `forge-jobs-ui` or
     `forge-charts` — they compile to `wasm32-unknown-unknown`; a crate
     that needs threads, the filesystem, or native sockets breaks the
     build there.
   If it's one of these and genuinely needed, that's a deliberate,
   documented decision (a note in the PR / README), not a quiet add.

3. **Vet the crate itself:**
   - **Version**: `cargo info <crate>` / crates.io — latest stable,
     release recency, maintenance signal. Pin to the precision the
     workspace uses (mostly major-only via `[workspace.dependencies]`).
   - **Features**: list them and take the **minimum**. Don't pull a
     default feature set that drags in a TLS stack, a second runtime, or
     `openssl` you don't want. Match how the existing deps are scoped
     (e.g. `tokio`'s explicit feature list).
   - **Layer fit**: state which crate(s) will use it. A dep used from
     `forge-jobs` must not bind the crate to a transport or UI. A dep used
     from `forge-jobs-ui` / `forge-charts` **must build on wasm**. A
     Postgres-only dep belongs behind the `postgres` feature, like `uuid`.
   - **License + advisories**: run `cargo deny check` if a `deny.toml`
     exists, else at least check the license is permissive
     (MIT/Apache-2.0, matching this workspace's `MIT OR Apache-2.0`) and
     scan crates.io / `cargo audit` for advisories.

4. **Place it correctly:**
   - Shared across crates → `[workspace.dependencies]` in the root
     `Cargo.toml`, then `<dep>.workspace = true` in members.
   - Single crate → that crate's `Cargo.toml`.
   - Optional / backend-specific → behind a Cargo feature (mirror the
     `postgres = ["sqlx/postgres", …]` pattern).
   - Match the existing alignment of the deps block by hand; `cargo fmt`
     doesn't touch `Cargo.toml`.

5. **Write the justification.** One line for the PR body: *what it does,
   why std/existing deps don't, which crate uses it, feature set taken,
   wasm-safe if it lands in a UI crate.* Example: "`uuid` 1 — stable job
   IDs for the Postgres adapter; SQLite uses rowids so it's gated behind
   the `postgres` feature; default features only."

## Output

Report: the crate + version + chosen features, where it's declared
(workspace vs crate, behind which feature), the advisory/license result,
and the one-line justification. Then **stop** — adding a dep is a normal
edit; list the changed `Cargo.toml`(s) and let the user commit. If the
verdict is "don't add it," say so and name the std/existing-dep path
instead.

## Red flags that mean "don't, or document a deliberate exception first"

- Brings its own async runtime, server stack, or `openssl`/native-TLS by
  default.
- Doesn't build on `wasm32-unknown-unknown` but is wanted in
  `forge-jobs-ui` / `forge-charts`.
- Unmaintained (no release in years) or a single-maintainer crate on the
  queue's hot dispatch path.
- Pulls dozens of transitive crates for a small need — especially into a
  **published** crate.
- Duplicates `tokio`, `sqlx`, `axum`, `serde`/`serde_json`, `chrono`,
  `thiserror`/`anyhow`, `tracing`, `async-trait`, `leptos`.
- A `forge-jobs` runtime need that bolts a transport/UI assumption onto
  the core — keep the runtime storage-trait-driven and put the concrete
  dep at the edge instead.
