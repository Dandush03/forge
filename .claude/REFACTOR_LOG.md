# Refactor log

Per-file refactor-pass status (see `.claude/skills/refactor-pass.md`).
`clean` = reviewed, met the bar as-is. `done` = reviewed + edited.
`skip` = out of scope this sweep.

## 2026-06-13 — per-worker queue-affinity branch (`feat/queue-affinity`)

Scope: the files the affinity feature + the H3/L7–L12 fixes touched.
Gates at sweep end: `fmt`, `clippy --workspace --all-targets`, `clippy
--target wasm32-unknown-unknown` (ui + charts), `RUSTDOCFLAGS=-D warnings
cargo doc`, and `cargo test --workspace` all green.

| File | Status | Notes |
|------|--------|-------|
| `forge-jobs/src/storage/types.rs` | clean | `PodRecord::handles` is the single eligibility predicate (S — one source of truth); `encode/decode_queues` + `validate_queue_name` cohesive with the type they serialize. |
| `forge-jobs/src/runtime.rs` | clean | `with_queues`/`with_worker_name` builders; `start()` validate→ensure→spawn; `queues_from_env`/`worker_name_from_env` small + documented. |
| `forge-jobs/src/runtime/rebalance.rs` | clean | `rebalance_once` is one cohesive pass; left un-extracted — splitting a linear algorithm into helpers would hurt readability more than the length costs. |
| `forge-jobs/src/storage.rs` | clean | Trait additions (`list_slot_assignments`, `pod_heartbeat` args, `list_live_pods` → `PodRecord`) are cohesive; both adapters implement them (L — substitutable). |
| `forge-jobs/src/storage/error.rs` | clean | New `Config` variant has a caller-meaningful `#[error]` message. |
| `forge-jobs/src/storage/postgres.rs` | clean | `pod_heartbeat`/`list_*` bind all values; no `format!` into SQL; uses the shared CSV helpers (no adapter divergence). |
| `forge-jobs/src/storage/sqlite/procs.rs` | done | Removed the duplicated `encode/decode_queues` (now shared in `storage::types`) — was an SRP/one-source-of-truth smell across adapters. |
| `forge-jobs-api/src/dto.rs` | done | Collapsed the `unassigned_queues` triple-nested scan to a `served` HashSet lookup (readability + O(Q·W·s)→O(Q+W·s)). Handler-shaped assembly, no queue logic. |
| `forge-jobs-api/src/handlers.rs` | clean | `queue_workers` is a shell: gather → delegate → map via the DTO. No queue logic. |
| `forge-jobs-api/src/router.rs` | clean | One route added, mirrors the existing `get(...)` shells. |
| `forge-jobs-ui/src/workers.rs` | clean | Poll timer established synchronously at mount + `Effect` for changes (matches the `queue_root` pattern, avoids the deferred-first-run anti-pattern); failures surface in-panel; no stray prints/`alert`. |
| `forge-jobs-ui/src/ipc.rs` | clean | `Worker`/`WorkerSlot`/`WorkersOverview` are plain DTOs with serde defaults; reached only through the `QueueIpc` trait. |
| `forge-jobs-ui/src/http.rs` | clean | `queue_workers` impl mirrors the other `get` calls. |

### Deliberate non-changes (flagged, not silently dropped)
- **`validate_queue_name` lives at the `start()` gate, not `QueueConfig::ensure_queue`.** That fully closes the CSV-corruption path (only declared queues are CSV-encoded). Moving name validation to the storage boundary would cover direct `ensure_queue` callers too, but it widens the `QueueConfig` contract across both adapters — a follow-up, not this sweep.
- **`queue_workers` issues four sequential storage reads.** Independent; a `try_join!` would cut tail latency. An efficiency nicety, not a design break — left for a focused change if the Workers tab cadence ever matters.
