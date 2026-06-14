# forge-jobs correctness / failover / scaling review

**Reviewed:** `forge-jobs` — queue runtime (`runtime.rs` + submodules) and
both storage adapters (`storage/sqlite/`, `storage/postgres.rs`), plus the
exposure surfaces in `forge-jobs-api`.
**Commit:** `4e64db5` · **Date:** 2026-06-10
**Refreshed:** 2026-06-13 — reviewed the per-schedule cron `dedupe_key`
(skip-if-in-flight) feature on the uncommitted 0.2.1 diff; appended **L6**.
**Refreshed:** 2026-06-13 — reviewed the per-worker queue-affinity +
Workers-tab feature on the uncommitted 0.2.x diff (`runtime.rs`,
`runtime/rebalance.rs`, the new `pod.worker_name`/`queues` columns + both
adapters, `forge-jobs-api` `/queue/workers`, `forge-jobs-ui` `workers.rs`);
appended **H3** and **L7–L12**. Pure DRY/efficiency observations from that
pass (duplicated `encode/decode_queues`, duplicated heartbeat-dot
thresholds, duplicated tab-poll boilerplate, the quadratic DTO scans) are
**out of scope here** — they belong to `refactor-pass`.
**Scope:** correctness, races, failover, cross-replica parity,
SQLite⇄Postgres parity, scaling, leak/exposure.
**Out of scope:** style/lint — `clippy -D warnings` and `fmt` are assumed
clean (verified at review time) and not re-litigated here.
**Severity target:** a multi-replica Postgres deployment sharing one queue
(the `--features postgres` k8s path). Findings that can also fire on the
default single-process SQLite build say so explicitly; ones marked
*PG-multi-replica only* cannot fire on the current Tauri/SQLite build.

## Architecture orientation

- **Four storage traits** (`JobQueue`, `ProcessRegistry`, `QueueConfig`,
  `CronStorage`) + `RateLimitStorage`, bundled in `Storage`. Two adapters:
  SQLite (WAL, split read/write pools, in-process `Notify` hub) and
  Postgres (`FOR UPDATE SKIP LOCKED`, `LISTEN/NOTIFY`).
- **Runtime loops** spawned by `QueueRuntime::start`: one supervisor per
  queue (scales workers to the pod's slot assignment), a reaper (revives
  `in_progress` rows with stale heartbeats), a cleanup task (retention), a
  cron service, a pod-heartbeat loop, a rebalancer, and a metrics roller.
- **Worker cycle:** `claim_next` (atomic single-statement claim; increments
  `attempts`, stamps `process_id`/`heartbeat_at`) → handler runs with a
  per-job cancel token → `finalize` (Done / Failed / Dead / Throttled) →
  idle wait on the storage's `wait_for_work`.
- **Coordination primitives:** a single cluster lease row (`cron_leader`,
  CAS-upsert keyed on DB `now()` in PG) elects the coordinator for cron,
  cleanup, rebalance, and metrics; a CAS on `cron_schedule.next_fire_at`
  (`try_advance_fire`) fences cross-replica double-fires; a queue-wide
  throttle gate (`queue.throttled_until` + `throttle_attempts` exponent
  with a 120 s decay grace) backs the whole fleet off a rate-limited
  upstream; per-job cancellation flows through `cancel_requested_at`
  observed by the worker's 10 s heartbeat tick.
- **Identity:** `host_id` is a fresh ULID per process boot; worker
  `process_id` = `{queue}-{slot}-{host_id}`. Job ids are monotonic ULIDs
  (FIFO within priority).

## HIGH

### H1 — Worker `finalize` carries no ownership guard; a stale worker's late finalize clobbers another worker's active claim

- **Where:** trait gap at `storage.rs::JobQueue::finalize` (no
  `process_id` parameter), exploited identically in both adapters:
  `sqlite/jobs.rs::finalize_done` / `finalize_throttled` (`WHERE id = ?2`
  only) and the finalize-path `append_error_and_update` with
  `guard_stale_before: None`; `postgres.rs::finalize_done` /
  `finalize_throttled` / `append_error_and_update(…, None)` likewise.
- **Why it matters (fires on SQLite too):** worker W1 claims job J, then
  stalls past the 60 s `STALE_THRESHOLD` (blocking handler, laptop
  suspend on the Tauri host, DB contention starving the heartbeat task).
  The reaper revives J → `failed`; W2 claims it (`in_progress`,
  `attempts + 1`). W1's handler eventually returns and `finalize` fires
  with only `WHERE id = …`:
  - W1 finalizes **Done** → J flips to `done` *while W2 is still
    executing it*; W2's later finalize re-transitions the row, and the
    queue-wide cool-down may be cleared mid-window.
  - W1 finalizes **Throttled** → J flips back to `pending` while W2 runs
    → a third worker can claim it → the same job genuinely running on
    2–3 workers concurrently. This is the double-execution amplifier:
    at-least-once delivery is expected, but unbounded *concurrent*
    execution of one row is not.
  The codebase already recognizes this exact class: the **reaper** path
  threads `guard_stale_before` into `append_error_and_update` so a
  revived-then-finalized row isn't clobbered. The **worker** path has no
  equivalent.
- **Fix:** add `process_id: &str` to `JobQueue::finalize` and guard every
  finalize UPDATE with `AND process_id = $N AND status = 'in_progress'`.
  Zero rows back = "I lost ownership" → log and return without touching
  the row (and without clearing/extending the queue cool-down). Mirror in
  both adapters in the same commit; add a smoke test that revives + re-
  claims and asserts the stale worker's finalize is a no-op.

### H2 — PG `wait_for_work` opens a dedicated `PgListener` connection per idle cycle — connection churn storm at scale *(PG-multi-replica only)*

- **Where:** `postgres.rs::wait_for_work` (`PgListener::connect_with`
  per call), driven by `runtime.rs::idle_wait` with `IDLE_POLL = 500 ms`.
- **Why it matters:** every idle worker calls `wait_for_work` in a loop;
  each call opens a brand-new dedicated Postgres connection, runs
  `LISTEN`, waits ≤ 500 ms, and drops it. One idle worker ≈ 2 connection
  setups/teardowns per second; a pod at `WORKER_CAP = 64` across a few
  queues ≈ >100/s, multiplied by replicas. That's TLS/auth handshake CPU
  on the DB, `max_connections` pressure against the pool the *workers*
  also need, and pg log spam — a scaling cliff on the exact path that's
  supposed to be cheap (idle).
- **Fix:** one long-lived listener task per process (a single `PgListener`
  can `listen` on many channels) that fans notifications out to an
  in-process per-queue `tokio::sync::Notify` hub — i.e. reuse the SQLite
  `NotifyHub` shape with the PG listener as the producer. `wait_for_work`
  then blocks on the in-process notify, exactly like the SQLite adapter.

### H3 — A configured queue that no live worker declares is silently unserved — jobs sit `pending` forever

- **Where:** `runtime.rs::QueueRuntime::start` (now spawns supervisors only
  for `self.queues`, no longer for every `config.list_queues()` row) +
  `runtime/rebalance.rs::rebalance_once` (the new `eligible` filter
  `p.queues.iter().any(|n| n == &q.name)` and the zero-out pass that
  follows it). Eligibility is decoded from `pod.queues` via
  `storage::sqlite::decode_queues` / `storage::postgres::decode_queues`
  (NULL/empty CSV → eligible for *no* queue). The sole operator-visible
  guard is `forge-jobs-api::dto::workers_overview_dto`'s
  `unassigned_queues`, rendered as the `forge-jobs-ui::workers` banner.
- **Why it matters (fires on every build, SQLite included):** the prior
  contract was *every pod runs every configured queue*, so a configured
  queue was always served by someone. Affinity inverts that: a queue runs
  only if some live pod's `FORGE_QUEUES` / `with_queues` set names it. A
  single typo (`FORGE_QUEUES=guthub`), or a queue added to `config` but to
  no worker's set, means every job enqueued there is never dequeued, never
  run, never requeued — it accumulates `pending` forever. `start()`'s
  fail-fast only catches the *totally empty* set, not a wrong/partial one.
  On a rolling Postgres upgrade the window widens to the whole fleet: a
  pre-upgrade pod (and any new pod before its first heartbeat writes the
  `queues` column) decodes to an empty set, so for every queue `eligible`
  is empty, `fair_shares(total, 0)` returns nothing, *and* the zero-out
  pass actively pins each such pod to 0 slots — all queues stall until
  every pod has re-heartbeated with the new code and a later rebalance
  tick runs. Self-healing for the upgrade case; permanent for the
  misconfig case. The new Workers-tab banner is the only signal, and only
  if someone is watching it. The new tests (`only_pods_that_declared_the
  _queue_get_slots`, `queue_with_no_eligible_pod_gets_no_positive_slots`)
  *encode this as intended* — they assert the queue is correctly starved —
  so the behavioral risk is unguarded by any alarm.
- **Fix:** make the coordinator loud, not just the panel — in
  `rebalance_once`, when a configured queue's `eligible` set is empty,
  `tracing::warn!(queue = %q.name, "rebalance: no live worker declares
  this queue; its jobs will not run")` so it reaches logs/alerts. Document
  the operational contract (every configured queue must appear in some
  worker's `FORGE_QUEUES`) in `operating-at-scale.md`. For the upgrade
  transient, distinguish a NULL `queues` column (pre-upgrade, *unknown*)
  from an empty CSV (declared none) at the `decode_queues` boundary and
  treat NULL as eligible-for-all for one `STALE_THRESHOLD`, so a
  pre-upgrade fleet keeps draining until it re-heartbeats.

## MEDIUM

### M1 — `heartbeat_job` observes lost ownership and ignores it; the stale worker keeps executing

- **Where:** `runtime.rs::heartbeat_loop` (`Ok(_) => {}` arm) +
  `sqlite/jobs.rs::heartbeat_job` / `postgres.rs::heartbeat_job` (0 rows
  = "row vanished or process_id no longer owns it; both treated as 'no
  cancel'").
- **Why it matters:** in the H1 scenario, the system *already knows* W1
  lost the row — its heartbeat UPDATE matches nothing — but nothing
  signals W1's handler, so it runs to completion (external side effects
  and all) and then attempts the clobbering finalize. With H1's guard in
  place the finalize becomes a no-op, but the duplicated side-effect
  window stays as long as the handler feels like running.
- **Fix:** make `heartbeat_job` return a tri-state (or have the runtime
  treat `None`-row as cancellation): on lost ownership, trip `job_cancel`
  just like a user cancel so the handler unwinds promptly. Cheap, and it
  converts unbounded concurrent execution into a ≤ `HEARTBEAT_INTERVAL`
  overlap.

### M2 — A cancel that lands after the handler finishes marks completed work `dead`

- **Where:** `runtime.rs::worker_loop` — `user_cancelled` is evaluated
  *after* the handler returns and overrides the handler's outcome with
  `FinalizeOutcome::Dead { "cancelled by user" }`.
- **Why it matters:** the heartbeat tick may set `job_cancel` from the DB
  flag moments before/while the handler returns `Done`. The work
  *happened* (the e-mail sent, the API call committed), but the row is
  recorded `dead — cancelled by user`. An operator reading the Dead tab
  reasonably retries it → genuine double execution, operator-induced but
  system-invited. The reverse window also exists: a cancel requested in
  the last ≤ 10 s of a short job is silently outraced by a normal `Done`
  — acceptable (cancel is best-effort) but worth stating in the docs.
- **Fix:** don't override a successful outcome: if the handler returned
  `Done`, finalize `Done` regardless of `job_cancel` (the cancel arrived
  too late, and saying so is more truthful). Keep the Dead override only
  for non-`Done` outcomes (where it prevents the backoff-retry loop the
  comment describes).

### M3 — Cron schedules evaluate in the server's local timezone

- **Where:** `runtime/cron.rs::next_cron_local`
  (`now.with_timezone(&Local)`).
- **Why it matters:** two replicas with different `TZ` (one container
  UTC, one host-local — common in mixed dev/k8s fleets) compute
  *different* `next_fire_at` for the same expression, so the effective
  cadence changes whenever cron leadership moves. The `try_advance_fire`
  CAS prevents double-*enqueues* but can't fix divergent schedules. DST
  transitions add the classic skip/double behavior for expressions in the
  2–3 a.m. range.
- **Fix:** evaluate in UTC by default (`sched.after(&now)` on the UTC
  time), or store an explicit IANA timezone per schedule row and evaluate
  in that. Either way, identical inputs must produce identical
  `next_fire_at` on every replica.

### M4 — PG single-row `requeue` lacks the dedupe-sibling pre-filter — unique-violation error where SQLite skips gracefully

- **Where:** `postgres.rs::requeue` (raw `UPDATE … SET status='pending'`).
  Compare: `sqlite/jobs.rs::requeue` uses `UPDATE OR IGNORE`;
  `postgres.rs::requeue_batch_by_status` *has* the
  `NOT EXISTS (… active sibling …)` predicate.
- **Why it matters:** retrying a `failed`/`dead` row whose `dedupe_key`
  has an active sibling trips the `jq_dedupe` partial UNIQUE index. On
  SQLite the operator's Retry button quietly returns "not requeued"; on
  Postgres the same click surfaces a 500 (`unique_violation` →
  `StorageError`). Same logical operation, different behavior per
  backend — exactly the parity class this review exists for.
- **Fix:** add the batch variant's `NOT EXISTS` active-sibling predicate
  to the single-row `requeue` (one-line WHERE addition), and a parity
  test that requeues a dead row with an active sibling on both adapters.

### M5 — API 500 bodies echo raw storage error strings (leak class)

- **Where:** `forge-jobs-api/src/error.rs::From<StorageError>` — the
  fallthrough arm `Self::Storage { msg: other.to_string() }`;
  `StorageError::Backend` wraps raw sqlx/driver error text.
- **Why it matters:** Postgres driver errors can carry connection detail
  (host, db, user), SQL fragments, and constraint names; SQLite errors
  carry file paths. All of it lands verbatim in the JSON body of a 500
  for any caller of an (by-design unauthenticated) HTTP surface. The
  caller needs "storage error, try again / call the operator", not the
  driver's internals. Leak findings rank independent of deployment scale.
- **Fix:** map the fallthrough arm to a generic
  `Storage { msg: "storage backend error" }` and `tracing::error!` the
  full string server-side (with the request route) so operators keep the
  detail in logs.

### M6 — A fresh pod runs the full cluster `max_workers` until its first slot assignment

- **Where:** `runtime.rs::resolve_target` — `Ok(None) =>
  q.max_workers` fallback (documented as intentional).
- **Why it matters:** during a rolling deploy every new pod spends its
  pre-first-rebalance window (up to `REBALANCE_TICK` + lease handoff)
  running the *entire* cluster total. N replacement pods → transient
  ~N× over-parallelism, aimed at exactly the upstreams the cluster-wide
  rate budget exists to protect; the token bucket absorbs some of it,
  but the thundering claim herd also spikes DB contention.
- **Fix:** bound the fallback — e.g. `max_workers /
  max(live_pods_estimate, 1)` (one `list_live_pods` read), or simply
  clamp the pre-assignment fallback to a small constant (1–2 workers).
  The next rebalance restores the fair share either way.

## LOW

### L1 — PG `revive_stale`'s `FOR UPDATE SKIP LOCKED` runs in autocommit, so it locks nothing past the SELECT

- **Where:** `postgres.rs::revive_stale` — `fetch_all(&self.pool)` with
  no enclosing transaction; row locks release at statement end.
- **Why it matters:** the lock reads as the cross-reaper fence, but the
  actual fence is the `status = 'in_progress' AND heartbeat_at < $N`
  guard inside `append_error_and_update` (which works). Two replicas'
  reapers can both scan the same rows; the loser's writes no-op but its
  `revived` count over-reports, and the misleading primitive invites a
  future edit that trusts it.
- **Fix:** either wrap scan + updates in one transaction (making the
  `SKIP LOCKED` real), or drop the `FOR UPDATE` clause and a comment
  stating the guard is the fence.

### L2 — PG `delete_batch_by_status`: the two subqueries see different snapshots → orphan `queue_event` rows

- **Where:** `postgres.rs::delete_batch_by_status` — two statements in
  one READ COMMITTED tx; each statement's subquery takes its own
  snapshot, so a row that becomes eligible between them is deleted with
  its events left behind (the SQLite twin is safe under the single-writer
  lock).
- **Why it matters:** chart-only skew (ghost `started` events), no
  job-state impact.
- **Fix:** materialize the victim ids once (`WITH victims AS (SELECT id …
  FOR UPDATE SKIP LOCKED) DELETE …`) or delete events by joining the
  second statement's id set.

### L3 — SQLite `clear_queue_cooldown` re-reads the wall clock instead of using the caller's `now`

- **Where:** `sqlite/jobs.rs::clear_queue_cooldown`
  (`Utc::now()` for `decay_before`; the PG twin derives it from the
  passed `now`).
- **Why it matters:** ms-level drift between the row's `updated_at` and
  the decay comparison; purely cosmetic today, but it's a parity seam —
  the two adapters compute the same predicate from different instants.
- **Fix:** derive `decay_before` from the `now_iso` parameter, mirroring
  PG.

### L4 — `delete()` on an `in_progress` row reports `true` ("deleted") for what is actually a cancel request

- **Where:** `sqlite/jobs.rs::delete` / `postgres.rs::delete` (documented
  in `storage.rs`).
- **Why it matters:** callers (bulk delete in the panel) count the row as
  deleted; it remains visible until the worker finalizes and a *second*
  delete removes it. Operators read "deleted N" and still see rows. The
  contract is documented, but the boolean conflates two different
  outcomes.
- **Fix:** consider a tri-state return (`Deleted` / `CancelRequested` /
  `NotFound`) at the trait level next time the trait takes a breaking
  change; until then, surface "cancellation requested" in the panel when
  the deleted row was in-progress.

### L5 — Coordinator lease uses the DB clock; pod liveness uses the app clock

- **Where:** `postgres.rs::try_cron_lease` (`now()` both sides — good,
  self-consistent) vs `rebalance.rs::rebalance_once` / `reap_stale`
  (`Utc::now()` bound from the app).
- **Why it matters:** with app↔DB clock drift, the leader (DB clock) can
  compute the live-pod set (app clock) against a skewed staleness
  horizon; bounded by drift magnitude and self-healing, but worth knowing
  when debugging "pod flapped out of the live set".
- **Fix:** none required; document, or move `stale_before` computation
  into SQL (`now() - interval`) on PG for one clock domain.

### L6 — A cron fire that collapses on its `dedupe_key` still records as fired — skip-if-in-flight is invisible

- **Where:** `runtime/cron.rs::process_row` (the `storage.jobs.enqueue`
  match arm) + `try_advance_fire` in both adapters.
- **Why it matters:** the new per-schedule `dedupe_key` collapses an
  overlapping fire to a no-op — `JobQueue::enqueue` returns
  `EnqueueOutcome::Deduped(existing)` and inserts nothing. But
  `process_row` matches `Ok(_) => report.fired += 1`, so a skip counts as
  a fire; and `try_advance_fire` has already set `last_fired_at = now` and
  cleared `last_error` *before* the enqueue runs. The `"cron: firing
  schedule"` info log also emits unconditionally (it precedes the
  enqueue). Net: a skipped tick is indistinguishable from a real one —
  `report.fired` increments, the log says "firing", and the Cron tab shows
  `last_fired_at` "just now". An operator who turned skip-if-in-flight *on*
  sees the schedule "firing every tick" while the queue is actually
  collapsing them; the feature's entire effect is unobservable, and any
  metric/alert built on the cron fired-count silently inflates. **No
  queue-correctness impact** — nothing double-runs, nothing is lost, and
  cadence resumes normally once the in-flight job finishes. Fires on both
  adapters, single-process SQLite included (it's local outcome-handling,
  not a race).
- **Fix:** match the outcome in `process_row` — on
  `EnqueueOutcome::Deduped` (or `outcome.is_deduped()`) increment a new
  `CronTickReport::skipped` counter and `tracing::debug!("cron: fire
  skipped — prior run in flight")` instead of bumping `fired`; reserve
  `fired` for `Enqueued`. Move the "firing" info log below the enqueue so
  it reflects the real outcome. Advancing `last_fired_at` on a skip is
  defensible (the schedule *did* evaluate), so leave the CAS as-is — the
  fix is purely in the report/log so fired is distinguishable from skipped.

### L7 — `GET /queue/workers` counts stale, un-reaped worker rows → inflated live / in-flight figures

- **Where:** `forge-jobs-api::handlers::queue_workers` calls
  `storage.procs.list(None)` — no liveness predicate (confirmed:
  `sqlite/procs.rs::list` / `postgres.rs::list` are bare
  `SELECT * FROM queue_process`) — folded by
  `dto::workers_overview_dto`, which counts every `ProcessRecord` whose
  `host_id` matches a *live* pod.
- **Why it matters:** `pod` rows use a 60 s liveness window, but
  `queue_process` rows are only removed by the reaper (`REAPER_TICK` 15 s
  vs `STALE_THRESHOLD` 60 s), so for up to ~75 s after a worker slot dies
  its row still counts. A live pod whose slot crashed reads `workers N+1`,
  and if that slot held a `current_job`, `in-flight 1` for a job that is
  no longer running. Cosmetic (monitoring only), both adapters.
- **Fix:** `ProcessRecord` already carries `heartbeat_at`; filter
  `now - p.heartbeat_at < WORKER_LIVENESS_SECS` in `workers_overview_dto`
  before counting (the handler already threads `now`), matching the pod
  window.

### L8 — Worker heartbeat-age (the health dot) is computed across two clock domains

- **Where:** `dto::workers_overview_dto`
  (`heartbeat_age_seconds = now - pod.heartbeat_at`, `now = Utc::now()` on
  the *API host*) vs `pod.heartbeat_at`, written with the *worker's*
  `Utc::now()` in `{sqlite,postgres}::pod_heartbeat`.
- **Why it matters:** same class as **L5** — in a multi-replica
  deployment the API host and the worker pod are different machines. With
  app↔app clock skew the subtraction straddles two clocks: a live worker
  can read `is-down` (age inflated) or a freshly-dead one stay green
  (negative age, clamped to 0 by `.max(0)`). Cosmetic, self-correcting as
  skew narrows.
- **Fix:** none strictly required (fold into L5's clock-domain note); or
  derive `now` from the DB clock (`SELECT now()`) on PG so both ends of
  the subtraction share one domain.

### L9 — Rebalancer zero-out pass writes O(pods × queues) no-op `set_slots` every tick

- **Where:** `runtime/rebalance.rs::rebalance_once` — the second loop
  issues `set_slots(q, host, 0)` for every live pod *not* in `eligible`,
  for every queue, every `REBALANCE_TICK` (5 s).
- **Why it matters:** in steady state nothing changes, yet a 50-pod /
  10-queue fleet where each pod owns one queue issues ~450 upserts every
  5 s, forever — WAL/replication churn on Postgres for zero state change.
  The wasteful-but-harmless **LOW** class (the "busy-poll when `NOTIFY`
  would do" shape).
- **Fix:** `list_slot_assignments` (added in this same diff) already
  exposes current rows; zero only pods that actually hold a *positive*
  stale assignment for a queue they no longer declare (diff
  desired-vs-current), so steady state writes nothing — this also folds
  the assign + zero passes into one idempotent pass.

### L10 — `unassigned_queues` flags only *undeclared* queues, not declared-but-unserved ones

- **Where:** `dto::workers_overview_dto` — `unassigned_queues` is the set
  of configured names no worker's `queues` contains.
- **Why it matters:** a queue *declared* by a worker but with
  `max_workers = 0` (paused) or otherwise sitting at 0 slots has nothing
  actually serving it, yet is reported as assigned → no banner, while its
  jobs don't run. Narrow (paused is usually intentional); cosmetic.
- **Fix:** also flag a queue whose summed live `SlotAssignment.slots` is 0
  (the handler already fetches `list_slot_assignments`), and word the
  banner to distinguish "no declarer" from "declared but no running slot".

### L11 — The `queues` CSV column corrupts a queue name containing a comma

- **Where:** `storage::sqlite::encode_queues` / `decode_queues` and the
  byte-identical `storage::postgres` pair (`queues.join(",")` ⇄
  `split(',')`), with no escaping; queue names are never validated against
  commas on the `with_queues` / `ensure_queue` path.
- **Why it matters:** `with_queues(["orders,eu"])` round-trips as two
  phantom queues `orders` + `eu`; the pod is never eligible for the real
  `orders,eu`, which then surfaces in `unassigned_queues` and is never
  served (an L7/H3 interaction). Both adapters corrupt *identically* —
  parity holds, it's a shared-helper bug, not a divergence. Pathological
  input today (no real queue name has a comma), so it's an unguarded
  invariant rather than a live bug.
- **Fix:** reject commas in queue names at the `with_queues` /
  `ensure_queue` boundary (the repo already validates operator strings —
  cf. CSS-color validation), or store a JSON array. Hoist the shared
  helper into `storage::types` while you're there so the encoding contract
  lives once (parity-preserving by construction).

### L12 — `WorkersTab` ignores runtime changes to the panel refresh cadence

- **Where:** `forge-jobs-ui::workers::WorkersTab` reads `PollIntervalMs`
  via `get_untracked()` once and builds a single
  `set_interval_with_handle`; the Overview poller in `queue_root.rs` wraps
  its timer in an `Effect` that re-tracks `poll_ms` and reinstalls on
  change.
- **Why it matters:** changing the header refresh-interval selector (or
  pausing it) reinstalls the Overview timer but not the Workers one, which
  keeps its mount-time cadence until remount. Cosmetic. (The
  `scheduled`/`timeline`/`resources` tabs hardcode their interval and also
  don't react — but they don't *read* the reactive context and then ignore
  it, which is the surprising part here.)
- **Fix:** mirror the Overview pattern — install the interval inside an
  `Effect` that reads `poll_ms.get()` so the timer re-creates on change;
  or drop to a hardcoded const like the sibling tabs, for honesty.

## Confirmed sound (reviewed, no change needed)

- **Per-worker queue affinity (0.2.x) — mechanism** — `with_queues`
  de-dups and drops empties; `start()` hard-errors (`StorageError::Config`)
  on an empty set, so "silently drain everything" is gone *by design*
  (test `start_without_declared_queues_errors`). `fair_shares(total, 0)`
  returns empty — a queue with no eligible pod can't divide-by-zero. Every
  caller of the changed signatures was updated (`pod_heartbeat` +
  `worker_name`/`queues`; `list_live_pods` → `Vec<PodRecord>`;
  `fair_fallback` + `queue_name`); `list_slot_assignments` is implemented
  on both adapters (the only two `impl ProcessRegistry`); the
  `/queue/workers` route and `QueueIpc::queue_workers` are wired and
  `HttpQueueIpc` is the sole impl. The residual is *visibility/scale*, not
  a claim/loss race — see H3 and L7–L12.
- **Affinity SQLite⇄Postgres parity** — `encode/decode_queues` are
  byte-identical; `pod_heartbeat` upserts `worker_name`/`queues` with the
  same `ON CONFLICT` shape; `list_live_pods` orders by `host_id ASC` on
  both, so `fair_shares`' remainder distribution stays deterministic
  across adapters. `list_slot_assignments` reads `slots` as `i32` (PG) vs
  `i64`-then-`try_from` (SQLite) — identical for the small counts involved.
  (The shared-helper comma bug, L11, is a parity-preserving defect.)
- **Affinity failover** — `reap_stale` still deletes the `pod` row (now
  including the new columns) and its `pod_slot_assignment` rows; per-boot
  ULID `host_id` means a reborn pod can't inherit a dead pod's slots or
  stale `queues`.
- **Affinity injection / leak surface** — `queues` reaches SQL only as a
  bound CSV string (`.bind(queues_csv)`); no `format!` into SQL, no new
  `NOTIFY` identifier introduced. `worker_name` / `queues` / `host_id` are
  operator-set labels, not secrets, and the new `WorkerDto` + route expose
  no payloads or DSNs. `StorageError::Config` falls through the API error
  map to a generic 500, but `start()` is never called inside a request
  handler, so it's latent, not reachable.

- **Claim atomicity** — both adapters claim via a single-statement
  `UPDATE … WHERE id = (SELECT … LIMIT 1 [FOR UPDATE SKIP LOCKED])
  RETURNING *` with a re-check (`AND status IN (…)` on SQLite, row lock
  on PG); two workers cannot claim the same row.
- **Dedupe** — `jq_dedupe` partial UNIQUE index backstops a pre-check;
  enqueue returns `Deduped(id)` on the lost race; claim-time
  active-sibling filter prevents the claim-loop the boot-time
  `cleanup_superseded_retries` unsticks; PG batch requeue pre-filters
  (single-row requeue is M4).
- **Cron `dedupe_key` wiring (0.2.1)** — `process_row` threads the
  schedule's stored `dedupe_key` into the `EnqueueRequest`, so an
  overlapping fire collapses against the same `pending`/`in_progress`
  predicate as any other dedupe. No new dedupe *mechanism* — it reuses the
  audited path above, so the atomicity + SQLite⇄Postgres parity carry over
  unchanged; `enqueue_in_tx` returns `Deduped` identically in both adapters
  (fast-path pre-check + `ON CONFLICT`/`OR IGNORE` race backstop). The CAS
  in `try_advance_fire` still fences cross-replica double-fires before the
  enqueue. Round-trip covered by `cron_ensure_then_list` in both smoke
  suites and the runtime collapse by `dedupe_keyed_schedule_skips_while_a_run_is_in_flight`.
  The only gap is observability, not behavior — see **L6**.
- **Throttle cool-down** — `extend_queue_cooldown` only bumps the
  exponent for the *first* throttle in a window (the `throttled_until <=
  now` predicate is an effective CAS on both backends), the counter is
  clamped at 30, and `clear_queue_cooldown` requires the window to have
  stayed quiet for the 120 s decay grace — the flapping-limiter
  oscillation is closed (commit `4e64db5`), with test coverage
  (`queue_cooldown_counter_survives_success_within_decay_grace`).
- **Cron double-fire fence** — `try_advance_fire` CAS on the exact
  `expected` timestamp; advance-before-enqueue trades a rare lost
  occurrence for never double-enqueueing (deliberate, documented).
- **Reaper revive** — `guard_stale_before` re-checks
  `status = 'in_progress' AND heartbeat_at < cutoff` inside the write, so
  a row its worker finalized between scan and write isn't clobbered;
  revive applies the per-queue failure backoff rather than making rows
  instantly hot.
- **Leadership** — one lease row gates cron, cleanup, rebalance, and
  metrics; PG compares and writes with the DB clock on both sides;
  SQLite's lease is a real CAS too (works even if two processes share a
  file). TTL (15 s) ≥ 3× tick keeps failover prompt without flapping.
- **Cluster rate limit** — token-bucket math runs server-side in one
  `UPDATE … RETURNING` (both adapters), so replicas can't double-spend
  the last token; `drain()` on a real 429 forces the next acquire to
  throttle; runtime `retry_after` shaping is clamped [1 s, 60 s].
- **NOTIFY identifier safety** — `escape_pg_ident` doubles quotes +
  truncates at NUL for the one place an identifier is interpolated;
  every value elsewhere is `.bind(…)` in both adapters (checked: no
  `format!` interpolation of user values into SQL).
- **Missed-wake handling** — SQLite `Notify::notify_one` stores a permit
  (no lost wakeups); PG's listen-window gap is bounded by the 500 ms
  idle-poll fallback (correctness holds; cost is H2).
- **Rebalancer** — `fair_shares` conserves totals (tested); stale pods
  *and their slot assignments* are deleted by `reap_stale`; host ids are
  per-boot ULIDs so assignments can't be inherited by a reborn pod.
- **Graceful shutdown** — `shutdown_graceful` drains with a timeout,
  aborts stragglers, deletes the host's process rows; worker exit
  deregisters; supervisor `drain_all` awaits handles (panics surface in
  logs).
- **Reference server exposure** — binds `127.0.0.1` by default; the
  router's unauthenticated nature is loudly documented at the mount
  point.
- **Error-history growth** — capped at `ERROR_HISTORY_CAP = 32` on every
  append path in both adapters.

## Suggested fix order

1. **H1 + M1 together** (one commit each or one PR): the finalize
   ownership guard plus heartbeat-driven lost-ownership cancel — they
   close the same double-execution hole from both ends, and they matter
   *today* on the SQLite/Tauri build (laptop suspend is the easy
   trigger).
2. **M2** — small worker_loop change, removes the falsely-dead records
   that invite operator-driven duplicates.
3. **M4 + L3** — adapter-parity pair; trivial diffs, do them while the
   SQL is warm.
4. **M5** — one-arm change in the API error mapping; independent.
5. **H2 + M6** — the PG-at-scale pair; needed before any real
   multi-replica Postgres deployment, harmless to land early.
6. **M3** — cron UTC/TZ decision (may want a schedule-table column;
   small design choice first).
7. **L1, L2, L4, L5** — opportunistic hardening.

**0.2.x per-worker affinity (new, pending):**

8. **H3** — make an unserved queue visible in logs (rebalancer `warn!`) +
   document the contract; decide the NULL-vs-empty `queues` upgrade-window
   behavior. Matters on *every* build today (a `FORGE_QUEUES` typo strands
   a queue on the single-process SQLite build too), so do it first of the
   new batch.
9. **L7 + L10** — Workers-tab accuracy pair (filter stale process rows;
   flag declared-but-zero-slot queues); both are single
   `dto::workers_overview_dto` edits.
10. **L9** — rebalance zero-out diff-not-rewrite; land it when touching
    `list_slot_assignments`.
11. **L11** — reject commas in queue names (and hoist the shared
    `encode/decode_queues` into `storage::types`).
12. **L8, L12** — opportunistic (clock-domain doc note; Workers-tab
    interval `Effect`).

## Fix status

| ID | Status      | Commit / note |
|----|-------------|---------------|
| H1 | fixed | `owner: Option<&str>` added to `JobQueue::finalize`; both adapters guard finalize UPDATEs with `AND process_id = ? AND status = 'in_progress'`; runtime passes `Some`; regression test `finalize_owner_guard_blocks_stale_worker_clobber` |
| H2 | fixed | One process-wide `LISTEN` task (single channel + payload-routed) feeds an in-process per-queue `WakeHub`; `wait_for_work` opens no connection; cancelled on `Drop`. Removed now-unneeded `escape_pg_ident` (payload is bound). PG smoke tests need Docker — compile + clippy verified |
| M1 | fixed | `heartbeat_job` returns `HeartbeatStatus` {Active, CancelRequested, Lost}; runtime trips `job_cancel` on `Lost` so a reaped + re-claimed worker stops executing |
| M2 | fixed | `worker_loop`: Dead-on-cancel override now applies only to non-`Done` outcomes; a `Done` stands |
| M3 | fixed | Cron evaluated in UTC (`next_cron_after`) → replica-independent `next_fire_at`; UI Cron tab adds a local-timezone tooltip on the fire labels |
| M4 | fixed | PG single-row `requeue` pre-filters with the active-sibling `NOT EXISTS` predicate; SQLite parity test `requeue_single_skips_when_dedupe_sibling_is_active` |
| M5 | fixed | `StorageError` 500 fallthrough returns generic `"storage backend error"`; full string logged at `tracing::error!` |
| M6 | fixed | `resolve_target` unassigned/error path uses `fair_fallback` (total ÷ live-pod count, rounded up) instead of the full total |
| L1 | fixed | Dropped the no-op `FOR UPDATE OF j SKIP LOCKED` from PG `revive_stale`'s autocommit SELECT; comment names the real fence (the guarded UPDATE) |
| L2 | fixed | PG `delete_batch_by_status` selects victims once in a CTE (`FOR UPDATE SKIP LOCKED`); events + rows delete from the same snapshot |
| L3 | fixed | SQLite `clear_queue_cooldown` derives `decay_before` from the passed `now_iso` (one clock domain, matches PG) |
| L4 | fixed | `JobQueue::delete` returns `DeleteOutcome` {Deleted, CancelRequested, NotFound}; API keeps its `u64` touched-count contract |
| L5 | fixed | Documented the lease (DB clock) vs pod-liveness (app clock) split + the 60s `STALE_THRESHOLD` drift assumption in `docs/operating-at-scale.md` ("Clock domains"); SQL-domain unification noted there as the future option |
| L6 | fixed | `process_row` matches the enqueue outcome: `EnqueueOutcome::Deduped` → new `CronTickReport::skipped` counter + `debug!` "fire skipped — prior run still in flight"; `Enqueued` → `fired` + the "fired schedule" info log (moved below the enqueue). `cron: tick` summary surfaces `skipped`. Test `dedupe_keyed_schedule_skips_while_a_run_is_in_flight` asserts the fired/skipped split |
| H3 | fixed | `32885fd` — `rebalance_once` `warn!`s per configured queue with no eligible pod; `PodRecord::handles` treats an empty (legacy) set as eligible-for-all so a rolling upgrade doesn't stall; contract documented in `operating-at-scale.md`; test `legacy_empty_queue_pod_is_eligible_for_every_queue` |
| L7 | fixed | `2373383` — `workers_overview_dto` filters `queue_process` rows by `heartbeat_at >= stale_before` before counting live/in-flight; dto test `stale_worker_rows_are_excluded_from_counts` |
| L8 | fixed | `dd8bf84` — documented the workers-view heartbeat-age clock-domain skew under "Clock domains" in `operating-at-scale.md` + a comment at the computation site (cf. L5) |
| L9 | fixed | `c8ae36e` — `rebalance_once` snapshots `list_slot_assignments` once and zeroes only pods holding a positive stale assignment (no more O(pods×queues) no-op writes); test `zero_out_skips_pods_with_no_prior_assignment` |
| L10 | fixed | `57854da` — `unassigned_queues` now flags any configured queue with no live worker holding a positive slot (covers paused / declared-but-zero-slot); dto test `unassigned_covers_declared_but_unserved_queues` |
| L11 | fixed | `7a1fb9b` — `validate_queue_name` rejects commas at the `start()` declaration gate; `encode/decode_queues` hoisted into `storage::types` (single source); test `start_with_comma_in_queue_name_errors` |
| L12 | fixed | `d410342` — `WorkersTab` installs the poll timer at mount and re-tracks `PollIntervalMs` via an `Effect`, mirroring the Overview poller |
