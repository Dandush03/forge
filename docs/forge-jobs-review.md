# forge-jobs correctness / failover / scaling review

**Reviewed:** `forge-jobs` — queue runtime (`runtime.rs` + submodules) and
both storage adapters (`storage/sqlite/`, `storage/postgres.rs`), plus the
exposure surfaces in `forge-jobs-api`.
**Commit:** `4e64db5` · **Date:** 2026-06-10
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

## Confirmed sound (reviewed, no change needed)

- **Claim atomicity** — both adapters claim via a single-statement
  `UPDATE … WHERE id = (SELECT … LIMIT 1 [FOR UPDATE SKIP LOCKED])
  RETURNING *` with a re-check (`AND status IN (…)` on SQLite, row lock
  on PG); two workers cannot claim the same row.
- **Dedupe** — `jq_dedupe` partial UNIQUE index backstops a pre-check;
  enqueue returns `Deduped(id)` on the lost race; claim-time
  active-sibling filter prevents the claim-loop the boot-time
  `cleanup_superseded_retries` unsticks; PG batch requeue pre-filters
  (single-row requeue is M4).
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
