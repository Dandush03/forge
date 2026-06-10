# correctness-review

Produce a **correctness/failover/scaling review** of a subsystem — the kind
of deep audit that finds the races, the lost updates, the under-load
cliffs, the cross-replica divergence. Apply when the user asks to "review
the design," "find the flaws/races," "audit <subsystem> for correctness,"
"what breaks under load / multi-replica," or to refresh an existing
review's fix-status. Read `README.md` and the workspace `Cargo.toml`
`[workspace.lints]` first — forge has no `AGENTS.md`; the conventions live
there and in `PUBLISH-REVIEW.md`.

This is **not** `refactor-pass`. That skill fixes style/lint/hygiene per
file. This one finds *behavioral* defects — races, lost updates,
double-fires, clock skew, cross-replica divergence, lease/heartbeat
expiry bugs, scaling cliffs — and produces a findings document. Style and
lint are explicitly **out of scope**; assume `clippy -D warnings` and
`fmt` are already clean and say so.

The prime target in this repo is **`forge-jobs`** — a cluster-shared job
queue with two storage adapters (SQLite default, Postgres optional), a
per-queue worker runtime, cron + scheduled jobs, a cluster-wide
rate-limit/throttle budget, and cancellation that must survive across
replicas. Postgres `LISTEN/NOTIFY`, `FOR UPDATE SKIP LOCKED` dispatch,
retry/backoff, and the SQLite⇄Postgres parity are exactly the surfaces
this audit exists for.

## Output: a findings document, not a task list

Write (or update) a markdown doc under `docs/<subsystem>-review.md`:

1. **Header** — what was reviewed, the commit/date, the scope line
   (correctness, races, failover, cross-replica parity, scaling), and
   one line stating what's out of scope (style/lint).
2. **Architecture orientation** — a few bullets so the doc stands alone:
   the storage trait + its two adapters, the runtime loops, the
   coordination primitives (advisory locks, `NOTIFY` channels, the
   throttle/lease columns). A reviewer who never opened the code should
   follow every finding from this section alone.
3. **HIGH / MEDIUM / LOW** sections. Each finding is one subsection:
   - A stable ID (`H1`, `M3`, `L2`) — these are referenced by commits
     forever, so never renumber a published ID; append new ones.
   - A one-line title naming the defect.
   - **Where**: `crate::module::function` (both adapters when it differs —
     `storage::postgres` vs `storage::sqlite`).
   - **Why it matters**: the concrete failure scenario, not the abstract
     risk — "with N workers, a job that fetched before the limit hit
     clears the cool-down and the fleet resumes into the still-active
     limit." Name the real path where it's the common case, not an edge.
   - If a *test encodes the buggy behavior as intended*, call that out.
   - **Fix:** the smallest change that closes it, named concretely (the
     SQL predicate, the CAS guard, the UNIQUE index, the `SKIP LOCKED`
     clause, the memory ordering) — enough that implementing it needs no
     re-derivation.
4. **Confirmed sound (reviewed, no change needed)** — what you checked
   and deliberately cleared. This is half the value: it stops the next
   reviewer re-litigating the dispatch lock or the cancellation path.
5. **Suggested fix order** — grouped by what the product depends on now
   (the SQLite single-process path) vs. what only bites on the
   multi-replica Postgres deployment.
6. **Fix status** table — see below.

## Severity model

Severity is **blast radius at the stated target**, not "how ugly." State
the target once in the header (e.g. "a multi-replica Postgres deployment
sharing one queue") and rank against it. Add the local-reality caveat: a
HIGH that only bites on the multi-replica Postgres path needs that
qualifier, since the default build is single-process SQLite and the
finding can't fire there.

- **HIGH** — a job executed twice (double-booking side effects), a job
  silently lost (dequeued, never run, never requeued), a cancellation
  that doesn't propagate across replicas, a cluster-wide rate budget
  that overshoots into a provider's limit, or a scaling cliff on the
  dispatch loop.
- **MEDIUM** — correctness hardening: over-counting throttle attempts,
  clock-skew misbehavior between DB `now()` and the runtime clock,
  cross-replica divergence with a narrow window, retry backoff that
  overshoots, a heartbeat/lease that expires a still-live worker.
- **LOW** — wasteful but harmless (busy-poll when `NOTIFY` would do),
  cosmetic divergence between adapters, dead code, unguarded invariants
  that hold today.

## Fix status table (the part that gets worked off)

End the doc with a dated table the implementation work consumes one row
at a time. This is the bridge from review to commits:

```
| ID | Status | Commit / note |
|----|--------|---------------|
| H1 | fixed  | `0e38d76` — dispatch now uses FOR UPDATE SKIP LOCKED |
| M7 | **pending** | route cancellation NOTIFY through one handler |
| H3 | reassessed (no change) | <why the original finding overstated it> |
```

- `fixed` rows link the commit. `pending` rows carry enough detail to
  start cold. `reassessed (no change)` is legitimate — a second look can
  downgrade a finding; record *why*, don't silently drop it.
- **Each fix commit references the ID in its subject** (`fix(forge-jobs):
  fence throttle decay behind a grace window (M5)`). That traceability is
  the whole point.

## Workflow

1. **Orient first.** Read the subsystem end to end before writing a
   single finding. Write the architecture-orientation section from that
   read — if you can't, you don't understand it well enough to review
   it.
2. **Hunt by category**, not by file:
   - **Concurrency**: two workers claiming the same row, SELECT-then-act
     windows, last-writer-wins `UPDATE`s, dispatch without `FOR UPDATE
     SKIP LOCKED`, throttle counter read-modify-write races, double-fire
     on a cron tick.
   - **Failover**: a worker that dies mid-job — does the lease/heartbeat
     expire and requeue, or does the row sit `in_progress` forever?
     Orphaned `in_progress` rows on ungraceful exit; idempotency of a
     replayed job; cancellation delivered to a replica that has already
     picked the job up.
   - **Clocks**: DB server clock (`now()` / `CURRENT_TIMESTAMP`) vs the
     runtime's `chrono::Utc::now()` drift — `throttled_until <= now`,
     `scheduled_at`, retry `run_at`, and the decay grace all straddle
     this boundary. Whose clock decides?
   - **Cross-replica parity**: two replicas sharing one Postgres queue
     with divergent in-memory view of the rate budget; `NOTIFY` delivered
     to zero/one/all listeners; the order of cancel-vs-claim under
     reordering. **And SQLite⇄Postgres parity**: the same logical
     operation must behave identically in both adapters (the throttle
     decay, the requeue predicate, the dedupe/UNIQUE handling). A bug
     fixed in one adapter and not the other is a finding.
   - **Dispatch path**: does claiming a job actually use a row lock /
     `SKIP LOCKED`, or can two workers grab it? Does the count-limited
     fetch hold the lock for the whole batch?
   - **Leak / exposure** (`forge-jobs-api` + `forge-jobs-ui`): the lens
     is *"imagine an operator screenshots this panel / pastes this log
     into Slack — what just escaped?"* Concrete checks:
     - **Secrets in the DOM.** No connection string, bearer token, or
       host credential ever lands in the Leptos panel HTML or a visible
       `<pre>`. The job inspector renders payloads — check that a payload
       carrying a secret isn't echoed verbatim into the drawer.
     - **Secrets in logs / traces.** Spans carrying a connection string
       or payload use `#[tracing::instrument(skip(...))]`. No
       full-payload dumps in a panic hook or an error log.
     - **Secrets in error messages.** API error strings (`forge-jobs-api`)
       never echo a payload or a DB DSN back to the caller.
     - **SQL injection / identifier escaping.** Every value goes through
       sqlx `.bind(...)` — never `format!()` into SQL. **Identifiers**
       (queue names interpolated into a `NOTIFY` channel or a
       `LISTEN`/`pg_notify` call) must be escaped/validated — this repo
       has already been bitten here (the NOTIFY quoted-identifier escape
       fix). Audit any place a queue/kind name reaches SQL text rather
       than a bind param.
     - **Unauthenticated API surface.** `forge-jobs-api` ships handlers
       that purge and mutate the queue with no built-in auth — verify the
       reference server documents that it must sit behind the host's
       auth, and that no new route silently widens what an unauthenticated
       caller can destroy.
     - **Input validation.** CSS colors / labels / any operator-supplied
       string rendered or fed to SQL is validated (the repo already
       validates CSS colors for this reason).

     The lens *isn't* a one-time pass — re-apply it any time a route, a
     DTO, or a logging span gets added. Each finding is a HIGH or MEDIUM
     in the leak class regardless of "blast radius at scale" because
     leaks are blast-radius-already-realized once they ship.
3. **Prove each finding** by tracing the exact code path; cite
   `crate::module::function` and lines. If you can't point at the lines,
   it's a hypothesis — mark it as one.
4. **Clear the rest deliberately** into Confirmed-sound.
5. **No autocommit.** This skill *writes the review doc*; that's a normal
   file edit — list it and stop. Implementing the fixes is separate work,
   gated on the user's go, one ID per logical commit.

## When to stop

The doc covers the stated scope, every finding has a Where + Why + Fix,
the Confirmed-sound section is non-empty, and the fix-status table
reflects reality (literal statuses: a half-done fix is `pending`, not
`fixed`). Summarize the HIGH count and the recommended first fix in the
final response.
