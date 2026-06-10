# Operating forge-jobs at scale

Practical notes for running `forge-jobs` on Postgres under real volume
(hundreds of thousands to millions of jobs/day, multiple replicas). The
defaults are tuned for the embedded/desktop case; this is what to know
before pointing a cluster at it.

## Pick the right backend

- **SQLite** (default) is single-writer. Every mutation funnels through
  one connection (the pool semaphore queues writers in Rust so SQLite
  never sees `SQLITE_BUSY`). That tops out around the low hundreds to
  ~1k commits/sec regardless of tuning. It's the right choice for a
  desktop app, a CLI, or a single-process service — not for a cluster.
- **Postgres** (`--features postgres`) handles concurrent writes
  natively (`SELECT … FOR UPDATE SKIP LOCKED`, `LISTEN/NOTIFY`) and is
  the only backend for a multi-replica deployment or sustained
  throughput above roughly **1k jobs/sec**. Everything below is Postgres.

## Postgres: vacuum & bloat (the thing that bites first)

The workload is relentlessly **UPDATE-heavy on `sync_queue`**: every
claim flips `status`, every in-flight job heartbeats (an UPDATE) on a
~10s tick, and every finalize transitions the row. Each UPDATE leaves a
dead tuple. At millions of jobs/day that's tens of millions of dead
tuples/day on one table. Postgres's stock autovacuum only fires at **20%
dead tuples**, which on a large table means it runs rarely and does huge
sweeps — meanwhile the table and its indexes bloat and claim latency
climbs.

Migration `20260610000001_scale_storage_tuning.sql` sets per-table
storage params to keep this in check:

- **`sync_queue`**: `fillfactor = 85` (leave 15% per-page free so the
  heartbeat/claim/finalize UPDATEs land as in-page **HOT** updates — no
  index churn, cheaper to vacuum) plus eager autovacuum
  (`autovacuum_vacuum_scale_factor = 0.05`).
- **`queue_event`**: append-only, so no fillfactor change; eager
  autovacuum to reclaim the dead tuples the retention sweep leaves.

**Important:** `fillfactor` only governs pages written *after* the
migration runs. On a **fresh deploy** it's effective immediately. On a
table that already has data, repack it once to apply the new factor:

```sql
VACUUM FULL sync_queue;          -- exclusive lock; maintenance window
-- or, online, with the pg_repack extension:
-- pg_repack -t sync_queue
```

### Monitor bloat

Watch dead-tuple ratio and last-autovacuum time per table:

```sql
SELECT relname,
       n_live_tup,
       n_dead_tup,
       round(n_dead_tup::numeric / nullif(n_live_tup, 0), 3) AS dead_ratio,
       last_autovacuum
  FROM pg_stat_user_tables
 WHERE relname IN ('sync_queue', 'queue_event')
 ORDER BY n_dead_tup DESC;
```

If `dead_ratio` on `sync_queue` is persistently above ~0.2 or
`last_autovacuum` is stale under load, autovacuum isn't keeping up —
lower the scale factor further or raise `autovacuum_max_workers` /
`autovacuum_vacuum_cost_limit` at the server level.

### Retention

Done/dead rows are deleted by the cleanup sweep after the per-queue
retention windows (`retain_done_for_days`, default 7; `retain_dead_for_days`,
default 30), and `queue_event` rows cascade-delete with their job. Tune
the windows down per queue if `sync_queue`/`queue_event` growth outpaces
what you want to retain for the charts.

## Connection pool sizing

The Postgres pool defaults to **`max_connections = 30`** (set
`max_connections` in `queue_database.toml`). Each replica draws from its
own pool: every busy worker holds a connection for `claim_next` /
heartbeat / finalize, plus the reaper, cleanup, cron, metrics, and UI
reads. The per-queue worker count is capped at `WORKER_CAP = 64`, so a
single pod running many workers can starve a 30-connection pool.

Rule of thumb per replica: **`max_connections ≳ peak concurrent workers
+ ~5`** (reaper + cleanup + cron + metrics + UI headroom). The dedicated
`LISTEN` connection lives *outside* the pool, so it doesn't count.

Also size the **server's** `max_connections` for the whole fleet:
`replicas × pool_max` must stay under it (use a pooler like PgBouncer in
transaction mode if that product gets large).

## Single-coordinator background work

One replica wins the `cron_leader` lease (15s TTL) and runs **all** the
cluster-wide background work — cron firing, retention cleanup,
rebalancing, and the metrics rollup. This is deliberate (it prevents
duplicate cron fires and redundant cleanup), but it means that work does
*not* scale horizontally: with very many queues or schedules, the leader
pod carries all of it on one core. If the leader's background load
becomes a ceiling, the lever is fewer/cheaper schedules and queues, not
more replicas.

## Cluster rate-limit contention

The cluster-wide token bucket serializes every `acquire(scope)` through a
single `rate_limit_state` row per scope (one `UPDATE … RETURNING`). For a
rate-limited queue running hot (hundreds of acquires/sec on one scope),
that single row's lock becomes a serialization point — throughput on that
scope is bounded by how fast Postgres can turn over one row, not by your
worker count. This is the correct behavior for *enforcing* a shared
budget; just don't route a high-throughput, **un**-throttled workload
through a rate-limit scope it doesn't need. Queues with no scope never
touch `rate_limit_state`.

## Timeline events are eventually-consistent

`queue_event` rows (the Overview timeline) are buffered in-process and
flushed in batches every ~2s (and on graceful shutdown), so they're off
the hot enqueue/claim/finalize transactions. A hard crash loses up to one
flush interval of chart events — a gap in the timeline, never lost or
duplicated job state. If you don't need the timeline at all at your
volume, the cheapest path is to stop reading it; the per-minute
`metric_bucket` rollups cover counts at 60s resolution independently.

## Load testing

Two harnesses ship with the crate (both Postgres-only, both need a live
DB via `DATABASE_URL` / `TEST_DATABASE_URL`):

- **`src/bin/loadgen.rs`** — whole-system soak: seeds a backlog, drives
  sustained enqueue/claim/finalize across N workers (and optional feeders),
  then reports throughput, claim-latency percentiles, and a
  `pg_stat_user_tables` bloat snapshot. Env-configurable
  (`LOADGEN_SEED`/`WORKERS`/`FEED`/`DURATION_SECS`). Run it at increasing
  `LOADGEN_SEED` to watch how claim latency tracks the ready-set depth.

  ```text
  docker compose up -d postgres
  DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5433/postgres \
    LOADGEN_SEED=200000 LOADGEN_WORKERS=8 \
    cargo run -p forge-jobs --features postgres --bin loadgen --release
  ```

- **`benches/queue_ops.rs`** — criterion microbenchmarks for `enqueue` and
  `claim_next`+`finalize` at a couple of table sizes, for per-op regression
  tracking: `cargo bench -p forge-jobs --features postgres`.

### Cold stats after a bulk backfill

Postgres plans `claim_next` from table statistics. Immediately after a
large bulk load (a backfill, a restore) the stats are stale and the planner
can pick a sequential scan + sort — claims run orders of magnitude slower
until autovacuum's auto-analyze fires (~one `autovacuum_naptime` later, 60s
by default). After any bulk insert, run `ANALYZE sync_queue` (or wait out a
naptime) before expecting normal claim latency. `loadgen` does this
automatically after seeding.

## Clock domains (debugging "a pod flapped out of the live set")

The coordinator lease compares and writes timestamps with the **database
clock** (`now()` on both sides — self-consistent). Pod-liveness horizons
(`reap_stale`, rebalance) are computed from the **application clock**
(`Utc::now()`) against the 60s `STALE_THRESHOLD`. With app↔DB clock drift
well under 60s — i.e. any NTP-synced fleet — this is a non-issue. If you
ever debug a pod flapping in and out of the live set under unusually
loose clocks, suspect drift between those two domains; moving the
staleness horizons into SQL (`now() - interval`) would unify them.
