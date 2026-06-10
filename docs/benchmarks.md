# forge-jobs vs Sidekiq vs solid_queue — a measured comparison

A like-for-like benchmark of three background-job systems, run with all
services on **one internal Docker network** (so every measurement crosses
the same container-to-container bridge — no host↔VM-proxy hop skewing one
system):

- **forge-jobs** (this project) — Rust, Postgres, push wake via `LISTEN/NOTIFY`.
- **Sidekiq** — Ruby, Redis, blocking-pop (`BRPOP`).
- **solid_queue** (Rails 8 default) — Ruby/Rails, Postgres, polling (0.1s).

> **Read this as a *relative* comparison, not an absolute throughput
> tuning.** Single machine (Docker Desktop, arm64), `no-op` jobs, 8-way
> concurrency, default configs. Exact numbers vary with warmth and
> hardware; the *ratios* and the architectural reasons behind them are the
> point. Reproduce it yourself with the harness in `forge-bench/` (see the
> bottom of this file).

## Method

- **Concurrency:** 8 workers/threads everywhere.
- **Pickup latency:** a producer enqueues at 50 jobs/s (workers stay mostly
  idle, isolating wake+claim); each job carries its enqueue timestamp and
  the worker records `now − enqueued_at` when it *starts* the job.
- **End-to-end latency:** same run, measured to when the job is *durably
  done* (forge: after `finalize`; Sidekiq: end of `perform`; solid_queue:
  `finished_at − created_at` from its own table, including mark-finished).
- **Throughput:** drain a backlog with 8 workers, jobs/s.
- **Worker RSS / datastore RSS / CPU:** sampled via `docker stats` over a
  throughput run, broken out per container.

## Latency (lower is better)

| | Pickup p50 | Pickup p99 | End-to-end p50 | End-to-end p99 |
|---|---|---|---|---|
| **Sidekiq** (Redis, BRPOP) | **0.47 ms** | 1.7 ms | **0.71 ms** | 2.7 ms |
| **forge-jobs** (PG, NOTIFY) | 1.4 ms | 3.8 ms | 2.0 ms | 5.6 ms |
| **solid_queue** (PG, poll 0.1s) | 62.7 ms | 130 ms | 65.2 ms | 132 ms |

The headline: **forge-jobs has ~45× lower pickup latency than solid_queue**
(single-digit-ms vs ~60 ms) despite both being durable-Postgres queues —
because forge is **push** (`NOTIFY` wakes a blocked worker immediately) and
solid_queue **polls** (pickup is floored by its poll interval). Sidekiq is
~3× lower again — Redis in-memory + blocking-pop is hard to beat on raw
latency, and forge lands much closer to Redis than to the polling DB queue.

## Throughput (8-way concurrency, drain a backlog)

| | jobs/s | notes |
|---|---|---|
| **Sidekiq** | ~7,500–15,000 | Redis, in-memory; no per-op fsync |
| **forge-jobs** | ~2,000 | Postgres; 2 durable txns/job (claim + finalize) |
| **solid_queue** | ~200 | does **not** improve with more *threads* (Ruby GVL on its per-job AR/ActiveJob work); scales by *processes* |

Order-of-magnitude: Sidekiq ≫ forge ≫ solid_queue. Redis wins on
in-memory speed; forge pays the durable-RDBMS tax (WAL, fsync, MVCC, two
transactions per job) but is ~10× the Ruby/Postgres queue. A notable
finding: solid_queue got *slower* at 8 threads than at 3 — Ruby's GVL means
its CPU-bound per-job work doesn't parallelize within one process.

## Resource footprint

**Worker process resident memory** — the clearest, most defensible
"compiled vs interpreted" signal (a Rust binary carries no VM/framework
runtime resident):

| Worker | RSS | vs forge |
|---|---|---|
| **forge-jobs (Rust)** | **13 MiB** | — |
| Sidekiq (Ruby) | 49 MiB | 3.7× larger |
| solid_queue (Rails) | 238 MiB | 18× larger |

**Datastore + CPU:**

| | Datastore RSS | Worker CPU / job |
|---|---|---|
| Sidekiq | Redis ~14 MiB | ~18 µs |
| forge-jobs | Postgres ~176 MiB | ~20 µs |
| solid_queue | Postgres ~176 MiB | higher (Rails overhead) |

Two honest notes:

- The forge *total* RAM (worker + Postgres ≈ 190 MiB) is higher than
  Sidekiq's (worker + Redis ≈ 64 MiB), but that difference is **Postgres
  vs Redis** (durable disk store vs in-memory), *not* Rust vs Ruby. Isolate
  the worker and the Rust process is the leanest of the three.
- **Worker CPU/job is comparable** between forge (Rust) and Sidekiq (Ruby)
  — both mostly wait on IO, and they do different per-job work (forge: two
  PG round-trips; Sidekiq: a Redis pop). So the measurable language win
  here is **memory footprint, not per-job CPU.** We don't claim a CPU
  improvement from Rust because the data doesn't show one.

## What this means

- **vs solid_queue** (forge's closest peer — both durable Postgres): forge
  wins decisively on the axes that matter for a responsive queue — ~45×
  lower pickup latency (push vs poll), ~30× lower end-to-end, ~10× higher
  throughput, and a far smaller worker footprint (Rust vs Rails).
- **vs Sidekiq:** Redis/in-memory is faster on latency and throughput —
  physics. forge's pitch is *not* "beats Redis"; it's **Postgres-durable
  and transactional-with-your-data, at push-driven single-digit-ms latency
  that's far closer to Redis than to a polling DB queue, with a 13 MiB
  worker.** If you already run Postgres and want jobs that commit in the
  same transaction as your data (no Redis to operate, no dual-write
  inconsistency), that's the trade forge is built for.

## Caveats

- Single machine, Docker Desktop (arm64), `no-op` jobs, default tuning.
  Real workloads with real handler work compress the relative differences.
- Throughput numbers vary with warmth (cold JIT/cache) — treat as
  order-of-magnitude.
- Three different applications, not one app rewritten three ways — the
  worker-memory comparison reflects each runtime's footprint, which is fair
  to attribute substantially to the language/framework, but it is an
  observation, not a controlled rewrite.
- All systems can scale out (more workers/processes/replicas); these are
  per-fixed-concurrency snapshots.

## Reproduce

The harness lives in `forge-bench/` (a sibling of this repo): a
`docker-compose.bench.yml` bringing up Postgres + Redis + a containerized
forge `loadgen` + a Sidekiq runner + a Rails/solid_queue runner, all on one
network. See its comments for the exact `docker-compose run` invocations
for each metric.
