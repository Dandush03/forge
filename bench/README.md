# bench — cross-system queue benchmark

The reproducible harness behind [`docs/benchmarks.md`](../docs/benchmarks.md).
Brings up **forge-jobs**, **Sidekiq**, and **solid_queue** with their
datastores (Postgres, Redis) on **one internal Docker network**, so every
measurement crosses the same container-to-container bridge rather than the
host↔VM networking hop (which is slower and noisier on Docker Desktop and
would skew whichever system runs on the host).

All three run at **8-way concurrency**, no-op jobs. Each load runner is
invoked one at a time via `docker-compose run` so they never contend on the
shared datastores.

## Layout

- `docker-compose.bench.yml` — postgres + redis + the three runner services.
- `forge.Dockerfile` — builds forge-jobs' `loadgen` (Rust) from the repo.
- `Gemfile` / `worker.rb` / `probe.rb` / `throughput.rb` / `sidekiq.Dockerfile`
  / `sidekiq-entrypoint.sh` — the Sidekiq runner.
- `solidq_app/` — a minimal Rails 8 app wired to solid_queue, with its own
  `Dockerfile`, `LatencyJob`, and `solidq_probe.rb` / `solidq_throughput.rb`.
- `measure.sh` — runs a system's throughput while sampling `docker stats`
  for CPU + peak RAM (worker and datastore broken out).

## Run

```sh
cd bench
docker-compose -f docker-compose.bench.yml up -d postgres redis

# Latency (pickup + end-to-end), 50 jobs/s, 8 workers:
docker-compose -f docker-compose.bench.yml run --rm \
  -e LOADGEN_PROBE=1 -e LOADGEN_PROBE_RATE=50 -e LOADGEN_WORKERS=8 forge
docker-compose -f docker-compose.bench.yml run --rm -e RATE=50 sidekiq probe
docker-compose -f docker-compose.bench.yml run --rm -e RATE=50 -e SQ_THREADS=8 solidq probe

# Throughput + resource usage (CPU/RAM per job, worker vs datastore):
./measure.sh forge 50000
./measure.sh sidekiq 50000
./measure.sh solidq 5000     # solid_queue is slow; smaller N

# Teardown (drops the volume):
docker-compose -f docker-compose.bench.yml down -v
```

## Notes

- `postgres:latest` is PG 18+, which needs the data volume mounted at
  `/var/lib/postgresql` (not `…/data`) — already set here.
- solid_queue's worker threads must be ≤ the DB pool, so the service sets
  `RAILS_MAX_THREADS=16`; it also scales by *processes*, not threads (Ruby
  GVL), so raising `SQ_THREADS` past a few doesn't help throughput.
- Numbers are relative (single machine, default tuning) — see the writeup
  for full caveats.
