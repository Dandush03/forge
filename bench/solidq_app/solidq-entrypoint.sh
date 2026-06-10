#!/usr/bin/env bash
# Load the solid_queue schema + bench tables into the (compose) Postgres,
# start a solid_queue worker in the background, then run the requested
# driver (probe = pickup latency, throughput = drain rate).
set -euo pipefail
export RAILS_ENV="${RAILS_ENV:-development}"

# Wait for Postgres to accept connections.
for _ in $(seq 1 30); do
  if bin/rails runner "ActiveRecord::Base.connection" >/dev/null 2>&1; then break; fi
  sleep 1
done

# solid_queue tables (force: :cascade → idempotent reset) + bench tables.
bin/rails runner "load Rails.root.join('db/queue_schema.rb')"
bin/rails runner "
  c = ActiveRecord::Base.connection
  c.execute('CREATE TABLE IF NOT EXISTS bench_lat (lat_us bigint)')
  c.execute('CREATE TABLE IF NOT EXISTS bench_done (n bigint)')
  c.execute('DELETE FROM bench_done')
  c.execute('INSERT INTO bench_done (n) VALUES (0)')
"

# solid_queue worker (config/queue.yml: workers poll every 0.1s — the default).
bin/jobs start >/tmp/jobs.log 2>&1 &
JOBS_PID=$!
sleep 5

case "${1:-}" in
  probe)      bin/rails runner solidq_probe.rb ;;
  throughput) bin/rails runner solidq_throughput.rb ;;
  *) echo "usage: probe | throughput" ;;
esac

# Hard-stop the worker (solid_queue's graceful SIGTERM shutdown can hang for
# minutes with many threads) so the container exits promptly.
kill -9 "$JOBS_PID" 2>/dev/null || true
pkill -9 -f solid-queue 2>/dev/null || true
exit 0
