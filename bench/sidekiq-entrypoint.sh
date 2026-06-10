#!/usr/bin/env bash
# Start a Sidekiq worker in the background, wait for it to connect, then run
# the requested driver (probe = pickup latency, throughput = drain rate).
set -euo pipefail

CONCURRENCY="${CONCURRENCY:-8}"
bundle exec sidekiq -r ./worker.rb -q bench -c "$CONCURRENCY" >/tmp/sidekiq.log 2>&1 &
SQ_PID=$!

# Give the worker a moment to boot + connect to Redis.
sleep 4

case "${1:-}" in
  probe)      bundle exec ruby probe.rb ;;
  throughput) bundle exec ruby throughput.rb ;;
  *) echo "usage: probe | throughput"; kill "$SQ_PID" 2>/dev/null || true; exit 1 ;;
esac

kill "$SQ_PID" 2>/dev/null || true
