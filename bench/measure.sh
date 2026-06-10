#!/usr/bin/env bash
# Resource-usage measurement for the queue comparison.
#
# Runs a system's throughput workload (detached + named) and samples
# `docker stats` once a second for that system's containers — the worker/
# runner AND its datastore — until the run exits. Reports approximate
# CPU-core-seconds consumed (→ CPU-ms per job) and peak RAM, so we can see
# which queue costs more CPU + memory per task.
#
# Usage: ./measure.sh <forge|sidekiq|solidq> [N]
#
# CPU is integrated from `docker stats` CPU% snapshots (100% = 1 core), so
# it's an estimate for relative comparison, not an exact accounting.
set -euo pipefail

SYS="${1:?usage: measure.sh <forge|sidekiq|solidq> [N]}"
N="${2:-20000}"
COMPOSE="docker-compose -f docker-compose.bench.yml"

case "$SYS" in
  forge)
    DS="forge-bench-postgres-1"
    # Drain N from a seeded backlog; loadgen prints "claimed+finalized N (R/s)".
    RUN=(-e "LOADGEN_SEED=$N" -e LOADGEN_WORKERS=8 -e LOADGEN_DURATION_SECS=25 -e LOADGEN_QUEUE=usage forge) ;;
  sidekiq)
    DS="forge-bench-redis-1"
    RUN=(-e "N=$N" -e CONCURRENCY=8 sidekiq throughput) ;;
  solidq)
    DS="forge-bench-postgres-1"
    RUN=(-e "N=$N" -e SQ_THREADS=8 solidq throughput) ;;
  *) echo "unknown system: $SYS"; exit 1 ;;
esac

NAME="${SYS}-usage"
docker rm -f "$NAME" >/dev/null 2>&1 || true

# Detached + named so docker stats can target it.
$COMPOSE run -d --name "$NAME" "${RUN[@]}" >/dev/null

STATS="/tmp/${SYS}-stats.csv"
: > "$STATS"
while [ "$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null || echo false)" = "true" ]; do
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "$NAME" "$DS" 2>/dev/null >> "$STATS" || true
  sleep 1
done

echo "=== $SYS ==="
LOG="$(docker logs "$NAME" 2>&1)"
echo "$LOG" | grep -iE "jobs/s|claimed\+finalized|drained" | tail -2

# Jobs actually processed (forge is duration-based; the others drain N).
PROCESSED="$(echo "$LOG" | grep -oE 'claimed\+finalized +[0-9]+' | grep -oE '[0-9]+' | head -1)"
[ -z "$PROCESSED" ] && PROCESSED="$(echo "$LOG" | grep -oE 'drained +[0-9]+' | grep -oE '[0-9]+' | head -1)"
[ -z "$PROCESSED" ] && PROCESSED="$N"

# Parse: per-container peak mem (MiB) + integrated CPU-core-seconds.
awk -F, -v n="$PROCESSED" '
function tomib(s,   v) {
  v=s+0
  if (s ~ /GiB/) return v*1024
  if (s ~ /KiB/) return v/1024
  return v # MiB or bytes-as-MiB-ish
}
{
  name=$1; cpu=$2; sub(/%/,"",cpu)
  split($3, m, " / "); used=tomib(m[1])
  cpusum[name]+=cpu/100.0          # core-fractions, 1 sample = ~1s
  if (used>peak[name]) peak[name]=used
}
END {
  total_cpu=0
  for (k in cpusum) {
    printf "  %-26s CPU ~%6.1f core-s   peakRAM %7.1f MiB\n", k, cpusum[k], peak[k]
    total_cpu+=cpusum[k]
  }
  printf "  %-26s CPU ~%6.1f core-s   (= %.2f CPU-ms / job over %d jobs)\n", "TOTAL", total_cpu, total_cpu*1000.0/n, n
}' "$STATS"

docker rm -f "$NAME" >/dev/null 2>&1 || true
