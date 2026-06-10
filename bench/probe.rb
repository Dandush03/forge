# Sidekiq pickup-latency probe: enqueue one job every 1/RATE seconds for DUR
# seconds, each carrying its enqueue timestamp, then report percentiles of
# the enqueue->perform latency the workers recorded. Run a sidekiq worker
# against queue `bench` first.
require_relative "worker"

rate = Integer(ENV.fetch("RATE", "50"))
dur  = Integer(ENV.fetch("DUR", "15"))
interval = 1.0 / rate

Sidekiq.redis { |r| r.del("bench:lat"); r.del("bench:e2e"); r.del("bench:done") }

deadline = Time.now + dur
n = 0
while Time.now < deadline
  LatencyJob.perform_async(now_us)
  n += 1
  sleep interval
end
# let stragglers drain
sleep 1

def pct(a, p) = a.empty? ? 0 : (a[[((p / 100.0) * a.size).ceil - 1, 0].max] || a.last)
def report(label, a)
  a = a.sort
  puts "sidekiq #{label}: n=#{a.size} " \
       "p50=#{pct(a, 50)}us p95=#{pct(a, 95)}us p99=#{pct(a, 99)}us max=#{a.last || 0}us"
end

pickup = Sidekiq.redis { |r| r.lrange("bench:lat", 0, -1) }.map(&:to_i)
e2e    = Sidekiq.redis { |r| r.lrange("bench:e2e", 0, -1) }.map(&:to_i)
report("pickup latency    (enqueue→start)", pickup)
report("end-to-end latency (enqueue→done)", e2e)
