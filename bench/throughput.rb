# Sidekiq throughput: bulk-enqueue N jobs, then time how long the workers
# take to drain them. Run a sidekiq worker against queue `bench` first.
require_relative "worker"

n = Integer(ENV.fetch("N", "50000"))
Sidekiq.redis { |r| r.del("bench:done"); r.del("bench:lat") }

# Bulk-enqueue with ts=0 (no latency recording) as fast as possible.
Sidekiq::Client.push_bulk("class" => LatencyJob, "args" => Array.new(n) { [0] }, "queue" => "bench")

start = Time.now
loop do
  done = Sidekiq.redis { |r| r.get("bench:done") }.to_i
  break if done >= n
  sleep 0.02
end
elapsed = Time.now - start
puts "sidekiq throughput: drained #{n} in #{elapsed.round(2)}s = #{(n / elapsed).round} jobs/s"
