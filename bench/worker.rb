# Sidekiq worker for the forge-jobs comparison benchmark.
#
# perform(enq_us): pickup-latency measurement. enq_us is the client-side
# CLOCK_REALTIME (microseconds) captured at enqueue; we compute now - enq_us
# = wall time from enqueue to the worker starting the job, and push it to a
# Redis list. enq_us == 0 means "throughput run, don't record latency".
require "sidekiq"

REDIS_URL = ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0")
Sidekiq.configure_server { |c| c.redis = { url: REDIS_URL } }
Sidekiq.configure_client { |c| c.redis = { url: REDIS_URL } }

def now_us
  (Process.clock_gettime(Process::CLOCK_REALTIME) * 1_000_000).to_i
end

class LatencyJob
  include Sidekiq::Job
  sidekiq_options queue: "bench", retry: false

  def perform(enq_us)
    # pickup = enqueue→worker starts; e2e = enqueue→perform done (Sidekiq
    # acks a job by removing it once perform returns, so end-of-perform is
    # the durable-done point — there's no separate finalize step).
    if enq_us.positive?
      Sidekiq.redis { |r| r.rpush("bench:lat", now_us - enq_us) }
    end
    Sidekiq.redis { |r| r.incr("bench:done") }
    if enq_us.positive?
      Sidekiq.redis { |r| r.rpush("bench:e2e", now_us - enq_us) }
    end
  end
end
