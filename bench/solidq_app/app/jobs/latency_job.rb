# Records pickup latency for the benchmark. `enq_us` is the client-side
# CLOCK_REALTIME (microseconds) captured at enqueue; on perform we write
# now - enq_us into bench_lat. enq_us == 0 means a throughput run (no
# latency, just count). bench_done is a single-row counter the driver polls.
class LatencyJob < ActiveJob::Base
  queue_as :bench

  def perform(enq_us)
    enq = enq_us.to_i
    return unless enq.positive?

    # Append-only insert (no contention). Throughput runs pass enq=0 and do
    # no extra work — drain is detected via solid_queue's own job table, so
    # nothing here serializes the workers.
    now = (Process.clock_gettime(Process::CLOCK_REALTIME) * 1_000_000).to_i
    ActiveRecord::Base.connection.execute("INSERT INTO bench_lat (lat_us) VALUES (#{now - enq})")
  end
end
