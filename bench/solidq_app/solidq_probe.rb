# Pickup-latency + end-to-end probe (run via `bin/rails runner`): enqueue one
# LatencyJob every 1/RATE s for DUR s.
#
#  - pickup  = enqueue→worker starts: recorded in-job into bench_lat
#              (client clock at enqueue vs worker clock at perform start).
#  - e2e     = enqueue→durably done: solid_queue's own bookkeeping,
#              finished_at - created_at on solid_queue_jobs (DB clock both
#              sides), so it includes solid_queue marking the job finished.
def now_us = (Process.clock_gettime(Process::CLOCK_REALTIME) * 1_000_000).to_i

rate = Integer(ENV.fetch("RATE", "50"))
dur  = Integer(ENV.fetch("DUR", "15"))
conn = ActiveRecord::Base.connection
conn.execute("TRUNCATE bench_lat")
SolidQueue::Job.delete_all # clean slate so finished rows are this run's

interval = 1.0 / rate
deadline = Time.now + dur
while Time.now < deadline
  LatencyJob.perform_later(now_us)
  sleep interval
end
sleep 2 # let the last jobs (≤ poll interval) drain

pct = ->(a, p) { a.empty? ? 0 : a[[((p / 100.0) * a.size).ceil - 1, 0].max] }
report = lambda do |label, rows|
  a = rows.map { |r| r.values.first.to_i }.sort
  puts "solid_queue #{label}: n=#{a.size} " \
       "p50=#{pct.call(a, 50)}us p95=#{pct.call(a, 95)}us " \
       "p99=#{pct.call(a, 99)}us max=#{a.last || 0}us"
end

report.call("pickup latency    (enqueue→start)",
            conn.execute("SELECT lat_us FROM bench_lat"))
report.call("end-to-end latency (enqueue→done)",
            conn.execute("SELECT EXTRACT(EPOCH FROM (finished_at - created_at)) * 1000000 AS us " \
                         "FROM solid_queue_jobs WHERE finished_at IS NOT NULL"))
