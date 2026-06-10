# Throughput (run via `bin/rails runner`): bulk-enqueue N jobs, then time how
# long solid_queue's workers take to drain them.
n = Integer(ENV.fetch("N", "50000"))

# Start from a clean slate so the drain count is unambiguous.
SolidQueue::Job.delete_all

# Bulk enqueue via ActiveJob's perform_all_later (solid_queue enqueues in
# batches). ts=0 → the job is a no-op; drain is detected from solid_queue's
# OWN bookkeeping (unfinished job count), so nothing the job does
# serializes the workers.
jobs = Array.new(n) { LatencyJob.new(0) }
ActiveJob.perform_all_later(jobs)

start = Time.now
loop do
  remaining = SolidQueue::Job.where(finished_at: nil).count
  break if remaining.zero?
  sleep 0.05
end
elapsed = Time.now - start
puts "solid_queue throughput: drained #{n} in #{elapsed.round(2)}s = #{(n / elapsed).round} jobs/s"
