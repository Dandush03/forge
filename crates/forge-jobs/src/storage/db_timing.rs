//! Per-call latency recorder for the storage backend.
//!
//! Each `JobQueue` trait method on the `SQLite` + Postgres backends opens
//! with either `OpTimer::read(&self.db_recorder)` or
//! `OpTimer::write(&self.db_recorder)` — the RAII timer records the
//! elapsed milliseconds on drop into the kind-tagged buffer, so an
//! early return through `?` is still counted. The metrics roller
//! drains both buffers once per tick and writes `db_read_ms` /
//! `db_write_ms` rollup rows with the percentiles + the sample count
//! (carrying throughput too — no parallel "db ops/min" metric needed).
//!
//! The read/write split mirrors `SQLite`'s pool layout: reads use the
//! multi-connection `read_pool`, writes serialize through the
//! `write_pool`. On Postgres the same classification applies even
//! though both routes share one pool — it lets the operator see
//! whether a hot path is read-heavy or write-heavy.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Hard cap per bucket so a panic in the metrics loop (which is the
/// only thing that drains) can't leak samples without bound. Past the
/// cap, `record()` increments a drop counter and discards the sample;
/// the recorded samples still produce a representative percentile,
/// just with a stale tail. The cap is set so a *healthy* tick under
/// peak load (~1000 calls/sec × 60s = 60k) would still flag a drop —
/// drops are a signal that something downstream is wrong, not noise
/// to mask.
const MAX_SAMPLES_PER_KIND: usize = 10_000;

/// Tags an `OpTimer` so its sample lands in the right bucket. Used to
/// build separate read/write throughput + latency series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Read,
    Write,
}

/// Thread-safe per-call sample buffers, split by [`OpKind`]. The
/// backend's `OpTimer` pushes one entry per `JobQueue` call into the
/// right slot; the metrics roller drains both buffers once per tick.
///
/// Locks are held only for the push / swap — never across `.await`.
/// A poisoned lock degrades to dropping the sample rather than
/// propagating the panic; the next tick's samples still flow.
#[derive(Debug, Default)]
pub struct DbRecorder {
    read_samples: Mutex<Vec<i64>>,
    write_samples: Mutex<Vec<i64>>,
    /// Count of samples dropped since the last `drain` because a
    /// bucket was at `MAX_SAMPLES_PER_KIND`. Reset to 0 on drain so
    /// the surfaced metric is "drops since last tick," not lifetime.
    dropped_since_drain: AtomicU64,
}

/// Drained snapshot of a recorder, split by kind. Returned by
/// [`DbRecorder::drain`] so the caller can write two rollup rows in
/// one round-trip.
///
/// `dropped` is the number of samples discarded between the previous
/// drain and this one (either bucket hit `MAX_SAMPLES_PER_KIND`).
/// Non-zero values are diagnostic: the metrics loop isn't draining
/// often enough, or it panicked and the supervisor hasn't restarted
/// it yet.
#[derive(Debug, Default)]
pub struct DrainedSamples {
    pub read: Vec<i64>,
    pub write: Vec<i64>,
    pub dropped: u64,
}

impl DbRecorder {
    /// Append one sample to the bucket selected by `kind`. Silently
    /// no-ops if the mutex is poisoned — a missed sample is
    /// preferable to crashing the worker. Past `MAX_SAMPLES_PER_KIND`
    /// the sample is dropped and the per-recorder drop counter
    /// (surfaced via [`DrainedSamples::dropped`]) increments.
    pub fn record(&self, kind: OpKind, ms: i64) {
        let bucket = match kind {
            OpKind::Read => &self.read_samples,
            OpKind::Write => &self.write_samples,
        };
        if let Ok(mut g) = bucket.lock() {
            if g.len() < MAX_SAMPLES_PER_KIND {
                g.push(ms);
            } else {
                self.dropped_since_drain.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Take all buffered samples for both kinds, leaving the buffers
    /// empty for the next tick. Also resets the drop counter and
    /// returns its pre-reset value in `DrainedSamples::dropped`.
    /// Returns empty `Vec`s on lock poisoning.
    pub fn drain(&self) -> DrainedSamples {
        let read = self
            .read_samples
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        let write = self
            .write_samples
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        let dropped = self.dropped_since_drain.swap(0, Ordering::Relaxed);
        DrainedSamples {
            read,
            write,
            dropped,
        }
    }
}

/// RAII timer that records elapsed ms on drop, into the bucket
/// matching its [`OpKind`] tag.
///
/// Designed to be assigned to a `_` binding at the top of each
/// instrumented method body so the elapsed time is measured even when
/// the body returns early via `?`. Use [`OpTimer::read`] for
/// `SELECT`-only paths and [`OpTimer::write`] for anything that
/// mutates rows.
#[derive(Debug)]
pub struct OpTimer<'r> {
    recorder: &'r DbRecorder,
    kind: OpKind,
    start: Instant,
}

impl<'r> OpTimer<'r> {
    #[must_use]
    pub fn read(recorder: &'r DbRecorder) -> Self {
        Self::with_kind(recorder, OpKind::Read)
    }

    #[must_use]
    pub fn write(recorder: &'r DbRecorder) -> Self {
        Self::with_kind(recorder, OpKind::Write)
    }

    fn with_kind(recorder: &'r DbRecorder, kind: OpKind) -> Self {
        Self {
            recorder,
            kind,
            start: Instant::now(),
        }
    }
}

impl Drop for OpTimer<'_> {
    fn drop(&mut self) {
        let ms = i64::try_from(self.start.elapsed().as_millis()).unwrap_or(i64::MAX);
        self.recorder.record(self.kind, ms);
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        reason = "unit tests crash loudly on setup failure"
    )]
    use super::*;

    #[test]
    fn recorder_buckets_by_kind_and_drains() {
        let r = DbRecorder::default();
        r.record(OpKind::Read, 5);
        r.record(OpKind::Write, 10);
        r.record(OpKind::Read, 7);
        let d = r.drain();
        assert_eq!(d.read, vec![5, 7]);
        assert_eq!(d.write, vec![10]);
        let empty = r.drain();
        assert!(
            empty.read.is_empty() && empty.write.is_empty(),
            "drain leaves both buckets empty"
        );
    }

    #[test]
    fn record_caps_each_bucket_and_counts_drops() {
        let r = DbRecorder::default();
        // Fill the read bucket to the cap.
        for _ in 0..MAX_SAMPLES_PER_KIND {
            r.record(OpKind::Read, 1);
        }
        // Past-cap pushes go to the drop counter, not the bucket.
        for _ in 0..50 {
            r.record(OpKind::Read, 999);
        }
        // Writes are an independent bucket; they still fit.
        r.record(OpKind::Write, 7);

        let d = r.drain();
        assert_eq!(
            d.read.len(),
            MAX_SAMPLES_PER_KIND,
            "read bucket capped at MAX_SAMPLES_PER_KIND"
        );
        assert!(
            d.read.iter().all(|&n| n == 1),
            "no past-cap (999) sample should have leaked into the kept buffer"
        );
        assert_eq!(d.write, vec![7]);
        assert_eq!(
            d.dropped, 50,
            "exactly the 50 past-cap pushes count as drops"
        );

        // Drain resets the counter — next tick starts clean.
        r.record(OpKind::Read, 2);
        let d2 = r.drain();
        assert_eq!(d2.read, vec![2]);
        assert_eq!(d2.dropped, 0, "drop counter resets on drain");
    }

    #[test]
    fn op_timer_records_into_the_right_bucket() {
        let r = DbRecorder::default();
        {
            let _t = OpTimer::write(&r);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        {
            let _t = OpTimer::read(&r);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        let d = r.drain();
        assert_eq!(d.read.len(), 1, "read bucket has the read sample");
        assert_eq!(d.write.len(), 1, "write bucket has the write sample");
        assert!(d.read[0] >= 2);
        assert!(d.write[0] >= 5);
    }
}
