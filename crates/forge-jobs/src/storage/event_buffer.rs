//! In-process buffer for `queue_event` timeline rows.
//!
//! `queue_event` is pure observability — it feeds the Overview timeline
//! chart and the per-minute metrics roller, and nothing about job
//! *correctness* reads it. Writing one row per state transition
//! (enqueued / started / retried / completed / failed) inside the hot
//! enqueue / claim / finalize transactions was the single biggest source
//! of write amplification at scale: every job paid 4–5 extra INSERTs,
//! some inside the very transactions that gate throughput.
//!
//! So events are now buffered here and flushed in batches by a
//! background task ([`crate::runtime`]'s `event_flush_loop`), exactly
//! mirroring the [`super::db_timing::DbRecorder`] pattern — a bounded
//! `Mutex<Vec<_>>` plus a drop counter, drained on a tick. The trade is
//! durability: a hard crash loses up to one flush interval of chart
//! events (a gap in the timeline, never a lost or duplicated job). That
//! is the exact semantics the `started` event already accepted; we
//! simply extend it to all five.
//!
//! Callers push *after* their state-change transaction commits, so a
//! rolled-back enqueue / finalize never emits a phantom event.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};

/// Hard cap on buffered events between flushes. Past this, `push`
/// discards the event and counts the drop (surfaced as a `warn!` on the
/// next flush) rather than growing without bound if the flush task
/// stalls or dies. Sized so a healthy 2 s flush window under heavy load
/// (tens of thousands of transitions) still fits with headroom — a drop
/// is a signal something downstream is wrong, not normal backpressure.
const MAX_BUFFERED_EVENTS: usize = 50_000;

/// One pending `queue_event` row. Backend-neutral: the timestamp is a
/// `DateTime<Utc>` and each adapter formats/binds it on flush (RFC3339
/// text for `SQLite`, `TIMESTAMPTZ` for Postgres).
#[derive(Debug, Clone)]
pub(super) struct EventRecord {
    pub(super) at: DateTime<Utc>,
    pub(super) kind: String,
    pub(super) queue_name: String,
    pub(super) job_id: Option<String>,
    /// Always one of the five compile-time event-type literals
    /// (`enqueued` / `started` / `retried` / `completed` / `failed`).
    pub(super) event_type: &'static str,
}

impl EventRecord {
    pub(super) fn new(
        at: DateTime<Utc>,
        kind: impl Into<String>,
        queue_name: impl Into<String>,
        job_id: Option<&str>,
        event_type: &'static str,
    ) -> Self {
        Self {
            at,
            kind: kind.into(),
            queue_name: queue_name.into(),
            job_id: job_id.map(ToOwned::to_owned),
            event_type,
        }
    }
}

/// Thread-safe append buffer for timeline events. `Arc`-shared across
/// every clone of a storage adapter (the runtime clones `Storage` into
/// each background loop), so the worker that pushes and the flush task
/// that drains see the same buffer.
///
/// The lock is held only for the push / swap — never across `.await`. A
/// poisoned lock degrades to dropping the event rather than propagating
/// the panic, matching [`super::db_timing::DbRecorder`].
#[derive(Debug, Default)]
pub(super) struct EventBuffer {
    events: Mutex<Vec<EventRecord>>,
    /// Events dropped since the last drain because the buffer was at
    /// `MAX_BUFFERED_EVENTS`. Reset on drain so the surfaced count is
    /// "drops since last flush," not lifetime.
    dropped: AtomicU64,
}

impl EventBuffer {
    /// Append one event. Silently no-ops on a poisoned lock; past the
    /// cap, increments the drop counter and discards the event.
    pub(super) fn push(&self, ev: EventRecord) {
        if let Ok(mut g) = self.events.lock() {
            if g.len() < MAX_BUFFERED_EVENTS {
                g.push(ev);
            } else {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Append several events at once (e.g. a committed `enqueue_bulk`).
    pub(super) fn push_all(&self, evs: impl IntoIterator<Item = EventRecord>) {
        if let Ok(mut g) = self.events.lock() {
            for ev in evs {
                if g.len() < MAX_BUFFERED_EVENTS {
                    g.push(ev);
                } else {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Take all buffered events, leaving the buffer empty, and return
    /// them with the drop count accumulated since the previous drain
    /// (which is reset to 0). Returns empty on a poisoned lock.
    pub(super) fn drain(&self) -> (Vec<EventRecord>, u64) {
        let events = self
            .events
            .lock()
            .map(|mut g| std::mem::take(&mut *g))
            .unwrap_or_default();
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        (events, dropped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(event_type: &'static str) -> EventRecord {
        EventRecord::new(Utc::now(), "k", "q", Some("id"), event_type)
    }

    #[test]
    fn push_then_drain_returns_events_and_empties() {
        let buf = EventBuffer::default();
        buf.push(ev("enqueued"));
        buf.push(ev("completed"));
        let (drained, dropped) = buf.drain();
        assert_eq!(drained.len(), 2);
        assert_eq!(dropped, 0);
        let (empty, _) = buf.drain();
        assert!(empty.is_empty(), "drain leaves the buffer empty");
    }

    #[test]
    fn push_caps_and_counts_drops() {
        let buf = EventBuffer::default();
        for _ in 0..MAX_BUFFERED_EVENTS {
            buf.push(ev("started"));
        }
        for _ in 0..7 {
            buf.push(ev("started"));
        }
        let (drained, dropped) = buf.drain();
        assert_eq!(drained.len(), MAX_BUFFERED_EVENTS);
        assert_eq!(dropped, 7, "past-cap pushes count as drops");
        // Counter resets on drain.
        buf.push(ev("failed"));
        let (_, dropped2) = buf.drain();
        assert_eq!(dropped2, 0);
    }
}
