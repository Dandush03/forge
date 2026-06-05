//! Per-queue in-process `Notify` for `JobQueue::wait_for_work`.
//!
//! `SQLite` has no LISTEN/NOTIFY equivalent. We approximate it with a
//! `tokio::sync::Notify` per queue, plus `notify_one` after each
//! enqueue. Because both producers and consumers live in the same
//! process for the `SQLite` backend, this is sufficient — no cross-pod
//! notification needed (that's a Redis/Postgres concern).
//!
//! `notify_one` is used (not `notify_waiters`) so an enqueue that
//! races with all-workers-busy still stores a permit; the next
//! worker to enter the select consumes it.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Notify, RwLock};

#[derive(Debug, Default)]
pub(super) struct NotifyHub {
    queues: RwLock<HashMap<String, Arc<Notify>>>,
}

impl NotifyHub {
    /// Get (or lazily create) the Notify for a queue.
    pub(super) async fn for_queue(&self, name: &str) -> Arc<Notify> {
        // Drop the read guard before acquiring the write lock — without
        // this binding, clippy's `significant_drop_in_scrutinee` fires
        // because the read guard would live past the early-return
        // branch and then deadlock against `write().await`.
        let cached = self.queues.read().await.get(name).cloned();
        if let Some(n) = cached {
            return n;
        }
        let mut w = self.queues.write().await;
        w.entry(name.to_owned())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }
}
