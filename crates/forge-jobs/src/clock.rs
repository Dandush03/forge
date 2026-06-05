use chrono::{DateTime, Utc};

/// Wall-clock abstraction the scheduler reads through, so tests can
/// advance time without sleeping. Per CLAUDE.md §8 ("inject a clock"
/// rather than `SystemTime::now` deep in business logic).
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
}

/// Production impl. `Utc::now()` ignores `tokio::time::pause()` —
/// this is fine outside tests because real time always moves.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
