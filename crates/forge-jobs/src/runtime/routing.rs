//! Job-kind → queue-name routing.
//!
//! The runtime asks the configured router for the destination queue
//! whenever an enqueue request omits `queue_name`.

/// Strategy for routing a job to one of the configured queues.
pub trait Router: Send + Sync + 'static {
    /// Pick the queue that should receive a job of this kind. The
    /// returned name must match a `queue.name` row registered via
    /// [`crate::runtime::QueueRuntime::ensure_queue`] or the job
    /// will languish in `pending`.
    fn route(&self, kind: &str) -> &'static str;
}

/// Routes every kind to the `"default"` queue. Useful for tests,
/// demos, and apps with a single shared queue.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultRouter;

impl Router for DefaultRouter {
    fn route(&self, _kind: &str) -> &'static str {
        "default"
    }
}

/// Routes by `kind`'s prefix to a per-source queue.
///
/// Used by tech-admin to split GH and Slack jobs across two queues
/// so each can be paused independently (e.g. when its source's
/// secret is missing) and throttled without affecting the other.
///
/// Routing rules:
///   - `gh_*`    → `"gh"`
///   - `slack_*` → `"slack"`
///   - anything else → `"default"`
#[derive(Debug, Default, Clone, Copy)]
pub struct KindPrefixRouter;

impl Router for KindPrefixRouter {
    fn route(&self, kind: &str) -> &'static str {
        if kind.starts_with("gh_") {
            "gh"
        } else if kind.starts_with("slack_") {
            "slack"
        } else {
            "default"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_gh_prefix_to_gh_queue() {
        let r = KindPrefixRouter;
        assert_eq!(r.route("gh_issue_refresh"), "gh");
        assert_eq!(r.route("gh_board_scan"), "gh");
    }

    #[test]
    fn routes_slack_prefix_to_slack_queue() {
        let r = KindPrefixRouter;
        assert_eq!(r.route("slack_channel_scan"), "slack");
        assert_eq!(r.route("slack_thread_refresh"), "slack");
    }

    #[test]
    fn unknown_prefix_routes_to_default() {
        let r = KindPrefixRouter;
        assert_eq!(r.route("noop_echo"), "default");
        assert_eq!(r.route("tickets_sync"), "default");
        assert_eq!(r.route(""), "default");
    }
}
