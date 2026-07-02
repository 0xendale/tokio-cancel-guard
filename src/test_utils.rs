//! Testing utilities for cancellation-safety verification.
//!
//! These helpers are intentionally public so that downstream crates can use
//! them when writing their own cancellation-safety tests. They have no
//! production use — import them only in `#[cfg(test)]` modules or
//! `[dev-dependencies]`.
//!
//! # Utilities
//!
//! - [`simulate_drop_after`] — race a future against a deadline, returning
//!   `None` if the future loses (i.e. was cancelled).
//! - [`TrackPolls`] — wrap a future to count how many times it is polled,
//!   useful for asserting state-machine transitions.

use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

/// Simulates a sudden runtime drop by forcefully cancelling the given future
/// after the specified duration using a `select!` race.
pub async fn simulate_drop_after<F: Future>(future: F, duration: Duration) -> Option<F::Output> {
    // Yield to ensure the future gets polled before we sleep, making drops more deterministic
    tokio::task::yield_now().await;
    tokio::select! {
        res = future => Some(res),
        _ = tokio::time::sleep(duration) => None,
    }
}

/// A wrapper future that counts how many times it was polled.
/// This is useful for deterministic testing of async state machines.
#[pin_project]
pub struct TrackPolls<F> {
    #[pin]
    inner: F,
    counter: Arc<AtomicUsize>,
}

impl<F> TrackPolls<F> {
    /// Creates a new `TrackPolls` wrapping the given future.
    /// Returns the wrapped future and an atomic counter that tracks poll invocations.
    pub fn new(future: F) -> (Self, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        (
            Self {
                inner: future,
                counter: counter.clone(),
            },
            counter,
        )
    }
}

impl<F: Future> Future for TrackPolls<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.counter.fetch_add(1, Ordering::SeqCst);
        this.inner.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Verifies that TrackPolls correctly counts each time the inner future is polled.
    /// A future that sleeps once then returns will be polled at least twice:
    /// once when it first suspends and once when the waker fires.
    #[tokio::test]
    async fn test_track_polls_counts_accurately() {
        let future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            42u32
        };

        let (tracked, counter) = TrackPolls::new(future);
        let result = tracked.await;

        assert_eq!(result, 42);
        // The sleep future suspends on the first poll (Pending) and resolves on
        // the second (Ready), so the wrapper must have been polled at least twice.
        assert!(
            counter.load(Ordering::SeqCst) >= 2,
            "expected at least 2 polls, got {}",
            counter.load(Ordering::SeqCst)
        );
    }

    /// Verifies that a future that returns immediately is polled exactly once.
    #[tokio::test]
    async fn test_track_polls_immediate_future_polled_once() {
        let future = async { "done" };
        let (tracked, counter) = TrackPolls::new(future);
        let result = tracked.await;

        assert_eq!(result, "done");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "an immediately-ready future should be polled exactly once"
        );
    }
}
