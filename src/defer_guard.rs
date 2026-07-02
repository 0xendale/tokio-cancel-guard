use pin_project::{pin_project, pinned_drop};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An explicit wrapper that couples an underlying future with a synchronous mitigation closure.
///
/// If the future is killed mid-execution by a runtime scheduler drop (e.g. by losing a
/// `tokio::select!` race), the mitigation closure is instantly executed within the drop thread context.
///
/// # Example
/// ```
/// use tokio_cancel_guard::DeferGuard;
/// use std::sync::{Arc, Mutex};
///
/// # #[tokio::main]
/// # async fn main() {
/// let db_lock = Arc::new(Mutex::new(false));
/// let db_clone = db_lock.clone();
///
/// let operation = async move {
///     *db_clone.lock().unwrap() = true; // Lock acquired
///     tokio::time::sleep(std::time::Duration::from_millis(50)).await;
/// };
///
/// let rollback = {
///     let db_clone = db_lock.clone();
///     move || {
///         *db_clone.lock().unwrap() = false; // Lock released on cancel
///     }
/// };
///
/// let guarded = DeferGuard::new(operation, rollback);
/// # }
/// ```
///
/// # Panics
///
/// Panics if polled again after it has already returned `Poll::Ready`. This upholds the
/// [`Future`] contract: once a future has completed it must not be polled again.
#[pin_project(PinnedDrop)]
pub struct DeferGuard<F, C>
where
    F: Future,
    C: FnOnce(),
{
    #[pin]
    inner_future: F,
    on_cancel: Option<C>,
    is_completed: bool,
}

impl<F, C> DeferGuard<F, C>
where
    F: Future,
    C: FnOnce(),
{
    /// Creates a new `DeferGuard` with a future and a rollback closure.
    pub fn new(future: F, cancel_hook: C) -> Self {
        Self {
            inner_future: future,
            on_cancel: Some(cancel_hook),
            is_completed: false,
        }
    }
}

impl<F, C> Future for DeferGuard<F, C>
where
    F: Future,
    C: FnOnce(),
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // Guard against double polling after a ready state
        if *this.is_completed {
            panic!("DeferGuard polled after completion");
        }

        match this.inner_future.poll(cx) {
            Poll::Ready(output) => {
                *this.is_completed = true;
                // Disarm the cancel hook so it is not executed during a normal drop
                let _ = this.on_cancel.take();
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pinned_drop]
impl<F, C> PinnedDrop for DeferGuard<F, C>
where
    F: Future,
    C: FnOnce(),
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if !*this.is_completed {
            // Future is being dropped while still pending -> Cancellation detected
            if let Some(rollback_logic) = this.on_cancel.take() {
                rollback_logic();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_no_cancellation_on_success() {
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = flag.clone();

        let future = async {
            sleep(Duration::from_millis(10)).await;
            42
        };

        let guard = DeferGuard::new(future, move || {
            *flag_clone.lock().unwrap() = true;
        });

        let result = guard.await;
        assert_eq!(result, 42);

        // Flag should be false since it completed normally
        assert_eq!(*flag.lock().unwrap(), false);
    }

    #[tokio::test]
    async fn test_cancellation_on_drop() {
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = flag.clone();

        let future = async {
            sleep(Duration::from_millis(50)).await;
        };

        let guard = DeferGuard::new(future, move || {
            *flag_clone.lock().unwrap() = true;
        });

        // Simulate a timeout where the guard is dropped before completion
        tokio::select! {
            _ = guard => {
                panic!("Guard should not have completed");
            }
            _ = sleep(Duration::from_millis(10)) => {
                // Timeout won, guard is dropped
            }
        }

        // Flag should be true since it was cancelled
        assert_eq!(*flag.lock().unwrap(), true);
    }
}
