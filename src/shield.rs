use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A primitive that guarantees a future will run to full completion once it has
/// been polled at least once, completely immune to outer macro drops.
///
/// If `tokio::select!` drops the `Shield` handler, the inner worker task is
/// detached and seamlessly moved to an implicit `tokio::spawn` context.
///
/// # Example
/// ```
/// use tokio_cancel_guard::Shield;
/// use tokio::time::{sleep, Duration};
///
/// # #[tokio::main]
/// # async fn main() {
/// let safe_future = Shield::new(async {
///     sleep(Duration::from_millis(50)).await;
///     // Will execute even if select cancels
/// });
///
/// tokio::select! {
///     _ = safe_future => {}
///     _ = sleep(Duration::from_millis(10)) => {
///         // Shield dropped here, runs in background
///     }
/// }
/// # }
/// ```
///
/// # Panics
///
/// Panics if polled again after it has already returned `Poll::Ready`. This upholds the
/// [`Future`] contract: once a future has completed it must not be polled again.
///
/// Does **not** panic if dropped outside a Tokio runtime context (e.g. during graceful
/// shutdown). In that case the inner future is silently discarded.
pub struct Shield<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    inner: Option<Pin<Box<F>>>,
}

impl<F> Shield<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    /// Wraps a future in a Shield.
    pub fn new(future: F) -> Self {
        Self {
            inner: Some(Box::pin(future)),
        }
    }
}

impl<F> Future for Shield<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut().expect("Shield polled after completion");

        match inner.as_mut().poll(cx) {
            Poll::Ready(output) => {
                // Completed normally
                self.inner = None;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> Drop for Shield<F>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // The Shield was dropped while still pending.
            // Migrate the future to a background task so it can run to completion.
            // Verifies a runtime is active before spawning to prevent panics on graceful shutdown.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = inner.await;
                });
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
    async fn test_shield_completes_normally() {
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = flag.clone();

        let future = async move {
            sleep(Duration::from_millis(10)).await;
            *flag_clone.lock().unwrap() = true;
            42
        };

        let shield = Shield::new(future);
        let result = shield.await;

        assert_eq!(result, 42);
        assert_eq!(*flag.lock().unwrap(), true);
    }

    #[tokio::test]
    async fn test_shield_detaches_on_drop() {
        let flag = Arc::new(Mutex::new(false));
        let flag_clone = flag.clone();

        let future = async move {
            sleep(Duration::from_millis(50)).await;
            *flag_clone.lock().unwrap() = true;
        };

        let shield = Shield::new(future);

        tokio::select! {
            _ = shield => {
                panic!("Shield should not have completed first");
            }
            _ = sleep(Duration::from_millis(10)) => {
                // Timeout wins, shield is dropped here
            }
        }

        // Immediately after drop, the flag should still be false because it's sleeping
        assert_eq!(*flag.lock().unwrap(), false);

        // Wait for the background task to complete
        sleep(Duration::from_millis(60)).await;

        // Flag should now be true because the background task ran to completion
        assert_eq!(*flag.lock().unwrap(), true);
    }

    /// Verifies that dropping a pending Shield outside of any Tokio runtime
    /// (e.g. during graceful shutdown when the executor has already stopped)
    /// does not panic.  The future is silently discarded because there is no
    /// runtime available to spawn it onto.
    #[test]
    fn test_shield_drop_without_runtime_does_not_panic() {
        // Build a Shield on the current (non-async) thread where no Tokio
        // runtime is active.
        let shield = Shield::new(async {
            // This future will never run; we only care that dropping it is safe.
        });

        // Dropping outside a runtime must not panic.
        drop(shield);
    }
}
