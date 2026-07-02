use pin_project::{pin_project, pinned_drop};
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An observability instrument that hooks into the future's drop lifecycle to inject
/// active span metrics into `tracing`.
///
/// Automatically generates semantic telemetry data, tracking the exact file, line,
/// and span ID where the cancellation event happened.
///
/// # Example
/// ```
/// use tokio_cancel_guard::CancelTrack;
///
/// let operation = async {
///     // some operation
/// };
///
/// let tracked = CancelTrack::new(operation, "database_write");
/// ```
///
/// # Panics
///
/// Panics if polled again after it has already returned `Poll::Ready`. This upholds the
/// [`Future`] contract: once a future has completed it must not be polled again.
#[pin_project(PinnedDrop)]
pub struct CancelTrack<F: Future> {
    #[pin]
    inner: F,
    task_identifier: Cow<'static, str>,
    is_completed: bool,
    span: Option<tracing::Span>,
}

impl<F: Future> CancelTrack<F> {
    /// Creates a new `CancelTrack` wrapping the given future.
    pub fn new(future: F, task_identifier: impl Into<Cow<'static, str>>) -> Self {
        Self {
            inner: future,
            task_identifier: task_identifier.into(),
            is_completed: false,
            span: None,
        }
    }
}

impl<F: Future> Future for CancelTrack<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if *this.is_completed {
            panic!("CancelTrack polled after completion");
        }

        // Capture the execution span during the first poll
        if this.span.is_none() {
            *this.span = Some(tracing::Span::current());
        }

        match this.inner.poll(cx) {
            Poll::Ready(out) => {
                *this.is_completed = true;
                Poll::Ready(out)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[pinned_drop]
impl<F: Future> PinnedDrop for CancelTrack<F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if !*this.is_completed {
            let default_span = tracing::Span::current();
            let span = this.span.as_ref().unwrap_or(&default_span);

            // Emit OTel attributes directly on the tracing::error! event
            // so they don't require the parent span to have them pre-declared.
            tracing::error!(
                target: "runtime::cancellation",
                parent: span,
                task_name = %this.task_identifier,
                span_id = ?span.id(),
                otel.status_code = "ERROR",
                otel.status_description = "Task abruptly cancelled inside select! macro loop",
                "Cancellation safety violation: task aborted mid-execution"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::sleep;
    use tracing::Level;

    // ---------------------------------------------------------------------------
    // Minimal writer that captures formatted log lines into a shared Vec<u8>.
    // ---------------------------------------------------------------------------

    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Verifies that dropping a CancelTrack-wrapped future that has not yet
    /// completed emits a `tracing::error!` event that contains the task name.
    ///
    /// Uses a current-thread executor so `set_default` applies to the entire
    /// test without interference from concurrent tests.
    #[tokio::test(flavor = "current_thread")]
    async fn test_cancel_track_emits_error_on_cancellation() {
        let output: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let output_for_writer = output.clone();

        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || SharedWriter(output_for_writer.clone()))
            .with_max_level(Level::ERROR)
            .finish();

        let _guard = tracing::subscriber::set_default(subscriber);

        let tracker = CancelTrack::new(
            async { sleep(Duration::from_millis(50)).await },
            "my_db_write",
        );

        tokio::select! {
            _ = tracker => panic!("Should not complete"),
            _ = sleep(Duration::from_millis(10)) => {
                // Timeout wins; the CancelTrack drop fires the error event here.
            }
        }

        let log = String::from_utf8(output.lock().unwrap().clone())
            .expect("log output is not valid UTF-8");

        assert!(
            log.contains("my_db_write"),
            "Expected tracing error event containing 'my_db_write', got:\n{log}"
        );
    }

    /// Verifies that a CancelTrack-wrapped future that completes normally does
    /// NOT emit any cancellation error event.
    #[tokio::test(flavor = "current_thread")]
    async fn test_cancel_track_no_event_on_normal_completion() {
        let output: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let output_for_writer = output.clone();

        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || SharedWriter(output_for_writer.clone()))
            .with_max_level(Level::ERROR)
            .finish();

        let _guard = tracing::subscriber::set_default(subscriber);

        CancelTrack::new(
            async { sleep(Duration::from_millis(5)).await },
            "normal_task",
        )
        .await;

        let log = String::from_utf8(output.lock().unwrap().clone())
            .expect("log output is not valid UTF-8");

        assert!(
            !log.contains("normal_task"),
            "Did not expect a cancellation event for a normally completing future, got:\n{log}"
        );
    }
}
