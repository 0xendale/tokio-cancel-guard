//! Explicit cancellation-safety primitives for the Tokio async runtime.
//!
//! When a future loses a `tokio::select!` race, Tokio drops it immediately —
//! at any internal `.await` point, without giving the future a chance to clean up.
//! This is correct-by-spec but dangerous in practice: staged database writes get
//! abandoned, locks are left held, and the drop happens without any trace in your
//! telemetry.
//!
//! This crate gives you three focused tools to make select-driven code safe:
//!
//! | Primitive | What it does |
//! |-----------|-------------|
//! | [`DeferGuard`] | Runs a synchronous rollback closure if the future is dropped mid-execution |
//! | [`Shield`] | Guarantees the future runs to completion even if select! drops it |
//! | [`CancelTrack`] | Emits a `tracing` error event at the exact drop site (requires `tracing` feature) |
//!
//! # Quick start
//!
//! ## `DeferGuard` — rollback on cancellation
//!
//! ```rust
//! use tokio_cancel_guard::DeferGuard;
//! use std::sync::{Arc, Mutex};
//!
//! # #[tokio::main]
//! # async fn main() {
//! let dirty = Arc::new(Mutex::new(false));
//! let dirty_clone = dirty.clone();
//!
//! let write_op = async move {
//!     *dirty_clone.lock().unwrap() = true;  // stage
//!     tokio::time::sleep(std::time::Duration::from_millis(50)).await;
//!     *dirty_clone.lock().unwrap() = false; // commit
//! };
//!
//! let rollback = {
//!     let dirty = dirty.clone();
//!     move || { *dirty.lock().unwrap() = false; }
//! };
//!
//! let guarded = DeferGuard::new(write_op, rollback);
//!
//! tokio::select! {
//!     _ = guarded => {}
//!     _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
//!         // timeout wins — rollback fires automatically
//!     }
//! }
//! # }
//! ```
//!
//! ## `Shield` — guaranteed completion
//!
//! ```rust
//! use tokio_cancel_guard::Shield;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let critical = async {
//!     tokio::time::sleep(std::time::Duration::from_millis(50)).await;
//!     // this always runs — even if select! drops the Shield handle
//! };
//!
//! tokio::select! {
//!     _ = Shield::new(critical) => {}
//!     _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
//!         // Shield migrates the future to a background task and lets it finish
//!     }
//! }
//! # }
//! ```
//!
//! ## `CancelTrack` — observability (feature `tracing`)
//!
//! ```rust,ignore
//! # #[cfg(feature = "tracing")]
//! use tokio_cancel_guard::CancelTrack;
//!
//! # #[cfg(feature = "tracing")]
//! # #[tokio::main]
//! # async fn main() {
//! let op = async {
//!     tokio::time::sleep(std::time::Duration::from_millis(50)).await;
//! };
//!
//! let tracked = CancelTrack::new(op, "my_write_task");
//! // On cancellation, emits a tracing::error! with task name, span ID,
//! // and OpenTelemetry-compatible otel.status_code / otel.status_description fields.
//! # }
//! ```
//!
//! # Feature flags
//!
//! | Flag | Enables |
//! |------|---------|
//! | `tracing` | [`CancelTrack`] — cancellation telemetry via the `tracing` crate |
//!
//! # Testing utilities
//!
//! The [`test_utils`] module ships two helpers for writing deterministic
//! cancellation tests in your own crate:
//!
//! - [`test_utils::simulate_drop_after`] — race a future against a timeout
//! - [`test_utils::TrackPolls`] — count how many times a future was polled

#[cfg(feature = "tracing")]
pub mod cancel_track;
pub mod defer_guard;
pub mod shield;
pub mod test_utils;

#[cfg(feature = "tracing")]
pub use cancel_track::CancelTrack;
pub use defer_guard::DeferGuard;
pub use shield::Shield;
