# tokio-cancel-guard

[![Crates.io](https://img.shields.io/crates/v/tokio-cancel-guard.svg)](https://crates.io/crates/tokio-cancel-guard)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Explicit cancellation-safety primitives for the [Tokio](https://tokio.rs) async runtime.

## The problem

`tokio::select!` drops every branch that loses the race — immediately, at whatever
internal `.await` point the future happened to be suspended at. No cleanup. No
notification. Just a synchronous `drop`.

This is correct by spec, but dangerous in practice:

- A staged database write is abandoned mid-way, leaving dirty state.
- A mutex is held by the dropped future and never released.
- The drop happens without any entry in your telemetry, so the first sign
  of trouble is a downstream corruption or a deadlock.

`tokio-cancel-guard` gives you three focused tools to make `select!`-driven code safe.

## Primitives

| Primitive | What it does |
|-----------|-------------|
| [`DeferGuard`](#deferguard--rollback-on-cancellation) | Runs a synchronous rollback closure if the future is dropped mid-execution |
| [`Shield`](#shield--guaranteed-completion) | Guarantees the future runs to completion even after `select!` drops its handle |
| [`CancelTrack`](#canceltrack--observability-feature-tracing) | Emits a `tracing::error!` at the exact drop site (requires `tracing` feature) |

---

### `DeferGuard` — rollback on cancellation

Pairs a future with a synchronous cleanup closure. If `select!` drops the future
before it completes, the closure runs immediately inside the `Drop` call.

```rust
use tokio_cancel_guard::DeferGuard;
use std::sync::{Arc, Mutex};

let dirty = Arc::new(Mutex::new(false));
let dirty_write = dirty.clone();

let write_op = async move {
    *dirty_write.lock().unwrap() = true;          // stage
    tokio::time::sleep(Duration::from_millis(50)).await;
    *dirty_write.lock().unwrap() = false;         // commit
};

let rollback = {
    let dirty = dirty.clone();
    move || { *dirty.lock().unwrap() = false; }   // fires on cancellation
};

tokio::select! {
    _ = DeferGuard::new(write_op, rollback) => {}
    _ = tokio::time::sleep(Duration::from_millis(10)) => {
        // timeout wins — rollback fires automatically, dirty state is cleared
    }
}
```

**When to use:** releasing locks, reverting shared state, decrementing counters,
closing handles — any cleanup that can be expressed as a synchronous closure.

---

### `Shield` — guaranteed completion

Wraps a future so that once it has been polled at least once it cannot be
cancelled. If `select!` drops the `Shield` handle, the inner future is
seamlessly migrated to a `tokio::spawn`ed background task and runs to
completion.

```rust
use tokio_cancel_guard::Shield;

let critical_write = async {
    // Persist a block to the database — must not be abandoned mid-write.
    tokio::time::sleep(Duration::from_millis(50)).await;
    flush_to_db().await;
};

tokio::select! {
    _ = Shield::new(critical_write) => {}
    _ = shutdown_signal() => {
        // The Shield handle is dropped, but the write continues in the background.
    }
}
```

**When to use:** finalising a transaction, emitting a critical event notification,
any operation where partial execution is worse than running over a timeout.

**Note:** `Shield` requires a Tokio runtime to be active when dropped. If dropped
outside a runtime (e.g. after the executor has stopped), the inner future is
silently discarded rather than panicking.

---

### `CancelTrack` — observability (feature `tracing`)

Wraps a future with a `tracing` instrument. On normal completion nothing
extra happens. On cancellation it emits a `tracing::error!` event at the
`runtime::cancellation` target, including:

- `task_name` — the identifier you provided at construction
- `span_id` — the tracing span that was active when the future was first polled
- `otel.status_code = "ERROR"` and `otel.status_description` — OpenTelemetry
  semantic convention fields for downstream correlation

```toml
# Cargo.toml
tokio-cancel-guard = { version = "0.1", features = ["tracing"] }
```

```rust
use tokio_cancel_guard::CancelTrack;

let op = async { /* ... */ };
let tracked = CancelTrack::new(op, "index_block_write");

tokio::select! {
    _ = tracked => {}
    _ = timeout => {
        // A tracing::error! event is emitted here, correlated to the active span.
    }
}
```

**When to use:** any production workload where you need visibility into which
tasks are being cancelled and under what span context.

---

## Installation

```toml
[dependencies]
tokio-cancel-guard = "0.1"

# With observability support:
tokio-cancel-guard = { version = "0.1", features = ["tracing"] }
```

## Feature flags

| Flag | Enables |
|------|---------|
| `tracing` | `CancelTrack` — cancellation telemetry via the [`tracing`](https://docs.rs/tracing) crate |

## Decision guide

```
Does the future hold state that must be cleaned up on cancellation?
├── Yes → DeferGuard (rollback closure)
└── No  → Should the future run to completion regardless of the select! race?
          ├── Yes → Shield (background detachment)
          └── Do you need observability into cancellation events?
                    └── Yes → CancelTrack (tracing integration)
```

Primitives can be composed. For example, you can wrap a `DeferGuard` inside a
`CancelTrack` if you need both rollback behaviour and telemetry.

## Testing utilities

The [`test_utils`] module ships helpers for writing deterministic cancellation
tests in your own crate:

```rust
use tokio_cancel_guard::test_utils::{simulate_drop_after, TrackPolls};
use std::time::Duration;

// Race a future against a 10 ms timeout — returns None if the future loses.
let result = simulate_drop_after(my_future, Duration::from_millis(10)).await;

// Count how many times a future is polled.
let (tracked, poll_count) = TrackPolls::new(my_future);
tracked.await;
println!("polled {} times", poll_count.load(Ordering::SeqCst));
```

## License

MIT
