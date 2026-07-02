use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio_cancel_guard::{DeferGuard, Shield, test_utils::simulate_drop_after};

struct DatabaseState {
    has_dirty_state: bool,
}

impl DatabaseState {
    fn new() -> Self {
        Self {
            has_dirty_state: false,
        }
    }

    fn stage_uncommitted_changes(&mut self) {
        self.has_dirty_state = true;
    }

    fn commit_transaction(&mut self) {
        self.has_dirty_state = false;
    }

    fn clear_dirty_uncommitted_state(&mut self) {
        self.has_dirty_state = false;
    }

    fn has_dirty_state(&self) -> bool {
        self.has_dirty_state
    }
}

#[tokio::test]
async fn test_database_rollback_on_abrupt_cancellation() {
    let internal_state = Arc::new(Mutex::new(DatabaseState::new()));
    let state_clone = internal_state.clone();

    // Create a future that simulates a slow batch write to memory
    let async_db_operation = async move {
        state_clone.lock().unwrap().stage_uncommitted_changes();
        tokio::time::sleep(Duration::from_millis(50)).await;
        state_clone.lock().unwrap().commit_transaction();
    };

    let rollback_hook = {
        let state_clone = internal_state.clone();
        move || {
            state_clone.lock().unwrap().clear_dirty_uncommitted_state();
        }
    };

    // Construct the guarded future
    let protected_future = DeferGuard::new(async_db_operation, rollback_hook);

    // Simulate a select loop where a timeout triggers instantly
    let _ = simulate_drop_after(protected_future, Duration::from_millis(10)).await;

    // Verify that the fallback block successfully reset the state machine
    let final_state = internal_state.lock().unwrap();
    assert!(
        !final_state.has_dirty_state(),
        "Rollback failed to trigger on task drop!"
    );
}

// --- Shield integration tests ---

/// Verifies that a Shield-wrapped future runs to completion in a background
/// task even when the outer select! branch loses the race and the Shield
/// handle is dropped.
#[tokio::test]
async fn test_shield_runs_to_completion_after_cancellation() {
    let completed = Arc::new(Mutex::new(false));
    let completed_clone = completed.clone();

    let critical_op = async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        *completed_clone.lock().unwrap() = true;
    };

    // The Shield handle is dropped when the timeout wins.
    tokio::select! {
        _ = Shield::new(critical_op) => {
            panic!("Shield should not have won the race");
        }
        _ = tokio::time::sleep(Duration::from_millis(10)) => {
            // Shield handle dropped here; inner future migrated to a background task.
        }
    }

    // The background task needs time to finish its 50 ms sleep.
    tokio::time::sleep(Duration::from_millis(70)).await;

    assert!(
        *completed.lock().unwrap(),
        "Shield did not run the future to completion in the background"
    );
}

/// Verifies that a Shield-wrapped future that wins the race normally
/// completes and returns its output without spawning anything.
#[tokio::test]
async fn test_shield_completes_normally_returns_output() {
    let result = Shield::new(async { 42u32 }).await;
    assert_eq!(result, 42);
}
