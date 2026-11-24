//! Live tests use only an explicitly supplied disposable fixture, never an ambient server.
//!
//! Every live test of this binary shares the fixture's one database (`MRSM_VALKEY_TEST_URL`), and
//! `valkey_replay::replay_live` flushes it and compares every key after each step. So a live test
//! holds its [`Database`] for its whole body, and the live tests of this process run one at a
//! time whatever `--test-threads` is. nextest runs each test in its own process instead and
//! serializes them with the `valkey-live` test group of `.config/nextest.toml`.

use tokio::sync::{Mutex, MutexGuard};

/// Held by the live test that is using the shared database.
static SHARED_DATABASE: Mutex<()> = Mutex::const_new(());

/// The shared database of the disposable fixture, reserved for one live test until dropped.
pub struct Database {
    pub url: String,
    _turn: MutexGuard<'static, ()>,
}

/// Wait for this process's turn on the shared database (no fixture is needed for this).
pub async fn turn() -> MutexGuard<'static, ()> {
    SHARED_DATABASE.lock().await
}

/// The disposable fixture's database, once no other live test of this process is using it, or
/// `None` (with a `SKIPPED` line) when no fixture was supplied.
pub async fn database() -> Option<Database> {
    let Ok(url) = std::env::var("MRSM_VALKEY_TEST_URL") else {
        eprintln!("SKIPPED: set MRSM_VALKEY_TEST_URL to an owned disposable Valkey instance");
        return None;
    };
    assert!(
        !url.is_empty(),
        "The disposable Valkey URL must not be empty"
    );
    assert_eq!(std::env::var("MRSM_TEST_ISOLATED").as_deref(), Ok("1"));
    Some(Database {
        url,
        _turn: turn().await,
    })
}

#[tokio::test]
async fn live_tests_take_turns_on_the_shared_database() {
    let first = turn().await;
    let mut second = std::pin::pin!(turn());
    let waiting = tokio::time::timeout(std::time::Duration::from_millis(50), &mut second).await;
    assert!(
        waiting.is_err(),
        "a second live test started while the first held the database"
    );
    drop(first);
    // Other live tests may be ahead of the second turn in the queue.
    let _second = second.await;
}
