//! Replays the connector-call trace recorded from the Python `ValkeyConnector`
//! (`fixtures/valkey_sequence.json`, from `fixtures/gen_valkey.py`) and compares every return
//! value and the full key/value state after each step.
//!
//! - `replay_in_memory` runs offline against the in-memory backend.
//! - `replay_live` runs against a real Valkey server when one is reachable at
//!   `MRSM_VALKEY_TEST_URL` (default `redis://localhost:6399/12`, the `valkey` service of
//!   `tests/docker-compose.yaml`, database 12 which it flushes). It is skipped with a message
//!   otherwise.

mod common;

use std::sync::Arc;
use std::time::Duration;

use meerschaum_core::Map;
use meerschaum_valkey::{FredBackend, MemoryBackend, ValkeyBackend, ValkeyConnector};

fn report(label: &str, mismatches: &[String], notes: &[String]) {
    for n in notes {
        eprintln!("[{label}] note: {n}");
    }
    for m in mismatches {
        eprintln!("[{label}] MISMATCH: {m}");
    }
}

#[tokio::test]
async fn replay_in_memory() {
    let mut conn =
        ValkeyConnector::with_backend("fixture", Map::new(), Arc::new(MemoryBackend::new()));
    conn.set_id_factory(common::id_factory());
    let (mismatches, notes) = common::replay(&conn).await;
    report("memory", &mismatches, &notes);
    assert!(mismatches.is_empty(), "{} mismatches", mismatches.len());
}

#[tokio::test]
async fn replay_live() {
    let url = std::env::var("MRSM_VALKEY_TEST_URL")
        .unwrap_or_else(|_| "redis://localhost:6399/12".to_string());
    let backend = match tokio::time::timeout(
        Duration::from_secs(3),
        FredBackend::connect(&url, Some(Duration::from_secs(3))),
    )
    .await
    {
        Ok(Ok(b)) => b,
        _ => {
            eprintln!("SKIPPED replay_live: no Valkey server reachable at {url}");
            return;
        }
    };
    if !matches!(backend.ping().await, Ok(true)) {
        eprintln!("SKIPPED replay_live: Valkey at {url} did not answer PING");
        return;
    }
    backend.flushdb().await.expect("flushdb");
    let mut conn = ValkeyConnector::with_backend("fixture", Map::new(), Arc::new(backend));
    conn.set_id_factory(common::id_factory());
    let (mismatches, notes) = common::replay(&conn).await;
    conn.backend().flushdb().await.expect("flushdb");
    report("live", &mismatches, &notes);
    assert!(mismatches.is_empty(), "{} mismatches", mismatches.len());
}
