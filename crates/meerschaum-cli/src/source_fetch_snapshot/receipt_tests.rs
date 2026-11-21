//! Owned lease and completion-receipt resource accounting; no database or network services.
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use super::*;

fn store(path: PathBuf, receipt_slots: usize) -> Arc<Store> {
    Arc::new(Store {
        path,
        root: OnceLock::new(),
        entries: Mutex::new(BTreeMap::new()),
        bytes: Arc::new(Semaphore::new(300)),
        slots: Arc::new(Semaphore::new(8)),
        pages: Arc::new(Semaphore::new(8)),
        receipt_slots: Arc::new(Semaphore::new(receipt_slots)),
        operations: Mutex::new(Operations::default()),
    })
}
fn actor(credential: &str) -> Binding {
    Binding {
        owner: "execution:original-grant:original-launch".into(),
        credential: credential.into(),
        fingerprint: "canonical-prepared-query-and-destination".into(),
    }
}
fn prepared(store: &Arc<Store>, actor: &Binding) -> String {
    let mut writer = store.writer().unwrap();
    writer
        .push(&Response {
            version: source_fetch::VERSION,
            rows: vec![meerschaum_core::Map::from_iter([(
                "id".into(),
                meerschaum_core::Value::Int(1),
            )])],
            dtypes: Default::default(),
            complete: true,
        })
        .unwrap();
    let mut snapshot = writer.finish(
        actor,
        "schema".into(),
        "trusted-configuration".into(),
        "canonical-query".into(),
    );
    snapshot.reserve_receipt().unwrap();
    snapshot.set_prepared("trusted native plan");
    snapshot.publish().id
}
fn result() -> meerschaum_core::success::SyncResult {
    meerschaum_connector::replace::replace_result(7, 100_321, 2, true)
}
fn directories(store: &Store) -> usize {
    std::fs::read_dir(&store.path)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("snapshot-"))
        .count()
}

#[tokio::test]
async fn duplicate_commit_waits_for_one_exclusive_lease_then_rotates_without_source_files() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path().join("owned"), 2);
    let original = actor("first-short-lived-bearer");
    let id = prepared(&store, &original);
    let cancellation = Arc::new(AtomicBool::new(false));
    let snapshot = store
        .take(&id, &original, "structured-operation", cancellation.clone())
        .unwrap();
    assert!(matches!(
        store.take(&id, &original, "structured-operation", cancellation),
        Err(SourceFetchError::Closed)
    ));
    let rotated = actor("new-short-lived-bearer");
    let mut pending = Box::pin(store.completed(
        &id,
        &rotated,
        "structured-operation",
        "trusted-configuration",
    ));
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let expected = result();
    snapshot.complete("structured-operation", expected.clone());
    drop(snapshot);
    let received = pending.await.unwrap().unwrap();
    assert_eq!((received.deleted, received.inserted), (7, 100_321));
    assert_eq!(directories(&store), 0, "receipt keeps no source files");
    assert_eq!(store.slots.available_permits(), 8);
    assert_eq!(
        store.bytes.available_permits(),
        300 - RECEIPT_UNITS as usize
    );
    assert!(matches!(
        store
            .completed(&id, &rotated, "changed-operation", "trusted-configuration")
            .await,
        Err(SourceFetchError::Stale)
    ));
    let stranger = Binding {
        owner: "execution:other-grant:other-launch".into(),
        ..actor("other-bearer")
    };
    assert!(matches!(
        store
            .completed(
                &id,
                &stranger,
                "structured-operation",
                "trusted-configuration"
            )
            .await,
        Err(SourceFetchError::Closed)
    ));
    assert!(matches!(
        store.close(
            &Cursor {
                id: id.clone(),
                page: 0
            },
            Some(&stranger.owner),
            &stranger.credential
        ),
        Err(SourceFetchError::Closed)
    ));
    store
        .close(
            &Cursor { id, page: 0 },
            Some(&rotated.owner),
            &rotated.credential,
        )
        .unwrap();
    assert_eq!(store.bytes.available_permits(), 300);
    assert_eq!(store.receipt_slots.available_permits(), 2);
}

#[tokio::test]
async fn cancellation_and_receipt_expiration_release_owned_quota_and_permit_fresh_work() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path().join("owned"), 1);
    let original = actor("first");
    let id = prepared(&store, &original);
    let cancelled = Arc::new(AtomicBool::new(false));
    let snapshot = store
        .take(&id, &original, "operation", cancelled.clone())
        .unwrap();
    store
        .close(
            &Cursor {
                id: id.clone(),
                page: 0,
            },
            Some(&original.owner),
            &original.credential,
        )
        .unwrap();
    assert!(cancelled.load(Ordering::Relaxed));
    assert!(matches!(
        store
            .completed(&id, &original, "operation", "trusted-configuration")
            .await,
        Err(SourceFetchError::Closed)
    ));
    drop(snapshot);
    assert_eq!(directories(&store), 0);
    assert_eq!(store.bytes.available_permits(), 300);
    let id = prepared(&store, &original);
    let snapshot = store
        .take(
            &id,
            &original,
            "operation",
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    snapshot.complete("operation", result());
    drop(snapshot);
    store
        .operations
        .lock()
        .unwrap()
        .receipts
        .get_mut(&Uuid::parse_str(&id).unwrap())
        .unwrap()
        .touched = Instant::now() - IDLE;
    assert!(matches!(
        store
            .completed(&id, &original, "operation", "trusted-configuration")
            .await,
        Err(SourceFetchError::Uncertain)
    ));
    assert_eq!(store.bytes.available_permits(), 300);
    assert_eq!(store.receipt_slots.available_permits(), 1);
    let id = prepared(&store, &original);
    store
        .close(
            &Cursor { id, page: 0 },
            Some(&original.owner),
            &original.credential,
        )
        .unwrap();
    assert_eq!(directories(&store), 0);
    assert_eq!(store.bytes.available_permits(), 300);
}

#[tokio::test]
async fn completed_receipt_quota_evicts_old_results_before_admitting_another_mutation() {
    let root = tempfile::tempdir().unwrap();
    let store = store(root.path().join("owned"), 1);
    let original = actor("first");
    let first = prepared(&store, &original);
    let snapshot = store
        .take(
            &first,
            &original,
            "first-operation",
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    snapshot.complete("first-operation", result());
    drop(snapshot);
    assert_eq!(store.operations.lock().unwrap().receipts.len(), 1);
    let second = prepared(&store, &original);
    assert_eq!(store.operations.lock().unwrap().receipts.len(), 0);
    assert!(
        store
            .completed(
                &first,
                &original,
                "first-operation",
                "trusted-configuration"
            )
            .await
            .unwrap()
            .is_none()
    );
    let snapshot = store
        .take(
            &second,
            &original,
            "second-operation",
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
    snapshot.complete("second-operation", result());
    drop(snapshot);
    assert_eq!(store.operations.lock().unwrap().receipts.len(), 1);
    assert_eq!(directories(&store), 0);
    assert_eq!(
        store.bytes.available_permits(),
        300 - RECEIPT_UNITS as usize
    );
}
