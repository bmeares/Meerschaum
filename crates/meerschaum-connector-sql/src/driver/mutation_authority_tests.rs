use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

struct OriginalAuthority {
    allowed: AtomicBool,
    checks: AtomicUsize,
    reader: SqlDriver,
    rolled_back: AtomicUsize,
    rollback_failed: AtomicUsize,
}

#[meerschaum_connector::async_trait]
impl meerschaum_connector::mutation::MutationCheck for OriginalAuthority {
    fn rollback_completed(&self, succeeded: bool) {
        if succeeded {
            self.rolled_back.fetch_add(1, Ordering::SeqCst);
        } else {
            self.rollback_failed.fetch_add(1, Ordering::SeqCst);
        }
    }
    async fn check(&self) -> std::result::Result<(), String> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        // A one-slot ordinary pool must remain usable for canonical authorization reads.
        self.reader
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .map_err(|error| error.to_string())?;
        if self.allowed.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("The original caller's write permission was revoked.".into())
        }
    }
}

async fn fixture() -> (tempfile::TempDir, SqlDriver, Arc<OriginalAuthority>) {
    let root = tempfile::tempdir().unwrap();
    let mut spec = ConnectionSpec::from_uri(
        &format!("sqlite:///{}", root.path().join("owned.db").display()),
        Some("scoped_mutation"),
        None,
    )
    .unwrap();
    spec.pool_size = 1;
    spec.max_overflow = 0;
    let driver = SqlDriver::connect(&spec).await.unwrap();
    driver
        .exec("CREATE TABLE guarded_rows (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let authority = Arc::new(OriginalAuthority {
        allowed: AtomicBool::new(true),
        checks: AtomicUsize::new(0),
        reader: driver.clone(),
        rolled_back: AtomicUsize::new(0),
        rollback_failed: AtomicUsize::new(0),
    });
    (root, driver, authority)
}

#[tokio::test]
async fn sqlite_waiting_writer_rechecks_after_lock_and_releases_refused_transaction() {
    let (_root, driver, authority) = fixture().await;
    let held = driver.begin_write().await.unwrap();
    let contender = driver.clone();
    let check = authority.clone();
    let mut pending = tokio::spawn(async move {
        meerschaum_connector::mutation::scope(
            check,
            contender.exec_with("INSERT INTO guarded_rows VALUES (?)", &[SqlParam::Int(1)]),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        let pool = driver.scoped_sqlite_pool.as_ref().unwrap();
        while pool.size() != 1 || pool.num_idle() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(
        authority.checks.load(Ordering::SeqCst),
        0,
        "no stale pre-lock admission"
    );
    authority.allowed.store(false, Ordering::SeqCst);
    held.rollback().await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("revoked"), "{error}");
    assert_eq!(authority.rolled_back.load(Ordering::SeqCst), 1);
    assert_eq!(authority.rollback_failed.load(Ordering::SeqCst), 0);
    assert_eq!(
        driver
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .unwrap(),
        Some(SqlValue::Int(0))
    );
    // The refused transaction returned its writer slot and did not strand the lock.
    authority.allowed.store(true, Ordering::SeqCst);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        meerschaum_connector::mutation::scope(
            authority.clone(),
            driver.exec_with("INSERT INTO guarded_rows VALUES (?)", &[SqlParam::Int(2)]),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, 1);
    assert_eq!(
        driver
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .unwrap(),
        Some(SqlValue::Int(1))
    );
    driver.close().await;
}

#[tokio::test]
async fn captured_transaction_authority_survives_scope_and_refusal_always_rolls_back() {
    let (_root, driver, authority) = fixture().await;
    let mut transaction =
        meerschaum_connector::mutation::scope(authority.clone(), driver.begin_write())
            .await
            .unwrap();
    transaction
        .exec("INSERT INTO guarded_rows VALUES (1)")
        .await
        .unwrap();
    authority.allowed.store(false, Ordering::SeqCst);
    let error = transaction
        .raw_execute("INSERT INTO guarded_rows VALUES (?)", &[SqlParam::Int(2)])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("revoked"), "{error}");
    authority.allowed.store(true, Ordering::SeqCst);
    assert!(
        transaction.commit().await.is_err(),
        "a refused transaction cannot be revived"
    );
    assert_eq!(authority.rolled_back.load(Ordering::SeqCst), 1);
    assert_eq!(authority.rollback_failed.load(Ordering::SeqCst), 0);
    assert_eq!(
        driver
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .unwrap(),
        Some(SqlValue::Int(0))
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        meerschaum_connector::mutation::scope(
            authority,
            driver.exec("INSERT INTO guarded_rows VALUES (3)"),
        ),
    )
    .await
    .expect("the scoped writer must remain usable after a refused transaction")
    .unwrap();
    assert_eq!(
        driver
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .unwrap(),
        Some(SqlValue::Int(1))
    );
    driver.close().await;
}

#[tokio::test]
async fn native_rollback_failure_is_reported_without_relabeling_it_as_authorization_cleanup() {
    let (_root, driver, authority) = fixture().await;
    let mut transaction =
        meerschaum_connector::mutation::scope(authority.clone(), driver.begin_write())
            .await
            .unwrap();
    transaction
        .exec("INSERT INTO guarded_rows VALUES (1)")
        .await
        .unwrap();
    // sqlx-sqlite 0.9.1 treats top-level ROLLBACK as a no-op once SQLite is already out of
    // a transaction (`!in_transaction()`). Open a nested SQLx savepoint and release it behind
    // SQLx's back while the outer transaction remains active so SQLx's explicit rollback runs
    // `ROLLBACK TO SAVEPOINT` against a missing savepoint and returns a real database failure.
    {
        use sqlx::Acquire;
        let TxInner::Sqlite(tx) = &mut transaction.inner else {
            panic!("expected SQLite transaction");
        };
        let nested = tx.begin().await.unwrap();
        // Leave SQLx's transaction depth at 2 without running the nested guard's Drop rollback.
        std::mem::forget(nested);
    }
    transaction
        .exec("RELEASE SAVEPOINT _sqlx_savepoint_1")
        .await
        .unwrap();
    authority.allowed.store(false, Ordering::SeqCst);
    let error = transaction.commit().await.unwrap_err();
    let message = error.to_string().to_ascii_lowercase();
    assert!(
        message.contains("savepoint") && !message.contains("revoked"),
        "{error}"
    );
    assert_eq!(authority.rolled_back.load(Ordering::SeqCst), 0);
    assert_eq!(authority.rollback_failed.load(Ordering::SeqCst), 1);
    assert_eq!(
        driver
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .unwrap(),
        Some(SqlValue::Int(0))
    );
    authority.allowed.store(true, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(5),
        meerschaum_connector::mutation::scope(
            authority,
            driver.exec("INSERT INTO guarded_rows VALUES (3)"),
        ),
    )
    .await
    .expect("the scoped writer must replace a connection whose rollback failed")
    .unwrap();
    assert_eq!(
        driver
            .value("SELECT COUNT(*) FROM guarded_rows")
            .await
            .unwrap(),
        Some(SqlValue::Int(1))
    );
    driver.close().await;
}
