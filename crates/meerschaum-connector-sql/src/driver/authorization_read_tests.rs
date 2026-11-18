use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

struct Original {
    reader: SqlDriver,
    other: Option<SqlDriver>,
    external: AtomicBool,
    change_query: AtomicBool,
    checks: AtomicUsize,
}
#[meerschaum_connector::async_trait]
impl meerschaum_connector::mutation::MutationCheck for Original {
    fn lock_snapshot_reads(&self) -> bool {
        true
    }
    async fn check(&self) -> std::result::Result<(), String> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        let sql = if self.change_query.load(Ordering::SeqCst) {
            "SELECT allowed + 0 FROM authority WHERE username = ?"
        } else {
            "SELECT allowed FROM authority WHERE username = ?"
        };
        let allowed = self
            .reader
            .value_with(sql, &[SqlParam::Text("alice".into())])
            .await
            .map_err(|error| error.to_string())?;
        if allowed.and_then(|value| value.as_i64()) != Some(1) {
            return Err("Registered database authority was revoked".into());
        }
        // Match the real source-schema proof's independently constructed read-only snapshot.
        let mut source = self
            .reader
            .begin_source_snapshot()
            .await
            .map_err(|error| error.to_string())?;
        source
            .read_frame("PRAGMA table_list")
            .await
            .map_err(|error| error.to_string())?;
        source.rollback().await.map_err(|error| error.to_string())?;
        if let Some(other) = &self.other
            && other
                .value("SELECT allowed FROM authority")
                .await
                .map_err(|error| error.to_string())?
                .and_then(|value| value.as_i64())
                != Some(1)
        {
            return Err("Other database authority was revoked".into());
        }
        if !self.external.load(Ordering::SeqCst) {
            return Err("External lease was revoked".into());
        }
        Ok(())
    }
}

async fn driver(path: &std::path::Path) -> SqlDriver {
    let mut spec = ConnectionSpec::from_uri(
        &format!("sqlite:///{}", path.display()),
        Some("snapshot-proof"),
        None,
    )
    .unwrap();
    spec.pool_size = 1;
    spec.max_overflow = 0;
    SqlDriver::connect(&spec).await.unwrap()
}
async fn fixture() -> (tempfile::TempDir, SqlDriver, Arc<Original>) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("main.db");
    let writer = driver(&path).await;
    writer.exec("CREATE TABLE authority (username TEXT PRIMARY KEY, allowed INTEGER); INSERT INTO authority VALUES ('alice', 1); CREATE TABLE payloads (id INTEGER, payload TEXT)").await.unwrap();
    let reader = driver(&path).await;
    assert!(
        !Arc::ptr_eq(&writer.lineage, &reader.lineage),
        "Fixture must use independent drivers"
    );
    let authority = Arc::new(Original {
        reader,
        other: None,
        external: AtomicBool::new(true),
        change_query: AtomicBool::new(false),
        checks: AtomicUsize::new(0),
    });
    (root, writer, authority)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locked_read_pin_survives_real_cache_spill_and_expires_with_its_transaction() {
    let (_root, writer, authority) = fixture().await;
    let probe = driver(_root.path().join("main.db").as_path()).await;
    let check: Arc<dyn meerschaum_connector::mutation::MutationCheck> = authority.clone();
    let mut transaction =
        meerschaum_connector::mutation::scope(check.clone(), writer.begin_write())
            .await
            .unwrap();
    let scope = transaction.authorization_read_scope().unwrap();
    // Fixture-only: force an actual early spill without disabling SQLite's locking behavior.
    transaction.exec("PRAGMA cache_size=8").await.unwrap();
    transaction.exec("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<4096) INSERT INTO payloads SELECT i, hex(zeroblob(2048)) FROM n").await.unwrap();
    let pending = tokio::spawn(async move { probe.value("SELECT COUNT(*) FROM payloads").await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !pending.is_finished(),
        "The fixture did not hold an exclusive SQLite spill lock"
    );
    tokio::time::timeout(
        Duration::from_secs(3),
        scope.scope(meerschaum_connector::mutation::recheck(&check)),
    )
    .await
    .unwrap()
    .unwrap();
    transaction
        .exec("INSERT INTO payloads VALUES (5000, 'tail')")
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(4097)
    );
    assert!(authority.checks.load(Ordering::SeqCst) >= 5);
    assert!(
        scope
            .scope(meerschaum_connector::mutation::recheck(&check))
            .await
            .is_err(),
        "A released write lock kept authorizing pinned reads"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locked_read_pin_observes_before_lock_database_revocation_and_live_external_revocation() {
    let (_root, writer, authority) = fixture().await;
    let mut blocker = writer.begin_write().await.unwrap();
    blocker
        .exec("UPDATE authority SET allowed=0")
        .await
        .unwrap();
    let contender = writer.clone();
    let check = authority.clone();
    let pending = tokio::spawn(async move {
        meerschaum_connector::mutation::scope(
            check,
            contender.exec("INSERT INTO payloads VALUES (1,'forbidden')"),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(authority.checks.load(Ordering::SeqCst), 0);
    blocker.commit().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    writer.exec("UPDATE authority SET allowed=1").await.unwrap();
    let mut transaction =
        meerschaum_connector::mutation::scope(authority.clone(), writer.begin_write())
            .await
            .unwrap();
    transaction
        .exec("INSERT INTO payloads VALUES (2,'rolled back')")
        .await
        .unwrap();
    authority.external.store(false, Ordering::SeqCst);
    assert!(
        transaction
            .exec("INSERT INTO payloads VALUES (3,'forbidden')")
            .await
            .is_err()
    );
    assert!(transaction.commit().await.is_err());
    assert_eq!(
        writer
            .value("SELECT COUNT(*) FROM payloads")
            .await
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(0)
    );
}

#[tokio::test]
async fn locked_read_pin_keeps_other_databases_fresh_and_refuses_unknown_queries() {
    let (root, writer, authority) = fixture().await;
    let other = driver(&root.path().join("other.db")).await;
    other
        .exec("CREATE TABLE authority (allowed INTEGER); INSERT INTO authority VALUES(1)")
        .await
        .unwrap();
    let mut authority = Arc::try_unwrap(authority).ok().unwrap();
    authority.other = Some(other.clone());
    let authority = Arc::new(authority);
    let mut transaction =
        meerschaum_connector::mutation::scope(authority.clone(), writer.begin_write())
            .await
            .unwrap();
    transaction
        .exec("INSERT INTO payloads VALUES (1,'rollback')")
        .await
        .unwrap();
    other.exec("UPDATE authority SET allowed=0").await.unwrap();
    assert!(
        transaction
            .exec("INSERT INTO payloads VALUES (2,'forbidden')")
            .await
            .is_err()
    );
    transaction.rollback().await.unwrap();
    other.exec("UPDATE authority SET allowed=1").await.unwrap();
    let mut transaction =
        meerschaum_connector::mutation::scope(authority.clone(), writer.begin_write())
            .await
            .unwrap();
    transaction
        .exec("INSERT INTO payloads VALUES (3,'must also roll back')")
        .await
        .unwrap();
    authority.change_query.store(true, Ordering::SeqCst);
    assert!(
        transaction
            .exec("INSERT INTO payloads VALUES (4,'forbidden')")
            .await
            .is_err()
    );
    transaction.rollback().await.unwrap();
    assert_eq!(
        writer
            .value("SELECT COUNT(*) FROM payloads")
            .await
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(0)
    );
}

#[tokio::test]
async fn locked_read_pin_refuses_a_replaced_native_database_file() {
    let (root, writer, authority) = fixture().await;
    let mut transaction =
        meerschaum_connector::mutation::scope(authority.clone(), writer.begin_write())
            .await
            .unwrap();
    let path = root.path().join("main.db");
    std::fs::rename(&path, root.path().join("original.db")).unwrap();
    std::fs::write(&path, b"replacement is not the locked database").unwrap();
    assert!(
        transaction
            .exec("INSERT INTO payloads VALUES (1,'forbidden')")
            .await
            .is_err(),
        "A filename match adopted a replaced database"
    );
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn locked_read_pin_refuses_oversized_authorization_rows_before_any_write() {
    struct Wide(SqlDriver);
    #[meerschaum_connector::async_trait]
    impl meerschaum_connector::mutation::MutationCheck for Wide {
        fn lock_snapshot_reads(&self) -> bool {
            true
        }
        async fn check(&self) -> std::result::Result<(), String> {
            self.0
                .read_frame("SELECT payload FROM payloads")
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
    }
    let (root, writer, _) = fixture().await;
    writer
        .exec("INSERT INTO payloads VALUES(1, hex(zeroblob(3145728)))")
        .await
        .unwrap();
    let check = Arc::new(Wide(driver(&root.path().join("main.db")).await));
    let result =
        meerschaum_connector::mutation::scope(check, writer.exec("DELETE FROM payloads")).await;
    assert!(
        result.is_err(),
        "A large raw authorization cell bypassed the pin byte bound"
    );
    assert_eq!(
        writer
            .value("SELECT COUNT(*) FROM payloads")
            .await
            .unwrap()
            .unwrap()
            .as_i64(),
        Some(1)
    );
}
