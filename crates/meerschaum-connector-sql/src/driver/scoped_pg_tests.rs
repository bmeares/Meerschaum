//! Real PostgreSQL scoped-write acceptance, included only by the test-only helper.
//! The coordinator supplies MRSM_TEST_PG_URI for its disposable owned PostgreSQL service.
//! Every test uses an independent UUID schema; no fixture starts a service or uses live data.

use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Original {
    reads: PgPool,
    metadata: String,
    accepted: Mutex<String>,
    allowed: AtomicBool,
    expires: Instant,
    checks: AtomicUsize,
    refuse_check: usize,
}

#[meerschaum_connector::async_trait]
impl MutationCheck for Original {
    async fn check(&self) -> std::result::Result<(), String> {
        let check = self.checks.fetch_add(1, Ordering::SeqCst) + 1;
        if !self.allowed.load(Ordering::SeqCst)
            || Instant::now() >= self.expires
            || (self.refuse_check != 0 && check == self.refuse_check)
        {
            return Err("The original source no longer authorizes this exact write.".into());
        }
        // Re-read registration, canonical target and the complete accepted metadata after
        // the destination lock, on the independent pool. No cached Access or synthetic scope.
        let query = format!(
            "SELECT definition FROM {} WHERE registration = 7 AND target = 'rows'",
            self.metadata
        );
        let current = sqlx::query_scalar::<_, String>(AssertSqlSafe(query.as_str()))
            .fetch_optional(&self.reads)
            .await
            .map_err(|e| e.to_string())?;
        if current.as_ref() != Some(&*self.accepted.lock().unwrap()) {
            return Err("The canonical registration, target or accepted old map changed.".into());
        }
        Ok(())
    }
}

struct Fixture {
    reads: PgPool,
    writer: Writer,
    options: PgConnectOptions,
    schema: String,
    relation: RelationName,
    authority: Arc<Original>,
    application: String,
}

impl Fixture {
    async fn create(refuse_check: usize, expires: Duration) -> Self {
        let uri = std::env::var("MRSM_TEST_PG_URI")
            .expect("This ignored test requires an explicit owned MRSM_TEST_PG_URI fixture");
        let spec = crate::uri::ConnectionSpec::from_uri(&uri, None, None).unwrap();
        assert_eq!(
            spec.host.as_deref(),
            Some("127.0.0.1"),
            "The owned PostgreSQL fixture must use an explicit loopback address"
        );
        assert!(
            spec.port.is_some_and(|port| port > 1024 && port != 5432),
            "The owned PostgreSQL fixture must use an explicit high port, never 5432"
        );
        assert!(
            spec.username
                .as_ref()
                .is_some_and(|value| !value.is_empty())
                && spec
                    .database
                    .as_ref()
                    .is_some_and(|value| !value.is_empty()),
            "The owned PostgreSQL fixture requires explicit user and database names"
        );
        let unique = uuid::Uuid::new_v4().simple().to_string();
        let application = format!("mrsm_scoped_pg_{unique}");
        let options = super::super::postgres::connect_options(&spec)
            .unwrap()
            .application_name(&application);
        // The one auth slot stays free even while both dedicated write slots are occupied.
        let reads = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .connect_with(options.clone())
            .await
            .unwrap();
        let schema = format!("mrsm_scoped_{unique}");
        let relation = RelationName::new(&schema, "rows").unwrap();
        sqlx::query(AssertSqlSafe(format!(
            "CREATE SCHEMA {}",
            identifier(&schema).unwrap()
        )))
        .execute(&reads)
        .await
        .unwrap();
        let create = format!(
            "CREATE TABLE {} (id bigint PRIMARY KEY, value text NOT NULL)",
            relation.quoted().unwrap()
        );
        sqlx::query(AssertSqlSafe(create.as_str()))
            .execute(&reads)
            .await
            .unwrap();
        let metadata = format!("{}.\"metadata\"", identifier(&schema).unwrap());
        let create = format!(
            "CREATE TABLE {metadata} (registration bigint PRIMARY KEY, target text NOT NULL, definition text NOT NULL)"
        );
        sqlx::query(AssertSqlSafe(create.as_str()))
            .execute(&reads)
            .await
            .unwrap();
        let accepted = "{\"source\":\"manual:fixture\",\"access\":{\"owner\":\"original\"},\"target\":\"rows\"}".to_string();
        let insert = format!("INSERT INTO {metadata} VALUES (7, 'rows', $1)");
        sqlx::query(AssertSqlSafe(insert.as_str()))
            .bind(&accepted)
            .execute(&reads)
            .await
            .unwrap();
        let limits = Limits {
            acquire: Duration::from_secs(2),
            lock: Duration::from_secs(2),
            statement: Duration::from_secs(4),
            transaction: Duration::from_secs(10),
        };
        let writer = Writer::lazy(options.clone(), Arc::new(()), limits).unwrap();
        let authority = Arc::new(Original {
            reads: reads.clone(),
            metadata,
            accepted: Mutex::new(accepted),
            allowed: AtomicBool::new(true),
            expires: Instant::now() + expires,
            checks: AtomicUsize::new(0),
            refuse_check,
        });
        Self {
            reads,
            writer,
            options,
            schema,
            relation,
            authority,
            application,
        }
    }

    async fn plan(&self) -> Plan {
        self.writer
            .capture(std::slice::from_ref(&self.relation))
            .await
            .unwrap()
    }

    async fn count(&self) -> i64 {
        let query = format!(
            "SELECT count(*)::bigint FROM {}",
            self.relation.quoted().unwrap()
        );
        sqlx::query_scalar(AssertSqlSafe(query.as_str()))
            .fetch_one(&self.reads)
            .await
            .unwrap()
    }

    async fn cleanup(self) {
        self.writer.pool.close().await;
        let sql = format!("DROP SCHEMA {} CASCADE", identifier(&self.schema).unwrap());
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(&self.reads)
            .await
            .unwrap();
        self.reads.close().await;
    }

    async fn wait_for_destination_lock(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_stat_activity \
                     WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'LOCK TABLE ONLY %')",
                ).bind(&self.application).fetch_one(&self.reads).await.unwrap();
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("The owned writer never waited for its exact destination lock");
    }
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn exact_destination_write_and_copy_keep_original_checks_and_free_auth_capacity() {
    let f = Fixture::create(0, Duration::from_secs(60)).await;
    let mut tx = f
        .writer
        .begin(f.plan().await, f.authority.clone())
        .await
        .unwrap();
    let sql = format!(
        "INSERT INTO {} (id,value) VALUES ($1,$2)",
        f.relation.quoted().unwrap()
    );
    assert_eq!(
        tx.execute_native(&sql, &[SqlParam::Int(1), SqlParam::Text("one".into())])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        tx.copy_csv(
            &f.relation,
            &["id".into(), "value".into()],
            b"2,two\n3,three\n"
        )
        .await
        .unwrap(),
        2
    );
    tx.commit().await.unwrap();
    assert_eq!(f.count().await, 3);
    assert!(f.authority.checks.load(Ordering::SeqCst) >= 6);

    // Two active writer transactions still leave the original read pool usable. A second
    // independent destination avoids intentionally contending with the first transaction.
    let other = RelationName::new(&f.schema, "other").unwrap();
    let sql = format!(
        "CREATE TABLE {} (id bigint PRIMARY KEY, value text)",
        other.quoted().unwrap()
    );
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .execute(&f.reads)
        .await
        .unwrap();
    let first = f
        .writer
        .begin(f.plan().await, f.authority.clone())
        .await
        .unwrap();
    let other_plan = f
        .writer
        .capture(std::slice::from_ref(&other))
        .await
        .unwrap();
    let second = f
        .writer
        .begin(other_plan, f.authority.clone())
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        mutation::recheck(&(f.authority.clone() as Arc<dyn MutationCheck>)),
    )
    .await
    .unwrap()
    .unwrap();
    first.rollback().await.unwrap();
    second.rollback().await.unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn existing_row_lock_wait_refreshes_original_role_before_any_write() {
    let f = Fixture::create(0, Duration::from_secs(60)).await;
    let sql = format!(
        "INSERT INTO {} VALUES (1,'original')",
        f.relation.quoted().unwrap()
    );
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .execute(&f.reads)
        .await
        .unwrap();
    let blocker = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(f.options.clone())
        .await
        .unwrap();
    let mut held = blocker.begin().await.unwrap();
    let sql = format!(
        "SELECT id FROM {} WHERE id=1 FOR UPDATE",
        f.relation.quoted().unwrap()
    );
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .fetch_all(&mut *held)
        .await
        .unwrap();
    let writer = f.writer.clone();
    let plan = f.plan().await;
    let authority = f.authority.clone();
    let pending = tokio::spawn(async move { writer.begin(plan, authority).await });
    f.wait_for_destination_lock().await;
    assert_eq!(f.authority.checks.load(Ordering::SeqCst), 0);
    f.authority.allowed.store(false, Ordering::SeqCst);
    held.commit().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_err(),
        "A revoked source acquired an admitted write transaction"
    );
    assert_eq!(f.count().await, 1);
    blocker.close().await;
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn exact_old_map_and_registration_are_reloaded_after_the_lock_wait() {
    // Change only one authority input at a time. A registration check must not mask an
    // absent complete-old-map comparison, and a target check must independently survive.
    for changed in ["definition", "registration", "target"] {
        let f = Fixture::create(0, Duration::from_secs(60)).await;
        let definition = if changed == "definition" {
            "{\"access\":\"peer-change\"}".to_string()
        } else {
            f.authority.accepted.lock().unwrap().clone()
        };
        let registration = if changed == "registration" { 8i64 } else { 7 };
        let target = if changed == "target" {
            "peer_rows"
        } else {
            "rows"
        };
        let blocker = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(f.options.clone())
            .await
            .unwrap();
        let mut held = blocker.begin().await.unwrap();
        let sql = format!(
            "LOCK TABLE ONLY {} IN ROW EXCLUSIVE MODE",
            f.relation.quoted().unwrap()
        );
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(&mut *held)
            .await
            .unwrap();
        let writer = f.writer.clone();
        let plan = f.plan().await;
        let authority = f.authority.clone();
        let pending = tokio::spawn(async move { writer.begin(plan, authority).await });
        f.wait_for_destination_lock().await;
        assert_eq!(f.authority.checks.load(Ordering::SeqCst), 0);
        let sql = format!(
            "UPDATE {} SET definition=$1,registration=$2,target=$3 WHERE registration=7",
            f.authority.metadata
        );
        assert_eq!(
            sqlx::query(AssertSqlSafe(sql.as_str()))
                .bind(&definition)
                .bind(registration)
                .bind(target)
                .execute(&f.reads)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        held.commit().await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(3), pending)
            .await
            .expect("The owned writer did not revalidate after its lock was released")
            .unwrap();
        assert!(
            outcome.is_err(),
            "The independent {changed} change was not refused"
        );
        assert_eq!(f.authority.checks.load(Ordering::SeqCst), 1);
        assert_eq!(f.count().await, 0);
        let sql = format!(
            "SELECT registration,target,definition FROM {}",
            f.authority.metadata
        );
        let peer = sqlx::query(AssertSqlSafe(sql.as_str()))
            .fetch_one(&f.reads)
            .await
            .unwrap();
        assert_eq!(
            peer.try_get::<i64, _>("registration").unwrap(),
            registration
        );
        assert_eq!(peer.try_get::<String, _>("target").unwrap(), target);
        assert_eq!(peer.try_get::<String, _>("definition").unwrap(), definition);
        blocker.close().await;
        f.cleanup().await;
    }
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn late_revocation_rolls_back_insert_and_copy_then_fresh_retry_uses_clean_pool() {
    let f = Fixture::create(0, Duration::from_secs(60)).await;
    let mut tx = f
        .writer
        .begin(f.plan().await, f.authority.clone())
        .await
        .unwrap();
    let sql = format!(
        "INSERT INTO {} VALUES (1,'uncommitted')",
        f.relation.quoted().unwrap()
    );
    tx.execute_native(&sql, &[]).await.unwrap();
    tx.copy_csv(
        &f.relation,
        &["id".into(), "value".into()],
        b"2,uncommitted\n",
    )
    .await
    .unwrap();
    f.authority.allowed.store(false, Ordering::SeqCst);
    assert!(tx.commit().await.is_err());
    assert_eq!(f.count().await, 0);
    f.authority.allowed.store(true, Ordering::SeqCst);
    let mut fresh = f
        .writer
        .begin(f.plan().await, f.authority.clone())
        .await
        .unwrap();
    fresh
        .copy_csv(&f.relation, &["id".into(), "value".into()], b"3,fresh\n")
        .await
        .unwrap();
    fresh.commit().await.unwrap();
    assert_eq!(f.count().await, 1);
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn cancelled_staged_write_rolls_back_before_the_writer_slot_is_reused() {
    let f = Fixture::create(0, Duration::from_secs(60)).await;
    let plan = f.plan().await;
    let writer = f.writer.clone();
    let authority = f.authority.clone();
    let sql = format!(
        "INSERT INTO {} VALUES (1,'cancelled')",
        f.relation.quoted().unwrap()
    );
    let staged = Arc::new(tokio::sync::Notify::new());
    let staged_child = staged.clone();
    let pending = tokio::spawn(async move {
        let mut tx = writer.begin(plan, authority).await.unwrap();
        tx.execute_native(&sql, &[]).await.unwrap();
        staged_child.notify_one();
        std::future::pending::<()>().await;
        tx.commit().await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(3), staged.notified())
        .await
        .expect("The owned write never reached its staged cancellation point");
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let mut fresh = tokio::time::timeout(
        Duration::from_secs(3),
        f.writer.begin(f.plan().await, f.authority.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    fresh
        .copy_csv(&f.relation, &["id".into(), "value".into()], b"2,fresh\n")
        .await
        .unwrap();
    fresh.commit().await.unwrap();
    let sql = format!(
        "SELECT id FROM {} ORDER BY id",
        f.relation.quoted().unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(sql.as_str()))
            .fetch_all(&f.reads)
            .await
            .unwrap(),
        vec![2]
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn copy_finish_requires_fresh_authority_and_poisoned_transaction_cannot_commit() {
    let f = Fixture::create(3, Duration::from_secs(60)).await;
    let mut tx = f
        .writer
        .begin(f.plan().await, f.authority.clone())
        .await
        .unwrap();
    assert!(
        tx.copy_csv(&f.relation, &["id".into(), "value".into()], b"1,refused\n")
            .await
            .is_err()
    );
    assert!(tx.commit().await.is_err());
    assert_eq!(f.count().await, 0);
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn replaced_target_and_other_driver_plans_are_refused_before_admission() {
    let f = Fixture::create(0, Duration::from_secs(60)).await;
    let plan = f.plan().await;
    let other = Writer::lazy(f.options.clone(), Arc::new(()), f.writer.limits).unwrap();
    assert!(
        other
            .begin(plan.clone(), f.authority.clone())
            .await
            .is_err()
    );
    let sql = format!("DROP TABLE {}", f.relation.quoted().unwrap());
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .execute(&f.reads)
        .await
        .unwrap();
    let sql = format!(
        "CREATE TABLE {} (id bigint PRIMARY KEY,value text NOT NULL)",
        f.relation.quoted().unwrap()
    );
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .execute(&f.reads)
        .await
        .unwrap();
    assert!(f.writer.begin(plan, f.authority.clone()).await.is_err());
    assert_eq!(f.authority.checks.load(Ordering::SeqCst), 0);
    assert_eq!(f.count().await, 0);
    other.pool.close().await;
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn blocked_lock_has_server_deadline_and_expired_source_never_admits() {
    let f = Fixture::create(0, Duration::from_millis(100)).await;
    let blocker = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(f.options.clone())
        .await
        .unwrap();
    let mut held = blocker.begin().await.unwrap();
    let sql = format!(
        "LOCK TABLE ONLY {} IN ROW SHARE MODE",
        f.relation.quoted().unwrap()
    );
    sqlx::query(AssertSqlSafe(sql.as_str()))
        .execute(&mut *held)
        .await
        .unwrap();
    let plan = f.plan().await;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        f.writer.begin(plan, f.authority.clone()),
    )
    .await
    .expect("The server lock timeout did not bound the owned writer wait");
    assert!(result.is_err());
    assert_eq!(f.authority.checks.load(Ordering::SeqCst), 0);
    held.rollback().await.unwrap();
    assert!(
        f.writer
            .begin(f.plan().await, f.authority.clone())
            .await
            .is_err()
    );
    assert_eq!(f.count().await, 0);
    blocker.close().await;
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "Requires an explicit owned PostgreSQL service in MRSM_TEST_PG_URI (loopback high port, never 5432)"]
async fn database_hooks_sequences_and_partition_routes_refuse_without_running_them() {
    let f = Fixture::create(0, Duration::from_secs(60)).await;
    let schema = identifier(&f.schema).unwrap();
    let cases = [
        (
            "serial_rows",
            format!("CREATE TABLE {schema}.serial_rows (id bigserial PRIMARY KEY,value text)"),
        ),
        (
            "checked_rows",
            format!("CREATE TABLE {schema}.checked_rows (id bigint CHECK (id>0),value text)"),
        ),
        (
            "partitioned_rows",
            format!(
                "CREATE TABLE {schema}.partitioned_rows (id bigint,value text) PARTITION BY RANGE(id)"
            ),
        ),
        (
            "policy_rows",
            format!("CREATE TABLE {schema}.policy_rows (id bigint,value text)"),
        ),
    ];
    for (name, sql) in cases {
        sqlx::query(AssertSqlSafe(sql.as_str()))
            .execute(&f.reads)
            .await
            .unwrap();
        if name == "policy_rows" {
            let sql = format!("ALTER TABLE {schema}.policy_rows ENABLE ROW LEVEL SECURITY");
            sqlx::query(AssertSqlSafe(sql.as_str()))
                .execute(&f.reads)
                .await
                .unwrap();
        }
        let name = RelationName::new(&f.schema, name).unwrap();
        match f.writer.capture(std::slice::from_ref(&name)).await {
            Ok(plan) => assert!(f.writer.begin(plan, f.authority.clone()).await.is_err()),
            Err(_) => assert_eq!(name.table, "partitioned_rows"),
        }
    }
    let sql = format!("SELECT is_called FROM {schema}.serial_rows_id_seq");
    assert!(
        !sqlx::query_scalar::<_, bool>(AssertSqlSafe(sql.as_str()))
            .fetch_one(&f.reads)
            .await
            .unwrap()
    );

    // This harmless, local transactional marker establishes that classification runs before
    // any table trigger. It is unrelated to source SQL/expression auditing.
    let sql = format!(
        "CREATE TABLE {schema}.audit (id bigint); CREATE FUNCTION {schema}.mark_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO {schema}.audit VALUES (1); RETURN NEW; END $$; CREATE TRIGGER mark_write BEFORE INSERT ON {} FOR EACH ROW EXECUTE FUNCTION {schema}.mark_write()",
        f.relation.quoted().unwrap()
    );
    sqlx::raw_sql(AssertSqlSafe(sql.as_str()))
        .execute(&f.reads)
        .await
        .unwrap();
    assert!(
        f.writer
            .begin(f.plan().await, f.authority.clone())
            .await
            .is_err()
    );
    let sql = format!("SELECT count(*)::bigint FROM {schema}.audit");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(AssertSqlSafe(sql.as_str()))
            .fetch_one(&f.reads)
            .await
            .unwrap(),
        0
    );
    assert_eq!(f.authority.checks.load(Ordering::SeqCst), 0);
    f.cleanup().await;
}
