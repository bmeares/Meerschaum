use super::*;

#[test]
fn session_lock_names_bind_the_database_schema_and_physical_table() {
    fn name(database: &str, schema: Option<&str>, table: &str, flavor: Flavor) -> String {
        session_initialization_lock_name(database, schema, table, flavor)
    }
    let mysql = name("owned", None, "mrsm_engine_metrics", Flavor::Mysql);
    assert_eq!(mysql.len(), 64);
    assert!(mysql.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(
        mysql,
        name("OWNED", None, "MRSM_ENGINE_METRICS", Flavor::Mariadb)
    );
    assert_ne!(
        mysql,
        name("other", None, "mrsm_engine_metrics", Flavor::Mysql)
    );
    assert_ne!(
        mysql,
        name("owned", None, "mrsm_engine_runs", Flavor::Mysql)
    );
    assert_ne!(
        name("owned", Some("a"), "table", Flavor::Mssql),
        name("owned", Some("b"), "table", Flavor::Mssql)
    );
    for flavor in [Flavor::Mysql, Flavor::Mariadb, Flavor::Mssql] {
        let long = "internal_".repeat(64);
        let physical = crate::ident::truncate_item_name(&long, flavor);
        assert_eq!(
            name("owned", None, &long, flavor),
            name("owned", None, &physical, flavor)
        );
    }
}

async fn session_test_driver(spec: &ConnectionSpec) -> SqlDriver {
    let driver = if spec.flavor.driver_family() == DriverFamily::Mysql {
        SqlDriver::new(
            spec,
            Pool::Mysql(
                sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(1)
                    .acquire_timeout(std::time::Duration::from_secs(1))
                    .connect_lazy_with(mysql::connect_options(spec).unwrap()),
            ),
        )
    } else {
        SqlDriver::connect_lazy(spec).unwrap()
    };
    driver.ready().await.unwrap();
    driver
}

async fn session_internal_initialization_locks(variable: &str, flavor: Flavor) {
    let Ok(uri) = std::env::var(variable) else {
        return;
    };
    let mut spec = ConnectionSpec::from_uri(&uri, Some("internal_lock"), None).unwrap();
    spec.flavor = flavor;
    spec.pool_size = 1;
    spec.max_overflow = 0;
    spec.params.insert("connect_timeout".into(), "1".into());
    let driver = session_test_driver(&spec).await;
    let contender = session_test_driver(&spec).await;
    let target = format!("mrsm_init_{}", uuid::Uuid::new_v4().simple());
    let schema_sql = if flavor.driver_family() == DriverFamily::Mysql {
        "SELECT DATABASE()"
    } else {
        "SELECT SCHEMA_NAME()"
    };
    let schema = driver.value(schema_sql).await.unwrap().unwrap();
    let SqlValue::Text(schema) = schema else {
        panic!("expected a selected database/schema");
    };
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let holder_driver = driver.clone();
    let holder_target = target.clone();
    let holder = tokio::spawn(async move {
        let _guard = holder_driver
            .lock_internal_initialization(&holder_target, Some(&schema))
            .await
            .unwrap()
            .unwrap();
        entered_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    entered_rx.await.unwrap();
    // Exercise the driver's own bounded timeout, including equivalent default schema.
    let timed_out = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        contender.lock_internal_initialization(&target, None),
    )
    .await
    .expect("the configured acquisition deadline must be enforced");
    assert!(matches!(
        timed_out,
        Err(crate::SqlError::Driver(sqlx::Error::PoolTimedOut))
    ));
    // An independently cancelled waiter must not leave a session lock in either pool.
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(150),
            contender.lock_internal_initialization(&target, None),
        )
        .await
        .is_err()
    );
    let other = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        contender.lock_internal_initialization(&format!("{target}_other"), None),
    )
    .await
    .expect("different targets must initialize independently")
    .unwrap()
    .unwrap();
    assert_eq!(
        contender
            .value("SELECT 1")
            .await
            .unwrap()
            .and_then(|v| v.as_i64()),
        Some(1),
        "an initialization lock must not hold the only pool slot"
    );
    drop(other);
    holder.abort();
    assert!(holder.await.unwrap_err().is_cancelled());
    let guard = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        contender.lock_internal_initialization(&target, None),
    )
    .await
    .expect("a cancelled holder must close its dedicated session")
    .unwrap()
    .unwrap();
    drop(guard);
    let guard = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        driver.lock_internal_initialization(&target, None),
    )
    .await
    .expect("a dropped guard must not return a locked connection to a pool")
    .unwrap()
    .unwrap();
    drop(guard);
    driver.close().await;
    contender.close().await;
}

#[tokio::test]
async fn mysql_internal_initialization_locks() {
    session_internal_initialization_locks("MRSM_TEST_MYSQL_URI", Flavor::Mysql).await;
}

#[tokio::test]
async fn mariadb_internal_initialization_locks() {
    session_internal_initialization_locks("MRSM_TEST_MARIADB_URI", Flavor::Mariadb).await;
}

#[cfg(feature = "mssql")]
#[tokio::test]
async fn mssql_internal_initialization_locks() {
    session_internal_initialization_locks("MRSM_TEST_MSSQL_URI", Flavor::Mssql).await;
}

#[tokio::test]
async fn cancelled_initializer_releases_database_lock() {
    let Ok(uri) = std::env::var("MRSM_TEST_PG_URI") else {
        return;
    };
    let spec = ConnectionSpec::from_uri(&uri, Some("w10_lock"), None).unwrap();
    let driver = SqlDriver::connect(&spec).await.unwrap();
    let target = format!("w10_lock_{}", uuid::Uuid::new_v4().simple());
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let holder_driver = driver.clone();
    let holder_target = target.clone();
    let holder = tokio::spawn(async move {
        let _guard = holder_driver
            .lock_internal_initialization(&holder_target, Some("public"))
            .await
            .unwrap()
            .unwrap();
        entered_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    entered_rx.await.unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(150),
            driver.lock_internal_initialization(&target, None),
        )
        .await
        .is_err(),
        "the unqualified target must share the public target's lock"
    );
    // A different physical target must initialize independently.
    let other = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        driver.lock_internal_initialization(&format!("{target}_other"), Some("public")),
    )
    .await
    .unwrap()
    .unwrap();
    drop(other);
    holder.abort();
    assert!(holder.await.unwrap_err().is_cancelled());
    // Cancellation of both a holder and a waiter leaves no pooled session lock.
    let guard = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        driver.lock_internal_initialization(&target, None),
    )
    .await
    .expect("cancellation must close the connection and release its lock")
    .unwrap();
    assert!(guard.is_some());
}
