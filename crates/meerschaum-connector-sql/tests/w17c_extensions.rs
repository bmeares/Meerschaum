//! Wave 17c: the extension status primitive and the updates behind `mrsm upgrade extensions`
//! against a real TimescaleDB server (`MRSM_TEST_TIMESCALE_URI`; the HA image on 5459 also
//! ships PostGIS and the toolkit with older versions). Each test creates its own scratch
//! database from `template0`, installs OLD extension versions the server still ships, and drops
//! the database afterwards.

use std::time::Duration;

use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::driver::SqlValue;
use meerschaum_connector_sql::extensions::{
    self, DatabaseStatus, UpdateKind, apply, plan, run_after_first_query, run_first_statement,
    spec_for_database, status,
};
use meerschaum_connector_sql::uri::ConnectionSpec;

const TIMEOUT: Duration = Duration::from_secs(20);

fn base_spec() -> Option<ConnectionSpec> {
    let uri = std::env::var("MRSM_TEST_TIMESCALE_URI").ok()?;
    if uri.is_empty() {
        return None;
    }
    Some(ConnectionSpec::from_uri(&uri, Some("w17c_ext"), None).expect("a valid URI"))
}

fn scratch_name() -> String {
    format!(
        "w17ck2_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    )
}

async fn create_db(base: &ConnectionSpec, name: &str) {
    run_first_statement(
        base,
        None,
        &format!("CREATE DATABASE \"{name}\" TEMPLATE template0"),
    )
    .await
    .expect("create the scratch database");
}

async fn drop_db(base: &ConnectionSpec, name: &str) {
    run_first_statement(
        base,
        None,
        &format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"),
    )
    .await
    .expect("drop the scratch database");
}

/// The versions of `ext` older than the default that have an update path to it, newest first.
async fn older_versions(spec: &ConnectionSpec, ext: &str) -> Vec<String> {
    let conn = SqlConnector::connect("w17c_ext_probe", spec.clone())
        .await
        .expect("connect");
    let sql = format!(
        "SELECT string_agg(version, ',' ORDER BY string_to_array(version, '.')::int[] DESC) \
         FROM (SELECT v.version FROM pg_available_extension_versions v \
         JOIN pg_available_extensions a ON a.name = v.name \
         WHERE v.name = '{ext}' AND v.version <> a.default_version \
         AND v.version ~ '^[0-9]+(\\.[0-9]+)*$' \
         AND EXISTS (SELECT 1 FROM pg_extension_update_paths(v.name) p \
         WHERE p.source = v.version AND p.target = a.default_version AND p.path IS NOT NULL)) t"
    );
    match conn.value(&sql).await {
        Some(SqlValue::Text(s)) => s.split(',').map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

/// `CREATE EXTENSION <ext> VERSION <v>` with the newest older version the server can install
/// (the library of an older TimescaleDB must still be shipped). Returns the version.
async fn create_old(spec: &ConnectionSpec, ext: &str) -> Option<String> {
    for version in older_versions(spec, ext).await.into_iter().take(4) {
        let sql = format!("CREATE EXTENSION \"{ext}\" VERSION '{version}' CASCADE");
        if run_first_statement(spec, None, &sql).await.is_ok() {
            return Some(version);
        }
    }
    None
}

async fn db_status(spec: &ConnectionSpec) -> DatabaseStatus {
    status(spec, None, TIMEOUT)
        .await
        .expect("status")
        .expect("a PostgreSQL flavor has extensions")
}

#[tokio::test(flavor = "multi_thread")]
async fn updates_old_extensions_to_the_default_versions() {
    let Some(base) = base_spec() else {
        eprintln!("MRSM_TEST_TIMESCALE_URI is not set; skipping.");
        return;
    };
    let name = scratch_name();
    create_db(&base, &name).await;
    let spec = spec_for_database(&base, Some(&name));
    let result = tokio::spawn({
        let spec = spec.clone();
        async move { updates_body(&spec).await }
    })
    .await;
    drop_db(&base, &name).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e.into_panic());
    }
}

async fn updates_body(spec: &ConnectionSpec) {
    let ts_old = create_old(spec, "timescaledb")
        .await
        .expect("the server ships an older TimescaleDB with an update path");
    let toolkit_old = create_old(spec, "timescaledb_toolkit").await;
    // PostGIS: the image ships only the current base script; installed current, it must not
    // be planned.
    let postgis = run_first_statement(spec, None, "CREATE EXTENSION IF NOT EXISTS postgis")
        .await
        .is_ok();

    let before = db_status(spec).await;
    assert!(before.superuser, "the test user must be a superuser");
    assert!(before.server_version_num >= 120000);
    let ts = before.extension("timescaledb").unwrap();
    assert_eq!(ts.installed, ts_old);
    assert!(ts.lags() && ts.updatable(), "{ts:?}");
    if let Some(old) = &toolkit_old {
        let tk = before.extension("timescaledb_toolkit").unwrap();
        assert_eq!(&tk.installed, old);
        assert!(tk.lags());
    }
    if postgis {
        assert!(!before.extension("postgis").unwrap().lags());
    }
    let planned = plan(&before);
    assert_eq!(planned[0].kind, UpdateKind::Timescaledb);
    assert_eq!(planned[0].statement, "ALTER EXTENSION timescaledb UPDATE;");
    assert!(planned.iter().all(|u| u.kind != UpdateKind::Postgis));
    assert_eq!(
        planned.len(),
        1 + usize::from(toolkit_old.is_some()),
        "{planned:?}"
    );

    // The rule `apply` works around is real: once a statement has loaded the old library,
    // TimescaleDB refuses the update in that session.
    let refused = run_after_first_query(spec, None, "ALTER EXTENSION timescaledb UPDATE")
        .await
        .expect_err("TimescaleDB must refuse the update after the old library was loaded");
    assert!(
        refused
            .to_string()
            .contains("cannot be updated after the old version has already been loaded"),
        "{refused}"
    );
    assert_eq!(
        db_status(spec)
            .await
            .extension("timescaledb")
            .unwrap()
            .installed,
        ts_old
    );

    for update in &planned {
        apply(spec, update)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", update.statement));
    }
    let after = db_status(spec).await;
    assert!(after.lagging().is_empty(), "{:?}", after.lagging());
    for ext in &after.extensions {
        if let Some(available) = &ext.available {
            assert_eq!(&ext.installed, available, "{}", ext.name);
        }
    }
    assert!(plan(&after).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn status_reads_another_database_and_the_server_facts() {
    let Some(base) = base_spec() else {
        eprintln!("MRSM_TEST_TIMESCALE_URI is not set; skipping.");
        return;
    };
    let name = scratch_name();
    create_db(&base, &name).await;
    let result = status(&base, Some(&name), TIMEOUT).await;
    drop_db(&base, &name).await;
    let s = result.unwrap().unwrap();
    assert_eq!(s.database, name);
    assert!(!s.server_version.is_empty());
    // `template0` has only `plpgsql`.
    assert!(
        s.extensions.iter().all(|e| e.name == "plpgsql"),
        "{:?}",
        s.extensions
    );
    // A superuser reads `shared_preload_libraries`; TimescaleDB servers preload it.
    assert!(s.preloaded("timescaledb").unwrap_or(false), "{s:?}");
}

#[tokio::test]
async fn non_postgres_flavors_have_no_extensions() {
    let spec = ConnectionSpec::from_uri("sqlite:///:memory:", Some("w17c_ext"), None).unwrap();
    assert!(status(&spec, None, TIMEOUT).await.unwrap().is_none());
    assert!(!extensions::supports_extensions(spec.flavor));
}

#[tokio::test]
async fn an_unreachable_server_times_out_quickly() {
    // TEST-NET-1 never answers: the bound holds even when the connect hangs.
    let spec =
        ConnectionSpec::from_uri("postgresql://u:p@192.0.2.1:5432/db", Some("w17c_ext"), None)
            .unwrap();
    let started = std::time::Instant::now();
    let result = status(&spec, None, Duration::from_millis(300)).await;
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
}
