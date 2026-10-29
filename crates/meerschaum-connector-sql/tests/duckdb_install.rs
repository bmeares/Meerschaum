//! The real download path of the DuckDB driver: the installer fetches the pinned release asset of
//! this platform (a SHA-256 mismatch fails the install), extracts the library into a temporary
//! root, the loader finds it in that cache, and a query runs. Network access is needed, so it runs
//! only when `MRSM_TEST_DUCKDB_DOWNLOAD` is set (an empty value counts as set): the hosted
//! `duckdb-platforms` CI jobs (Linux arm64, macOS, Windows) set it.

use meerschaum_connector_sql::driver::{SqlDriver, SqlValue};
use meerschaum_connector_sql::duckdb_lib::{self, NullReporter, Settings, Source};
use meerschaum_connector_sql::uri::ConnectionSpec;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_pinned_library_downloads_verifies_loads_and_runs() {
    if std::env::var("MRSM_TEST_DUCKDB_DOWNLOAD").is_err() {
        eprintln!("Skipping the DuckDB download test: set MRSM_TEST_DUCKDB_DOWNLOAD.");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let settings = Settings {
        root: Some(root.path().to_path_buf()),
        lib: None,
        no_auto_install: false,
    };
    let path = duckdb_lib::install(&settings, false, &NullReporter)
        .await
        .unwrap();
    assert!(path.is_file(), "{path:?}");
    assert!(
        path.starts_with(duckdb_lib::cache_dir(root.path())),
        "{path:?}"
    );
    // A second install keeps the cached file (no download).
    let again = duckdb_lib::install(&settings, false, &NullReporter)
        .await
        .unwrap();
    assert_eq!(again, path);

    // The system loader path may hold another DuckDB; this test's cache comes first only when
    // the process has not loaded one, which is the case in a fresh test process.
    let loaded = duckdb_lib::ensure_loaded(&settings).await.unwrap();
    assert_eq!(loaded.source, Source::Cache, "{loaded:?}");
    assert_eq!(loaded.version, format!("v{}", duckdb_lib::VERSION));

    let spec = ConnectionSpec::from_uri("duckdb:///:memory:", None, None).unwrap();
    let driver = SqlDriver::connect(&spec).await.unwrap();
    assert_eq!(
        driver.value("SELECT 42").await.unwrap(),
        Some(SqlValue::Int(42))
    );
    // The official build bundles ICU.
    assert!(matches!(
        driver
            .value("SELECT TIMESTAMPTZ '2024-01-02 00:00:00+00' AT TIME ZONE 'Asia/Tokyo'")
            .await
            .unwrap(),
        Some(SqlValue::Timestamp(_))
    ));
}
