//! `MRSM_NO_AUTO_INSTALL` also turns off DuckDB's own extension auto-installer (which would
//! otherwise download known extensions into `~/.duckdb` on first use). Its own test binary: it
//! sets the variable for the whole process, before any connection exists. Needs the real library
//! (`MRSM_DUCKDB_LIB`, skipped without it like the other library tests).

use meerschaum_connector_sql::driver::{SqlDriver, SqlValue};
use meerschaum_connector_sql::uri::ConnectionSpec;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_auto_install_turns_off_the_extension_installer() {
    if std::env::var("MRSM_DUCKDB_LIB").is_err() {
        eprintln!("Skipping the DuckDB extension test: set MRSM_DUCKDB_LIB.");
        return;
    }
    // SAFETY: the only test of this binary, set before any other thread reads the environment.
    unsafe { std::env::set_var("MRSM_NO_AUTO_INSTALL", "1") };
    let spec = ConnectionSpec::from_uri("duckdb:///:memory:", None, None).unwrap();
    let driver = SqlDriver::connect(&spec).await.unwrap();
    assert_eq!(
        driver
            .value("SELECT CAST(current_setting('autoinstall_known_extensions') AS VARCHAR)")
            .await
            .unwrap(),
        Some(SqlValue::Text("false".into()))
    );
}
