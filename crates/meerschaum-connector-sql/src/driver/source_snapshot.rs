//! Private SQLite source connections with minimal native row prefetch.
//!
//! A snapshot must never inherit the ordinary connector's 8192-row prefetch queue.
//! This pool is separate from both canonical metadata reads and scoped writers.

use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

use super::{SqlTransaction, TxInner, sqlite};
use crate::uri::ConnectionSpec;

pub(super) fn pool(spec: &ConnectionSpec) -> Option<SqlitePool> {
    // Anonymous/in-memory databases cannot be safely reopened by a separate read pool.
    // Refuse rather than read an unrelated empty database with the same logical label.
    let database = spec.resolved_database().ok().flatten()?;
    if database.is_empty() || database == ":memory:" {
        return None;
    }
    let options = sqlite::read_only_connect_options(spec)
        .ok()?
        .row_buffer_size(1);
    let geopackage = spec.flavor == crate::flavor::Flavor::Geopackage;
    Some(
        sqlite::refresh_schema_on_acquire(SqlitePoolOptions::new())
            .after_connect(move |connection, _| {
                Box::pin(async move {
                    // Compose the same GPKG hook as ordinary connections before applying this
                    // dedicated pool's permanent source-only native row limit.
                    if geopackage {
                        super::gpkg_functions::register(connection).await?;
                    }
                    limit_row_length(connection).await
                })
            })
            .max_connections(8)
            .min_connections(0)
            .acquire_timeout(std::time::Duration::from_secs(20))
            .connect_lazy_with(options),
    )
}

/// Apply the fixed host cap only to this dedicated source pool for its entire lifetime.
/// Ordinary readers/writers are separate physical connections and retain their own limits.
/// https://sqlite.org/c3ref/limit.html and https://sqlite.org/limits.html define this
/// per-connection maximum for individual strings/blobs and encoded SELECT/INSERT rows.
#[allow(unsafe_code)]
async fn limit_row_length(
    connection: &mut sqlx::sqlite::SqliteConnection,
) -> Result<(), sqlx::Error> {
    const CAP: i32 = 8 * 1024 * 1024;
    let mut guard = connection.lock_handle().await?;
    // SAFETY: SQLx's LockedSqliteHandle guarantees its worker performs no FFI on this
    // connection or its statements while this guard exists. The pointer comes from that
    // live locked handle, is used only for synchronous sqlite3_limit calls, and never
    // escapes this function. SQLx and this crate use the same pinned libsqlite3-sys 0.37
    // SQLite type/library (also used by gpkg_functions). Constants are host-owned and valid.
    // This pool is dedicated and read-only for its lifetime, so no cross-pool restoration
    // or cancellation-dependent cleanup is necessary; lowering this limit is permanent.
    let actual = unsafe {
        let handle = guard.as_raw_handle().as_ptr();
        libsqlite3_sys::sqlite3_limit(handle, libsqlite3_sys::SQLITE_LIMIT_LENGTH, CAP);
        libsqlite3_sys::sqlite3_limit(handle, libsqlite3_sys::SQLITE_LIMIT_LENGTH, -1)
    };
    if actual <= 0 || actual > CAP {
        return Err(sqlx::Error::Protocol(
            "Could not apply the native source row bound.".into(),
        ));
    }
    Ok(())
}

pub(super) async fn begin(pool: &SqlitePool) -> crate::Result<SqlTransaction> {
    Ok(SqlTransaction {
        authorization_pin: None,
        authorization_read: None,
        inner: TxInner::Sqlite(pool.begin().await?),
        mutation_check: None,
        write_lock: false,
        refused: false,
    })
}

/// Account borrowed raw values before copying them into a native Frame batch. A single
/// SQLite/SQLx row has already been allocated, within this dedicated pool's permanent
/// native 8MiB row limit. Include copied row-map column keys and per-cell bookkeeping.
pub(super) fn row_bytes(row: &sqlx::sqlite::SqliteRow) -> crate::Result<usize> {
    use sqlx::{Column, Row, TypeInfo, ValueRef};
    let mut size = 0usize;
    for (index, column) in row.columns().iter().enumerate() {
        let raw = row.try_get_raw(index)?;
        let bytes = if raw.is_null() {
            0
        } else {
            match raw.type_info().name() {
                "TEXT" | "BLOB" => row.try_get::<&[u8], _>(index)?.len(),
                _ => 16,
            }
        };
        size = size
            .saturating_add(column.name().len())
            .saturating_add(bytes)
            .saturating_add(16);
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::SqlDriver;

    async fn driver() -> (tempfile::TempDir, SqlDriver) {
        let root = tempfile::tempdir().unwrap();
        let mut spec = ConnectionSpec::from_uri(
            &format!("sqlite:///{}", root.path().join("source.db").display()),
            Some("source"),
            None,
        )
        .unwrap();
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let mut driver = SqlDriver::connect(&spec).await.unwrap();
        // One physical dedicated reader proves reuse rather than replacement by another
        // clean connection after a failed row or VM operation.
        let pool = driver.snapshot_sqlite_pool.take().unwrap();
        driver.snapshot_sqlite_pool = Some(
            pool.options()
                .clone()
                .max_connections(1)
                .connect_lazy_with((*pool.connect_options()).clone()),
        );
        (root, driver)
    }

    #[tokio::test]
    async fn raw_text_and_column_keys_bound_frame_batches_and_oversize_refusal_reuses_pool() {
        let (_root, driver) = driver().await;
        driver.exec("CREATE TABLE wide_rows (id INTEGER, payload TEXT); INSERT INTO wide_rows SELECT 1, replace(hex(zeroblob(131072)), '00', 'x'); INSERT INTO wide_rows SELECT id + 1, payload FROM wide_rows; INSERT INTO wide_rows SELECT id + 2, payload FROM wide_rows").await.unwrap();
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        let mut rows = 0usize;
        let mut batches = 0usize;
        transaction
            .for_each_sqlite_batch("SELECT * FROM wide_rows", 2048, 200_000, |frame| {
                assert_eq!(
                    frame.height(),
                    1,
                    "raw text bytes flush before the frame batch accumulates"
                );
                rows += frame.height();
                batches += 1;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!((rows, batches), (4, 4));
        transaction.rollback().await.unwrap();
        let name = "column_name_".repeat(200);
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        let mut rows = 0;
        transaction
            .for_each_sqlite_batch(
                &format!("SELECT id AS \"{name}\" FROM wide_rows"),
                2048,
                4096,
                |frame| {
                    assert_eq!(
                        frame.height(),
                        1,
                        "copied row-map column keys count toward the same budget"
                    );
                    rows += frame.height();
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(rows, 4);
        transaction.rollback().await.unwrap();
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        let mut accepted = false;
        let result = transaction
            .for_each_sqlite_batch("SELECT payload FROM wide_rows", 2048, 1024, |_| {
                accepted = true;
                Ok(())
            })
            .await;
        assert!(result.is_err());
        assert!(
            !accepted,
            "one oversized row never reaches Frame/row-map conversion"
        );
        transaction.rollback().await.unwrap();
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        assert_eq!(
            transaction
                .read_frame("SELECT count(*) AS n FROM wide_rows")
                .await
                .unwrap()
                .height(),
            1
        );
        transaction.rollback().await.unwrap();
        assert_eq!(driver.snapshot_sqlite_pool.as_ref().unwrap().size(), 1);
    }

    #[tokio::test]
    async fn source_read_pool_clears_vm_budget_before_an_unbudgeted_long_read() {
        let (_root, driver) = driver().await;
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        transaction
            .set_sqlite_read_budget(
                std::time::Instant::now(),
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .await
            .unwrap();
        let query = "WITH RECURSIVE numbers(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM numbers WHERE n < 10000) SELECT sum(n) AS total FROM numbers";
        assert!(transaction.read_frame(query).await.is_err());
        transaction.clear_sqlite_read_budget().await.unwrap();
        transaction.rollback().await.unwrap();
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        let frame = transaction.read_frame(query).await.unwrap();
        assert_eq!(
            frame.column("total").unwrap().data.get(0).py_str(),
            "50005000"
        );
        transaction.rollback().await.unwrap();
        assert_eq!(driver.snapshot_sqlite_pool.as_ref().unwrap().size(), 1);
    }
    #[tokio::test]
    async fn native_row_limit_refuses_ten_mib_before_batch_and_leaves_ordinary_pool_unchanged() {
        let (_root, driver) = driver().await;
        driver.exec("CREATE TABLE oversized_rows (payload BLOB); INSERT INTO oversized_rows VALUES (zeroblob(10485760))").await.unwrap();
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        let mut accepted = false;
        let result = transaction
            .for_each_sqlite_batch(
                "SELECT payload FROM oversized_rows",
                2048,
                8 * 1024 * 1024,
                |_| {
                    accepted = true;
                    Ok(())
                },
            )
            .await;
        assert!(
            result.is_err(),
            "native SQLite must refuse its source connection's 8MiB row limit"
        );
        assert!(
            !accepted,
            "oversized row never reaches native batch conversion"
        );
        transaction.rollback().await.unwrap();
        let frame = driver
            .read_frame("SELECT payload FROM oversized_rows")
            .await
            .unwrap();
        assert_eq!(
            frame.height(),
            1,
            "ordinary native connections retain their original row limit"
        );
        if let meerschaum_kernel::ColumnData::Native(series) = &frame.columns()[0].data {
            assert!(series.estimated_size() >= 10 * 1024 * 1024);
        } else {
            panic!("native BLOB column expected");
        }
        let mut transaction = driver.begin_source_snapshot().await.unwrap();
        assert_eq!(
            transaction
                .read_frame("SELECT 1 AS value")
                .await
                .unwrap()
                .height(),
            1
        );
        transaction.rollback().await.unwrap();
        assert_eq!(driver.snapshot_sqlite_pool.as_ref().unwrap().size(), 1);
    }
}
