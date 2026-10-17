//! Driver integration tests for the non-`sqlx` families (`tiberius`, `duckdb`, `oracle`).
//!
//! - MSSQL: `MRSM_TEST_MSSQL_URI`, e.g.
//!   `mssql://sa:supersecureSECRETPASSWORD123!@localhost:1439/master`.
//! - DuckDB (cargo feature `duckdb`): `MRSM_TEST_DUCKDB_PATH`, a directory for temporary database
//!   files (empty means the system temporary directory).
//! - Oracle (cargo feature `oracle`, Instant Client required): `MRSM_TEST_ORACLE_URI`, e.g.
//!   `oracle://system:oracle@localhost:1529/xe`.

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use futures_util::StreamExt;
use meerschaum_connector_sql::driver::{SqlDriver, SqlParam, SqlValue};
use meerschaum_connector_sql::uri::ConnectionSpec;

#[allow(dead_code)]
async fn driver_for(env: &str, flavor: &str) -> Option<(SqlDriver, Option<std::path::PathBuf>)> {
    let Ok(value) = std::env::var(env) else {
        eprintln!("Skipping {flavor}: set {env}.");
        return None;
    };
    if flavor == "duckdb" {
        let dir = if value.is_empty() {
            std::env::temp_dir()
        } else {
            std::path::PathBuf::from(value)
        };
        let path = dir.join(format!("_mrsm_rs_{}.duckdb", uuid::Uuid::new_v4().simple()));
        let uri = format!("duckdb:///{}", path.display());
        let spec = ConnectionSpec::from_uri(&uri, None, None).unwrap();
        return Some((SqlDriver::connect(&spec).await.unwrap(), Some(path)));
    }
    let spec = ConnectionSpec::from_uri(&value, None, None).unwrap();
    Some((SqlDriver::connect(&spec).await.unwrap(), None))
}

/// The shared checks: DDL, transactions, bound parameters, streaming, `value`.
#[allow(dead_code)]
async fn exercise(driver: &SqlDriver, q: impl Fn(&str) -> String, ph: impl Fn(usize) -> String) {
    let table = format!(
        "w5sql_drv_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let t = q(&table);
    let flavor = driver.flavor();
    let (bigint, float, text, boolean) = match flavor.as_str() {
        "oracle" => ("NUMBER(19)", "BINARY_DOUBLE", "NVARCHAR2(200)", "NUMBER(1)"),
        "mssql" => ("BIGINT", "FLOAT", "NVARCHAR(200)", "BIT"),
        _ => ("BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN"),
    };
    driver
        .exec(&format!(
            "CREATE TABLE {t} ({} {bigint}, {} {float}, {} {text}, {} {boolean})",
            q("id"),
            q("val"),
            q("txt"),
            q("flag")
        ))
        .await
        .unwrap();

    let insert = format!(
        "INSERT INTO {t} ({}, {}, {}, {}) VALUES ({}, {}, {}, {})",
        q("id"),
        q("val"),
        q("txt"),
        q("flag"),
        ph(1),
        ph(2),
        ph(3),
        ph(4)
    );
    let flag = |b: bool| {
        if flavor.as_str() == "oracle" {
            SqlParam::Int(b as i64)
        } else {
            SqlParam::Bool(b)
        }
    };
    let results = driver
        .exec_many_with(&[
            (
                insert.clone(),
                vec![
                    SqlParam::Int(1),
                    SqlParam::Float(1.5),
                    SqlParam::Text("a".into()),
                    flag(true),
                ],
            ),
            (
                insert.clone(),
                vec![
                    SqlParam::Int(2),
                    SqlParam::Null,
                    SqlParam::Null,
                    flag(false),
                ],
            ),
        ])
        .await
        .unwrap();
    assert_eq!(results, vec![Some(1), Some(1)]);

    // A failing batch rolls back.
    let results = driver
        .exec_queries(
            &[
                format!("INSERT INTO {t} ({}) VALUES (3)", q("id")),
                format!(
                    "INSERT INTO {} ({}) VALUES (4)",
                    q("w4sql_missing_table"),
                    q("id")
                ),
            ],
            true,
            true,
        )
        .await
        .unwrap();
    assert_eq!(results, vec![Some(1), None]);

    let count = driver
        .value(&format!("SELECT COUNT(*) FROM {t}"))
        .await
        .unwrap()
        .and_then(|v| v.as_i64());
    assert_eq!(count, Some(2));

    let order = format!("SELECT * FROM {t} ORDER BY {}", q("id"));
    let batches = driver.read(&order).await.unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    let ids: Vec<i64> = match batch.column(0).data_type() {
        DataType::Int64 => batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec(),
        other => {
            let c = arrow::compute::cast(batch.column(0), &DataType::Int64)
                .unwrap_or_else(|_| panic!("ids as {other:?}"));
            c.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        }
    };
    assert_eq!(ids, vec![1, 2]);
    let vals = arrow::compute::cast(batch.column(1), &DataType::Float64).unwrap();
    let vals = vals.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(vals.value(0), 1.5);
    assert!(vals.is_null(1));
    let txt = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(txt.value(0), "a");
    if flavor.as_str() != "oracle" {
        let flags = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flags.value(0));
        assert!(!flags.value(1));
    }

    // Streaming in chunks of one row.
    let stream = driver.read_stream(&order, 1).await.unwrap();
    let batches: Vec<_> = stream.collect().await;
    assert_eq!(batches.len(), 2);

    // An empty result keeps its schema.
    let empty = driver
        .read_frame(&format!("SELECT * FROM {t} WHERE {} = 999", q("id")))
        .await
        .unwrap();
    assert_eq!(empty.height(), 0);
    assert_eq!(empty.names().len(), 4);

    // Bound reads.
    let value = driver
        .value_with(
            &format!("SELECT {} FROM {t} WHERE {} = {}", q("txt"), q("id"), ph(1)),
            &[SqlParam::Int(1)],
        )
        .await
        .unwrap();
    assert_eq!(value, Some(SqlValue::Text("a".into())));

    // Transactions.
    let mut tx = driver.begin().await.unwrap();
    tx.exec(&format!("DELETE FROM {t}")).await.unwrap();
    tx.rollback().await.unwrap();
    let count = driver
        .value(&format!("SELECT COUNT(*) FROM {t}"))
        .await
        .unwrap()
        .and_then(|v| v.as_i64());
    assert_eq!(count, Some(2));

    driver.exec(&format!("DROP TABLE {t}")).await.unwrap();
}

#[cfg(feature = "mssql")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mssql_driver() {
    let Some((driver, _)) = driver_for("MRSM_TEST_MSSQL_URI", "mssql").await else {
        return;
    };
    exercise(&driver, |s| format!("[{s}]"), |n| format!("@P{n}")).await;

    // TDS types.
    let df = driver
        .read_frame(
            "SELECT CAST(1.25 AS NUMERIC(10, 2)) AS n, \
             CAST('2024-01-01 01:02:03.000004' AS DATETIME2) AS dt, \
             CAST('2024-01-01 01:02:03 +00:00' AS DATETIMEOFFSET) AS dto, \
             CAST('deadbeef-abba-babe-cafe-decafc0ffee5' AS UNIQUEIDENTIFIER) AS u, \
             CAST(0x0102 AS VARBINARY(MAX)) AS b, N'héllo' AS s",
        )
        .await
        .unwrap();
    assert_eq!(df.height(), 1);
    let v = driver
        .value("SELECT CAST(0x0102 AS VARBINARY(10))")
        .await
        .unwrap();
    assert_eq!(v, Some(SqlValue::Bytes(vec![1, 2])));
}

#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_driver() {
    let Some((driver, path)) = driver_for("MRSM_TEST_DUCKDB_PATH", "duckdb").await else {
        return;
    };
    exercise(&driver, |s| format!("\"{s}\""), |n| format!("${n}")).await;
    driver.close().await;
    if let Some(p) = path {
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file(p.with_extension("duckdb.wal"));
    }
}

#[cfg(feature = "oracle")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_driver() {
    let Some((driver, _)) = driver_for("MRSM_TEST_ORACLE_URI", "oracle").await else {
        return;
    };
    exercise(&driver, |s| format!("\"{s}\""), |n| format!(":{n}")).await;
}
