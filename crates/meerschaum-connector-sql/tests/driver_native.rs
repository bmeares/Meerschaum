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

    // `sql <label> "CREATE VIEW ... AS SELECT ..."` reaches the driver through `read` (v4 routes any
    // query containing `select` there): batch-leading statements run as is, like pyodbc's
    // `cursor.execute`, instead of nesting in `sp_executesql` ("Incorrect syntax near the keyword
    // 'VIEW'"). They return no rows.
    let tag = &uuid::Uuid::new_v4().simple().to_string()[..8];
    let (view, view2, proc_) = (
        format!("w17bo_v_{tag}"),
        format!("w17bo_v2_{tag}"),
        format!("w17bo_p_{tag}"),
    );
    let exists = |kind: &'static str, name: String| {
        let driver = &driver;
        async move {
            driver
                .value(&format!(
                    "SELECT COUNT(*) FROM sys.{kind} WHERE name = '{name}'"
                ))
                .await
                .unwrap()
                .and_then(|v| v.as_i64())
        }
    };
    for statement in [
        format!("CREATE VIEW dbo.{view} AS SELECT 1 AS a"),
        format!("CREATE OR ALTER VIEW dbo.{view2} AS SELECT 1 AS a"),
        format!("CREATE PROCEDURE dbo.{proc_} AS SELECT 1 AS a"),
    ] {
        let frame = driver
            .read_frame(&statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
        assert_eq!(frame.height(), 0, "{statement}");
    }
    assert_eq!(exists("views", view.clone()).await, Some(1));
    assert_eq!(exists("views", view2.clone()).await, Some(1));
    assert_eq!(exists("procedures", proc_.clone()).await, Some(1));
    // A reading `SELECT` still returns its rows, and the views read back.
    let frame = driver
        .read_frame(&format!("SELECT * FROM dbo.{view}"))
        .await
        .unwrap();
    assert_eq!(frame.height(), 1);
    driver.exec(&format!("DROP VIEW dbo.{view}")).await.unwrap();
    driver
        .exec(&format!("DROP VIEW dbo.{view2}"))
        .await
        .unwrap();
    driver
        .exec(&format!("DROP PROCEDURE dbo.{proc_}"))
        .await
        .unwrap();
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

    // An unconstrained `NUMBER` (`COUNT(*)`, expressions) reads as python-oracledb gives it: an int
    // when every value is integral, a float when one has a fraction; declared precision / scale keeps
    // its decimal.
    let df = driver
        .read_frame("SELECT COUNT(*) AS n, 1 + 1 AS two, 3 / 2 AS frac, CAST(1.25 AS NUMBER(10, 2)) AS d FROM dual")
        .await
        .unwrap();
    let dtypes = df.dtypes();
    assert_eq!(dtypes["n"], "int64", "{dtypes:?}");
    assert_eq!(dtypes["two"], "int64", "{dtypes:?}");
    assert_eq!(dtypes["frac"], "float64", "{dtypes:?}");
    assert_eq!(dtypes["d"], "object", "{dtypes:?}");
    assert_eq!(df.data("n").unwrap().values()[0].py_str(), "1");
    assert_eq!(df.data("frac").unwrap().values()[0].py_str(), "1.5");
    // An integral value too big for an `i64` keeps its exact digits.
    let big = driver
        .read_frame("SELECT 12345678901234567890 AS big FROM dual")
        .await
        .unwrap();
    assert_eq!(
        big.data("big").unwrap().values()[0].py_str(),
        "12345678901234567890"
    );
    assert_eq!(big.dtypes()["big"], "object");
    // A stream handed out batch by batch keeps one schema whatever later chunks hold: the column is
    // exact decimal text (never cast down), here ints and then a float / a huge integer.
    for (last, expected) in [
        ("2.5", vec!["1", "2", "2.5"]),
        (
            "12345678901234567890",
            vec!["1", "2", "12345678901234567890"],
        ),
    ] {
        let sql = format!(
            "SELECT n FROM (SELECT 1 AS n, 1 AS o FROM dual UNION ALL SELECT 2, 2 FROM dual UNION ALL SELECT {last}, 3 FROM dual) ORDER BY o"
        );
        let stream = driver.read_stream(&sql, 1).await.unwrap();
        let schema = stream.schema();
        let batches = stream.collect_batches().await.unwrap();
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|b| b.schema() == schema), "{schema:?}");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        let values: Vec<String> = batches
            .iter()
            .map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_string()
            })
            .collect();
        assert_eq!(values, expected);
    }
    // A result collected as a frame is typed from all of its values, across chunks of rows
    // (`DEFAULT_CHUNKSIZE` is 100000): ints and then one float give float64, ints and then a huge
    // integer keep every digit.
    let spanning = |last: &str| {
        format!(
            "SELECT CASE WHEN LEVEL = 100001 THEN {last} ELSE LEVEL END AS n FROM dual CONNECT BY LEVEL <= 100001"
        )
    };
    let df = driver.read_frame(&spanning("2.5")).await.unwrap();
    assert_eq!(df.dtypes()["n"], "float64");
    let values = df.data("n").unwrap().values();
    assert_eq!(values.len(), 100_001);
    assert_eq!(values[0].py_str(), "1.0");
    assert_eq!(values[99_999].py_str(), "100000.0");
    assert_eq!(values[100_000].py_str(), "2.5");
    let df = driver
        .read_frame(&spanning("12345678901234567890"))
        .await
        .unwrap();
    assert_eq!(df.dtypes()["n"], "object");
    let values = df.data("n").unwrap().values();
    assert_eq!(values[0].py_str(), "1");
    assert_eq!(values[100_000].py_str(), "12345678901234567890");
    // Integral chunks stay `int64`.
    let ints = driver
        .read_frame("SELECT 1 AS n FROM dual UNION ALL SELECT 2 FROM dual")
        .await
        .unwrap();
    assert_eq!(ints.dtypes()["n"], "int64");
}
