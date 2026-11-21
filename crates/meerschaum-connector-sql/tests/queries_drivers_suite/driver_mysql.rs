//! MySQL / MariaDB driver integration tests. Run only when `MRSM_TEST_MYSQL_URI` is set, e.g.
//! `MRSM_TEST_MYSQL_URI=mysql://test:test1234@localhost:3309/testdb` (see the crate README).

use std::panic::AssertUnwindSafe;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Int64Array, StringArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use futures_util::FutureExt;
use meerschaum_connector_sql::driver::{SqlDriver, SqlParam, SqlValue};
use meerschaum_connector_sql::uri::ConnectionSpec;

#[tokio::test]
async fn mysql_types_and_exec_queries() {
    let Ok(uri) = std::env::var("MRSM_TEST_MYSQL_URI") else {
        eprintln!("Skipping: set MRSM_TEST_MYSQL_URI to run the MySQL driver tests.");
        return;
    };
    let spec = ConnectionSpec::from_uri(&uri, None, None).unwrap();
    let driver = SqlDriver::connect(&spec).await.unwrap();
    let table = format!("w5sql_test_{}", uuid::Uuid::new_v4().simple());

    driver
        .exec(&format!(
            "CREATE TABLE `{table}` (\
                `id` BIGINT, `dt` DATETIME(6), `ts` TIMESTAMP(6) NULL, `num` DECIMAL(38, 20),\
                `doc` JSON, `raw` BLOB, `uid` CHAR(36), `txt` TEXT, `flag` BOOLEAN,\
                `u` INT UNSIGNED)"
        ))
        .await
        .unwrap();

    let result = AssertUnwindSafe(async {
        let inserted = driver
            .exec_queries(
                &[
                    format!(
                        "INSERT INTO `{table}` VALUES (1, '2024-01-01 01:02:03.000004', \
                         '2024-01-01 00:00:00', 1.25, '{{\"a\": [1, 2]}}', X'0102', \
                         'deadbeef-abba-babe-cafe-decafc0ffee5', 'hello', TRUE, 7)"
                    ),
                    format!(
                        "INSERT INTO `{table}` VALUES (2, NULL, NULL, -10.5, NULL, NULL, \
                         NULL, NULL, FALSE, NULL)"
                    ),
                ],
                true,
                true,
            )
            .await
            .unwrap();
        assert_eq!(inserted, vec![Some(1), Some(1)]);

        let batches = driver
            .read(&format!("SELECT * FROM `{table}` ORDER BY `id`"))
            .await
            .unwrap();
        let batch = &batches[0];
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(schema.field(3).data_type(), &DataType::Decimal128(38, 20));
        assert_eq!(schema.field(5).data_type(), &DataType::Binary);
        assert_eq!(schema.field(7).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(8).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(9).data_type(), &DataType::UInt32);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        let dts = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(dts.value(0), 1_704_070_923_000_004);
        assert!(dts.is_null(1));
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_704_067_200_000_000);
        let nums = batch
            .column(3)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(nums.value_as_string(0), "1.25000000000000000000");
        assert_eq!(nums.value_as_string(1), "-10.50000000000000000000");
        // MySQL stores JSON as text (MariaDB's JSON is a LONGTEXT alias reported as a blob/text).
        let doc_field = schema.field(4);
        if doc_field.data_type() == &DataType::Utf8 {
            let docs = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!(docs.value(0).contains("\"a\""));
        }
        let raw = batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(raw.value(0), &[1, 2]);
        let txt = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(txt.value(0), "hello");
        let flags = batch
            .column(8)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flags.value(0));
        assert!(!flags.value(1));
        let us = batch
            .column(9)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(us.value(0), 7);
        assert!(us.is_null(1));

        assert_eq!(
            driver
                .value(&format!("SELECT COUNT(*) FROM `{table}`"))
                .await
                .unwrap(),
            Some(SqlValue::Int(2))
        );

        let results = driver
            .exec_queries(
                &[
                    format!("INSERT INTO `{table}` (`id`) VALUES (3)"),
                    "SELECT * FROM `_mrsm_rs_missing_table`".to_string(),
                ],
                true,
                true,
            )
            .await
            .unwrap();
        assert_eq!(results, vec![Some(1), None]);
        assert_eq!(
            driver
                .value(&format!("SELECT COUNT(*) FROM `{table}`"))
                .await
                .unwrap(),
            Some(SqlValue::Int(2))
        );

        let stream = driver
            .read_stream(&format!("SELECT * FROM `{table}` WHERE 1 = 0"), 10)
            .await
            .unwrap();
        assert_eq!(stream.schema().fields().len(), 10);
    })
    .catch_unwind()
    .await;

    driver
        .exec(&format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// The session's `Com_stmt_prepare` and `Com_stmt_close` counters, read on the pool's only
/// connection (the status statement itself is prepared once and then cached).
async fn stmt_counters(driver: &SqlDriver) -> (i64, i64) {
    let batches = driver
        .read("SHOW SESSION STATUS WHERE Variable_name IN ('Com_stmt_prepare', 'Com_stmt_close')")
        .await
        .unwrap();
    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let get = |name: &str| {
        (0..batch.num_rows())
            .find(|i| names.value(*i) == name)
            .map(|i| values.value(i).parse::<i64>().unwrap())
            .unwrap()
    };
    (get("Com_stmt_prepare"), get("Com_stmt_close"))
}

/// Small statements are prepared once per connection and never closed: `sqlx` 0.9.0 does not set
/// `TCP_NODELAY`, so a `COM_STMT_CLOSE` (no reply) followed by the pool's ping on release stalls
/// ~40 ms and keeps the connection busy (wave 17b, `Backend::PERSISTENT_STATEMENTS`).
#[tokio::test]
async fn mysql_small_statements_are_prepared_once() {
    for var in ["MRSM_TEST_MYSQL_URI", "MRSM_TEST_MARIADB_URI"] {
        let Ok(uri) = std::env::var(var) else {
            eprintln!("Skipping: set {var} to run this test.");
            continue;
        };
        let mut spec = ConnectionSpec::from_uri(&uri, None, None).unwrap();
        // One connection, so every statement and the counters share one session.
        spec.pool_size = 1;
        spec.max_overflow = 0;
        let driver = SqlDriver::connect(&spec).await.unwrap();
        let (prepared0, closed0) = stmt_counters(&driver).await;
        for i in 0..50 {
            let value = driver
                .value_with("SELECT ? + 1 AS x", &[SqlParam::Int(i)])
                .await
                .unwrap();
            assert_eq!(value.and_then(|v| v.as_i64()), Some(i + 1), "{var}");
            driver.value("SELECT 1 AS x").await.unwrap();
        }
        let (prepared1, closed1) = stmt_counters(&driver).await;
        // Two distinct statements, each prepared once.
        assert!(
            prepared1 - prepared0 <= 3,
            "{var}: {} prepares for 100 statements",
            prepared1 - prepared0
        );
        assert_eq!(closed1 - closed0, 0, "{var}: statements were closed");
        driver.close().await;
    }
}
