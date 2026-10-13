//! PostgreSQL driver integration tests. Run only when `MRSM_TEST_PG_URI` is set, e.g.
//! `MRSM_TEST_PG_URI=postgresql://test:test1234@localhost:5559/testdb` (see the crate README).
//! Set `MRSM_TEST_POSTGIS_URI` as well to exercise geometry columns.

use arrow::array::{
    Array, BinaryArray, Decimal128Array, FixedSizeBinaryArray, Int32Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use meerschaum_sql::driver::batch::META_DTYPE;
use meerschaum_sql::driver::{SqlDriver, SqlValue};
use meerschaum_sql::uri::ConnectionSpec;

fn table_name() -> String {
    format!("w5sql_test_{}", uuid::Uuid::new_v4().simple())
}

async fn driver_from_env(var: &str) -> Option<SqlDriver> {
    let Ok(uri) = std::env::var(var) else {
        eprintln!("Skipping: set {var} to run the PostgreSQL driver tests.");
        return None;
    };
    let spec = ConnectionSpec::from_uri(&uri, None, None).unwrap();
    Some(SqlDriver::connect(&spec).await.unwrap())
}

#[tokio::test]
async fn postgres_types_and_exec_queries() {
    let Some(driver) = driver_from_env("MRSM_TEST_PG_URI").await else {
        return;
    };
    let table = table_name();
    let create = format!(
        "CREATE TABLE \"{table}\" (\
            \"id\" INT, \"dt\" TIMESTAMPTZ, \"naive\" TIMESTAMP, \"num\" NUMERIC,\
            \"doc\" JSONB, \"raw\" BYTEA, \"uid\" UUID, \"txt\" TEXT, \"d\" DATE)"
    );
    driver.exec(&create).await.unwrap();

    let result = AssertUnwindSafe(async {
        let inserted = driver
            .exec_queries(
                &[
                    format!(
                        "INSERT INTO \"{table}\" VALUES (1, '2024-01-01 00:00:00+00', \
                         '2024-01-01 01:02:03.000004', 1.25, '{{\"a\": [1, 2]}}', '\\x0102', \
                         'deadbeef-abba-babe-cafe-decafc0ffee5', 'hello', '2024-02-03')"
                    ),
                    format!(
                        "INSERT INTO \"{table}\" VALUES (2, '2024-01-02 00:00:00-05', NULL, \
                         -10.5, NULL, NULL, NULL, NULL, NULL)"
                    ),
                ],
                true,
                true,
            )
            .await
            .unwrap();
        assert_eq!(inserted, vec![Some(1), Some(1)]);

        let batches = driver
            .read(&format!("SELECT * FROM \"{table}\" ORDER BY \"id\""))
            .await
            .unwrap();
        let batch = &batches[0];
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(schema.field(3).data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
        assert_eq!(
            schema
                .field(4)
                .metadata()
                .get(META_DTYPE)
                .map(String::as_str),
            Some("json")
        );
        assert_eq!(schema.field(5).data_type(), &DataType::Binary);
        assert_eq!(schema.field(6).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            schema
                .field(6)
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.uuid")
        );
        assert_eq!(schema.field(8).data_type(), &DataType::Date32);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        let dts = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(dts.value(0), 1_704_067_200_000_000);
        assert_eq!(dts.value(1), 1_704_171_600_000_000);
        let naive = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(naive.value(0), 1_704_070_923_000_004);
        assert!(naive.is_null(1));
        let nums = batch
            .column(3)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(nums.value_as_string(0), "1.25");
        assert_eq!(nums.value_as_string(1), "-10.50");
        let docs = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(docs.value(0), "{\"a\": [1, 2]}");
        let raw = batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(raw.value(0), &[1, 2]);
        let uids = batch
            .column(6)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            uuid::Uuid::from_slice(uids.value(0)).unwrap().to_string(),
            "deadbeef-abba-babe-cafe-decafc0ffee5"
        );
        assert!(uids.is_null(1));

        assert_eq!(
            driver
                .value(&format!("SELECT COUNT(*) FROM \"{table}\""))
                .await
                .unwrap(),
            Some(SqlValue::Int(2))
        );

        // A failure aborts the PostgreSQL transaction: with break_on_error + rollback the
        // first insert is undone.
        let results = driver
            .exec_queries(
                &[
                    format!("INSERT INTO \"{table}\" (\"id\") VALUES (3)"),
                    "SELECT * FROM \"_mrsm_rs_missing_table\"".to_string(),
                ],
                true,
                true,
            )
            .await
            .unwrap();
        assert_eq!(results, vec![Some(1), None]);
        assert_eq!(
            driver
                .value(&format!("SELECT COUNT(*) FROM \"{table}\""))
                .await
                .unwrap(),
            Some(SqlValue::Int(2))
        );

        // Zero rows still yield a schema.
        let stream = driver
            .read_stream(&format!("SELECT * FROM \"{table}\" WHERE 1 = 0"), 10)
            .await
            .unwrap();
        assert_eq!(stream.schema().fields().len(), 9);
    })
    .catch_unwind()
    .await;

    driver
        .exec(&format!("DROP TABLE IF EXISTS \"{table}\""))
        .await
        .unwrap();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn postgis_geometry_as_ewkb() {
    let Some(driver) = driver_from_env("MRSM_TEST_POSTGIS_URI").await else {
        return;
    };
    let has_postgis = driver
        .value("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        .await
        .unwrap()
        .is_some();
    if !has_postgis {
        eprintln!("Skipping: the postgis extension is not installed.");
        return;
    }
    let table = table_name();
    driver
        .exec(&format!(
            "CREATE TABLE \"{table}\" (\"geom\" GEOMETRY(POINT, 4326));\
             INSERT INTO \"{table}\" VALUES (ST_GeomFromText('POINT(1 2)', 4326))"
        ))
        .await
        .unwrap();
    let batches = driver
        .read(&format!("SELECT \"geom\" FROM \"{table}\""))
        .await;
    driver
        .exec(&format!("DROP TABLE IF EXISTS \"{table}\""))
        .await
        .unwrap();
    let batches = batches.unwrap();
    let schema = batches[0].schema();
    let field = schema.field(0);
    assert_eq!(field.data_type(), &DataType::Binary);
    assert_eq!(
        field.metadata().get(META_DTYPE).map(String::as_str),
        Some("geometry")
    );
    let geom = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    // EWKB: little-endian, type POINT with the SRID flag, SRID 4326, x=1, y=2.
    let bytes = geom.value(0);
    assert_eq!(bytes[0], 1);
    assert_eq!(&bytes[1..5], &[0x01, 0x00, 0x00, 0x20]);
    assert_eq!(&bytes[5..9], &4326u32.to_le_bytes());
    assert_eq!(&bytes[9..17], &1f64.to_le_bytes());
    assert_eq!(&bytes[17..25], &2f64.to_le_bytes());
}
