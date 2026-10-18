//! The DuckDB driver against the real, dynamically loaded library (`MRSM_DUCKDB_LIB`: the path of
//! DuckDB's `libduckdb`, v1.4 or newer; the tests skip without it, like the other service tests).
//! With `MRSM_TEST_DUCKDB_DOWNLOAD` set instead, the first connection downloads the pinned library
//! into `MRSM_ROOT_DIR/drivers` (the implicit first-use install; the hosted `duckdb-platforms` CI
//! jobs use this on Linux arm64, macOS and Windows).
//!
//! Covers what the driver decodes and binds through the C API: every scalar type, the text of the
//! nested / interval / enum / bit / `BIGNUM` types, aware timestamp binding in a non-UTC session,
//! and the library's bundled ICU (the old bundled build lacked it).

use chrono::{NaiveDate, TimeZone, Utc};
use meerschaum_connector_sql::driver::{SqlDriver, SqlParam, SqlValue};
use meerschaum_connector_sql::uri::ConnectionSpec;

async fn driver() -> Option<SqlDriver> {
    if std::env::var("MRSM_DUCKDB_LIB").is_err()
        && std::env::var("MRSM_TEST_DUCKDB_DOWNLOAD").is_err()
    {
        eprintln!("Skipping DuckDB library tests: set MRSM_DUCKDB_LIB.");
        return None;
    }
    let spec = ConnectionSpec::from_uri("duckdb:///:memory:", None, None).unwrap();
    Some(SqlDriver::connect(&spec).await.unwrap())
}

fn text(v: Option<SqlValue>) -> String {
    match v {
        Some(SqlValue::Text(s)) => s,
        other => panic!("expected text, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalar_types_decode() {
    let Some(d) = driver().await else { return };
    assert_eq!(
        d.value("SELECT 300::SMALLINT").await.unwrap(),
        Some(SqlValue::Int(300))
    );
    assert_eq!(
        d.value("SELECT 18446744073709551615::UBIGINT")
            .await
            .unwrap(),
        Some(SqlValue::UInt(u64::MAX))
    );
    assert_eq!(
        d.value("SELECT 2.5::DOUBLE").await.unwrap(),
        Some(SqlValue::Float(2.5))
    );
    assert_eq!(
        d.value("SELECT NULL::INTEGER").await.unwrap(),
        Some(SqlValue::Null)
    );
    assert_eq!(
        d.value("SELECT 123456789012345678901234567890::HUGEINT")
            .await
            .unwrap(),
        Some(SqlValue::Text("123456789012345678901234567890".into()))
    );
    assert_eq!(
        d.value("SELECT 123.45::DECIMAL(5,2)").await.unwrap(),
        Some(SqlValue::Decimal("123.45".parse().unwrap()))
    );
    // Wider than a 96-bit decimal mantissa: exact text, not a rounded value.
    assert_eq!(
        text(
            d.value("SELECT 123456789012345678901234567890.123456::DECIMAL(38,6)")
                .await
                .unwrap()
        ),
        "123456789012345678901234567890.123456"
    );
    assert_eq!(
        d.value("SELECT uuid '12345678-1234-5678-1234-567812345678'")
            .await
            .unwrap(),
        Some(SqlValue::Uuid(
            uuid::Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap()
        ))
    );
    assert_eq!(
        d.value("SELECT '\\x01\\x02'::BLOB").await.unwrap(),
        Some(SqlValue::Bytes(vec![1, 2]))
    );
    assert_eq!(
        d.value("SELECT DATE '2024-03-05'").await.unwrap(),
        Some(SqlValue::Date(NaiveDate::from_ymd_opt(2024, 3, 5).unwrap()))
    );
    assert_eq!(
        d.value("SELECT TIMESTAMP '2024-01-02 03:04:05.123456'")
            .await
            .unwrap(),
        Some(SqlValue::Timestamp(
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_micro_opt(3, 4, 5, 123_456)
                .unwrap()
        ))
    );
    assert_eq!(
        d.value("SELECT TIMESTAMPTZ '2024-01-02 03:04:05+02'")
            .await
            .unwrap(),
        Some(SqlValue::TimestampTz(
            Utc.with_ymd_and_hms(2024, 1, 2, 1, 4, 5).unwrap()
        ))
    );
    // Nanosecond and second timestamps read as microsecond timestamps.
    assert!(matches!(
        d.value("SELECT TIMESTAMP_NS '2024-01-01 00:00:01.123456789'")
            .await
            .unwrap(),
        Some(SqlValue::Timestamp(_))
    ));
    assert_eq!(
        text(
            d.value("SELECT 'a long string that does not fit the 12 byte inline form'")
                .await
                .unwrap()
        ),
        "a long string that does not fit the 12 byte inline form"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_and_special_types_read_as_duckdb_text() {
    let Some(d) = driver().await else { return };
    // PARITY: v4 read these through the Arrow export (debug text); they are DuckDB's own text now.
    assert_eq!(
        text(d.value("SELECT [1, 2, 3]").await.unwrap()),
        "[1, 2, 3]"
    );
    assert_eq!(
        text(d.value("SELECT {'a': 1, 'b': 'it''s'}").await.unwrap()),
        "{'a': 1, 'b': 'it\\'s'}"
    );
    assert_eq!(
        text(d.value("SELECT [[1], [2, 3]]").await.unwrap()),
        "[[1], [2, 3]]"
    );
    assert_eq!(
        text(d.value("SELECT [NULL, 'a']").await.unwrap()),
        "[NULL, a]"
    );
    assert_eq!(
        text(
            d.value("SELECT INTERVAL '1 year 2 months 3 days 4 hours'")
                .await
                .unwrap()
        ),
        "1 year 2 months 3 days 04:00:00"
    );
    assert_eq!(text(d.value("SELECT '1010'::BIT").await.unwrap()), "1010");
    assert_eq!(
        text(d.value("SELECT TIMETZ '10:11:12+02'").await.unwrap()),
        "10:11:12+02"
    );
    // BIGNUM (VARINT) decodes to its decimal text instead of a placeholder.
    assert_eq!(text(d.value("SELECT 10::BIGNUM").await.unwrap()), "10");
    assert_eq!(
        text(d.value("SELECT (-300)::BIGNUM").await.unwrap()),
        "-300"
    );
    assert_eq!(
        text(
            d.value("SELECT '1267650600228229401496703205376'::BIGNUM")
                .await
                .unwrap()
        ),
        "1267650600228229401496703205376"
    );
    d.exec("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
        .await
        .unwrap();
    assert_eq!(text(d.value("SELECT 'ok'::mood").await.unwrap()), "ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_parameters_round_trip() {
    let Some(d) = driver().await else { return };
    d.exec("CREATE TABLE p (i BIGINT, f DOUBLE, s VARCHAR, b BLOB, n INTEGER, ts TIMESTAMP, tz TIMESTAMPTZ, d DATE, t BOOLEAN)")
        .await
        .unwrap();
    let when = Utc.with_ymd_and_hms(2023, 11, 14, 22, 13, 20).unwrap();
    let changed = d
        .exec_with(
            "INSERT INTO p VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[
                SqlParam::Int(-5),
                SqlParam::Float(2.25),
                SqlParam::Text("héllo".into()),
                SqlParam::Bytes(vec![0, 1, 255]),
                SqlParam::Null,
                SqlParam::Timestamp(when.naive_utc()),
                SqlParam::TimestampTz(when),
                SqlParam::Date(NaiveDate::from_ymd_opt(2022, 1, 8).unwrap()),
                SqlParam::Bool(true),
            ],
        )
        .await
        .unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        d.value("SELECT s FROM p").await.unwrap(),
        Some(SqlValue::Text("héllo".into()))
    );
    assert_eq!(
        d.value("SELECT b FROM p").await.unwrap(),
        Some(SqlValue::Bytes(vec![0, 1, 255]))
    );
    assert_eq!(
        d.value("SELECT n FROM p").await.unwrap(),
        Some(SqlValue::Null)
    );
    assert_eq!(
        d.value("SELECT d FROM p").await.unwrap(),
        Some(SqlValue::Date(NaiveDate::from_ymd_opt(2022, 1, 8).unwrap()))
    );
}

/// An aware timestamp keeps its instant whatever the session `TimeZone` is (v4 bound it as a
/// naive timestamp, which DuckDB then read in the session zone).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aware_timestamps_keep_their_instant_in_a_non_utc_session() {
    let Some(d) = driver().await else { return };
    // An in-memory database has one pooled connection, so the session setting sticks.
    d.exec("SET TimeZone = 'Asia/Tokyo'").await.unwrap();
    let when = Utc.with_ymd_and_hms(2023, 11, 14, 22, 13, 20).unwrap();
    let v = d
        .value_with(
            "SELECT epoch_ms($1::TIMESTAMPTZ)",
            &[SqlParam::TimestampTz(when)],
        )
        .await
        .unwrap();
    assert_eq!(v, Some(SqlValue::Int(when.timestamp_millis())));
}

/// Sessions run in UTC whatever the host's zone is (see the `PARITY` note in the driver).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sessions_start_in_utc() {
    let Some(d) = driver().await else { return };
    assert_eq!(
        text(d.value("SELECT current_setting('TimeZone')").await.unwrap()),
        "UTC"
    );
}

/// The official library bundles ICU (`AT TIME ZONE`, named zones); the old bundled build did not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn icu_is_available() {
    let Some(d) = driver().await else { return };
    assert!(matches!(
        d.value("SELECT TIMESTAMPTZ '2024-01-02 00:00:00+00' AT TIME ZONE 'Asia/Tokyo'")
            .await
            .unwrap(),
        Some(SqlValue::Timestamp(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn errors_carry_duckdb_text_and_batches_run() {
    let Some(d) = driver().await else { return };
    let err = d.exec("SELEC 1").await.unwrap_err().to_string();
    assert!(err.contains("syntax error"), "{err}");
    // Several statements without parameters run as a batch.
    d.exec("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (1), (2)")
        .await
        .unwrap();
    assert_eq!(
        d.value("SELECT count(*) FROM t").await.unwrap(),
        Some(SqlValue::Int(2))
    );
}

// ---- every row of a vector, not only the first -------------------------------------------------

use arrow::array::{Array, FixedSizeBinaryArray, TimestampMicrosecondArray};
use arrow::util::display::array_value_to_string;

/// Read `SELECT x, CAST(x AS VARCHAR) FROM (<values>) t(x)` and require our text of `x` to equal
/// DuckDB's own cast to `VARCHAR` on every row (and `NULL` where DuckDB has `NULL`).
async fn matches_the_varchar_cast(d: &SqlDriver, values: &str) {
    let sql = format!("SELECT x, CAST(x AS VARCHAR) AS expected FROM ({values}) t(x)");
    let batches = d.read(&sql).await.unwrap();
    let mut rows = 0;
    for batch in &batches {
        let (ours, theirs) = (batch.column(0), batch.column(1));
        for i in 0..batch.num_rows() {
            let show =
                |a: &dyn Array| (!a.is_null(i)).then(|| array_value_to_string(a, i).unwrap());
            assert_eq!(
                show(ours.as_ref()),
                show(theirs.as_ref()),
                "row {i} of {values}"
            );
            rows += 1;
        }
    }
    assert!(rows >= 2, "{values}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn intervals_decode_on_every_row() {
    let Some(d) = driver().await else { return };
    matches_the_varchar_cast(
        &d,
        "VALUES (INTERVAL '1 month'), (INTERVAL '2 days 3 hours'), (INTERVAL '5 years 7 seconds'), \
         (NULL::INTERVAL), (INTERVAL '-1 day'), (INTERVAL '1 day -3 hours'), (INTERVAL '-500 ms'), \
         (INTERVAL '25 hours'), (INTERVAL '13 months'), (INTERVAL '1 microsecond'), (INTERVAL '0 seconds'), \
         (INTERVAL '1.5 seconds')",
    )
    .await;
    // The repro of the stride bug: row 1 was read from row 0's neighbor.
    let rows = d
        .read("SELECT x FROM (VALUES (INTERVAL '1 month'), (INTERVAL '2 days 3 hours'), (INTERVAL '5 years 7 seconds'), (NULL::INTERVAL)) t(x)")
        .await
        .unwrap();
    let column = rows[0].column(0);
    let shown: Vec<Option<String>> = (0..column.len())
        .map(|i| (!column.is_null(i)).then(|| array_value_to_string(column, i).unwrap()))
        .collect();
    assert_eq!(
        shown,
        vec![
            Some("1 month".into()),
            Some("2 days 03:00:00".into()),
            Some("5 years 00:00:07".into()),
            None
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_integers_uuids_decimals_and_times_decode_on_every_row() {
    let Some(d) = driver().await else { return };
    // HUGEINT / UHUGEINT are exact numeric text: the extremes (39 digits) do not fit DECIMAL(38,0).
    matches_the_varchar_cast(
        &d,
        "VALUES (1::HUGEINT), (-2::HUGEINT), (170141183460469231731687303715884105727::HUGEINT), \
         ((-170141183460469231731687303715884105727)::HUGEINT), (NULL::HUGEINT), (4611686018427387904::HUGEINT)",
    )
    .await;
    matches_the_varchar_cast(
        &d,
        "VALUES (1::UHUGEINT), (340282366920938463463374607431768211455::UHUGEINT), (NULL::UHUGEINT), \
         (18446744073709551616::UHUGEINT)",
    )
    .await;
    // Every decimal width: i16, i32, i64 and i128 storage.
    for ty in [
        "DECIMAL(4,1)",
        "DECIMAL(9,3)",
        "DECIMAL(18,6)",
        "DECIMAL(38,6)",
        "DECIMAL(38,0)",
    ] {
        matches_the_varchar_cast(
            &d,
            &format!(
                "VALUES (CAST(12 AS {ty})), (CAST(-5 AS {ty})), (NULL::{ty}), (CAST(7 AS {ty})), (CAST(99 AS {ty}))"
            ),
        )
        .await;
    }
    matches_the_varchar_cast(
        &d,
        "VALUES (123456789012345678901234567890.123456::DECIMAL(38,6)), (-0.5::DECIMAL(38,6)), (NULL::DECIMAL(38,6)), \
         (99999999999999999999999999999999.999999::DECIMAL(38,6))",
    )
    .await;
    matches_the_varchar_cast(
        &d,
        "VALUES (TIMETZ '10:11:12+02'), (TIMETZ '23:59:59.5-05:30'), (TIMETZ '00:00:00+00'), \
         (NULL::TIMETZ), (TIMETZ '01:02:03.123456+14')",
    )
    .await;
    matches_the_varchar_cast(
        &d,
        "VALUES ('1010'::BIT), ('1'::BIT), ('11111111000'::BIT), (NULL::BIT), ('0'::BIT)",
    )
    .await;

    // UUIDs: every row's 16 bytes.
    let batches = d
        .read(
            "SELECT x, CAST(x AS VARCHAR) FROM (VALUES (uuid '12345678-1234-5678-1234-567812345678'), \
             (uuid 'ffffffff-ffff-ffff-ffff-ffffffffffff'), (NULL::UUID), \
             (uuid '00000000-0000-0000-0000-000000000001'), (uuid '80000000-0000-0000-0000-000000000000')) t(x)",
        )
        .await
        .unwrap();
    let uuids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let expected = [
        Some("12345678-1234-5678-1234-567812345678"),
        Some("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        None,
        Some("00000000-0000-0000-0000-000000000001"),
        Some("80000000-0000-0000-0000-000000000000"),
    ];
    for (i, want) in expected.iter().enumerate() {
        let got = (!uuids.is_null(i))
            .then(|| uuid::Uuid::from_slice(uuids.value(i)).unwrap().to_string());
        assert_eq!(got.as_deref(), *want, "uuid row {i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_values_are_the_varchar_cast_on_every_row() {
    let Some(d) = driver().await else { return };
    for values in [
        "VALUES ([1, 2, 3]), (NULL), ([]), ([4]), ([NULL, 5])",
        "VALUES (['a', 'b', NULL]), (['a, b', 'c]d', 'it''s', '', 'NULL', ' x', '[1]', '{x}']), (NULL), (['é', '日本']), (['a\"b', 'k=v', 'p:q'])",
        "VALUES ([[1], [2, 3]]), ([[]]), (NULL), ([[4], NULL])",
        "VALUES ([TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00.5']), ([TIMESTAMP '2024-02-03 04:05:06.123456']), (NULL)",
        "VALUES ([DATE '2024-01-01', DATE '2024-12-31']), (NULL), ([DATE '1999-09-09'])",
        "VALUES ([INTERVAL '1 day', INTERVAL '90 minutes']), (NULL), ([INTERVAL '-1 month'])",
        "VALUES ([1.5::DOUBLE, 2.0::DOUBLE, 1e100::DOUBLE]), ([0.25::DOUBLE]), (NULL), ([-3.75::DOUBLE, 1e-7::DOUBLE])",
        "VALUES ([true, false]), (NULL), ([false])",
        "VALUES ([1.50::DECIMAL(5,2), -2.25::DECIMAL(5,2)]), (NULL), ([0.00::DECIMAL(5,2)])",
        "VALUES ([uuid '12345678-1234-5678-1234-567812345678']), (NULL), ([uuid 'ffffffff-ffff-ffff-ffff-ffffffffffff'])",
        "VALUES ({'a': 1, 'b': 'x', 'c': NULL}), ({'a': 2, 'b': NULL, 'c': 'y'}), (NULL), ({'a': 3, 'b': 'p,q', 'c': 'it''s'})",
        "VALUES ({'a b': 'it''s', 'c,d': 'e:f'}), ({'a b': NULL, 'c,d': ''}), (NULL)",
        "VALUES ({'x': ['p', 'q'], 'y': {'z': 1}}), ({'x': [], 'y': {'z': NULL}}), (NULL)",
        "VALUES (MAP {'k': 'v', 'k2': NULL}), (MAP {'a,b': 'c=d'}), (NULL), (MAP {'a b': '1'})",
        "VALUES (MAP {1: 'one', 2: 'two'}), (MAP {3: NULL}), (NULL)",
        "VALUES ([1, 2, 3]::INTEGER[3]), (NULL), ([4, 5, 6]::INTEGER[3])",
        "VALUES (union_value(a := 'x')::UNION(a VARCHAR, b INTEGER)), (union_value(b := 2)::UNION(a VARCHAR, b INTEGER)), (NULL)",
    ] {
        matches_the_varchar_cast(&d, values).await;
    }
    d.exec("CREATE TYPE mood2 AS ENUM ('sad', 'ok', 'happy')")
        .await
        .unwrap();
    matches_the_varchar_cast(
        &d,
        "VALUES ('ok'::mood2), ('sad'::mood2), (NULL::mood2), ('happy'::mood2)",
    )
    .await;
    matches_the_varchar_cast(
        &d,
        "VALUES (['ok'::mood2, 'sad'::mood2]), (NULL), (['happy'::mood2])",
    )
    .await;
    matches_the_varchar_cast(
        &d,
        "VALUES (['\\x01\\xff'::BLOB, 'abc'::BLOB, 'a\\x5Cb'::BLOB, 'q\\x27r'::BLOB]), (NULL), ([NULL::BLOB])",
    )
    .await;
}

/// `infinity` timestamps and dates read as Python's `datetime.max` / `datetime.min` (what the
/// Python `duckdb` package gives v4) instead of failing the whole query.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn infinite_timestamps_and_dates_read_as_the_python_limits() {
    let Some(d) = driver().await else { return };
    let batches = d
        .read(
            "SELECT x FROM (VALUES ('infinity'::TIMESTAMP), ('-infinity'::TIMESTAMP), \
             (TIMESTAMP '2024-01-01 00:00:00'), (NULL::TIMESTAMP)) t(x)",
        )
        .await
        .unwrap();
    let ts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts.value(0), 253_402_300_799_999_999); // 9999-12-31 23:59:59.999999
    assert_eq!(ts.value(1), -62_135_596_800_000_000); // 0001-01-01 00:00:00
    assert_eq!(ts.value(2), 1_704_067_200_000_000);
    assert!(ts.is_null(3));
    let batches = d
        .read("SELECT x FROM (VALUES ('infinity'::TIMESTAMPTZ), ('-infinity'::TIMESTAMPTZ)) t(x)")
        .await
        .unwrap();
    let tz = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(tz.value(0), 253_402_300_799_999_999);
    assert_eq!(tz.value(1), -62_135_596_800_000_000);
    let dates = d
        .read("SELECT x FROM (VALUES ('infinity'::DATE), ('-infinity'::DATE), (DATE '2024-01-01')) t(x)")
        .await
        .unwrap();
    let days = dates[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .unwrap();
    assert_eq!(days.value(0), 2_932_896); // 9999-12-31
    assert_eq!(days.value(1), -719_162); // 0001-01-01
    assert_eq!(days.value(2), 19_723);
}
