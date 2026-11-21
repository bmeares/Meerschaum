//! The `Value` / frame boundary: SQLite query → Arrow batches (tagged fields) → kernel `Frame`
//! → `enforce_dtypes` → Arrow, and back into a `Frame` without dtype detection.

use arrow::array::{
    Array, Decimal128Array, Float64Array, Int64Array, LargeBinaryArray, LargeStringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use indexmap::IndexMap;
use meerschaum_connector_sql::driver::{SqlDriver, batches_to_frame};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::dtypes::field_meta;
use meerschaum_kernel::enforce::{EnforceOptions, enforce_dtypes};
use meerschaum_kernel::frame::ColumnData;
use meerschaum_kernel::{Frame, Value, detect};

struct TempDb {
    path: std::path::PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("_mrsm_rs_frame_{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path.display(), suffix));
        }
    }
}

fn pipe_dtypes() -> IndexMap<String, String> {
    [
        ("id", "int"),
        ("dt", "datetime"),
        ("temp", "float"),
        ("meta", "json"),
        ("raw", "bytes"),
        ("amount", "numeric"),
        ("station", "str"),
    ]
    .into_iter()
    .map(|(c, t)| (c.to_string(), t.to_string()))
    .collect()
}

fn meta<'a>(schema: &'a arrow::datatypes::Schema, col: &str, key: &str) -> Option<&'a str> {
    schema
        .field_with_name(col)
        .unwrap()
        .metadata()
        .get(key)
        .map(String::as_str)
}

#[tokio::test]
async fn sqlite_to_frame_to_enforced_arrow() {
    let db = TempDb::new();
    let spec =
        ConnectionSpec::from_uri(&format!("sqlite:///{}", db.path.display()), None, None).unwrap();
    let driver = SqlDriver::connect(&spec).await.unwrap();
    driver
        .exec(
            "CREATE TABLE \"weather\" (\
                \"id\" INTEGER, \"dt\" DATETIME, \"temp\" REAL, \"meta\" TEXT, \"raw\" BLOB,\
                \"amount\" REAL, \"station\" TEXT);\
             INSERT INTO \"weather\" VALUES (1, '2024-01-01 00:00:00', 20.5, '{\"a\": 1}', X'0102', 1.25, 'A');\
             INSERT INTO \"weather\" VALUES (2, '2024-01-01 01:00:00', NULL, '[1, 2]', NULL, 3.5, 'B');\
             INSERT INTO \"weather\" VALUES (3, NULL, 18.0, NULL, X'03', NULL, NULL);",
        )
        .await
        .unwrap();

    // 1. SQLite → Arrow batches with `meerschaum:*` field tags.
    let sql = "SELECT * FROM \"weather\" ORDER BY \"id\"";
    let batches = driver.read(sql).await.unwrap();
    let schema = batches[0].schema();
    assert_eq!(meta(&schema, "raw", field_meta::DTYPE), Some("bytes"));
    assert_eq!(meta(&schema, "raw", field_meta::DB_TYPE), Some("BLOB"));
    assert_eq!(meta(&schema, "id", field_meta::DB_TYPE), Some("INTEGER"));

    // 2. Arrow → kernel Frame: every column keeps its tags; the declared dtypes come from the
    //    tags (through core's `get_pd_type_from_db_type`), not from the values.
    let frame = batches_to_frame(&batches).unwrap();
    assert_eq!(frame.height(), 3);
    let raw = frame.column("raw").unwrap();
    assert_eq!(raw.tags.dtype.as_deref(), Some("bytes"));
    assert_eq!(raw.tags.db_type.as_deref(), Some("BLOB"));
    let source = frame.source_dtypes();
    assert_eq!(source["raw"], "bytes");
    assert_eq!(source["dt"], "datetime64[us]");
    assert_eq!(source["meta"], "string[pyarrow]");
    assert_eq!(detect::get_bytes_cols(&frame), vec!["raw".to_string()]);
    assert_eq!(
        detect::get_special_cols(&frame)
            .get("raw")
            .map(String::as_str),
        Some("bytes")
    );
    // The same frame comes straight from the driver.
    let direct = driver.read_frame(sql).await.unwrap();
    assert_eq!(direct.names(), frame.names());
    assert_eq!(direct.source_dtypes(), source);

    // 3. enforce_dtypes with the pipe's dtypes.
    let enforced = enforce_dtypes(&frame, &pipe_dtypes(), &EnforceOptions::default()).unwrap();
    assert_eq!(enforced.dtypes()["dt"], "datetime64[us, UTC]");
    match &enforced.column("meta").unwrap().data {
        ColumnData::Object(values) => {
            assert!(matches!(values[0], Value::Json(_)));
            assert_eq!(values[0].py_str(), "{'a': 1}");
            assert!(values[2].is_na());
        }
        other => panic!("meta should be an object column, got {other:?}"),
    }
    assert_eq!(
        detect::get_numeric_cols(&enforced),
        vec!["amount".to_string()]
    );

    // 4. Frame → Arrow.
    let out = enforced.to_record_batch().unwrap();
    let schema = out.schema();
    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("dt").unwrap().data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        meta(&schema, "meta", field_meta::EXTENSION_NAME),
        Some("arrow.json")
    );
    assert_eq!(meta(&schema, "meta", field_meta::DTYPE), Some("json"));
    assert_eq!(meta(&schema, "amount", field_meta::DTYPE), Some("numeric"));

    let col = |name: &str| out.column(schema.index_of(name).unwrap()).clone();
    let ids = col("id");
    let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);
    let dts = col("dt");
    let dts = dts
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(dts.value(1), 1_704_070_800_000_000);
    assert!(dts.is_null(2));
    let temps = col("temp");
    let temps = temps.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(temps.value(0), 20.5);
    assert!(temps.is_null(1));
    let metas = col("meta");
    let metas = metas.as_any().downcast_ref::<LargeStringArray>().unwrap();
    assert_eq!(metas.value(0), "{\"a\":1}");
    assert_eq!(metas.value(1), "[1,2]");
    let raws = col("raw");
    let raws = raws.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    assert_eq!(raws.value(0), &[1, 2]);
    assert!(raws.is_null(1));
    let amounts = col("amount");
    let amounts = amounts.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(amounts.value_as_string(0), "1.25");
    assert_eq!(amounts.value_as_string(1), "3.50");
    assert!(amounts.is_null(2));

    // 5. Arrow → Frame again: the tags written in step 4 restore the object columns, and
    //    enforcing the same dtypes is a fixed point.
    let again = Frame::from_record_batch(&out).unwrap();
    assert_eq!(
        again.column("meta").unwrap().tags.dtype.as_deref(),
        Some("json")
    );
    assert_eq!(detect::get_json_cols(&again), vec!["meta".to_string()]);
    let reenforced = enforce_dtypes(&again, &pipe_dtypes(), &EnforceOptions::default())
        .unwrap()
        .to_record_batch()
        .unwrap();
    assert_eq!(reenforced.num_rows(), out.num_rows());
    for field in out.schema().fields() {
        let ix = out.schema().index_of(field.name()).unwrap();
        assert_eq!(
            reenforced.column(ix).as_ref(),
            out.column(ix).as_ref(),
            "column {} changed on the second pass",
            field.name()
        );
    }
}

#[test]
fn tagged_json_column_needs_no_detection() {
    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    // A JSON column as the PostgreSQL driver tags it (`ColumnKind::Json`), with only NULLs:
    // value inspection could not tell it is JSON, the tag does.
    let field = Field::new("doc", DataType::Utf8, true).with_metadata(HashMap::from([
        (field_meta::DTYPE.to_string(), "json".to_string()),
        (field_meta::DB_TYPE.to_string(), "JSONB".to_string()),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StringArray::from(vec![None::<&str>, None]))],
    )
    .unwrap();
    let frame = batches_to_frame(&[batch]).unwrap();
    assert_eq!(detect::get_json_cols(&frame), vec!["doc".to_string()]);
    assert_eq!(frame.source_dtypes()["doc"], "json");
}
