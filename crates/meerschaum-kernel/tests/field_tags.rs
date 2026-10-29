use indexmap::IndexMap;
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::geometry::Geometry;
use meerschaum_kernel::{Column, Frame, Value};

fn enforce(column: Column, dtype: &str) -> Frame {
    let dtypes = IndexMap::from([(column.name.clone(), dtype.to_string())]);
    pipe_enforce_dtypes(
        PipeInput::Frame(Frame::new(vec![column]).unwrap()),
        &PipeDtypesContext {
            dtypes: dtypes.clone(),
            explicit_dtypes: dtypes,
            enforce: true,
            tz_aware: false,
            mixed_numerics: false,
        },
    )
    .unwrap()
}

#[test]
fn bytes_to_geometry_updates_authoritative_field_tag() {
    let wkb = Geometry::from_wkt("POINT (1 2)").unwrap().to_wkb(false);
    let mut col = Column::object("shape", vec![Value::Bytes(wkb)]);
    col.tags.dtype = Some("bytes".into());
    col.tags.db_type = Some("BLOB".into());
    let out = enforce(col, "geometry[4326]");
    let col = out.column("shape").unwrap();
    assert_eq!(col.tags.dtype.as_deref(), Some("geometry[4326]"));
    assert_eq!(col.tags.db_type, None);
    assert!(matches!(col.data.values()[0], Value::Geometry(_)));
    let batch = out.to_record_batch().unwrap();
    assert_eq!(
        batch
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("geoarrow.wkb")
    );
    assert_eq!(
        batch
            .schema()
            .field(0)
            .metadata()
            .get("meerschaum:dtype")
            .map(String::as_str),
        Some("geometry[4326]")
    );
}

#[test]
fn native_cast_updates_tag_and_failed_cast_retains_source() {
    let mut col = Column::object("n", vec![Value::Str("12".into())]);
    col.tags.dtype = Some("str".into());
    assert_eq!(
        enforce(col.clone(), "int")
            .column("n")
            .unwrap()
            .tags
            .dtype
            .as_deref(),
        Some("int")
    );
    col.data = meerschaum_kernel::ColumnData::Object(vec![Value::Str("not an integer".into())]);
    let out = enforce(col, "int");
    assert_ne!(out.column("n").unwrap().tags.dtype.as_deref(), Some("int"));
    assert_eq!(
        out.data("n").unwrap().values(),
        vec![Value::Str("not an integer".into())]
    );
}
