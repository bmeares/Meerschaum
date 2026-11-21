//! Null-only columns of `pd.DataFrame(docs).convert_dtypes()` (wave 13). Reference values were
//! read from pandas 2.3: a missing key or a float `nan` next to `None` is `float64` and becomes
//! `Int64`; `[None, None]` and `pd.NA` stay `object`.

use indexmap::IndexMap;
use meerschaum_kernel::ColumnData;
use meerschaum_kernel::docs::{convert_dtypes, frame_from_docs};
use meerschaum_kernel::value::Value;

fn is_int64(data: &ColumnData) -> bool {
    matches!(data, ColumnData::Native(s) if s.dtype() == &polars::prelude::DataType::Int64)
}

#[test]
fn a_column_missing_from_some_documents_and_none_in_the_others_is_int64() {
    let mut first = IndexMap::new();
    first.insert("id".to_string(), Value::Int(1));
    first.insert("empty".to_string(), Value::Null);
    let mut second = IndexMap::new();
    second.insert("id".to_string(), Value::Int(2));
    let frame = frame_from_docs(&[first, second]).unwrap();
    assert!(is_int64(&frame.column("empty").unwrap().data));
}

#[test]
fn a_column_missing_from_every_other_document_only_is_int64() {
    let mut first = IndexMap::new();
    first.insert("id".to_string(), Value::Int(1));
    first.insert("x".to_string(), Value::Null);
    let second = {
        let mut m = IndexMap::new();
        m.insert("id".to_string(), Value::Int(2));
        m.insert("x".to_string(), Value::Float(f64::NAN));
        m
    };
    let frame = frame_from_docs(&[first, second]).unwrap();
    assert!(is_int64(&frame.column("x").unwrap().data));
}

#[test]
fn none_only_and_pd_na_columns_stay_object() {
    assert!(matches!(
        convert_dtypes("a", &[Value::Null, Value::Null]),
        ColumnData::Object(_)
    ));
    assert!(matches!(
        convert_dtypes("a", &[Value::NA, Value::Null]),
        ColumnData::Object(_)
    ));
}
