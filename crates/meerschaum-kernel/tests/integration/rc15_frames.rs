//! rc15 frame helpers: `Frame::concat` (a replacing sync joins the chunks of a fetch into one
//! frame) and `Frame::null_empty_strings` (Oracle stores `''` as `NULL`).

use meerschaum_kernel::Frame;
use meerschaum_kernel::frame::{Column, ColumnData};
use meerschaum_kernel::value::Value;
use polars::prelude::*;

fn native(name: &str, values: &[Option<i64>]) -> Column {
    Column::native(Series::new(name.into(), values))
}

#[test]
fn concat_keeps_native_columns_and_fills_missing_ones() {
    let a = Frame::new(vec![
        native("id", &[Some(1), Some(2)]),
        Column::object("v", vec![Value::Str("a".into()), Value::Null]),
    ])
    .unwrap();
    let b = Frame::new(vec![
        native("id", &[Some(3)]),
        Column::object("w", vec![Value::Float(1.5)]),
    ])
    .unwrap();
    let out = Frame::concat(&[a, b.clone()]).unwrap();
    assert_eq!(out.height(), 3);
    assert_eq!(out.names(), vec!["id", "v", "w"]);
    // One native dtype in every frame: still native.
    assert!(matches!(out.data("id").unwrap(), ColumnData::Native(_)));
    assert_eq!(
        out.data("id").unwrap().values(),
        vec![Value::Int(1), Value::Int(2), Value::Int(3)]
    );
    // Missing in a frame: nulls in its rows.
    assert_eq!(
        out.data("v").unwrap().values(),
        vec![Value::Str("a".into()), Value::Null, Value::Null]
    );
    assert_eq!(
        out.data("w").unwrap().values(),
        vec![Value::Null, Value::Null, Value::Float(1.5)]
    );
    // Different dtypes: an object column of the values.
    let c = Frame::new(vec![Column::object("id", vec![Value::Str("x".into())])]).unwrap();
    let out = Frame::concat(&[b, c]).unwrap();
    assert!(matches!(out.data("id").unwrap(), ColumnData::Object(_)));
    assert_eq!(
        out.data("id").unwrap().values(),
        vec![Value::Int(3), Value::Str("x".into())]
    );
    // Nothing to join.
    assert_eq!(Frame::concat(&[]).unwrap().height(), 0);
}

#[test]
fn empty_strings_become_nulls_in_the_given_columns_only() {
    let mut df = Frame::new(vec![
        Column::native(Series::new(
            "code".into(),
            &[Some(""), Some("None"), None, Some("a")],
        )),
        Column::object(
            "label",
            vec![
                Value::Str("".into()),
                Value::Str("x".into()),
                Value::Null,
                Value::Str("".into()),
            ],
        ),
        Column::object(
            "other",
            vec![Value::Str("".into()), Value::Null, Value::Null, Value::Null],
        ),
    ])
    .unwrap();
    df.null_empty_strings(&["code".to_string(), "label".to_string()]);
    assert!(matches!(df.data("code").unwrap(), ColumnData::Native(_)));
    assert_eq!(
        df.data("code").unwrap().values(),
        vec![
            Value::Null,
            Value::Str("None".into()),
            Value::Null,
            Value::Str("a".into())
        ]
    );
    assert_eq!(
        df.data("label").unwrap().values(),
        vec![
            Value::Null,
            Value::Str("x".into()),
            Value::Null,
            Value::Null
        ]
    );
    // Other columns are left alone.
    assert_eq!(df.data("other").unwrap().get(0), Value::Str("".into()));
}
