use std::sync::Arc;

use meerschaum_kernel::{Column, ColumnData, Frame, Value};

fn objects<'a>(frame: &'a Frame, name: &str) -> &'a Arc<Vec<Value>> {
    let ColumnData::Object(values) = frame.data(name).unwrap() else {
        panic!("expected object storage");
    };
    values
}

#[test]
fn frame_clones_share_objects_and_mutations_leave_the_original_unchanged() {
    let original = Frame::new(vec![
        Column::object("json", vec![Value::json(serde_json::json!({"x": [1, 2]}))]),
        Column::object("text", vec![Value::Str(String::new())]),
    ])
    .unwrap();
    let mut cloned = original.clone();
    assert!(Arc::ptr_eq(
        objects(&original, "json"),
        objects(&cloned, "json")
    ));
    assert!(Arc::ptr_eq(
        objects(&original, "text"),
        objects(&cloned, "text")
    ));

    cloned.null_empty_strings(&["json".into(), "text".into()]);
    assert!(Arc::ptr_eq(
        objects(&original, "json"),
        objects(&cloned, "json")
    ));
    assert!(!Arc::ptr_eq(
        objects(&original, "text"),
        objects(&cloned, "text")
    ));
    assert_eq!(cloned.data("text").unwrap().get(0), Value::Null);
    assert_eq!(
        original.data("text").unwrap().get(0),
        Value::Str(String::new())
    );

    let ColumnData::Object(values) = &mut cloned.column_mut("json").unwrap().data else {
        unreachable!();
    };
    let Value::Json(doc) = &mut Arc::make_mut(values)[0] else {
        unreachable!();
    };
    let mut edited = doc.to_json();
    edited["x"][0] = serde_json::json!(99);
    Arc::make_mut(values)[0] = Value::json(edited);
    assert_eq!(
        original.data("json").unwrap().get(0),
        Value::json(serde_json::json!({"x": [1, 2]}))
    );
}

#[test]
fn compact_json_preserves_nested_values_numbers_order_and_python_repr() {
    let mut a = serde_json::Map::new();
    a.insert(
        "z".into(),
        serde_json::json!([null, true, "é🐍", 1.2345678901234567]),
    );
    a.insert(
        "a".into(),
        serde_json::json!({"unsigned": u64::MAX, "signed": i64::MIN}),
    );
    let source = serde_json::Value::Object(a);
    let cell = Value::json(source.clone());
    let Value::Json(compact) = &cell else {
        unreachable!()
    };
    assert_eq!(compact.to_json(), source);
    assert_eq!(compact.as_text(), source.to_string());
    assert_eq!(cell.py_str(), meerschaum_core::pyfmt::py_repr_json(&source));
    let reversed = serde_json::json!({"a": source["a"], "z": source["z"]});
    assert_eq!(cell, Value::json(reversed));
    let original = Frame::new(vec![Column::object("j", vec![cell.clone(), Value::Null])]).unwrap();
    let selected = original.take(&[1, 0]);
    assert_eq!(selected.data("j").unwrap().get(1), cell);
    assert_eq!(selected.data("j").unwrap().get(0), Value::Null);
}

#[test]
fn compact_json_can_restore_deep_already_parsed_containers() {
    let mut nested = serde_json::json!({"leaf": 1.2345678901234567});
    for _ in 0..200 {
        nested = serde_json::Value::Array(vec![nested]);
    }
    let value = Value::json(nested.clone());
    let Value::Json(compact) = &value else {
        unreachable!()
    };
    assert_eq!(compact.to_json(), nested);
    assert!(value.to_core_value().is_some());
}
