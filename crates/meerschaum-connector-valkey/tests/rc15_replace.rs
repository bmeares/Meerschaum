//! rc15 on Valkey (in-memory backend): replacing a window (`SyncOptions::replace`, Q19) and
//! null-like strings kept as text on declared text / index columns (Q18).

use meerschaum_connector::traits::{DataQuery, DateBound, SyncOptions};
use meerschaum_connector_valkey::ValkeyConnector;
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::docs::frame_from_docs;
use meerschaum_kernel::value::{Value as KValue, parse_datetime_value};

fn ts(day: u32) -> KValue {
    KValue::Datetime(parse_datetime_value(&format!("2024-01-0{day}T00:00:00+00:00")).unwrap())
}

fn bound(day: u32) -> DateBound {
    match ts(day) {
        KValue::Datetime(d) => DateBound::Datetime(d),
        _ => unreachable!(),
    }
}

fn frame(rows: &[(u32, &str, i64)]) -> Frame {
    let docs: Vec<indexmap::IndexMap<String, KValue>> = rows
        .iter()
        .map(|(d, id, v)| {
            [
                ("dt".to_string(), ts(*d)),
                ("id".to_string(), KValue::Str(id.to_string())),
                ("v".to_string(), KValue::Int(*v)),
            ]
            .into_iter()
            .collect()
        })
        .collect();
    frame_from_docs(&docs).unwrap()
}

fn pipe(conn: &ValkeyConnector, location: &str) -> Pipe {
    let params = Value::from_json_str(
        r#"{"columns": {"datetime": "dt", "id": "id"}, "dtypes": {"id": "string"}}"#,
    )
    .unwrap()
    .to_map();
    Pipe::builder("rc15", "replace")
        .location(Some(location))
        .instance(&conn.to_string())
        .parameters(params)
        .build()
        .unwrap()
}

async fn rows(conn: &ValkeyConnector, pipe: &Pipe, params: Option<Map>) -> usize {
    conn.get_pipe_data(
        pipe,
        &DataQuery {
            params,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .map_or(0, |df| df.height())
}

#[tokio::test]
async fn replace_clears_the_window_then_writes_its_rows() {
    let conn = ValkeyConnector::in_memory("rc15replace");
    let p = pipe(&conn, "window");
    assert!(conn.register_pipe(&p).await.success);
    let res = conn
        .sync_pipe(
            &p,
            frame(&[(1, "a", 1), (2, "a", 2), (3, "a", 3), (4, "a", 4)]),
            &SyncOptions::default(),
        )
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(rows(&conn, &p, None).await, 4);

    // [2, 4) is replaced by one row; the row of day 5 is outside the window and not written.
    let res = conn
        .sync_pipe(
            &p,
            frame(&[(2, "b", 20), (5, "a", 5)]),
            &SyncOptions {
                begin: Some(bound(2)),
                end: Some(bound(4)),
                replace: true,
                ..Default::default()
            },
        )
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        res.message,
        "Replaced 2 rows with 1 row.\nSkipped 1 row outside the window."
    );
    assert_eq!(rows(&conn, &p, None).await, 3);

    // An empty frame clears the window.
    let empty = frame(&[(1, "a", 1)]).clear();
    let res = conn
        .sync_pipe(
            &p,
            empty,
            &SyncOptions {
                begin: Some(bound(1)),
                end: Some(bound(3)),
                replace: true,
                ..Default::default()
            },
        )
        .await;
    assert!(res.success, "{}", res.message);
    assert_eq!(rows(&conn, &p, None).await, 1);
}

#[tokio::test]
async fn null_like_strings_are_values_on_text_index_columns() {
    let conn = ValkeyConnector::in_memory("rc15markers");
    let p = pipe(&conn, "markers");
    assert!(conn.register_pipe(&p).await.success);
    let res = conn
        .sync_pipe(
            &p,
            frame(&[(1, "NA", 1), (1, "None", 2), (1, "x", 3)]),
            &SyncOptions::default(),
        )
        .await;
    assert!(res.success, "{}", res.message);
    let only = |v: &str| {
        let mut m = Map::new();
        m.insert("id".into(), Value::Str(v.into()));
        Some(m)
    };
    assert_eq!(rows(&conn, &p, only("NA")).await, 1);
    assert_eq!(rows(&conn, &p, only("None")).await, 1);
    let mut null = Map::new();
    null.insert("id".into(), Value::Null);
    assert_eq!(rows(&conn, &p, Some(null)).await, 0);
}
