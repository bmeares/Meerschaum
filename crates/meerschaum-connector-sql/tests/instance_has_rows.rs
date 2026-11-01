//! `pipe_has_rows_checked`, which decides whether an incremental `copy pipes` may fill a
//! destination without a sync time: a missing table has no rows, and an existing one is probed
//! for one row matching the filters (never a full count).
//!
//! SQLite always; the server flavors when their URIs are set.

mod common;
use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, Value};
use meerschaum_kernel::value::Value as KValue;

flavor_tests!(test_pipe_has_rows_checked, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "has_rows",
        Some("foo"),
        r#"{"columns": {"datetime": "dt", "id": "station"}}"#,
    );
    let has_rows = |params: Option<Map>| {
        let (conn, pipe) = (&c.conn, &pipe);
        async move {
            InstanceConnector::pipe_has_rows_checked(conn, pipe, params.as_ref())
                .await
                .unwrap()
        }
    };
    let station = |name: &str| {
        let mut params = Map::new();
        params.insert("station".into(), Value::Str(name.into()));
        Some(params)
    };

    // No table yet: proven empty.
    assert!(!has_rows(None).await);
    ok(c.sync(
        &pipe,
        vec![doc(vec![
            ("dt", KValue::Str("2024-01-01 00:00:00".into())),
            ("station", KValue::Str("x".into())),
            ("val", KValue::Int(1)),
        ])],
    )
    .await);
    assert!(has_rows(None).await);
    assert!(has_rows(station("x")).await);
    assert!(!has_rows(station("y")).await);
    let _ = c.conn.drop_pipe(&pipe).await;
});
