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

// `pipe_exists_checked`, which decides whether `copy pipes` reads a source's rows: a missing
// table is `false`, an existing one `true`.
flavor_tests!(test_pipe_exists_checked, |c: Ctx| async move {
    let pipe = c.pipe(
        "test",
        "exists_checked",
        None,
        r#"{"columns": {"id": "station"}}"#,
    );
    assert!(
        !InstanceConnector::pipe_exists_checked(&c.conn, &pipe)
            .await
            .unwrap()
    );
    ok(
        c.sync(&pipe, vec![doc(vec![("station", KValue::Str("x".into()))])])
            .await,
    );
    assert!(
        InstanceConnector::pipe_exists_checked(&c.conn, &pipe)
            .await
            .unwrap()
    );
    let _ = c.conn.drop_pipe(&pipe).await;
});

/// An instance whose database cannot be opened: the checked probe is an error, never "no
/// table" (`table_exists` reads the same failure as `false`).
#[tokio::test]
async fn sqlite_existence_failures_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!(
        "sqlite:///{}",
        dir.path().join("no-such-directory").join("x.db").display()
    );
    let broken = meerschaum_connector_sql::SqlConnector::from_uri("rs_exists_broken", &uri)
        .await
        .unwrap();
    let pipe = meerschaum_core::Pipe::builder("test", "exists_broken")
        .build()
        .unwrap();
    assert!(
        InstanceConnector::pipe_exists_checked(&broken, &pipe)
            .await
            .is_err()
    );
}
