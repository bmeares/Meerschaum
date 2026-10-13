mod common;
use common::*;
use meerschaum_connector::{DataQuery, DateBound, SyncOptions};
use meerschaum_core::Pipe;
use meerschaum_kernel::value::Value as KValue;

flavor_tests!(internal_upsert, |c: Ctx| async move {
    let pipe = Pipe::builder("mrsm", &c.prefix)
        .instance(&c.conn.keys())
        .kwarg("temporary", true)
        .kwarg("static", true)
        .kwarg("autoincrement", false)
        .kwarg("upsert", true)
        .kwarg("columns", params(r#"{"primary":"key"}"#))
        .kwarg("dtypes", params(r#"{"key":"string","value":"int"}"#))
        .kwarg("target", c.prefix.clone())
        .build()
        .unwrap();
    let first = c
        .sync(
            &pipe,
            vec![doc(vec![
                ("key", KValue::Str("a".into())),
                ("value", KValue::Int(1)),
            ])],
        )
        .await;
    assert!(first.success, "{}", first.message);
    let update = meerschaum_kernel::Frame::from_object_columns(vec![
        ("key".into(), vec![KValue::Str("a".into())]),
        ("value".into(), vec![KValue::Int(2)]),
    ])
    .unwrap();
    c.conn.enable_query_log();
    let result = c
        .conn
        .sync_pipe(
            &pipe,
            update,
            &SyncOptions {
                check_existing: false,
                ..Default::default()
            },
        )
        .await;
    assert!(result.success, "{}", result.message);
    let log = c.conn.take_query_log();
    assert_eq!(
        log.len(),
        1,
        "warm internal state uses one statement: {log:#?}"
    );
    assert!(!log.iter().any(|s| s.contains("CREATE TABLE")), "{log:#?}");
    assert!(
        log.iter()
            .any(|s| s.starts_with("INSERT INTO") || s.starts_with("MERGE INTO")),
        "{log:#?}"
    );
    assert_eq!(c.docs(&pipe).await[0]["value"], KValue::Int(2));
    assert!(c.conn.drop_pipe(&pipe).await.success);
});

#[tokio::test]
async fn sqlite_cached_state_timezone_and_raw_docs() {
    let c = ctx("sqlite", "cached").await.unwrap();
    let mut pipe = c.pipe("cached", "state", None, r#"{"columns":{"datetime":"dt","id":"id"},"dtypes":{"dt":"datetime","id":"int","value":"int"}}"#);
    let rows = vec![doc(vec![
        ("dt", udt(2024, 1, 1, 0, 0)),
        ("id", KValue::Int(1)),
        ("value", KValue::Int(1)),
    ])];
    assert!(c.sync(&pipe, rows).await.success);
    let time = c
        .conn
        .get_sync_time(&pipe, None, true, false)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(time, DateBound::Datetime(t) if t.tz.is_some()));
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    let raw = c
        .conn
        .driver()
        .read_docs_frame(&format!("SELECT dt FROM {}", c.conn.pipe_table_name(&sp)))
        .await
        .unwrap();
    assert!(
        matches!(&raw.data("dt").unwrap().values()[0], KValue::Str(s) if s == "2024-01-01 00:00:00.000000")
    );
    let remote = Pipe::builder(&c.conn.keys(), "remote_utc")
        .instance(&c.conn.keys())
        .kwarg("temporary", true)
        .kwarg("columns", params(r#"{"datetime":"dt"}"#))
        .kwarg("dtypes", params(r#"{"dt":"datetime"}"#))
        .parameters(params(
            &serde_json::json!({"fetch": {
                "definition": format!("SELECT dt FROM {}", c.conn.pipe_table_name(&sp))
            }})
            .to_string(),
        ))
        .build()
        .unwrap();
    let remote_time = c
        .conn
        .get_sync_time(&remote, None, true, true)
        .await
        .unwrap();
    assert!(matches!(remote_time, Some(DateBound::Datetime(t)) if t.tz.is_some()));
    let cached = c.conn.get_pipe_columns_types(&pipe).await.unwrap();
    let frame = c
        .conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    c.conn.enable_query_log();
    let opts = SyncOptions {
        check_existing: false,
        ..Default::default()
    };
    assert!(
        c.conn
            .sync_pipe_mut_cached(&mut pipe, frame, &opts, Some(cached))
            .await
            .success
    );
    let log = c.conn.take_query_log();
    assert!(
        !log.iter()
            .any(|q| q.contains("pragma_table_info") || q.contains("sqlite_master")),
        "{log:#?}"
    );
    c.conn.delete_pipe(&pipe).await;
}

#[tokio::test]
async fn sqlite_database_placeholder() {
    use meerschaum_sql::uri::{ConnectionSpec, ConnectorAttributes};
    let root = std::env::var("MRSM_ROOT_DIR").expect("tests require an isolated MRSM_ROOT_DIR");
    for spec in [
        ConnectionSpec::from_uri("sqlite:///{SQLITE_DB_PATH}", Some("w5sql"), None).unwrap(),
        ConnectionSpec::from_attributes(&ConnectorAttributes {
            flavor: Some(meerschaum_sql::Flavor::Sqlite),
            database: Some("{SQLITE_DB_PATH}".into()),
            ..Default::default()
        })
        .unwrap(),
    ] {
        let database = spec.resolved_database().unwrap().unwrap();
        assert!(std::path::Path::new(&database).starts_with(std::fs::canonicalize(&root).unwrap()));
        assert!(!spec.engine_str().unwrap().contains("{SQLITE_DB_PATH}"));
        let driver = meerschaum_sql::driver::SqlDriver::connect(&spec)
            .await
            .unwrap();
        assert_eq!(
            driver.value("SELECT 1").await.unwrap().unwrap().as_i64(),
            Some(1)
        );
    }
}

flavor_tests!(internal_cursor_upsert, |c: Ctx| async move {
    let pipe = Pipe::builder("mrsm", &c.prefix)
        .instance(&c.conn.keys())
        .kwarg("temporary", true)
        .parameters(params(&format!(r#"{{"target":"{}_cursors","static":true,"upsert":true,"columns":{{"parent":"parent","child":"child"}},"dtypes":{{"parent":"string","child":"string","seq":"int"}}}}"#, c.prefix)))
        .build().unwrap();
    let frame = |children: &[&str], seqs: &[i64]| {
        meerschaum_kernel::Frame::from_object_columns(vec![
            (
                "parent".into(),
                children
                    .iter()
                    .map(|_| KValue::Str("quoted'parent".into()))
                    .collect(),
            ),
            (
                "child".into(),
                children.iter().map(|c| KValue::Str((*c).into())).collect(),
            ),
            (
                "seq".into(),
                seqs.iter().copied().map(KValue::Int).collect(),
            ),
        ])
        .unwrap()
    };
    let options = SyncOptions {
        check_existing: false,
        ..Default::default()
    };
    c.conn.enable_query_log();
    let first = c
        .conn
        .sync_pipe(&pipe, frame(&["a", "b"], &[1, 1]), &options)
        .await;
    assert!(
        first.success,
        "{}\n{:#?}",
        first.message,
        c.conn.take_query_log()
    );
    c.conn.enable_query_log();
    let result = c
        .conn
        .sync_pipe(&pipe, frame(&["b", "c"], &[2, 3]), &options)
        .await;
    assert!(result.success, "{}", result.message);
    let log = c.conn.take_query_log();
    assert_eq!(log.len(), 1, "{log:#?}");
    let mut rows = c.docs(&pipe).await;
    rows.sort_by_key(|d| d["child"].py_str());
    assert_eq!(
        rows.iter().map(|d| d["seq"].clone()).collect::<Vec<_>>(),
        vec![KValue::Int(1), KValue::Int(2), KValue::Int(3)]
    );
    assert!(c.conn.drop_pipe(&pipe).await.success);
});

#[tokio::test]
async fn sqlite_docs_keep_mixed_storage_classes() {
    let c = ctx("sqlite", "mixed_docs").await.unwrap();
    let frame = c.conn.driver().read_docs_frame(
        "SELECT 1 AS value UNION ALL SELECT 'text' UNION ALL SELECT 1.5 UNION ALL SELECT X'00FF' UNION ALL SELECT NULL"
    ).await.unwrap();
    assert_eq!(
        frame.data("value").unwrap().values(),
        vec![
            KValue::Int(1),
            KValue::Str("text".into()),
            KValue::Float(1.5),
            KValue::Bytes(vec![0, 255]),
            KValue::Null,
        ]
    );
}
