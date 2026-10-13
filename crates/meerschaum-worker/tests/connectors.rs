//! Custom `@make_connector` connectors through the worker: a plain connector with `fetch()` and
//! an `IS_INSTANCE = True` connector driven through the `InstanceConnector` trait.

mod common;

use std::sync::Arc;

use meerschaum_connector::{DataQuery, DateBound, InstanceConnector, PipesKeysQuery, SyncOptions};
use meerschaum_core::{Pipe, Value as CValue};
use meerschaum_kernel::docs::frame_from_docs;
use meerschaum_kernel::value::{Value as KValue, parse_datetime_value};
use meerschaum_sql::SqlConnector;
use meerschaum_worker::PythonConnector;
use meerschaum_worker::sync::{PythonSyncOptions, sync_python_pipe};

use common::{TestEnv, assert_same_data, v4_compare};

fn pipe(ck: &str, mk: &str, instance: &str, params: &str) -> Pipe {
    Pipe::builder(ck, mk)
        .instance(instance)
        .parameters(CValue::from_json_str(params).unwrap().to_map())
        .build()
        .expect("valid pipe")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_custom_connector_fetch_matches_v4() {
    require_python!();
    let dir = tempfile::tempdir().unwrap();
    let rs_uri = format!("sqlite:///{}", dir.path().join("rs.db").display());
    let env = TestEnv::with_env(vec![
        ("MRSM_SQL_RS".into(), rs_uri.clone()),
        (
            "MRSM_SQL_V4REF".into(),
            format!("sqlite:///{}", dir.path().join("v4.db").display()),
        ),
    ]);
    let pool = env.pool(1);
    let source = PythonConnector::connect("wkfeed:alpha", pool.clone())
        .await
        .expect("describes wkfeed:alpha");
    assert_eq!(source.info().connector_type, "wkfeed");
    assert_eq!(source.info().class_name, "WkfeedConnector");
    assert!(source.info().has_fetch);
    assert!(!source.info().is_instance);

    let instance: Arc<dyn InstanceConnector> =
        Arc::new(SqlConnector::from_uri("rs", &rs_uri).await.unwrap());
    let mut p = pipe("wkfeed:alpha", "readings", "sql:rs", "{}");
    let st = sync_python_pipe(&source, instance, &mut p, &PythonSyncOptions::default()).await;
    assert!(st.success, "{}", st.message);

    let result = v4_compare(
        &env,
        "wkfeed:alpha",
        &["readings"],
        1,
        "sql:v4ref",
        "sql:rs",
    );
    assert_same_data(&result, &["readings"], "sql:v4ref", "sql:rs");
    assert_eq!(
        result["data"]["readings"]["sql:rs"]["records"][0]["sensor"],
        "alpha"
    );
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_instance_connector_methods() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(2);
    let conn = PythonConnector::connect("wkmem:main", pool.clone())
        .await
        .expect("describes wkmem:main");
    assert!(conn.info().is_instance);
    assert!(conn.info().has_method("sync_pipe"));
    // v4's base `InstanceConnector` defines the maintenance stubs.
    assert!(conn.info().has_method("compress_pipe"));

    let p = pipe(
        "test:wk",
        "mem",
        "wkmem:main",
        r#"{"columns": {"datetime": "dt", "id": "id"}}"#,
    );
    assert_eq!(conn.get_pipe_id(&p).await.unwrap(), None);
    let st = conn.register_pipe(&p).await;
    assert!(st.success, "{}", st.message);
    let st = conn.register_pipe(&p).await;
    assert!(!st.success);
    assert!(st.message.contains("already registered"), "{}", st.message);
    assert_eq!(
        conn.get_pipe_id(&p).await.unwrap(),
        Some(meerschaum_connector::PipeId::Int(1))
    );
    let attrs = conn.get_pipe_attributes(&p).await.unwrap();
    assert_eq!(attrs.get("metric_key"), Some(&CValue::Str("mem".into())));
    assert!(!conn.pipe_exists(&p).await.unwrap());

    let docs: Vec<_> = (0..3)
        .map(|i| {
            [
                (
                    "dt".to_string(),
                    KValue::Datetime(
                        parse_datetime_value(&format!("2024-05-01T00:0{i}:00+00:00")).unwrap(),
                    ),
                ),
                ("id".to_string(), KValue::Int(1)),
                ("val".to_string(), KValue::Float(i as f64 * 1.5)),
            ]
            .into_iter()
            .collect()
        })
        .collect();
    let df = frame_from_docs(&docs).unwrap();
    let st = conn.sync_pipe(&p, df, &SyncOptions::default()).await;
    assert!(st.success, "{}", st.message);
    assert_eq!(st.message, "Inserted 3 rows (check_existing=True).");
    assert!(conn.pipe_exists(&p).await.unwrap());

    assert_eq!(
        conn.get_pipe_rowcount(&p, &DataQuery::default())
            .await
            .unwrap(),
        Some(3)
    );
    let df = conn
        .get_pipe_data(&p, &DataQuery::default())
        .await
        .unwrap()
        .expect("data");
    assert_eq!(df.height(), 3);
    let begin = DateBound::parse("2024-05-01T00:01:00+00:00").unwrap();
    let df = conn
        .get_pipe_data(
            &p,
            &DataQuery {
                begin: Some(begin.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(df.height(), 2);
    let newest = conn.get_sync_time(&p, None, true).await.unwrap();
    assert_eq!(
        newest,
        Some(DateBound::parse("2024-05-01T00:02:00+00:00").unwrap())
    );
    // `get_backtrack_data` is not defined: the trait default (sync time minus backtrack).
    let df = conn
        .get_backtrack_data(&p, 1, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(df.height(), 2);

    let keys = conn
        .fetch_pipes_keys(&PipesKeysQuery::default())
        .await
        .unwrap();
    assert_eq!(format!("{keys:?}").matches("mem").count(), 1, "{keys:?}");

    // Forwarded to v4's base-class stub, which fails with its own message.
    let st = conn.compress_pipe(&p).await;
    assert!(!st.success, "{}", st.message);

    let st = conn.clear_pipe(&p, &DataQuery::default()).await;
    assert!(st.success, "{}", st.message);
    assert_eq!(
        conn.get_pipe_rowcount(&p, &DataQuery::default())
            .await
            .unwrap(),
        Some(0)
    );
    assert!(conn.delete_pipe(&p).await.success);
    assert_eq!(conn.get_pipe_id(&p).await.unwrap(), None);
    pool.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_source_into_custom_instance_matches_v4() {
    require_python!();
    let env = TestEnv::new();
    let pool = env.pool(2);
    let instance = PythonConnector::connect("wkmem:rs", pool.clone())
        .await
        .unwrap();
    let source = PythonConnector::connect("plugin:wk_shapes", pool.clone())
        .await
        .unwrap();
    let metrics = ["docs", "generator"];
    for metric in metrics {
        let mut p = pipe("plugin:wk_shapes", metric, "wkmem:rs", "{}");
        let st = sync_python_pipe(
            &source,
            Arc::new(instance.clone()),
            &mut p,
            &PythonSyncOptions::default(),
        )
        .await;
        assert!(st.success, "{metric}: {}", st.message);
    }
    let result = v4_compare(
        &env,
        "plugin:wk_shapes",
        &metrics,
        1,
        "wkmem:v4",
        "wkmem:rs",
    );
    assert_same_data(&result, &metrics, "wkmem:v4", "wkmem:rs");
    pool.shutdown().await;
}
