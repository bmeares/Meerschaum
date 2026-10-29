//! Native fetch and in-place sync preserve a declared parent's bounded window.

mod common;

use common::*;
use meerschaum_connector::pipe_ops::frame_to_docs;
use meerschaum_connector::{InstanceConnector, InstanceProvider};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::instance::fetch::FetchArgs;
use meerschaum_connector_sql::queries::metadef::{Bound, ParentSpec};
use meerschaum_core::Pipe;
use std::sync::Arc;

async fn bounded_window(flavor: &'static str, inplace: bool, schema: Option<&str>) {
    let context = match schema {
        Some(schema) => common::ctx_in_schema(flavor, "parent_window", schema).await,
        None => common::ctx(flavor, "parent_window").await,
    };
    let Some(c) = context else {
        return;
    };
    let parent = c.pipe(
        "source",
        "readings",
        None,
        r#"{"columns":{"datetime":"ts"},"dtypes":{"ts":"int","value":"int"},"precision":"second"}"#,
    );
    assert!(c.conn.register_pipe(&parent).await.success);
    ok(c.sync(
        &parent,
        (1..=4)
            .map(|ts| doc(vec![("ts", i(ts)), ("value", i(ts * 10))]))
            .collect(),
    )
    .await);
    let reference = format!(
        "Pipe('{}', '{}', instance='{}')",
        parent.connector_keys(),
        parent.metric_key(),
        c.conn.keys()
    );
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&parent).unwrap());
    let child = c.pipe(
        "source",
        "lag",
        None,
        &serde_json::json!({
            "columns":{"datetime":"ts"},
            "dtypes":{"ts":"int","previous":"int"},
            "precision":"second", "fetch":{"backtrack_minutes":0},
            "parent": reference,
            "sql": format!("SELECT ts, LAG(value) OVER (ORDER BY ts) AS previous FROM {table}"),
        })
        .to_string(),
    );
    let docs = if inplace {
        let result = c
            .conn
            .sync_pipe_inplace(&child, None, Bound::Int(2), Bound::Int(4), true)
            .await;
        assert!(result.success, "{}", result.message);
        c.docs(&child).await
    } else {
        frame_to_docs(
            &c.conn
                .fetch(
                    &child,
                    &FetchArgs {
                        begin: Bound::Int(2),
                        end: Bound::Int(4),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap(),
        )
    };
    assert_eq!(docs.len(), 2, "{docs:?}");
    let first = docs
        .iter()
        .find(|row| as_int(&row["ts"]) == Some(2))
        .unwrap();
    assert!(
        first["previous"].is_na(),
        "The first row must not see data outside the parent's window: {docs:?}"
    );
    let next = docs
        .iter()
        .find(|row| as_int(&row["ts"]) == Some(3))
        .unwrap();
    assert_eq!(as_int(&next["previous"]), Some(20));
    assert_eq!(c.docs(&parent).await.len(), 4);
    if !inplace {
        let unbounded = c
            .conn
            .fetch(
                &child,
                &FetchArgs {
                    begin: Bound::None,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unbounded.height(), 4);
        let overridden = c
            .conn
            .fetch(
                &child,
                &FetchArgs {
                    begin: Bound::Int(2),
                    end: Bound::Int(4),
                    parent: Some(ParentSpec {
                        target: parent.target(None).unwrap(),
                        schema: c.conn.pipe_table_schema(&parent),
                        dt_col: None,
                        dt_typ: None,
                        precision_unit: "second".into(),
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap();
        let rows = frame_to_docs(&overridden);
        let first = rows
            .iter()
            .find(|row| as_int(&row["ts"]) == Some(2))
            .unwrap();
        assert_eq!(as_int(&first["previous"]), Some(10));
    }
    if inplace {
        c.delete(&child).await;
    }
    c.delete(&parent).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_fetch_sqlite() {
    bounded_window("sqlite", false, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_inplace_sqlite() {
    bounded_window("sqlite", true, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_fetch_postgresql() {
    bounded_window("postgresql", false, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_inplace_postgresql() {
    bounded_window("postgresql", true, None).await;
}

struct ForeignParent(Arc<SqlConnector>);
impl InstanceProvider for ForeignParent {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == self.0.keys()).then(|| self.0.clone() as Arc<dyn InstanceConnector>)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreign_parent_metadata_sqlite() {
    let c = common::ctx("sqlite", "foreign_parent").await.unwrap();
    let source = c.pipe(
        "source",
        "readings",
        None,
        r#"{"columns":{"datetime":"ts"},"dtypes":{"ts":"int","value":"int"}}"#,
    );
    ok(c.sync(
        &source,
        vec![
            doc(vec![("ts", i(1)), ("value", i(10))]),
            doc(vec![("ts", i(2)), ("value", i(20))]),
        ],
    )
    .await);
    let foreign = Arc::new(
        SqlConnector::from_uri("parent_metadata", "sqlite:///:memory:")
            .await
            .unwrap(),
    );
    let metadata = Pipe::builder("lookup", "readings")
        .instance(&foreign.keys())
        .parameters(params(
            &serde_json::json!({
                "target": source.target(None).unwrap(), "columns":{"datetime":"ts"},
                "dtypes":{"ts":"int"},"precision":"second",
            })
            .to_string(),
        ))
        .build()
        .unwrap();
    assert!(foreign.register_pipe(&metadata).await.success);
    c.conn
        .set_instance_provider(Arc::new(ForeignParent(foreign.clone())));
    let table = c.conn.pipe_table_name(&c.conn.sql_pipe(&source).unwrap());
    let child = c.pipe(
        "source",
        "bounded",
        None,
        &serde_json::json!({
            "columns":{"datetime":"ts"},"dtypes":{"ts":"int"},"precision":"second",
            "parent":format!("Pipe('lookup', 'readings', instance='{}')",foreign.keys()),
            "sql":format!("SELECT ts, LAG(value) OVER (ORDER BY ts) AS previous FROM {table}"),
        })
        .to_string(),
    );
    let rows = frame_to_docs(
        &c.conn
            .fetch(
                &child,
                &FetchArgs {
                    begin: Bound::Int(2),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert!(rows[0]["previous"].is_na(), "{rows:?}");
    c.delete(&source).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_fetch_postgresql_schema() {
    bounded_window("postgresql", false, Some("parent_pushdown")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_inplace_postgresql_schema() {
    bounded_window("postgresql", true, Some("parent_pushdown")).await;
}
