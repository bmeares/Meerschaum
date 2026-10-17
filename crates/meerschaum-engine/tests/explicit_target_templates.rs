//! `{{ Pipe('a', 'b') }}` renders the referenced pipe's *registered* table: a reference is built
//! from keys only, so the engine's registry must load the parameters (an explicit `target`) from
//! the referenced pipe's own instance, including a different instance (wave 8 task O; the CLI's
//! `sync pipes` failed with `no such table` before).

mod common;

use std::sync::Arc;

use common::*;
use meerschaum_connector::{Connector, DataQuery, InstanceConnector, InstanceProvider};
use meerschaum_core::Pipe;
use meerschaum_engine::{Engine, RunOptions, Selection, StaticRegistry, SyncArgs};
use serde_json::json;

/// Resolves the second instance for the SQL connector's own cross-instance templates.
struct Other(Arc<dyn InstanceConnector>);

impl InstanceProvider for Other {
    fn instance(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        (keys == "sql:other").then(|| self.0.clone())
    }
}

fn a_params() -> serde_json::Value {
    json!({
        "sql": "SELECT ts, id, v FROM src",
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        "target": "custom_a_table",
    })
}

fn b_params(reference: &str) -> serde_json::Value {
    json!({
        "sql": format!("SELECT ts, id, v * 2 AS v FROM {reference}"),
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        "target": "custom_b_table",
    })
}

async fn setup(tag: &str) -> Env {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0), ('2026-01-01 01:00:00', 1, 2.0)",
    )
    .await;
    env.register("a_raw", &a_params()).await;
    env.register(
        "b_clean",
        &b_params("{{ Pipe('sql:eng', 'a_raw', instance='sql:eng') }}"),
    )
    .await;
    env
}

async fn rows_of(env: &Env, metric: &str, params: &serde_json::Value) -> usize {
    let pipe = env.pipe(metric, params);
    env.conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .expect("read")
        .map_or(0, |df| df.height())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bare_sync_renders_the_explicit_target_of_a_reference() {
    let env = setup("explicit_bare").await;
    let engine = env.engine();
    let a = env.pipe("a_raw", &json!({}));
    let res = engine.sync_pipe(&a, None, &SyncArgs::default()).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(rows_of(&env, "a_raw", &a_params()).await, 2);

    // `b_clean` is built from keys only (no parameters), as a CLI selection does.
    let b = env.pipe("b_clean", &json!({}));
    let res = engine.sync_pipe(&b, None, &SyncArgs::default()).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        rows_of(&env, "b_clean", &b_params("custom_a_table")).await,
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_run_of_the_child_alone_renders_the_explicit_target() {
    let env = setup("explicit_graph").await;
    let engine = env.engine();
    let a = env.pipe("a_raw", &json!({}));
    assert!(
        engine
            .sync_pipe(&a, None, &SyncArgs::default())
            .await
            .success
    );
    // Only `b_clean` is selected, so `a_raw` is not among the graph's known pipes.
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::refs([env.pipe_ref("b_clean")])),
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(
        rows_of(&env, "b_clean", &b_params("custom_a_table")).await,
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reference_to_a_pipe_on_another_instance_renders_its_registered_target() {
    let env = env("explicit_cross").await;
    env.exec("CREATE TABLE custom_a_table (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec(
        "INSERT INTO custom_a_table VALUES ('2026-01-01 00:00:00', 1, 1.0), \
         ('2026-01-01 01:00:00', 1, 2.0)",
    )
    .await;
    // `a_raw` lives (is registered) on a second instance, with an explicit target.
    let other = connect(&env.dir.join("other.db")).await;
    let a_other = Pipe::builder("sql:other", "a_raw")
        .instance("sql:other")
        .parameters(json_map(&a_params()))
        .build()
        .unwrap();
    assert!(other.register_pipe(&a_other).await.success);
    env.register(
        "b_clean",
        &b_params("{{ Pipe('sql:other', 'a_raw', instance='sql:other') }}"),
    )
    .await;

    let main: Arc<dyn InstanceConnector> = env.conn.clone();
    let main_src: Arc<dyn Connector> = env.conn.clone();
    let other_instance: Arc<dyn InstanceConnector> = other.clone();
    env.conn
        .set_instance_provider(Arc::new(Other(other_instance.clone())));
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, main)
            .with_source(INSTANCE, main_src)
            .with_instance("sql:other", other_instance)
            .with_table_reference(|pipe: &Pipe| {
                let target = pipe.target(None).map_err(|e| e.to_string())?;
                Ok(meerschaum_sql::ident::sql_item_name(
                    &target,
                    meerschaum_sql::Flavor::Sqlite,
                    None,
                ))
            }),
    );
    let engine = Engine::new(registry, env.config());
    let b = env.pipe("b_clean", &json!({}));
    let res = engine.sync_pipe(&b, None, &SyncArgs::default()).await;
    assert!(res.success, "{}", res.message);
    assert_eq!(
        rows_of(&env, "b_clean", &b_params("custom_a_table")).await,
        2
    );
}
