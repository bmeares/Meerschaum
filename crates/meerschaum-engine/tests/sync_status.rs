//! The per-pipe sync status (rc15 contract C2) every engine sync records on SQLite: bare syncs
//! (`Engine::sync_pipe`, the path of Python's `pipe.sync()` and worker callbacks), flat and graph
//! runs (one record per node, after its retries), failures counting until a success, the
//! debounce of successes and reads that see the latest sync, the status owner of nested syncs,
//! and forgetting a deleted, dropped or cleared pipe for good.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use meerschaum_connector::InstanceConnector;
use meerschaum_connector::sync_status::{SyncStatus, reset_debounce, stored_status};
use meerschaum_core::Pipe;
use meerschaum_engine::{Depth, Engine, PipeRef, RunOptions, Selection, StaticRegistry, SyncArgs};
use serde_json::json;

fn unique(tag: &str) -> String {
    format!(
        "st_{tag}_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    )
}

fn sql_params(sql: String) -> serde_json::Value {
    json!({
        "sql": sql,
        "columns": {"datetime": "ts", "id": "id"},
        "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
    })
}

async fn status(env: &Env, pipe: &Pipe) -> Option<SyncStatus> {
    env.conn
        .get_sync_status(pipe)
        .await
        .expect("read the sync status")
}

/// The stored row, without writing what this process still holds for the pipe first.
async fn stored(env: &Env, pipe: &Pipe) -> Option<SyncStatus> {
    stored_status(env.conn.as_ref(), pipe)
        .await
        .expect("read the stored sync status")
}

/// An engine over `env`'s database under instance keys of its own, so that no other test's read
/// of `sql:eng`'s statuses writes this test's held successes.
fn engine_on(env: &Env, instance: &str) -> Engine {
    let conn: Arc<dyn InstanceConnector> = env.conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = env.conn.clone();
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance(instance, conn)
            .with_source(INSTANCE, source),
    );
    Engine::new(registry, env.config())
}

/// The pipe `sql:eng / metric` on the instance keys `instance`.
fn pipe_on(instance: &str, metric: &str, parameters: &serde_json::Value) -> Pipe {
    Pipe::builder(INSTANCE, metric)
        .instance(instance)
        .parameters(json_map(parameters))
        .build()
        .unwrap()
}

fn one_row() -> Vec<meerschaum_connector::pipe_ops::Doc> {
    vec![
        [
            (
                "ts".to_string(),
                meerschaum_kernel::value::Value::Str("2026-01-01".into()),
            ),
            ("v".to_string(), meerschaum_kernel::value::Value::Int(1)),
        ]
        .into_iter()
        .collect(),
    ]
}

async fn create_source(env: &Env, src: &str, hours: std::ops::Range<u32>) {
    env.exec(&format!(
        "CREATE TABLE IF NOT EXISTS {src} (ts TIMESTAMP, id INTEGER, v REAL)"
    ))
    .await;
    insert(env, src, hours).await;
}

async fn insert(env: &Env, src: &str, hours: std::ops::Range<u32>) {
    let rows = hours
        .map(|h| format!("('2026-01-01 {h:02}:00:00', {h}, {h}.5)"))
        .collect::<Vec<_>>()
        .join(", ");
    env.exec(&format!("INSERT INTO {src} (ts, id, v) VALUES {rows}"))
        .await;
}

fn expected_source(graph_run: bool) -> &'static str {
    // A test process is not a job unless the environment says so.
    match meerschaum_connector::sync_status::detect_source(graph_run).0 {
        meerschaum_connector::sync_status::SyncSource::Job => "job",
        _ if graph_run => "engine",
        _ => "cli",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_sync_records_its_outcome_and_failures_count_until_a_success() {
    let env = env("status_bare").await;
    let metric = unique("bare");
    let src = format!("{metric}_src");
    let pipe = env
        .register(&metric, &sql_params(format!("SELECT ts, id, v FROM {src}")))
        .await;
    let engine = env.engine();
    assert!(status(&env, &pipe).await.is_none(), "never synced");

    // The source table does not exist yet: two failed syncs.
    for n in 1..=2 {
        let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
        assert!(!result.success, "{}", result.message);
        let s = status(&env, &pipe).await.expect("a failure is recorded");
        assert!(!s.success);
        assert_eq!(s.consecutive_failures, n);
        assert!(s.last_failure.is_some() && s.last_success.is_none());
        assert_eq!(s.last_attempt, s.last_failure);
        assert!(!s.message.is_empty());
    }

    // The first success after failures is written at once and resets the count.
    create_source(&env, &src, 0..3).await;
    let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(result.success, "{}", result.message);
    let s = status(&env, &pipe).await.unwrap();
    assert!(s.success, "{s:?}");
    assert_eq!(s.consecutive_failures, 0);
    assert_eq!(s.rows_inserted, 3);
    assert!(s.last_success.is_some() && s.last_failure.is_some());
    assert!(s.last_success > s.last_failure);
    assert!(s.duration_seconds.is_some());
    assert_eq!(s.source, expected_source(false));
    assert_eq!(s.pipe_ref, pipe.pipe_ref());
    assert_eq!(s.location_key, None);
    assert_eq!(s.instance_keys, INSTANCE);

    // The bulk read has it, keyed by `pipe_ref`; read under other keys it is re-keyed.
    let all = env.conn.get_sync_statuses(INSTANCE).await.unwrap();
    assert!(all.iter().any(|x| x.pipe_ref == pipe.pipe_ref()));
    let other = env.conn.get_sync_statuses("sql:other").await.unwrap();
    assert!(
        other
            .iter()
            .any(|x| x.pipe_ref == format!("sql:other|{INSTANCE}|{metric}|None"))
    );

    // Deleting the pipe (the instance method every surface calls) forgets its status.
    assert!(
        InstanceConnector::delete_pipe(env.conn.as_ref(), &pipe)
            .await
            .success
    );
    assert!(status(&env, &pipe).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successes_within_the_window_are_held_then_written() {
    let env = env("status_debounce").await;
    let instance = format!("sql:{}", unique("held"));
    let engine = engine_on(&env, &instance);
    let metric = unique("debounce");
    let src = format!("{metric}_src");
    create_source(&env, &src, 0..3).await;
    let pipe = pipe_on(
        &instance,
        &metric,
        &sql_params(format!("SELECT ts, id, v FROM {src}")),
    );
    let first = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(first.success, "{}", first.message);
    assert_eq!(status(&env, &pipe).await.unwrap().rows_inserted, 3);

    // A second success right away is held: the stored row still shows the first ...
    insert(&env, &src, 3..5).await;
    let second = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(second.success, "{}", second.message);
    assert_eq!(second.inserted, 2, "{}", second.message);
    assert_eq!(stored(&env, &pipe).await.unwrap().rows_inserted, 3);

    // ... and written when the window ends.
    let deadline = std::time::Instant::now()
        + meerschaum_connector::sync_status::DEBOUNCE
        + Duration::from_secs(5);
    loop {
        let s = stored(&env, &pipe).await.unwrap();
        if s.rows_inserted == 2 {
            assert!(s.success);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the held success was never written: {s:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // A read in this process sees the latest sync: it writes a held success first.
    insert(&env, &src, 5..6).await;
    let third = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(third.success, "{}", third.message);
    assert_eq!(stored(&env, &pipe).await.unwrap().rows_inserted, 2, "held");
    assert_eq!(status(&env, &pipe).await.unwrap().rows_inserted, 1);
    assert_eq!(stored(&env, &pipe).await.unwrap().rows_inserted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_success_never_brings_back_a_removed_pipe() {
    let env = env("status_forget").await;
    let instance = format!("sql:{}", unique("forget"));
    let engine = engine_on(&env, &instance);
    let params = json!({"columns": {"datetime": "ts"}});
    let deleted = pipe_on(&instance, &unique("deleted"), &params);
    let dropped = pipe_on(&instance, &unique("dropped"), &params);
    let cleared = pipe_on(&instance, &unique("cleared"), &params);
    for pipe in [&deleted, &dropped, &cleared] {
        // The second success is held by the debounce.
        for _ in 0..2 {
            let input = meerschaum_kernel::enforce::PipeInput::Docs(one_row());
            let result = engine
                .sync_pipe(pipe, Some(input), &SyncArgs::default())
                .await;
            assert!(result.success, "{}", result.message);
        }
    }
    let conn = env.conn.as_ref();
    assert!(InstanceConnector::delete_pipe(conn, &deleted).await.success);
    assert!(InstanceConnector::drop_pipe(conn, &dropped).await.success);
    let everything = meerschaum_connector::DataQuery::default();
    assert!(
        InstanceConnector::clear_pipe(conn, &cleared, &everything)
            .await
            .success
    );
    for pipe in [&deleted, &dropped, &cleared] {
        assert!(stored(&env, pipe).await.is_none(), "{pipe}");
    }

    // The window ends: nothing is written back, and a read finds nothing either.
    tokio::time::sleep(meerschaum_connector::sync_status::DEBOUNCE + Duration::from_secs(1)).await;
    for pipe in [&deleted, &dropped, &cleared] {
        assert!(stored(&env, pipe).await.is_none(), "{pipe}");
        assert!(status(&env, pipe).await.is_none(), "{pipe}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runs_record_each_node_once_flat_as_cli_and_graph_as_engine() {
    let env = env("status_runs").await;
    let (parent, child) = (unique("parent"), unique("child"));
    let src = format!("{parent}_src");
    create_source(&env, &src, 0..4).await;
    let parent_pipe = env
        .register(&parent, &sql_params(format!("SELECT ts, id, v FROM {src}")))
        .await;
    let child_pipe = env
        .register(
            &child,
            &sql_params(format!(
                "SELECT ts, id, v FROM {{{{ Pipe('{INSTANCE}', '{parent}', instance='{INSTANCE}') }}}}"
            )),
        )
        .await;
    let (p_ref, c_ref) = (
        PipeRef::from_pipe(&parent_pipe),
        PipeRef::from_pipe(&child_pipe),
    );
    let engine = env.engine();

    // A flat run: both nodes, the command line's source.
    let flat = engine
        .run(RunOptions {
            selection: Some(Selection::refs([p_ref.clone(), c_ref.clone()])),
            instances: vec![INSTANCE.into()],
            ..Default::default()
        })
        .await;
    assert!(flat.success, "{}", flat.message);
    for pipe in [&parent_pipe, &child_pipe] {
        let s = status(&env, pipe).await.expect("each node is recorded");
        assert!(s.success, "{s:?}");
        assert_eq!(s.source, expected_source(false), "{s:?}");
    }
    let child_before = status(&env, &child_pipe).await.unwrap();

    // A graph run from the parent: `engine` (the debounce of the last successes is reset).
    reset_debounce(p_ref.as_str());
    reset_debounce(c_ref.as_str());
    insert(&env, &src, 4..6).await;
    let graph = engine
        .run(RunOptions {
            selection: Some(Selection::refs([p_ref.clone()]).with_children(Some(Depth::All))),
            instances: vec![INSTANCE.into()],
            yes: true,
            ..Default::default()
        })
        .await;
    assert!(graph.success, "{}", graph.message);
    let s = status(&env, &parent_pipe).await.unwrap();
    assert_eq!(s.source, expected_source(true), "{s:?}");
    assert_eq!(s.rows_inserted, 2, "{s:?}");
    let ran_child = graph
        .record
        .as_ref()
        .and_then(|r| r.node(&c_ref))
        .is_some_and(|n| n.status == meerschaum_engine::NodeStatus::Succeeded);
    let s = status(&env, &child_pipe).await.unwrap();
    assert!(ran_child, "{}", graph.message);
    assert_eq!(s.source, expected_source(true), "{s:?}");
    assert!(s.last_attempt > child_before.last_attempt);

    // A node the planner skips (nothing new upstream) is not a sync: its status stays.
    reset_debounce(c_ref.as_str());
    let child_now = status(&env, &child_pipe).await.unwrap();
    let again = engine
        .run(RunOptions {
            selection: Some(Selection::refs([c_ref.clone()]).with_parents(Some(Depth::All))),
            instances: vec![INSTANCE.into()],
            ..Default::default()
        })
        .await;
    assert!(again.success, "{}", again.message);
    let skipped = again
        .record
        .as_ref()
        .and_then(|r| r.node(&c_ref))
        .is_some_and(|n| n.status == meerschaum_engine::NodeStatus::Skipped);
    assert!(skipped, "{}", again.message);
    assert_eq!(status(&env, &child_pipe).await.unwrap(), child_now);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_retries_is_recorded_once() {
    let env = env("status_retry").await;
    let metric = unique("retry");
    let mut params = sql_params(format!("SELECT ts, id, v FROM {metric}_missing"));
    params["retry"] = json!({"max": 2, "backoff_seconds": 0});
    let pipe = env.register(&metric, &params).await;
    let result = env
        .engine()
        .run(RunOptions {
            selection: Some(Selection::refs([PipeRef::from_pipe(&pipe)])),
            instances: vec![INSTANCE.into()],
            ..Default::default()
        })
        .await;
    assert!(!result.success);
    let s = status(&env, &pipe).await.expect("the failure is recorded");
    assert!(!s.success);
    // Three attempts, one failed sync.
    assert_eq!(s.consecutive_failures, 1, "{s:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sync_owned_by_its_caller_is_not_recorded_twice() {
    let env = env("status_owner").await;
    let metric = unique("owner");
    let pipe = env
        .register(&metric, &json!({"columns": {"datetime": "ts"}}))
        .await;
    let docs = one_row();
    let args = SyncArgs {
        status_owner: Some(PipeRef::from_pipe(&pipe)),
        ..Default::default()
    };
    let result = env
        .engine()
        .sync_pipe(
            &pipe,
            Some(meerschaum_kernel::enforce::PipeInput::Docs(docs.clone())),
            &args,
        )
        .await;
    assert!(result.success, "{}", result.message);
    assert!(status(&env, &pipe).await.is_none(), "the owner records it");

    // A temporary pipe has no status either.
    let mut temp = env.pipe(&unique("temp"), &json!({"columns": {"datetime": "ts"}}));
    temp.temporary = true;
    let result = env
        .engine()
        .sync_pipe(
            &temp,
            Some(meerschaum_kernel::enforce::PipeInput::Docs(docs)),
            &SyncArgs::default(),
        )
        .await;
    assert!(result.success, "{}", result.message);
    assert!(status(&env, &temp).await.is_none());
}

/// The same record on a Valkey instance (`MRSM_TEST_VALKEY_URI`; empty: the disposable container
/// on 6399, database 13): a bare sync records, failures count, reads match, deleting forgets.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_valkey_instance_keeps_sync_statuses() {
    let Ok(raw) = std::env::var("MRSM_TEST_VALKEY_URI") else {
        eprintln!("skipped: MRSM_TEST_VALKEY_URI is not set");
        return;
    };
    let uri = if raw.is_empty() {
        "valkey://localhost:6399/13".to_string()
    } else {
        raw
    };
    let label = unique("vk");
    let keys = format!("valkey:{label}");
    let conn: std::sync::Arc<dyn InstanceConnector> = std::sync::Arc::new(
        meerschaum_connector_valkey::ValkeyConnector::from_uri(&label, &uri)
            .await
            .expect("connect to Valkey"),
    );
    let registry = std::sync::Arc::new(
        meerschaum_engine::StaticRegistry::new().with_instance(&keys, conn.clone()),
    );
    let dir = temp_dir("status_valkey");
    let engine = meerschaum_engine::Engine::new(
        registry,
        meerschaum_engine::EngineConfig {
            home_instance: keys.clone(),
            lease_dir: dir.join("leases"),
            ..Default::default()
        },
    );
    let pipe = Pipe::builder("plugin:rc15g", &unique("readings"))
        .instance(&keys)
        .parameters(json_map(&json!({"columns": {"datetime": "ts"}})))
        .build()
        .unwrap();
    let docs = vec![
        [
            (
                "ts".to_string(),
                meerschaum_kernel::value::Value::Str("2026-01-01".into()),
            ),
            ("v".to_string(), meerschaum_kernel::value::Value::Int(1)),
        ]
        .into_iter()
        .collect(),
    ];
    let result = engine
        .sync_pipe(
            &pipe,
            Some(meerschaum_kernel::enforce::PipeInput::Docs(docs)),
            &SyncArgs::default(),
        )
        .await;
    assert!(result.success, "{}", result.message);
    let s = conn
        .get_sync_status(&pipe)
        .await
        .unwrap()
        .expect("recorded");
    assert!(s.success && s.rows_inserted == 1, "{s:?}");

    let failed = meerschaum_connector::sync_status::SyncOutcome::new(
        false,
        "boom",
        meerschaum_connector::sync_status::SyncSource::Job,
        Some("vk-job".into()),
    );
    meerschaum_connector::sync_status::record_now(conn.as_ref(), &pipe, &[failed.clone(), failed])
        .await
        .unwrap();
    let s = conn.get_sync_status(&pipe).await.unwrap().unwrap();
    assert_eq!((s.success, s.consecutive_failures), (false, 2), "{s:?}");
    assert_eq!(s.job.as_deref(), Some("vk-job"));
    assert!(s.last_success.is_some());
    let all = conn.get_sync_statuses(&keys).await.unwrap();
    assert!(all.iter().any(|x| x.pipe_ref == pipe.pipe_ref()));

    assert!(
        InstanceConnector::delete_pipe(conn.as_ref(), &pipe)
            .await
            .success
    );
    assert!(conn.get_sync_status(&pipe).await.unwrap().is_none());
    let _ = std::fs::remove_dir_all(&dir);
}
