//! `ConnectorRegistry::source_for_sync` receives the sync's keyword arguments (begin, end,
//! params, chunk settings, plugin-specific extras), in the shape Python plugins get them
//! (`meerschaum_engine::sync::sync_kwargs`), both from a bare `sync_pipe` and from an engine
//! run's per-node options. Native (C ABI) plugin sources are built from the same map.

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use meerschaum_connector::DateBound;
use meerschaum_connector::{Connector, InstanceConnector};
use meerschaum_core::{Map, Value};
use meerschaum_engine::sync::sync_kwargs;
use meerschaum_engine::{
    ConnectorRegistry, Engine, KeyFilter, NodeSyncOptions, RunOptions, Selection, SyncArgs,
    SyncBegin,
};
use serde_json::json;

struct Recording {
    inner: Arc<dyn ConnectorRegistry>,
    seen: Mutex<Vec<Map>>,
}

impl ConnectorRegistry for Recording {
    fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        self.inner.instance(keys)
    }
    fn source(&self, keys: &str) -> Result<Option<Arc<dyn Connector>>, String> {
        self.inner.source(keys)
    }
    fn source_for_sync(
        &self,
        keys: &str,
        args: &SyncArgs,
    ) -> Result<Option<Arc<dyn Connector>>, String> {
        self.seen.lock().unwrap().push(sync_kwargs(args));
        self.inner.source_for_sync(keys, args)
    }
    fn table_reference(&self, pipe: &meerschaum_core::Pipe) -> Result<String, String> {
        self.inner.table_reference(pipe)
    }
}

fn extra() -> Map {
    let mut m = Map::new();
    m.insert("api_key".into(), Value::Str("abc".into()));
    m.insert(
        "station_ids".into(),
        Value::List(vec![Value::Int(7), Value::Int(9)]),
    );
    m
}

async fn setup(tag: &str) -> (Env, Arc<Recording>) {
    let env = env(tag).await;
    env.exec("CREATE TABLE src (ts TEXT, id INTEGER, v REAL)")
        .await;
    env.exec("INSERT INTO src VALUES ('2026-01-01 00:00:00', 1, 1.0)")
        .await;
    env.register(
        "kw",
        &json!({
            "sql": "SELECT ts, id, v FROM src",
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": "datetime", "id": "int", "v": "float"},
        }),
    )
    .await;
    let rec = Arc::new(Recording {
        inner: env.registry.clone(),
        seen: Mutex::new(Vec::new()),
    });
    (env, rec)
}

fn check(kw: &Map) {
    assert_eq!(kw.get("api_key"), Some(&Value::Str("abc".into())));
    assert_eq!(
        kw.get("station_ids"),
        Some(&Value::List(vec![Value::Int(7), Value::Int(9)]))
    );
    assert_eq!(kw.get("check_existing"), Some(&Value::Bool(false)));
    assert_eq!(kw.get("chunksize"), Some(&Value::Int(3)));
    assert_eq!(kw.get("force"), Some(&Value::Bool(true)));
    assert_eq!(kw.get("retries"), Some(&Value::Int(4)));
    let params = kw.get("params").and_then(Value::as_map).expect("params");
    assert_eq!(params.get("id"), Some(&Value::Int(1)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bare_sync_passes_kwargs() {
    let (env, rec) = setup("kwargs_bare").await;
    let engine = Engine::new(rec.clone(), env.config());
    let mut params = Map::new();
    params.insert("id".into(), Value::Int(1));
    let args = SyncArgs {
        extra: extra(),
        begin: SyncBegin::At(DateBound::parse("2025-12-31T00:00:00+00:00").unwrap()),
        params: Some(params),
        check_existing: false,
        chunksize: Some(3),
        force: true,
        retries: 4,
        ..Default::default()
    };
    let pipe = env.pipe("kw", &json!({}));
    let res = engine.sync_pipe(&pipe, None, &args).await;
    assert!(res.success, "{}", res.message);
    let seen = rec.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    check(&seen[0]);
    assert!(matches!(seen[0].get("begin"), Some(Value::Str(s)) if s.starts_with("2025-12-31")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_runs_pass_kwargs() {
    let (env, rec) = setup("kwargs_run").await;
    let engine = Engine::new(rec.clone(), env.config());
    let mut params = Map::new();
    params.insert("id".into(), Value::Int(1));
    let result = engine
        .run(RunOptions {
            selection: Some(Selection::filter(KeyFilter::default())),
            sync: NodeSyncOptions {
                extra: extra(),
                params: Some(params),
                check_existing: false,
                chunksize: Some(3),
                force: true,
                retries: 4,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    assert!(result.success, "{}", result.message);
    let seen = rec.seen.lock().unwrap();
    assert!(!seen.is_empty());
    check(&seen[0]);
}
