//! The host registry and callback path use the engine's native sync flow.
mod common;
use common::*;
use meerschaum_connector::DataQuery;
use meerschaum_core::{Map, Value};
use meerschaum_engine::{ConnectorRegistry, python::PythonRegistry};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_worker::{CallbackRequest, FetchChunk, Manifest, PoolConfig, WorkerPool};
use std::sync::Arc;

#[tokio::test]
async fn manifest_routes_python_seams_without_starting_workers() {
    let env = env("wiring_manifest").await;
    let manifest: Manifest = serde_json::from_value(serde_json::json!({
        "schema_version": 1, "meerschaum_version": "test", "python_version": "test",
        "plugins": {
            "fetcher": {"name":"fetcher", "data_functions":["fetch"]},
            "syncer": {"name":"syncer", "data_functions":["sync"]},
            "subscriber": {"name":"subscriber", "data_functions":["subscribe"]}
        },
        "connector_types": {"custom": {"type":"custom", "plugin":"fetcher", "class_name":"Custom", "is_instance":true, "is_thread_safe":false, "methods":["fetch", "sync_pipe"]}},
        "pre_sync_hooks": ["fetcher"]
    })).unwrap();
    let pool = WorkerPool::new(PoolConfig::default());
    let registry = PythonRegistry::new(
        env.registry.clone(),
        pool.clone(),
        Arc::new(manifest),
        env.config(),
    );
    assert!(registry.source("sql:eng").unwrap().is_some());
    assert!(registry.source("plugin:fetcher").unwrap().is_some());
    assert!(registry.plugin_sync("plugin:fetcher").is_none());
    assert!(registry.plugin_sync("plugin:syncer").is_some());
    assert!(registry.legacy_stream("plugin:syncer").is_none());
    registry.enable_legacy_streams();
    assert!(registry.legacy_stream("plugin:syncer").is_some());
    assert!(registry.stream_source("plugin:subscriber").is_some());
    assert!(registry.legacy_stream("plugin:subscriber").is_none());
    assert!(registry.instance("custom:test").is_ok());
    assert!(registry.hooks().is_some());
    assert!(registry.is_python("custom:test"));
    assert!(!registry.is_python("sql:eng"));
    assert_eq!(pool.stats().spawned, 0);
}

#[tokio::test]
async fn callback_uses_native_dtype_enforcement_and_diff() {
    let env = env("wiring_callback").await;
    let pool = WorkerPool::new(PoolConfig::default());
    let registry = PythonRegistry::new(
        env.registry.clone(),
        pool.clone(),
        Arc::new(Manifest::default()),
        env.config(),
    );
    let pipe = env.pipe(
        "wiring_callback",
        &serde_json::json!({"columns":{"id":"id"}, "dtypes":{"id":"int", "value":"float"}}),
    );
    let handler = registry.callbacks();
    for value in ["1.5", "2.5", "2.5"] {
        let docs = vec![
            [
                ("id".into(), KValue::Int(1)),
                ("value".into(), KValue::Str(value.into())),
            ]
            .into_iter()
            .collect(),
        ];
        let result = handler
            .handle(CallbackRequest {
                op: "sync".into(),
                call_id: 1,
                pipe: Some(pipe.clone()),
                chunk: Some(FetchChunk::Docs(docs)),
                kwargs: Map::new(),
            })
            .await;
        assert!(result.success, "{}", result.message);
    }
    let frame = env
        .conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.height(), 1);
    assert_eq!(
        frame.column("value").unwrap().data.get(0),
        KValue::Float(2.5)
    );
    assert_eq!(pool.stats().spawned, 0);
    let missing = handler
        .handle(CallbackRequest {
            op: "sync".into(),
            call_id: 1,
            pipe: None,
            chunk: None,
            kwargs: [("begin".into(), Value::Null)].into_iter().collect(),
        })
        .await;
    assert!(!missing.success);
    drop(registry);
    let stopped = handler
        .handle(CallbackRequest {
            op: "sync".into(),
            call_id: 1,
            pipe: Some(pipe),
            chunk: None,
            kwargs: Map::new(),
        })
        .await;
    assert_eq!(stopped.message, "The engine has stopped.");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plugin_sync_calls_back_into_native_engine() {
    use meerschaum_engine::{Engine, SyncArgs};
    use meerschaum_worker::{WorkerOptions, ensure_manifest};
    use std::process::Command;
    use std::time::Duration;
    let env = env("wiring_plugin").await;
    let plugins = env.dir.join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let python = meerschaum_worker::worker::default_python();
    let process_env = vec![
        (
            "MRSM_ROOT_DIR".into(),
            env.dir.to_string_lossy().into_owned(),
        ),
        (
            "MRSM_PLUGINS_DIR".into(),
            plugins.to_string_lossy().into_owned(),
        ),
        ("PYTHONPATH".into(), repo.to_string_lossy().into_owned()),
        ("PYTHONDONTWRITEBYTECODE".into(), "1".into()),
        ("UV_PYTHON".into(), python.to_string_lossy().into_owned()),
        (
            "MRSM_SQL_ENG".into(),
            format!("sqlite:///{}", env.db.display()),
        ),
    ];
    let probe = Command::new(&python)
        .args(["-c", "import pyarrow, pandas"])
        .envs(process_env.clone())
        .output();
    if !probe.is_ok_and(|p| p.status.success()) {
        eprintln!("Skipping plugin integration: Python pyarrow/pandas unavailable.");
        return;
    }
    #[cfg(unix)]
    {
        let shared = repo.join("test_root/venvs/mrsm");
        if shared.is_dir() {
            std::fs::create_dir_all(env.dir.join("venvs")).unwrap();
            std::os::unix::fs::symlink(shared, env.dir.join("venvs/mrsm")).unwrap();
        }
    }
    std::fs::write(
        plugins.join("wiring_sync.py"),
        "def sync(pipe, **kw):\n    return pipe.sync([{'id': 1, 'value': '3.5'}])\n",
    )
    .unwrap();
    std::fs::write(plugins.join("wiring_fetch.py"), "def fetch(pipe, custom_value=None, **kw):\n    return [{'id': 2, 'value': custom_value}]\n").unwrap();
    let initialized = Command::new(&python)
        .args([
            "-c",
            r#"import os, venv
for name in ('wiring_sync', 'wiring_fetch'):
    venv.create(os.path.join(os.environ['MRSM_ROOT_DIR'], 'venvs', name), with_pip=False)
import meerschaum
meerschaum.get_config('meerschaum')
from meerschaum.plugins import import_plugins
import_plugins()
"#,
        ])
        .envs(process_env.clone())
        .env_remove("MRSM_CORE")
        .output()
        .unwrap();
    assert!(
        initialized.status.success(),
        "{}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let pool = WorkerPool::new(PoolConfig {
        size: 1,
        call_timeout: Some(Duration::from_secs(60)),
        worker: WorkerOptions {
            python,
            env: process_env,
            env_remove: vec!["MRSM_CORE".into()],
            current_dir: Some(repo),
            ..Default::default()
        },
        ..Default::default()
    });
    let (manifest, _) = ensure_manifest(
        &pool,
        &env.dir.join("cache/plugins_manifest.json"),
        &[plugins],
    )
    .await
    .unwrap();
    let registry = PythonRegistry::new(
        env.registry.clone(),
        pool.clone(),
        Arc::new(manifest),
        env.config(),
    );
    let engine = Engine::new(registry.clone(), env.config());
    let pipe = meerschaum_core::Pipe::builder("plugin:wiring_sync", "wiring_native_callback")
        .instance(INSTANCE)
        .parameters(json_map(
            &serde_json::json!({"columns":{"id":"id"}, "dtypes":{"value":"float"}}),
        ))
        .build()
        .unwrap();
    let config = env.config();
    let sync_env = meerschaum_engine::sync::SyncEnv {
        registry: registry.as_ref(),
        defaults: &config.defaults,
        default_backtrack_minutes: config.default_backtrack_minutes,
        filter_params_index_limit: config.filter_params_index_limit,
        known: None,
    };
    let observer = CallbackObserver(std::sync::atomic::AtomicUsize::new(0));
    let result = meerschaum_engine::sync::sync_pipe(
        &sync_env,
        &pipe,
        None,
        &SyncArgs::default(),
        Some(&observer),
    )
    .await;
    assert!(result.success, "{}", result.message);
    assert_eq!(
        observer.0.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the callback must preserve the enclosing node's ledger observer"
    );
    assert_eq!(
        result.inserted, 1,
        "callback counts belong to the enclosing node"
    );
    let result = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(result.success, "{}", result.message);
    let data = env
        .conn
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.height(), 1);
    assert_eq!(
        data.column("value").unwrap().data.get(0),
        KValue::Float(3.5)
    );
    let fetch_pipe = meerschaum_core::Pipe::builder("plugin:wiring_fetch", "wiring_kwargs")
        .instance(INSTANCE)
        .parameters(json_map(&serde_json::json!({"columns":{"id":"id"}})))
        .build()
        .unwrap();
    let fetch_args = SyncArgs {
        extra: [("custom_value".into(), Value::Int(73))]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let fetched = engine.sync_pipe(&fetch_pipe, None, &fetch_args).await;
    assert!(fetched.success, "{}", fetched.message);
    let data = env
        .conn
        .get_pipe_data(&fetch_pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.column("value").unwrap().data.get(0), KValue::Int(73));
    pool.shutdown().await;
}

struct CallbackObserver(std::sync::atomic::AtomicUsize);
#[meerschaum_connector::async_trait]
impl meerschaum_engine::sync::DeltaObserver for CallbackObserver {
    async fn before_commit(
        &self,
        _: &meerschaum_core::Pipe,
        delta: &meerschaum_kernel::Frame,
    ) -> Result<(), String> {
        self.0
            .fetch_add(delta.height(), std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
