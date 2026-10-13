use meerschaum_connector::{Connector, DataQuery, InstanceConnector, PipesKeysQuery, SyncOptions};
use meerschaum_core::Pipe;
use meerschaum_plugin::{LoadedPlugin, discovery, host, serde_json::json};
use std::{path::PathBuf, process::Command, sync::OnceLock};

fn artifact(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let root = tempfile::tempdir().unwrap();
        let plugins = tempfile::tempdir().unwrap();
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "meerschaum-plugin", "--examples"])
            .env("MRSM_ROOT_DIR", root.path())
            .env("MRSM_PLUGINS_DIR", plugins.path())
            .status()
            .unwrap();
        assert!(status.success());
    });
    // Derive the shared target/profile from this test executable, including custom profiles.
    let executable = std::env::current_exe().unwrap();
    let profile = executable.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

#[tokio::test]
async fn cdylib_fetch_sync_instance_actions_hooks_and_panic() {
    // SAFETY: the integration test builds this SDK-owned plugin from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_demo")) }.unwrap();
    let connector = plugin
        .connector(
            "native_demo",
            "plugin_abi_memory",
            json!({"synthetic":true}),
        )
        .unwrap();
    assert!(connector.is_instance());
    assert!(connector.test_connection().await.unwrap());
    let pipe = Pipe::builder("plugin:native_demo", "plugin_abi")
        .instance("native_demo:plugin_abi_memory")
        .build()
        .unwrap();
    assert!(connector.register_pipe(&pipe).await.success);
    assert!(matches!(
        connector
            .acquire_pipe_lease(&pipe, "plugin_abi", std::time::Duration::from_secs(10))
            .await,
        Err(meerschaum_connector::ConnectorError::NotImplemented(_))
    ));
    let source = plugin.source(json!({"synthetic": true}));
    assert_eq!(source.connector_keys(), "plugin:native_demo");
    assert!(!source.is_instance());
    let data = source
        .fetch(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    drop(source);
    assert_eq!(data.height(), 3);
    let expected = data.to_record_batch().unwrap();
    assert!(
        connector
            .sync_pipe(&pipe, data, &SyncOptions::default())
            .await
            .success
    );
    let stored = connector
        .get_pipe_data(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.to_record_batch().unwrap(), expected);
    assert_eq!(
        connector
            .get_pipe_rowcount(&pipe, &DataQuery::default())
            .await
            .unwrap(),
        Some(3)
    );
    assert_eq!(
        connector
            .fetch_pipes_keys(&PipesKeysQuery::default())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        connector
            .get_sync_time(&pipe, None, false)
            .await
            .unwrap()
            .unwrap()
            .py_str(),
        "3"
    );
    assert_eq!(
        plugin.action("hello", json!({})).unwrap().message,
        "Hello from native_demo"
    );
    assert!(
        plugin
            .action("panic", json!({}))
            .unwrap_err()
            .to_string()
            .contains("panicked")
    );
    assert!(plugin.action("hello", json!({})).unwrap().success);
    let kwargs = json!({"success_tuple":[true,"Success"],"sync_timestamp":"2024-01-01T00:00:00Z","sync_complete_timestamp":"2024-01-01T00:00:01Z","sync_duration":1.0});
    for post in [false, true] {
        let hook = plugin
            .hook(post, if post { "after" } else { "before" }, kwargs.clone())
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&hook.message).unwrap(),
            kwargs
        );
    }
    let context = json!({"query":{"begin":5,"end":null,"params":{"id":["_1",null]}}});
    assert_eq!(
        connector.call("echo", context.clone(), None).unwrap().value,
        context
    );
    // Drop the library owners before inspecting the returned, materialized frame.
    drop(connector);
    drop(plugin);
    assert_eq!(stored.to_record_batch().unwrap(), expected);
}

#[test]
fn incompatible_and_truncated_abi_rejected() {
    // SAFETY: test fixture exports the documented permanent header only.
    let err = unsafe { LoadedPlugin::load(artifact("bad_abi")) }
        .err()
        .unwrap();
    assert!(err.to_string().contains("ABI 99"));
    assert!(host::validate_header(1, 8).is_err());
}

#[test]
fn manifest_roundtrip_matches_worker_schema() {
    // SAFETY: trusted SDK integration fixture.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_demo")) }.unwrap();
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../meerschaum-worker/schema/manifest.schema.json"
    ))
    .unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let roundtrip: serde_json::Value =
        serde_json::from_slice(&serde_json::to_vec(&plugin.manifest).unwrap()).unwrap();
    validator.validate(&roundtrip).unwrap();
    assert_eq!(roundtrip, plugin.manifest);
    assert!(host::validate_manifest(&json!({"schema_version":99})).is_err());
    assert_eq!(
        schema,
        serde_json::from_str::<serde_json::Value>(include_str!("../schema/manifest.schema.json"))
            .unwrap()
    );
}

#[test]
fn discovery_precedence_cache_and_invalidation() {
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("first");
    let second = root.path().join("second");
    for dir in [&first, &second] {
        let plugin = dir.join("native_demo");
        std::fs::create_dir_all(&plugin).unwrap();
        std::fs::write(
            plugin.join("plugin.toml"),
            include_str!("../examples/plugin.toml"),
        )
        .unwrap();
        std::fs::copy(
            artifact("native_demo"),
            plugin.join(discovery::library_name("native_demo")),
        )
        .unwrap();
        std::fs::write(
            plugin.join("__init__.py"),
            "# Native entry takes precedence\n",
        )
        .unwrap();
    }
    let dirs = vec![first.clone(), second];
    let candidates = discovery::discover(&dirs).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].directory, first.join("native_demo"));
    let cache = root.path().join("cache/manifest.json");
    // SAFETY: discovery roots contain only copies of the trusted SDK example.
    let a = unsafe { discovery::cached_manifest(&dirs, &cache) }.unwrap();
    let modified = std::fs::metadata(&cache).unwrap().modified().unwrap();
    let b = unsafe { discovery::cached_manifest(&dirs, &cache) }.unwrap();
    assert_eq!(a, b);
    assert_eq!(
        modified,
        std::fs::metadata(&cache).unwrap().modified().unwrap()
    );
    let tree = first.join("native_demo");
    std::fs::create_dir(tree.join("__pycache__")).unwrap();
    std::fs::write(tree.join("__pycache__/demo.pyc"), "ignored").unwrap();
    assert_eq!(
        candidates[0].fingerprint,
        discovery::fingerprint(&tree).unwrap()
    );
    std::fs::write(
        tree.join("plugin.toml"),
        format!("{}\n# edited\n", include_str!("../examples/plugin.toml")),
    )
    .unwrap();
    let c = unsafe { discovery::cached_manifest(&dirs, &cache) }.unwrap();
    assert_ne!(a["fingerprint"], c["fingerprint"]);
    std::fs::write(&cache, "broken JSON").unwrap();
    assert_eq!(
        c,
        unsafe { discovery::cached_manifest(&dirs, &cache) }.unwrap()
    );
}
