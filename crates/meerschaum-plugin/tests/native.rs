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

#[test]
fn registry_loads_lazily_and_exposes_routes_pages_hooks() {
    use meerschaum_plugin::NativePlugins;
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    let tree = plugins.join("native_demo");
    std::fs::create_dir_all(&tree).unwrap();
    std::fs::write(
        tree.join("plugin.toml"),
        include_str!("../examples/plugin.toml"),
    )
    .unwrap();
    std::fs::copy(
        artifact("native_demo"),
        tree.join(discovery::library_name("native_demo")),
    )
    .unwrap();
    let cache = root.path().join("cache/native.json");
    let natives = NativePlugins::open(std::slice::from_ref(&plugins), &cache).unwrap();
    assert_eq!(natives.names(), vec!["native_demo".to_string()]);
    assert!(natives.has_data_function("native_demo", "fetch"));
    assert!(natives.is_instance_type("native_demo"));
    assert!(!natives.is_instance_type("missing"));
    assert_eq!(
        natives.hooks(false),
        vec![("native_demo".to_string(), vec!["before".to_string()])]
    );
    assert!(natives.source("native_demo").unwrap().is_ok());
    assert!(natives.source("missing").is_none());
    let plugin = natives.plugin("native_demo").unwrap();
    let routes = plugin.call_json("routes", json!({})).unwrap();
    assert_eq!(routes[0]["path"], "/native-demo/ping");
    let page = plugin
        .call_json("page", json!({"username": "ada"}))
        .unwrap();
    assert!(page["html"].as_str().unwrap().contains("ada"));
    assert!(natives.plugin("missing").is_err());
    // A directory without native plugins is an empty set and writes no cache.
    let empty =
        NativePlugins::open(&[root.path().join("nothing")], &root.path().join("c.json")).unwrap();
    assert!(empty.is_empty() && empty.names().is_empty());
}

#[test]
fn broken_plugins_are_skipped_and_the_rest_load() {
    use meerschaum_plugin::NativePlugins;
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    // Sorts before the good plugin: a bad descriptor must not abort the scan.
    let bad_descriptor = plugins.join("a_bad_descriptor");
    std::fs::create_dir_all(&bad_descriptor).unwrap();
    std::fs::write(bad_descriptor.join("plugin.toml"), "name = [not toml").unwrap();
    let no_library = plugins.join("b_no_library");
    std::fs::create_dir_all(&no_library).unwrap();
    std::fs::write(
        no_library.join("plugin.toml"),
        "name = \"no_library\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    let corrupt = plugins.join("c_corrupt");
    std::fs::create_dir_all(&corrupt).unwrap();
    std::fs::write(
        corrupt.join("plugin.toml"),
        "name = \"corrupt\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    std::fs::write(
        corrupt.join(discovery::library_name("corrupt")),
        "not a library",
    )
    .unwrap();
    let good = plugins.join("native_demo");
    std::fs::create_dir_all(&good).unwrap();
    std::fs::write(
        good.join("plugin.toml"),
        include_str!("../examples/plugin.toml"),
    )
    .unwrap();
    std::fs::copy(
        artifact("native_demo"),
        good.join(discovery::library_name("native_demo")),
    )
    .unwrap();
    let dirs = vec![plugins];
    let report = discovery::discover_report(&dirs);
    assert_eq!(report.candidates.len(), 2);
    assert_eq!(report.skipped.len(), 1 + 1);
    assert!(report.skipped[0].directory.ends_with("a_bad_descriptor"));
    assert!(report.skipped[1].reason.contains("missing native library"));
    let cache = root.path().join("cache/manifest.json");
    // SAFETY: only the trusted SDK example is loadable; the corrupt library fails to load.
    let (manifest, skipped) = unsafe { discovery::cached_manifest_report(&dirs, &cache) }.unwrap();
    assert_eq!(skipped.len(), 3);
    assert!(skipped[2].directory.ends_with("c_corrupt"));
    assert!(manifest["plugins"].get("native_demo").is_some());
    assert!(manifest["plugins"].get("corrupt").is_none());
    let natives = NativePlugins::open(&dirs, &cache).unwrap();
    assert_eq!(natives.names(), vec!["native_demo".to_string()]);
}

#[tokio::test]
async fn fetch_kwargs_reach_the_plugin_without_begin_end_params() {
    // SAFETY: the integration test builds this SDK-owned plugin from the repository.
    let plugin = unsafe { LoadedPlugin::load(artifact("native_demo")) }.unwrap();
    let pipe = Pipe::builder("plugin:native_demo", "kwargs")
        .instance("native_demo:kwargs_memory")
        .build()
        .unwrap();
    let mut kwargs = meerschaum_core::Map::new();
    kwargs.insert("rows".into(), meerschaum_core::Value::Int(5));
    kwargs.insert("begin".into(), meerschaum_core::Value::Str("x".into()));
    kwargs.insert("params".into(), meerschaum_core::Value::Null);
    let source = plugin
        .source(json!({"synthetic": true}))
        .with_fetch_kwargs(&kwargs);
    let data = source
        .fetch(&pipe, &DataQuery::default())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.height(), 5);
    // Without kwargs the plugin keeps its default three rows.
    let plain = plugin.source(json!({"synthetic": true}));
    assert_eq!(
        plain
            .fetch(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .unwrap()
            .height(),
        3
    );
}
