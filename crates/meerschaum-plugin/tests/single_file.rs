//! Minimal Rust fetch plugins: generated adapter, discovery, and the real native ABI.
use meerschaum_plugin::{discovery, sdk::FetchOutput, serde_json::json};
mod support {
    pub mod compile_single_file;
}
use support::compile_single_file::compile;

#[test]
fn rows_handle_missing_cells_empty_data_and_errors() {
    let frame = vec![json!({"id": 1}), json!({"id": 2, "label": "two"})]
        .into_frame()
        .unwrap()
        .unwrap();
    assert_eq!(frame.height(), 2);
    assert_eq!(frame.width(), 2);
    assert_eq!(frame.row(0)[1], meerschaum_kernel::Value::Null);
    assert!(
        Vec::<serde_json::Value>::new()
            .into_frame()
            .unwrap()
            .is_none()
    );
    assert!(vec![json!(42)].into_frame().is_err());
    let failed: meerschaum_plugin::Result<Vec<serde_json::Value>> = Err(
        meerschaum_plugin::Error::Message("source unavailable".into()),
    );
    assert_eq!(
        failed.into_frame().unwrap_err().to_string(),
        "source unavailable"
    );
}

#[test]
fn single_file_is_discovered_compiled_loaded_and_invalidated() {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    let builds = root.path().join("cache/native_build");
    std::fs::create_dir(&plugins).unwrap();
    let source = plugins.join("minimal.rs");
    std::fs::write(
        &source,
        r#"
fn fetch(pipe: &Pipe) -> Vec<Value> {
    vec![json!({"id": 1, "metric": pipe.metric_key()})]
}
"#,
    )
    .unwrap();
    let found = discovery::discover_report_in(std::slice::from_ref(&plugins), &builds);
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    let candidate = &found.candidates[0];
    assert_eq!(candidate.name, "minimal");
    assert_eq!(candidate.directory, source);
    let spec = candidate.build.as_ref().unwrap();
    assert_eq!(std::fs::read_dir(&plugins).unwrap().count(), 1);

    compile(candidate);
    // SAFETY: the fixture above is trusted code built with this SDK.
    let library = unsafe { meerschaum_plugin::LoadedPlugin::load(&candidate.library) }.unwrap();
    assert_eq!(
        library.manifest["plugins"]["minimal"]["data_functions"],
        json!(["fetch"])
    );
    let connector = std::sync::Arc::new(library).source(json!({}));
    let reply = connector
        .call(
            "fetch",
            json!({"pipe": {"keys": {"connector_keys": "plugin:minimal", "metric_key": "readings"}}}),
            None,
        )
        .unwrap();
    let frame = reply.data.unwrap();
    assert_eq!(frame.height(), 1);
    assert_eq!(frame.row(0)[0], meerschaum_kernel::Value::Int(1));
    assert_eq!(
        frame.row(0)[1],
        meerschaum_kernel::Value::Str("readings".into())
    );
    std::fs::write(
        spec.target_dir.join("mrsm-source-fingerprint"),
        &candidate.fingerprint,
    )
    .unwrap();
    let cached = discovery::discover_report_in(std::slice::from_ref(&plugins), &builds);
    assert!(cached.candidates[0].build.is_none());
    let registry = meerschaum_plugin::NativePlugins::open(
        std::slice::from_ref(&plugins),
        &root.path().join("cache/native_plugins_manifest.json"),
    )
    .unwrap();
    assert!(registry.contains("minimal"));
    assert_eq!(registry.plugin_entry("minimal")["path"], json!(source));
    assert!(registry.source("minimal").unwrap().is_ok());
    std::fs::write(&source, "fn fetch() -> Vec<Value> { vec![] }").unwrap();
    let edited = discovery::discover_report_in(&[plugins], &builds);
    assert!(edited.candidates[0].build.is_some());
    assert_ne!(candidate.fingerprint, edited.candidates[0].fingerprint);
}

#[test]
fn precedence_invalid_names_and_failed_builds() {
    let root = tempfile::tempdir().unwrap();
    let first = root.path().join("first");
    let second = root.path().join("second");
    for directory in [&first, &second] {
        std::fs::create_dir(directory).unwrap();
        std::fs::write(
            directory.join("minimal.rs"),
            "fn fetch() -> Vec<Value> { vec![] }",
        )
        .unwrap();
    }
    std::fs::write(first.join("invalid name.rs"), "").unwrap();
    let builds = root.path().join("builds");
    let found = discovery::discover_report_in(&[first.clone(), second], &builds);
    assert_eq!(found.candidates.len(), 1);
    assert_eq!(found.candidates[0].directory, first.join("minimal.rs"));
    assert_eq!(found.skipped.len(), 1);
    assert!(found.skipped[0].reason.contains("filename"));
    std::fs::write(
        builds.join("minimal/mrsm-build-failed"),
        &found.candidates[0].fingerprint,
    )
    .unwrap();
    let failed = discovery::discover_report_in(std::slice::from_ref(&first), &builds);
    assert!(failed.candidates.is_empty());
    assert!(
        failed
            .skipped
            .iter()
            .any(|s| s.reason.contains("last build failed"))
    );
    std::fs::write(
        first.join("minimal.rs"),
        "fn fetch() -> Vec<Value> { vec![json!({})] }",
    )
    .unwrap();
    let retry = discovery::discover_report_in(&[first], &builds);
    assert!(retry.candidates[0].build.is_some());
}

fn compiled(
    source: &str,
) -> (
    tempfile::TempDir,
    std::sync::Arc<meerschaum_plugin::LoadedPlugin>,
) {
    compiled_entry(source, false)
}

fn compiled_entry(
    source: &str,
    legacy: bool,
) -> (
    tempfile::TempDir,
    std::sync::Arc<meerschaum_plugin::LoadedPlugin>,
) {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("minimal.rs"), source).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    if legacy {
        let wrapper = found.candidates[0]
            .build
            .as_ref()
            .unwrap()
            .manifest_path
            .parent()
            .unwrap()
            .join("lib.rs");
        let original = std::fs::read_to_string(&wrapper).unwrap();
        let body = &original
            [original.find("mod user {").unwrap()..original.find("#[unsafe(no_mangle)]").unwrap()];
        let code = format!(
            "{body}\n{}",
            r#"#[unsafe(no_mangle)] pub extern "C" fn mrsm_plugin_v1() -> *const meerschaum_plugin::abi::MrsmPluginV1 {
            &meerschaum_plugin::sdk::Export::<SingleFilePlugin>::VTABLE
        }"#
        );
        std::fs::write(wrapper, code).unwrap();
    }
    compile(&found.candidates[0]);
    let plugin =
        unsafe { meerschaum_plugin::LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    (root, plugin)
}

fn request(method: &str) -> meerschaum_plugin::Request {
    meerschaum_plugin::Request {
        method: method.into(),
        connector_type: "plugin".into(),
        label: "minimal".into(),
        config: json!({}),
        context: json!({"pipe": {"keys": {"connector_keys": "plugin:minimal", "metric_key": "reading", "instance_keys": "sql:local"}, "parameters": {}}, "query": {"begin": 2, "end": "2026-01-01T00:00:00Z"}}),
    }
}

#[test]
fn optional_arguments_are_compiled_and_bound_by_name_for_fetch_and_sync() {
    for signature in [
        "",
        "pipe: &Pipe",
        "begin: Option<Bound>",
        "end: Option<Bound>",
        "pipe: &Pipe, begin: Option<Bound>",
        "pipe: &Pipe, end: Option<Bound>",
        "pipe: &Pipe, begin: Option<Bound>, end: Option<Bound>",
        "end: Option<Bound>, begin: Option<Bound>, pipe: &Pipe",
    ] {
        let mut checks = String::new();
        if signature.contains("pipe:") {
            checks.push_str("assert_eq!(pipe.metric_key(), \"reading\");");
        }
        if signature.contains("begin:") {
            checks.push_str("assert_eq!(begin, Some(Bound::Int(2)));");
        }
        if signature.contains("end:") {
            checks.push_str(
                "assert_eq!(end, Some(Bound::Datetime(\"2026-01-01T00:00:00Z\".into())));",
            );
        }
        let (_root, plugin) = compiled(&format!(
            r#"
            fn fetch({signature}) -> Result<Vec<Value>> {{ {checks} Ok(vec![json!({{"id": 1}})]) }}
            fn sync({signature}) -> SuccessTuple {{ {checks} SuccessTuple::success() }}
        "#
        ));
        assert_eq!(
            plugin
                .invoke(&request("fetch"), None)
                .unwrap()
                .data
                .unwrap()
                .height(),
            1
        );
        let mut req = request("sync");
        // Sync uses flat bounds, unlike fetch's query envelope.
        req.context["begin"] = req.context["query"]["begin"].clone();
        req.context["end"] = req.context["query"]["end"].clone();
        req.context.as_object_mut().unwrap().remove("query");
        assert_eq!(
            plugin.invoke(&req, None).unwrap().value,
            json!([true, "Success"])
        );
    }
}

#[test]
fn sync_only_plugin_accepts_data_and_tuple_results() {
    let (_root, plugin) = compiled(
        r#"
        fn sync(data: Option<Frame>, _pipe: &Pipe) -> Result<(bool, String)> {
            let frame = data.unwrap();
            assert_eq!(frame.rows()[0], json!({"id": 1, "flag": true, "value": 1.25, "nested": {"x": 2}}));
            Ok((true, format!("rows={}", frame.height())))
        }
    "#,
    );
    assert_eq!(
        plugin.manifest["plugins"]["minimal"]["data_functions"],
        json!(["sync"])
    );
    let frame = vec![json!({"id": 1, "flag": true, "value": 1.25, "nested": {"x": 2}})]
        .into_frame()
        .unwrap()
        .unwrap();
    assert_eq!(
        plugin.invoke(&request("sync"), Some(&frame)).unwrap().value,
        json!([true, "rows=1"])
    );
    assert_eq!(
        plugin
            .invoke(&request("sync_pipe"), Some(&frame))
            .unwrap()
            .value,
        json!([true, "rows=1"])
    );
}

#[test]
fn host_operations_cross_the_abi_and_missing_hosts_and_panics_are_errors() {
    use meerschaum_plugin::{Frame, HostServices, Request, Result};
    struct Host {
        panic: bool,
    }
    impl HostServices for Host {
        fn call(
            &self,
            request: &Request,
            method: &str,
            data: Option<Frame>,
        ) -> Result<serde_json::Value> {
            assert_eq!(request.label, "minimal");
            if self.panic {
                panic!("host panic fixture");
            }
            match method {
                "get_sync_time" => Ok(json!(7)),
                "sync" => {
                    assert_eq!(data.unwrap().row(0)[0], meerschaum_kernel::Value::Int(8));
                    Ok(json!([true, "written"]))
                }
                _ => unreachable!(),
            }
        }
    }
    let (_root, plugin) = compiled(
        r#"
        fn sync(pipe: &Pipe) -> Result<SuccessTuple> {
            let Some(Bound::Int(last)) = pipe.get_sync_time()? else { panic!("missing sync time") };
            pipe.sync(vec![json!({"id": last + 1})])
        }
    "#,
    );
    let req = request("sync");
    assert!(
        plugin
            .invoke(&req, None)
            .err()
            .unwrap()
            .to_string()
            .contains("unavailable")
    );
    assert_eq!(
        plugin
            .invoke_with_host(&req, None, Some(&Host { panic: false }))
            .unwrap()
            .value,
        json!([true, "written"])
    );
    assert!(
        plugin
            .invoke_with_host(&req, None, Some(&Host { panic: true }))
            .is_err()
    );
    // TLS restoration and the serialized call lock survive the host's panic.
    assert_eq!(
        plugin
            .invoke_with_host(&req, None, Some(&Host { panic: false }))
            .unwrap()
            .value,
        json!([true, "written"])
    );
    assert!(plugin.invoke(&req, None).is_err());
}

#[test]
fn reentrant_host_calls_fail_instead_of_waiting_on_the_plugin_lock() {
    struct Recursive(std::sync::Arc<meerschaum_plugin::LoadedPlugin>);
    impl meerschaum_plugin::HostServices for Recursive {
        fn call(
            &self,
            request: &meerschaum_plugin::Request,
            _: &str,
            _: Option<meerschaum_plugin::Frame>,
        ) -> meerschaum_plugin::Result<serde_json::Value> {
            self.0.invoke(request, None).map(|r| r.value)
        }
    }
    let (_root, plugin) = compiled(
        "fn fetch(pipe: &Pipe) -> Result<Vec<Value>> { pipe.get_sync_time()?; Ok(vec![]) }",
    );
    let error = plugin
        .invoke_with_host(&request("fetch"), None, Some(&Recursive(plugin.clone())))
        .err()
        .unwrap();
    assert!(error.to_string().contains("recursive call"), "{error}");
}

#[test]
fn unsupported_signatures_have_actionable_errors() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("minimal.rs");
    for source in [
        "fn fetch(request: &Request) {}",
        "fn fetch(other: usize) {}",
        "async fn fetch() {}",
        "fn fetch<T>() {}",
        "fn fetch(_: &Pipe) {}",
        "#[cfg(unix)] fn fetch() {}",
        "fn helper() {}",
    ] {
        std::fs::write(&file, source).unwrap();
        assert!(
            meerschaum_plugin::source::prepare_single_file(
                &file,
                "minimal",
                &root.path().join("generated")
            )
            .is_err(),
            "{source}"
        );
    }
}

#[test]
fn libraries_without_the_optional_host_extension_still_load_and_run() {
    struct UnusedHost;
    impl meerschaum_plugin::HostServices for UnusedHost {
        fn call(
            &self,
            _: &meerschaum_plugin::Request,
            _: &str,
            _: Option<meerschaum_plugin::Frame>,
        ) -> meerschaum_plugin::Result<serde_json::Value> {
            panic!("old ABI cannot call the host");
        }
    }
    let (_root, plugin) = compiled_entry(
        "fn fetch() -> Vec<Value> { vec![json!({\"id\": 1})] }",
        true,
    );
    assert_eq!(
        plugin
            .invoke_with_host(&request("fetch"), None, Some(&UnusedHost))
            .unwrap()
            .data
            .unwrap()
            .height(),
        1
    );
}

#[test]
fn cargo_builds_are_lightweight_shared_and_keep_failure_diagnostics() {
    use meerschaum_plugin::source;
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    let builds = root.path().join("builds");
    std::fs::create_dir(&plugins).unwrap();
    let file = plugins.join("first.rs");
    std::fs::write(
        &file,
        "fn fetch() -> Vec<Value> { vec![json!({\"id\": 1})]; }",
    )
    .unwrap();
    let discover = || discovery::discover_report_in(std::slice::from_ref(&plugins), &builds);
    std::fs::write(
        &file,
        "fn fetch() -> Vec<Value> { vec![json!({\"id\": 1})] }",
    )
    .unwrap();
    let cold = discover();
    let candidate = &cold.candidates[0];
    let start = std::time::Instant::now();
    source::build(
        &candidate.name,
        candidate.build.as_ref().unwrap(),
        &candidate.library,
    )
    .unwrap();
    eprintln!("Cold valid plugin build: {:?}", start.elapsed());
    std::fs::write(
        &file,
        "fn fetch() -> Vec<Value> { vec![json!({\"id\": 1})]; }",
    )
    .unwrap();
    let found = discover();
    let first = &found.candidates[0];
    let start = std::time::Instant::now();
    let reason =
        source::build(&first.name, first.build.as_ref().unwrap(), &first.library).unwrap_err();
    eprintln!("Invalid edit diagnostic: {:?}", start.elapsed());
    assert!(reason.contains("remove this semicolon"), "{reason}");
    assert!(
        reason.contains(&format!("{}:1:", file.display())),
        "compiler should point to the user's source: {reason}"
    );
    let failed = discover();
    assert!(failed.skipped[0].reason.contains("remove this semicolon"));
    std::fs::write(
        &file,
        "fn fetch() -> Vec<Value> { /* corrected */ vec![json!({\"id\": 1})] }",
    )
    .unwrap();
    let found = discover();
    let first = &found.candidates[0];
    let start = std::time::Instant::now();
    source::build(&first.name, first.build.as_ref().unwrap(), &first.library).unwrap();
    eprintln!(
        "Fixed plugin build: {:?}; library {} bytes",
        start.elapsed(),
        first.library.metadata().unwrap().len()
    );
    let lock = std::fs::read_to_string(builds.join("first/source/Cargo.lock")).unwrap();
    for heavy in ["meerschaum", "polars", "arrow", "tokio"] {
        assert!(!lock.contains(heavy), "unexpected dependency {heavy}");
    }
    assert!(!builds.join("first/mrsm-build-error.txt").exists());
    assert!(discover().candidates[0].build.is_none());
    let shared = first
        .build
        .as_ref()
        .unwrap()
        .shared_target
        .as_ref()
        .unwrap();
    let dependency = std::fs::read_dir(shared.join("release/deps"))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("libserde_json-")
                && p.extension().is_some_and(|e| e == "rlib")
        })
        .unwrap();
    let modified = dependency.metadata().unwrap().modified().unwrap();
    std::fs::write(
        plugins.join("second.rs"),
        "fn fetch() -> Vec<Value> { vec![json!({\"id\": 2})] }",
    )
    .unwrap();
    let found = discover();
    let second = found
        .candidates
        .iter()
        .find(|c| c.name == "second")
        .unwrap();
    assert_eq!(
        second.build.as_ref().unwrap().shared_target.as_ref(),
        Some(shared)
    );
    let start = std::time::Instant::now();
    source::build(
        &second.name,
        second.build.as_ref().unwrap(),
        &second.library,
    )
    .unwrap();
    eprintln!("Second plugin build: {:?}", start.elapsed());
    assert_eq!(dependency.metadata().unwrap().modified().unwrap(), modified);
    for (candidate, id) in [(first, 1), (second, 2)] {
        let plugin = unsafe { meerschaum_plugin::LoadedPlugin::load(&candidate.library) }.unwrap();
        assert_eq!(
            plugin
                .invoke(&request("fetch"), None)
                .unwrap()
                .data
                .unwrap()
                .row(0)[0],
            meerschaum_kernel::Value::Int(id)
        );
    }
}

#[test]
fn lightweight_bounds_serialize_and_plugin_panics_are_contained() {
    let (_root, plugin) = compiled(
        r#"
        fn fetch(begin: Option<Bound>, end: Option<Bound>) -> Vec<Value> {
            vec![json!({"begin": begin, "end": end})]
        }
        fn sync() -> SuccessTuple { panic!("plugin panic fixture") }
    "#,
    );
    let frame = plugin
        .invoke(&request("fetch"), None)
        .unwrap()
        .data
        .unwrap();
    assert_eq!(frame.row(0)[0], meerschaum_kernel::Value::Int(2));
    assert_eq!(
        frame.row(0)[1],
        meerschaum_kernel::Value::Str("2026-01-01T00:00:00Z".into())
    );
    assert!(
        plugin
            .invoke(&request("sync"), None)
            .err()
            .unwrap()
            .to_string()
            .contains("native plugin panicked")
    );
    assert!(plugin.invoke(&request("fetch"), None).is_ok());
}

#[test]
fn actions_are_discovered_and_receive_arguments_without_a_fetch_function() {
    let (_root, plugin) = compiled(
        r#"
        /// Greet the supplied names.
        #[make_action(name = "greet", daemon = false)]
        fn greeting(kwargs: &Value, action: Vec<String>) -> Result<(bool, String)> {
            assert_eq!(kwargs["debug"], true);
            Ok((true, format!("Hello, {}!", action.join(", "))))
        }
        #[make_action]
        fn quiet(kwargs: &Value) { assert!(kwargs.as_object().unwrap().is_empty()); }
        #[make_action]
        fn fail() -> SuccessTuple { SuccessTuple::fail("expected failure") }
        #[make_action]
        fn panic_action() { panic!("contained action panic") }
        fn helper() -> usize { 1 }
    "#,
    );
    assert_eq!(
        plugin.manifest["plugins"]["minimal"]["data_functions"],
        json!([])
    );
    let actions = &plugin.manifest["plugins"]["minimal"]["actions"];
    assert_eq!(actions[0]["name"], "greet");
    assert_eq!(actions[0]["daemon"], false);
    assert_eq!(actions[0]["doc"], "Greet the supplied names.");
    assert_eq!(actions.as_array().unwrap().len(), 4);
    assert_eq!(
        plugin
            .action("greet", json!({"action": ["Alice", "Bob"], "debug": true}))
            .unwrap()
            .message,
        "Hello, Alice, Bob!"
    );
    assert!(plugin.action("quiet", json!({})).unwrap().success);
    assert!(!plugin.action("fail", json!({})).unwrap().success);
    assert!(plugin.action("panic_action", json!({})).is_err());
    assert!(plugin.action("quiet", json!({})).unwrap().success);
    assert!(matches!(
        plugin.invoke(&request("unknown"), None),
        Err(meerschaum_plugin::Error::Unsupported(_))
    ));
}

#[test]
fn connector_modules_dispatch_private_methods_and_keep_helpers_private() {
    let (_root, plugin) = compiled(
        r#"
        fn fetch() -> Vec<Value> { vec![json!({"id": 9})] }
        fn register() -> Option<Value> { None }
        #[make_action]
        fn hello() -> (bool, &'static str) { (true, "hello") }
        /// Synthetic station readings.
        #[make_connector(name = "readings")]
        mod station {
            fn helper() -> i64 { 3 }
            fn register(connector: &Connector) -> Result<Value> {
                assert_eq!(connector.label(), "office");
                assert_eq!(connector.connector_type(), "readings");
                Ok(json!({"columns": {"datetime": "id"}}))
            }
            fn fetch(connector: &Connector, pipe: &Pipe, begin: Option<Bound>, kwargs: &Value) -> Vec<Value> {
                assert_eq!(pipe.metric_key(), "reading");
                assert_eq!(begin, Some(Bound::Int(2)));
                assert_eq!(kwargs["rows"], 1);
                vec![json!({"id": helper(), "station": connector.config()["station"]})]
            }
            fn sync(data: Option<Frame>) -> (bool, String) {
                (true, format!("rows={}", data.unwrap().height()))
            }
        }
        #[make_connector]
        mod second {
            fn fetch() -> Vec<Value> { vec![json!({"id": 4})] }
        }
    "#,
    );
    let descriptor = &plugin.manifest["connector_types"]["readings"];
    assert_eq!(descriptor["plugin"], "minimal");
    assert_eq!(descriptor["is_instance"], false);
    assert_eq!(descriptor["doc"], "Synthetic station readings.");
    assert_eq!(descriptor["methods"], json!(["register", "fetch", "sync"]));
    let mut req = request("register");
    req.connector_type = "readings".into();
    req.label = "office".into();
    req.config = json!({"station": "north"});
    req.context["kwargs"] = json!({"rows": 1});
    assert_eq!(
        plugin.invoke(&req, None).unwrap().value,
        json!({"columns": {"datetime": "id"}})
    );
    req.method = "fetch".into();
    let frame = plugin.invoke(&req, None).unwrap().data.unwrap();
    assert_eq!(frame.row(0)[0], meerschaum_kernel::Value::Int(3));
    assert_eq!(
        frame.row(0)[1],
        meerschaum_kernel::Value::Str("north".into())
    );
    req.method = "sync".into();
    assert_eq!(
        plugin.invoke(&req, Some(&frame)).unwrap().value,
        json!([true, "rows=1"])
    );
    req.method = "helper".into();
    assert!(matches!(
        plugin.invoke(&req, None),
        Err(meerschaum_plugin::Error::Unsupported(_))
    ));
    req.connector_type = "second".into();
    req.method = "fetch".into();
    assert_eq!(
        plugin.invoke(&req, None).unwrap().data.unwrap().row(0)[0],
        meerschaum_kernel::Value::Int(4)
    );
    assert_eq!(
        plugin.invoke(&request("register"), None).unwrap().value,
        json!(null)
    );
    assert_eq!(
        plugin
            .invoke(&request("fetch"), None)
            .unwrap()
            .data
            .unwrap()
            .row(0)[0],
        meerschaum_kernel::Value::Int(9)
    );
}

#[test]
fn shorthand_errors_report_original_file_and_line() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("minimal.rs");
    for (source, message) in [
        (
            "#[make_action]\nfn hello(action: &Value) {}",
            "must have type Vec<String>",
        ),
        ("#[make_action]\nasync fn hello() {}", "synchronous"),
        ("#[make_action]\nfn hello(pipe: &Pipe) {}", "actions accept"),
        (
            "#[make_action]\n#[make_action]\nfn hello() {}",
            "only one plugin annotation",
        ),
        (
            "#[make_action(name=\"x\")] fn one() {}\n#[make_action(name=\"x\")] fn two() {}",
            "duplicate action",
        ),
        ("#[make_connector] mod sql { fn fetch() {} }", "reserved"),
        (
            "#[make_connector(instance)] mod db { fn fetch() {} }",
            "packaged plugin",
        ),
        ("#[make_connector] mod data;", "inline module"),
        (
            "#[make_connector] mod data { fn helper() {} }",
            "fetch and/or sync",
        ),
        (
            "#[make_connector] mod data { #[make_action] fn hello() {} }",
            "top-level",
        ),
        ("#[make_connector] struct Data;", "inline module"),
        (
            "#[make_connector(name=\"x\")] mod one {fn fetch(){}}\n#[make_connector(name=\"x\")] mod two {fn fetch(){}}",
            "duplicate connector",
        ),
        ("#[make_action]\nfn fetch() {}", "reserved plugin functions"),
    ] {
        std::fs::write(&file, source).unwrap();
        let error = meerschaum_plugin::source::prepare_single_file(
            &file,
            "minimal",
            &root.path().join("generated"),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains(message), "expected {message:?}: {error}");
        assert!(error.contains(&file.display().to_string()), "{error}");
    }
    std::fs::write(
        &file,
        "// an introductory comment\n#[make_action]\nfn hello(kwargs: usize) {}\n",
    )
    .unwrap();
    let error = meerschaum_plugin::source::prepare_single_file(
        &file,
        "minimal",
        &root.path().join("generated"),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("minimal.rs:3:"), "{error}");
}

#[test]
fn cargo_dependency_blocks_build_relative_crates_with_features_and_are_cached() {
    use meerschaum_plugin::source;
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    let helper = plugins.join("helper");
    std::fs::create_dir_all(helper.join("src")).unwrap();
    std::fs::write(helper.join("Cargo.toml"), "[package]\nname = 'helper'\nversion = '0.1.0'\nedition = '2024'\n[features]\ngreeting = []\n").unwrap();
    std::fs::write(
        helper.join("src/lib.rs"),
        "#[cfg(feature = \"greeting\")] pub fn greeting() -> &'static str { \"dependency works\" }",
    )
    .unwrap();
    let file = plugins.join("minimal.rs");
    let content = r#"//! ```cargo
//! [dependencies]
//! greeting_lib = { package = "helper", path = "helper", features = ["greeting"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! ```
#[derive(serde::Serialize)]
struct Greeting { message: &'static str }
#[make_action]
fn hello() -> (bool, String) {
    let data = serde_json::to_value(Greeting { message: greeting_lib::greeting() }).unwrap();
    (true, data["message"].as_str().unwrap().to_owned())
}
"#;
    std::fs::write(&file, content).unwrap();
    let builds = root.path().join("build");
    let found = discovery::discover_report_in(std::slice::from_ref(&plugins), &builds);
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    let candidate = &found.candidates[0];
    source::build(
        &candidate.name,
        candidate.build.as_ref().unwrap(),
        &candidate.library,
    )
    .unwrap();
    let library = unsafe { meerschaum_plugin::LoadedPlugin::load(&candidate.library) }.unwrap();
    assert_eq!(
        library.action("hello", json!({})).unwrap().message,
        "dependency works"
    );
    let cached = discovery::discover_report_in(std::slice::from_ref(&plugins), &builds);
    assert!(cached.candidates[0].build.is_none());
    assert!(builds.join("minimal/source/Cargo.lock").is_file());
    assert!(!plugins.join("Cargo.lock").exists());
    assert!(!plugins.join("Cargo.toml").exists());
    std::fs::write(
        &file,
        content.replace(
            "features = [\"greeting\"]",
            "features = [\"greeting\"], default-features = false",
        ),
    )
    .unwrap();
    let changed = discovery::discover_report_in(&[plugins], &builds);
    assert!(changed.candidates[0].build.is_some());
    assert_ne!(
        changed.candidates[0].fingerprint,
        cached.candidates[0].fingerprint
    );
}
