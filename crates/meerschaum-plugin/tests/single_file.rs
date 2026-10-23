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
        let code = std::fs::read_to_string(&wrapper).unwrap().replace(
            "meerschaum_plugin::export_plugin!(SingleFilePlugin);",
            r#"#[unsafe(no_mangle)] pub extern "C" fn mrsm_plugin_v1() -> *const meerschaum_plugin::abi::MrsmPluginV1 {
                &meerschaum_plugin::sdk::Export::<SingleFilePlugin>::VTABLE
            }"#,
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
            Ok((true, format!("rows={}", data.unwrap().height())))
        }
    "#,
    );
    assert_eq!(
        plugin.manifest["plugins"]["minimal"]["data_functions"],
        json!(["sync"])
    );
    let frame = vec![json!({"id": 1})].into_frame().unwrap().unwrap();
    assert_eq!(
        plugin.invoke(&request("sync"), Some(&frame)).unwrap().value,
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
