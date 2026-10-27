//! Host calls across both plugin ABIs against an in-memory host: the Arrow ABI (the
//! `native_host` example: frames both ways through the call's stream) and the rows ABI (a
//! single-file plugin), hosts older than the new calls, and plugins whose name, version and
//! manifest come from their Cargo package. All data is synthetic.
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_plugin::{
    Bound, Frame, FrameExt, HostCall, HostReply, HostServices, LoadedPlugin, Request, Result,
    api::RouteRequest, discovery,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Command,
    sync::{Mutex, OnceLock},
};
mod support {
    pub mod compile_single_file;
}

/// The examples' libraries, built once.
fn example(name: &str) -> PathBuf {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        let status = Command::new(env!("CARGO"))
            .args(["build", "-p", "meerschaum-plugin", "--examples"])
            .status()
            .unwrap();
        assert!(status.success());
    });
    let profile = std::env::current_exe().unwrap();
    let profile = profile.parent().unwrap().parent().unwrap();
    profile.join("examples").join(format!(
        "{}{name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

/// Pipes kept as JSON rows by metric, and a plugin config: what a host's instance would hold.
#[derive(Default)]
struct Memory {
    pipes: Mutex<BTreeMap<String, Vec<Value>>>,
    config: Mutex<Value>,
    calls: Mutex<Vec<String>>,
    /// The arguments of each `sync`.
    syncs: Mutex<Vec<Value>>,
}

fn in_window(row: &Value, begin: &Option<Bound>, end: &Option<Bound>) -> bool {
    let at = Bound::from(row["ts"].as_str().unwrap_or_default())
        .to_datetime()
        .unwrap();
    begin
        .as_ref()
        .is_none_or(|b| b.to_datetime().unwrap() <= at)
        && end.as_ref().is_none_or(|e| at < e.to_datetime().unwrap())
}

impl Memory {
    fn rows(&self, metric: &str) -> Vec<Value> {
        self.pipes
            .lock()
            .unwrap()
            .get(metric)
            .cloned()
            .unwrap_or_default()
    }
    fn values(&self, metric: &str) -> Vec<i64> {
        self.rows(metric)
            .iter()
            .map(|r| r["value"].as_i64().unwrap())
            .collect()
    }
}

impl HostServices for Memory {
    fn call(&self, _: &Request, _: &str, _: Option<Frame>) -> Result<Value> {
        unreachable!("every call goes through host_call")
    }

    fn host_call(
        &self,
        request: &Request,
        call: &HostCall,
        data: Option<PipeInput>,
    ) -> Result<HostReply> {
        assert!(!call.plugin.is_empty());
        self.calls.lock().unwrap().push(call.method.clone());
        let pipe = call
            .pipe
            .clone()
            .unwrap_or_else(|| request.context["pipe"].clone());
        let metric = pipe["keys"]["metric_key"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let (begin, end) = (call.bound("begin")?, call.bound("end")?);
        let frame = |rows: Vec<Value>| -> Result<Frame> {
            Frame::from_rows(rows, &[("ts", "datetime"), ("value", "int")])
        };
        Ok(match call.method.as_str() {
            "pipe" => HostReply::json(json!({
                "keys": {"connector_keys": call.arg("connector_keys"), "metric_key": call.arg("metric_key"),
                    "location_key": call.arg("location_key"), "instance_keys": "memory:test"},
                "parameters": {"columns": {"datetime": "ts"}}, "temporary": false,
            })),
            "get_data" => {
                if !self.pipes.lock().unwrap().contains_key(&metric) {
                    return Ok(HostReply::data(None));
                }
                let rows = self
                    .rows(&metric)
                    .into_iter()
                    .filter(|r| in_window(r, &begin, &end))
                    .collect();
                HostReply::data(Some(frame(rows)?))
            }
            "sync" => {
                self.syncs.lock().unwrap().push(call.args.clone());
                // `pipe.replace(rows, begin, end)`: the window's rows go first (no rows: only that).
                if call.arg("replace") == true {
                    let mut pipes = self.pipes.lock().unwrap();
                    let rows = pipes.entry(metric.clone()).or_default();
                    rows.retain(|r| !in_window(r, &begin, &end));
                }
                let frame = match data {
                    Some(PipeInput::Frame(frame)) => {
                        // An Arrow plugin's frame arrives typed.
                        assert!(
                            frame.dtypes()["ts"].starts_with("datetime64"),
                            "{:?}",
                            frame.dtypes()
                        );
                        frame
                    }
                    Some(input) => meerschaum_kernel::enforce::pipe_enforce_dtypes(
                        input,
                        &meerschaum_kernel::enforce::PipeDtypesContext {
                            dtypes: [("ts".to_string(), "datetime".to_string())].into(),
                            explicit_dtypes: Default::default(),
                            enforce: true,
                            tz_aware: true,
                            mixed_numerics: false,
                        },
                    )
                    .unwrap(),
                    None => return Ok(HostReply::json(json!([true, "No data to sync."]))),
                };
                let rows = frame.to_rows();
                let count = rows.len();
                self.pipes
                    .lock()
                    .unwrap()
                    .entry(metric)
                    .or_default()
                    .extend(rows);
                HostReply::json(json!([true, format!("Inserted {count}")]))
            }
            "clear" => {
                let mut pipes = self.pipes.lock().unwrap();
                let rows = pipes.entry(metric).or_default();
                rows.retain(|r| !in_window(r, &begin, &end));
                HostReply::json(json!([true, "Cleared"]))
            }
            "get_rowcount" => HostReply::json(json!(self.rows(&metric).len())),
            "exists" => HostReply::json(json!(self.pipes.lock().unwrap().contains_key(&metric))),
            "get_columns_types" => HostReply::json(json!({"ts": "TIMESTAMP", "value": "BIGINT"})),
            "read_sql" => {
                assert!(
                    call.str_arg("query")
                        .unwrap()
                        .starts_with("SELECT COUNT(*)")
                );
                let n = self.rows("parent").len();
                HostReply::data(Some(Frame::from_rows([json!({"n": n})], &[])?))
            }
            "exec_sql" => HostReply::json(json!([true, call.arg("query")])),
            "plugin_config" => HostReply::json(self.config.lock().unwrap().clone()),
            "write_plugin_config" => {
                *self.config.lock().unwrap() = call.arg("config").clone();
                HostReply::json(json!([true, "Wrote"]))
            }
            other => return Err(meerschaum_plugin::Error::Unsupported(other.into())),
        })
    }
}

fn sync_request(plugin: &str, metric: &str, begin: Value, end: Value) -> Request {
    Request {
        method: "sync".into(),
        connector_type: "plugin".into(),
        label: plugin.into(),
        config: Value::Null,
        context: json!({
            "pipe": {"keys": {"connector_keys": format!("plugin:{plugin}"), "metric_key": metric,
                "location_key": null, "instance_keys": "memory:test"},
                "parameters": {"columns": {"datetime": "ts"}}, "temporary": false},
            "begin": begin, "end": end,
        }),
    }
}

fn action_request(name: &str) -> Request {
    Request {
        method: "action".into(),
        connector_type: String::new(),
        label: String::new(),
        config: Value::Null,
        context: json!({"name": name, "kwargs": {"mrsm_instance": "memory:test"}}),
    }
}

#[test]
fn arrow_plugins_make_every_host_call_with_frames_both_ways() {
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(example("native_host")) }.unwrap();
    // The manifest is built, not written: the SDK filled in the envelope and the version.
    assert_eq!(plugin.name, "native_host");
    assert_eq!(plugin.version, env!("CARGO_PKG_VERSION"));
    assert_eq!(
        plugin.manifest["meerschaum_version"],
        env!("CARGO_PKG_VERSION")
    );
    let entry = &plugin.manifest["plugins"]["native_host"];
    assert_eq!(entry["data_functions"], json!(["sync"]));
    assert_eq!(
        entry["api_routes"][1]["path"],
        "/native-host/{metric}/clear"
    );

    let host = Memory::default();
    let reply = plugin
        .invoke_with_host(
            &sync_request("native_host", "parent", json!(""), Value::Null),
            None,
            Some(&host),
        )
        .unwrap();
    assert_eq!(reply.value, json!([true, "Inserted 3"]));
    assert_eq!(host.values("parent"), [0, 1, 2]);

    // The child reads the parent's window through the stream and replaces its own window with
    // it, in one call.
    let window =
        |begin: &str, end: &str| sync_request("native_host", "child", json!(begin), json!(end));
    let reply = plugin
        .invoke_with_host(
            &window("2024-01-01T01:00:00+00:00", "2024-01-01T03:00:00+00:00"),
            None,
            Some(&host),
        )
        .unwrap();
    assert_eq!(reply.value, json!([true, "Inserted 2"]));
    assert_eq!(host.values("child"), [10, 20]);
    let calls = host.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        [
            "plugin_config",
            "sync",
            "pipe",
            "plugin_config",
            "get_data",
            "sync"
        ]
    );
    assert_eq!(
        host.syncs.lock().unwrap().clone(),
        [
            json!({}),
            json!({"replace": true, "begin": "2024-01-01T01:00:00+00:00",
                "end": "2024-01-01T03:00:00+00:00"}),
        ]
    );
    // No rows with both bounds empties the window (no data crosses).
    let trimmed = plugin
        .action_with_host("native_host_trim", json!({}), Some(&host))
        .unwrap();
    assert!(trimmed.success, "{}", trimmed.message);
    assert_eq!(host.values("child"), [20]);

    // Actions write and read the plugin's config and run SQL.
    let configured = plugin
        .action_with_host("native_host_configure", json!({}), Some(&host))
        .unwrap();
    let written: Value =
        serde_json::from_str(configured.message.strip_prefix("config: ").unwrap()).unwrap();
    assert_eq!(written, json!({"rows": 4, "multiplier": 100}));
    // The next parent sync reads the new row count.
    host.pipes.lock().unwrap().clear();
    plugin
        .invoke_with_host(
            &sync_request("native_host", "parent", json!(""), Value::Null),
            None,
            Some(&host),
        )
        .unwrap();
    assert_eq!(host.values("parent"), [0, 1, 2, 3]);
    let counted = plugin
        .invoke_with_host(&action_request("native_host_sql"), None, Some(&host))
        .unwrap();
    assert_eq!(
        counted.value,
        json!([true, r#"parent rows: 4; columns: ["ts", "value"]"#])
    );

    // A route's host calls (a caller's permissions are the host's to enforce).
    let route = RouteRequest {
        name: "rowcount".into(),
        method: "GET".into(),
        path: "/native-host/child/rowcount".into(),
        path_params: [("metric".to_string(), "child".to_string())].into(),
        ..Default::default()
    };
    let response = plugin.api_route_with_host(&route, Some(&host)).unwrap();
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body, json!({"rowcount": 0, "exists": false}));
    // Without a host, the same call explains why it cannot run.
    let error = plugin.api_route(&route).unwrap_err().to_string();
    assert!(error.contains("host calls are unavailable"), "{error}");
}

#[test]
fn an_older_host_reports_the_new_calls_as_needing_an_upgrade() {
    /// A host from before the new calls.
    struct Old;
    impl HostServices for Old {
        fn call(&self, _: &Request, method: &str, _: Option<Frame>) -> Result<Value> {
            Err(meerschaum_plugin::Error::Unsupported(format!(
                "unsupported Pipe host operation: {method}"
            )))
        }
    }
    // SAFETY: the test builds this SDK-owned example from the repository.
    let plugin = unsafe { LoadedPlugin::load(example("native_host")) }.unwrap();
    let error = plugin
        .invoke_with_host(
            &sync_request("native_host", "parent", json!(""), Value::Null),
            None,
            Some(&Old),
        )
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("`host::plugin_config()`"), "{error}");
    assert!(error.contains("upgrade Meerschaum to 5.0.0rc15"), "{error}");
}

const SINGLE_FILE: &str = r#"
fn sync(pipe: &Pipe, begin: Option<Bound>, end: Option<Bound>) -> Result<SuccessTuple> {
    let factor = host::plugin_config()?["factor"].as_i64().unwrap_or(1);
    if pipe.metric_key() == "parent" {
        return pipe.sync(vec![
            json!({"ts": "2024-01-01T00:00:00+00:00", "value": 1}),
            json!({"ts": Bound::from_timestamp(1_704_070_800).to_string(), "value": 2}),
        ]);
    }
    let parent = host::pipe(pipe.connector_keys(), "parent", None, None)?;
    assert_eq!(parent.to_string(), "Pipe('plugin:minimal', 'parent', instance='memory:test')");
    let n = host::read_sql(parent.instance_keys(), "SELECT COUNT(*) AS n")?[0]["n"].as_i64().unwrap();
    let rows: Vec<Value> = parent
        .get_data(begin.clone(), end.clone(), None)?
        .into_iter()
        .map(|mut row| { row["value"] = json!(row["value"].as_i64().unwrap() * factor * n); row })
        .collect();
    assert!(pipe.clear(begin, end, None)?.success);
    pipe.sync(rows)
}

#[make_action]
fn rewrite() -> Result<SuccessTuple> {
    let child = host::pipe("plugin:minimal", "child", None, None)?;
    child.replace(
        vec![json!({"ts": "2024-01-01T01:00:00+00:00", "value": 7})],
        Some(Bound::from("2024-01-01T00:00:00+00:00")),
        None,
    )
}

#[make_action]
fn configure() -> Result<SuccessTuple> {
    host::write_plugin_config(&json!({"factor": 5}))?;
    let missing = host::pipe("plugin:minimal", "missing", None, None)?;
    Ok(SuccessTuple::ok(format!(
        "{} {} {} {}",
        host::plugin_config()?,
        missing.exists()?,
        missing.get_data(None, None, None)?.len(),
        host::exec_sql("memory:test", "VACUUM")?.message,
    )))
}
"#;

#[test]
fn single_file_plugins_make_every_host_call_with_rows() {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("minimal.rs"), SINGLE_FILE).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    support::compile_single_file::compile(&found.candidates[0]);
    // SAFETY: the fixture above is trusted code built with this SDK.
    let plugin = unsafe { LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    let host = Memory::default();
    let configured = plugin
        .action_with_host("configure", json!({}), Some(&host))
        .unwrap();
    assert_eq!(configured.message, r#"{"factor":5} false 0 VACUUM"#);
    plugin
        .invoke_with_host(
            &sync_request("minimal", "parent", json!(""), Value::Null),
            None,
            Some(&host),
        )
        .unwrap();
    assert_eq!(host.values("parent"), [1, 2]);
    let reply = plugin
        .invoke_with_host(
            &sync_request(
                "minimal",
                "child",
                json!("2024-01-01T01:00:00+00:00"),
                Value::Null,
            ),
            None,
            Some(&host),
        )
        .unwrap();
    assert_eq!(reply.value, json!([true, "Inserted 1"]));
    // factor 5 times the parent's 2 rows (read_sql) times the value 2.
    assert_eq!(host.values("child"), [20]);
    // `replace` sends its rows with the window (a `null` bound is the rows' own, for the host).
    let rewritten = plugin
        .action_with_host("rewrite", json!({}), Some(&host))
        .unwrap();
    assert!(rewritten.success, "{}", rewritten.message);
    assert_eq!(
        host.syncs.lock().unwrap().last().unwrap(),
        &json!({"replace": true, "begin": "2024-01-01T00:00:00+00:00", "end": null})
    );
    assert_eq!(host.values("child"), [7]);
    // Host calls outside a host-enabled call explain themselves.
    let error = plugin
        .invoke(&action_request("configure"), None)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("host calls are unavailable"), "{error}");
}

const STREAMS: &str = r#"
#[make_connector]
mod ticker {
    fn subscribe(pipe: &Pipe, kwargs: &Value) -> Result<impl FnMut(Duration) -> Batch> {
        Ok(move |_timeout: Duration| {
            // Each batch reads the plugin's config through the host (-1 without one).
            let step = host::plugin_config()
                .map(|config| config["step"].as_i64().unwrap_or(0))
                .unwrap_or(-1);
            Batch::Rows(vec![json!({"id": step})])
        })
    }
}

#[make_executor]
mod procs {
    fn get_job_names() -> Result<Vec<String>> {
        Ok(vec![host::plugin_config()?["job"].as_str().unwrap_or("none").to_string()])
    }
}
"#;

#[test]
fn stream_batches_and_executors_make_host_calls() {
    use meerschaum_plugin::subscribe::NativeBatch;
    use std::{sync::Arc, time::Duration};
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("streams.rs"), STREAMS).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    support::compile_single_file::compile(&found.candidates[0]);
    // SAFETY: the fixture above is trusted code built with this SDK.
    let plugin = unsafe { LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    let host = Arc::new(Memory::default());
    *host.config.lock().unwrap() = json!({"step": 7, "job": "nightly"});
    let pipe = meerschaum_core::Pipe::builder("ticker:main", "ticks")
        .instance("memory:test")
        .build()
        .unwrap();
    let rows = |connector: meerschaum_plugin::NativeConnector| {
        let subscription = connector.subscribe(&pipe, json!({})).unwrap();
        match subscription.next_batch(Duration::from_millis(10)).unwrap() {
            NativeBatch::Data { frame, .. } => frame.to_rows(),
            _ => panic!("no batch"),
        }
    };
    // A batch (not serialized with the plugin's other calls) makes host calls with the
    // connector's host services; without them it is told they are unavailable.
    let hosted = plugin
        .connector("ticker", "main", json!({}))
        .unwrap()
        .with_host_services(host.clone());
    assert_eq!(rows(hosted), [json!({"id": 7})]);
    let bare = plugin.connector("ticker", "main", json!({})).unwrap();
    assert_eq!(rows(bare), [json!({"id": -1})]);
    // So does every executor method.
    let executor = plugin
        .executor("procs", "main", json!({}))
        .unwrap()
        .with_host_services(host.clone());
    assert_eq!(
        executor.call("get_job_names", json!({})).unwrap(),
        json!(["nightly"])
    );
    let error = plugin
        .executor("procs", "main", json!({}))
        .unwrap()
        .call("get_job_names", json!({}))
        .unwrap_err()
        .to_string();
    assert!(error.contains("host calls are unavailable"), "{error}");
}

/// The rlibs of `prefix` in the test's dependency directory, newest first.
fn rlibs(deps: &Path, prefix: &str) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(deps)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().unwrap().to_string_lossy();
            name.starts_with(&format!("lib{prefix}-")) && name.ends_with(".rlib")
        })
        .collect();
    found.sort_by_key(|p| std::cmp::Reverse(p.metadata().unwrap().modified().unwrap()));
    found
}

#[test]
fn a_cargo_plugin_is_named_and_versioned_by_its_package() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("lib.rs");
    std::fs::write(
        &source,
        r#"
use meerschaum_plugin::prelude::*;

#[derive(Default)]
struct Weather;

impl Plugin for Weather {
    fn manifest(&self) -> Value {
        Manifest::new().fetch().post_sync_hook("report").into()
    }
    fn fetch(&self, _: &Request) -> Result<Option<Frame>> {
        Frame::from_rows(vec![json!({"ts": "2024-01-01", "temp": 21})], &[("ts", "datetime"), ("temp", "float")]).map(Some)
    }
}

export_plugin!(Weather);
"#,
    )
    .unwrap();
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let library = root.path().join(format!(
        "{}weather.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ));
    // Several builds may leave rlibs of the SDK in the shared deps directory: the newest that
    // links is this test's.
    let mut errors = Vec::new();
    let built = rlibs(&deps, "meerschaum_plugin").iter().take(4).any(|sdk| {
        let output = Command::new("rustc")
            .args([
                "--edition=2024",
                "--crate-type=cdylib",
                "--crate-name=my_weather",
            ])
            .arg(&source)
            .arg("--extern")
            .arg(format!("meerschaum_plugin={}", sdk.display()))
            .arg("-L")
            .arg(format!("dependency={}", deps.display()))
            .arg("-o")
            .arg(&library)
            // What Cargo sets for a package `my-weather` at 2.3.4.
            .env("CARGO_PKG_NAME", "my-weather")
            .env("CARGO_PKG_VERSION", "2.3.4")
            .output()
            .unwrap();
        errors.push(String::from_utf8_lossy(&output.stderr).into_owned());
        output.status.success()
    });
    assert!(built, "{}", errors.join("\n"));
    // SAFETY: the fixture above is trusted code built with this SDK.
    let plugin = unsafe { LoadedPlugin::load(&library) }.unwrap();
    assert_eq!(plugin.name, "my-weather");
    assert_eq!(plugin.version, "2.3.4");
    assert_eq!(
        plugin.manifest["meerschaum_version"],
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(
        plugin.manifest["plugins"]["my-weather"],
        json!({"name": "my-weather", "version": "2.3.4", "data_functions": ["fetch"],
            "post_sync_hooks": ["report"]})
    );
    assert_eq!(plugin.manifest["post_sync_hooks"], json!(["my-weather"]));
    let frame = plugin
        .invoke(
            &Request {
                method: "fetch".into(),
                connector_type: "plugin".into(),
                label: "my-weather".into(),
                config: Value::Null,
                context: json!({}),
            },
            None,
        )
        .unwrap()
        .data
        .unwrap();
    assert_eq!(frame.dtypes()["ts"], "datetime64[us, UTC]");
    assert_eq!(frame.dtypes()["temp"], "float64");
}
