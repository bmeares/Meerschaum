//! Single-file spellings of the packaged SDK's remaining surfaces, at the ABI: an instance
//! connector (`#[make_connector(instance)]`), a console page (`#[web_page]`), the SDK's own
//! return types (`Box<dyn Subscription>`, `Box<dyn LogStream>`), `ChannelSubscription` and
//! `mcp::message`, and helpers named `subscribe` that stay helpers. All data is synthetic; every
//! test uses temporary directories.
use meerschaum_connector::{Connector, DataQuery, InstanceConnector};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_plugin::{
    LoadedPlugin, discovery,
    executor::{LogEvent, MonitorLogs},
    mcp::PromptGet,
    serde_json::json,
    subscribe::NativeBatch,
};
use std::{sync::Arc, time::Duration};
mod support {
    pub mod compile_single_file;
}

const SOURCE: &str = r##"//! The packaged SDK's remaining surfaces in one file.
use std::collections::BTreeMap;
use std::sync::Mutex;

static STORE: Mutex<BTreeMap<String, (Value, Vec<Value>)>> = Mutex::new(BTreeMap::new());

fn key(pipe: &Pipe) -> String {
    format!("{}|{}", pipe.connector_keys(), pipe.metric_key())
}

/// Keeps pipes and their rows in memory.
#[make_connector(instance)]
mod memory {
    use super::*;

    fn register_pipe(pipe: &Pipe) -> SuccessTuple {
        STORE.lock().unwrap().insert(key(pipe), (pipe.parameters().clone(), Vec::new()));
        SuccessTuple::ok("Registered.")
    }

    fn pipe_exists(pipe: &Pipe) -> bool {
        STORE.lock().unwrap().contains_key(&key(pipe))
    }

    fn sync_pipe(pipe: &Pipe, data: Vec<Value>, check_existing: Option<bool>) -> (bool, String) {
        let mut store = STORE.lock().unwrap();
        let entry = store.entry(key(pipe)).or_insert_with(|| (json!({}), Vec::new()));
        let count = data.len();
        entry.1.extend(data);
        (true, format!("Inserted {count} rows (check_existing={check_existing:?})."))
    }

    fn get_pipe_data(pipe: &Pipe, begin: Option<Bound>, params: &Value) -> Vec<Value> {
        let start = match begin {
            Some(Bound::Int(n)) => n,
            _ => i64::MIN,
        };
        STORE.lock().unwrap().get(&key(pipe)).map(|(_, rows)| {
            rows.iter()
                .filter(|r| r["id"].as_i64().unwrap_or_default() >= start)
                .filter(|r| params["color"].is_null() || r["color"] == params["color"])
                .cloned()
                .collect()
        }).unwrap_or_default()
    }

    fn get_sync_time(pipe: &Pipe, newest: bool) -> Option<i64> {
        let store = STORE.lock().unwrap();
        let ids = store.get(&key(pipe))?.1.iter().filter_map(|r| r["id"].as_i64()).collect::<Vec<_>>();
        if newest { ids.into_iter().max() } else { ids.into_iter().min() }
    }

    fn fetch_pipes_keys(query: &Value) -> Result<Value> {
        let wanted = query["metric_keys"].as_array().cloned().unwrap_or_default();
        Ok(Value::Array(STORE.lock().unwrap().keys()
            .filter_map(|k| k.split_once('|'))
            .filter(|(_, m)| wanted.is_empty() || wanted.contains(&json!(m)))
            .map(|(c, m)| json!([c, m, null]))
            .collect()))
    }

    fn acquire_pipe_lease(holder: &str, ttl_seconds: f64) -> bool {
        holder == "me" && ttl_seconds > 0.0
    }

    fn edit_pipe(pipe: &Pipe, patch: bool, connector: &Connector) -> SuccessTuple {
        SuccessTuple::ok(format!("{} {} {}", connector.label(), key(pipe), patch))
    }

    /// An ordinary helper: not an instance method.
    fn describe() -> String {
        "memory".into()
    }
}

/// A report for the console.
#[web_page("sf-parity/report", login_required = false, page_group = "Parity")]
fn report(page: &PageRequest) -> String {
    format!("<p>{} {:?} {:?}</p>", page.path, page.query_param("q"), page.username)
}

#[web_page]
fn missing() -> (u16, &'static str) {
    (404, "<p>Nothing here.</p>")
}

/// The packaged SDK's spelling: `Box<dyn Subscription>`.
#[make_connector]
mod boxed {
    use super::*;

    fn subscribe(pipe: &Pipe) -> Result<Box<dyn Subscription>> {
        let count = pipe.parameters()["fetch"]["count"].as_u64().unwrap_or(2);
        let (sender, subscription) = ChannelSubscription::new(1);
        std::thread::spawn(move || {
            for id in 0..count {
                if sender.send(Frame::from_rows(vec![json!({"id": id})]).unwrap()).is_err() {
                    return;
                }
            }
        });
        Ok(Box::new(subscription))
    }

    /// A helper named like an instance method stays a helper in a source connector.
    fn pipe_exists() -> bool {
        true
    }
}

/// Helpers named `subscribe` stay helpers (as before streaming connectors).
#[make_connector]
mod legacy {
    use super::*;

    fn subscribe(topic: &str) -> String {
        format!("topic:{topic}")
    }

    fn fetch() -> Vec<Value> {
        vec![json!({"t": subscribe("x")})]
    }
}

fn subscribe(pipe: &Pipe) -> Vec<Value> {
    vec![json!({"metric": pipe.metric_key()})]
}

fn fetch(pipe: &Pipe) -> Vec<Value> {
    subscribe(pipe)
}

/// One assistant message.
#[mcp_prompt(arguments = ["who"])]
fn sf_reply(arguments: &Value) -> Vec<Value> {
    vec![meerschaum_plugin::mcp::message("assistant", format!("Hello, {}.", arguments["who"].as_str().unwrap_or("?")))]
}

/// Logs through the packaged SDK's `Box<dyn LogStream>`.
#[make_executor]
mod boxlogs {
    use super::*;

    fn get_job_names() -> Vec<String> {
        vec!["a".into()]
    }

    fn monitor_logs(name: &str) -> Box<dyn LogStream> {
        let mut lines = vec![format!("{name} boxed")];
        Box::new(move |_timeout: Duration| match lines.pop() {
            Some(line) => LogEvent::Lines(vec![line]),
            None => LogEvent::End(None),
        })
    }
}
"##;

fn compiled(source: &str) -> (tempfile::TempDir, Arc<LoadedPlugin>) {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("parity.rs"), source).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    support::compile_single_file::compile(&found.candidates[0]);
    // SAFETY: the fixture above is trusted code built with this crate's runtime.
    let plugin = unsafe { LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    (root, plugin)
}

fn pipe(connector: &str, metric: &str, parameters: serde_json::Value) -> Pipe {
    let mut pipe = Pipe::builder(connector, metric)
        .instance("memory:abi")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(&parameters.to_string())
            .unwrap()
            .to_map(),
    );
    pipe
}

#[test]
fn every_remaining_surface_has_a_single_file_spelling() {
    let (_root, plugin) = compiled(SOURCE);
    let manifest = &plugin.manifest;

    // The instance connector: its instance methods, not its helpers.
    let memory = &manifest["connector_types"]["memory"];
    assert_eq!(memory["is_instance"], true);
    assert_eq!(
        memory["methods"],
        json!([
            "register_pipe",
            "pipe_exists",
            "sync_pipe",
            "get_pipe_data",
            "get_sync_time",
            "fetch_pipes_keys",
            "acquire_pipe_lease",
            "edit_pipe"
        ])
    );
    assert_eq!(memory["doc"], "Keeps pipes and their rows in memory.");
    // Source connectors keep `is_instance: false` and their instance-named helpers.
    assert_eq!(manifest["connector_types"]["boxed"]["is_instance"], false);
    assert_eq!(
        manifest["connector_types"]["boxed"]["methods"],
        json!(["subscribe"])
    );
    // The helpers named `subscribe` stay helpers.
    let legacy = &manifest["connector_types"]["legacy"];
    assert_eq!(legacy["methods"], json!(["fetch"]));
    assert!(legacy.get("is_streaming").is_none());
    assert_eq!(
        manifest["plugins"]["parity"]["data_functions"],
        json!(["fetch"])
    );
    // Pages: `@web_page`'s path, page_key and defaults.
    assert_eq!(
        manifest["plugins"]["parity"]["web_pages"],
        json!([
            {"function": "report", "login_required": false, "skip_navbar": false,
                "dark_theme": true, "mode": "html", "page_group": "Parity",
                "path": "/dash/sf-parity/report", "page_key": "Sf Parity/report"},
            {"function": "missing", "login_required": true, "skip_navbar": false,
                "dark_theme": true, "mode": "html", "path": "/dash/missing",
                "page_key": "Missing"},
        ])
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    let instance = plugin.connector("memory", "abi", json!({})).unwrap();
    assert!(instance.is_instance());
    let readings = pipe(
        "plugin:parity",
        "readings",
        json!({"columns": {"datetime": "id"}}),
    );
    rt.block_on(async {
        assert!(!instance.pipe_exists(&readings).await.unwrap());
        let registered = instance.register_pipe(&readings).await;
        assert!(registered.success, "{}", registered.message);
        assert!(instance.pipe_exists(&readings).await.unwrap());
        let rows = Frame::from_object_columns(vec![
            (
                "id".into(),
                vec![
                    meerschaum_kernel::Value::Int(1),
                    meerschaum_kernel::Value::Int(2),
                    meerschaum_kernel::Value::Int(3),
                ],
            ),
            (
                "color".into(),
                vec![
                    meerschaum_kernel::Value::Str("red".into()),
                    meerschaum_kernel::Value::Str("blue".into()),
                    meerschaum_kernel::Value::Str("red".into()),
                ],
            ),
        ])
        .unwrap();
        let synced = instance
            .sync_pipe(
                &readings,
                rows,
                &meerschaum_connector::SyncOptions {
                    check_existing: false,
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(
            synced.message, "Inserted 3 rows (check_existing=Some(false)).",
            "the options' fields bind by name"
        );
        assert_eq!(
            instance.get_sync_time(&readings, None, true).await.unwrap(),
            Some(meerschaum_connector::DateBound::Int(3))
        );
        let query = DataQuery {
            begin: Some(meerschaum_connector::DateBound::Int(2)),
            params: Some(
                Value::from_json_str(r#"{"color": "red"}"#)
                    .unwrap()
                    .to_map(),
            ),
            ..Default::default()
        };
        let data = instance
            .get_pipe_data(&readings, &query)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data.height(), 1, "begin and params bind from the query");
        let keys = instance
            .fetch_pipes_keys(&meerschaum_connector::PipesKeysQuery::default())
            .await
            .unwrap();
        assert!(format!("{keys:?}").contains("readings"), "{keys:?}");
        assert!(
            instance
                .acquire_pipe_lease(&readings, "me", Duration::from_secs(5))
                .await
                .unwrap()
        );
        let edited = instance.edit_pipe(&readings, true).await;
        assert_eq!(edited.message, "abi plugin:parity|readings true");
        // Methods the module leaves out are not implemented, so the engine can fall back.
        assert!(instance.get_pipe_rowcount(&readings, &query).await.is_err());
        assert!(!instance.drop_pipe(&readings).await.success);
        assert!(instance.connector_type() == "memory");
    });

    // Pages answer the console's `page` call.
    let page = plugin
        .call_json(
            "page",
            json!({"path": "sf-parity/report", "function": "report",
                "query": "?q=a%20b+c&x=1", "username": "ada"}),
        )
        .unwrap();
    assert_eq!(
        page,
        json!({"html": "<p>sf-parity/report Some(\"a b c\") Some(\"ada\")</p>"})
    );
    let missing = plugin
        .call_json(
            "page",
            json!({"path": "missing", "function": "missing", "query": "", "username": ""}),
        )
        .unwrap();
    assert_eq!(
        missing,
        json!({"html": "<p>Nothing here.</p>", "status": 404})
    );

    // `Box<dyn Subscription>` over a `ChannelSubscription`.
    let boxed = plugin.connector("boxed", "abi", json!({})).unwrap();
    assert!(boxed.is_streaming());
    let subscription = boxed
        .subscribe(
            &pipe("boxed:abi", "s", json!({"fetch": {"count": 3}})),
            json!({}),
        )
        .unwrap();
    let mut heights = Vec::new();
    loop {
        match subscription.next_batch(Duration::from_millis(500)).unwrap() {
            NativeBatch::Data { frame, ack, .. } => {
                assert_eq!(ack, None);
                heights.push(frame.height());
            }
            NativeBatch::Timeout => continue,
            NativeBatch::End => break,
        }
    }
    assert_eq!(heights, vec![1, 1, 1]);
    subscription.close().unwrap();

    // The legacy helpers still run.
    let fetched = rt
        .block_on(
            plugin
                .connector("legacy", "abi", json!({}))
                .unwrap()
                .fetch(&pipe("legacy:abi", "l", json!({})), &DataQuery::default()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(fetched.height(), 1);
    let fetched = rt
        .block_on(plugin.source(json!({})).fetch(
            &pipe("plugin:parity", "m", json!({})),
            &DataQuery::default(),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(fetched.height(), 1);

    // `mcp::message` with a role.
    let (_, messages) = plugin
        .mcp_prompt(&PromptGet {
            name: "sf_reply".into(),
            arguments: [("who".to_string(), json!("ada"))].into_iter().collect(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        messages,
        vec![json!({"role": "assistant", "content": {"type": "text", "text": "Hello, ada."}})]
    );

    // `Box<dyn LogStream>` from `monitor_logs`.
    let executor = plugin.executor("boxlogs", "abi", json!({})).unwrap();
    let mut logs = executor.monitor_logs("a", MonitorLogs::default()).unwrap();
    assert_eq!(
        logs.next_lines(Duration::from_millis(10)).unwrap(),
        LogEvent::Lines(vec!["a boxed".into()])
    );
    assert!(matches!(
        logs.next_lines(Duration::from_millis(10)).unwrap(),
        LogEvent::End(None)
    ));
}
