//! `begin` / `end` reach a single-file plugin's `Option<Bound>` parameters wherever the host
//! puts them: `context.query` (`fetch`, `get_pipe_data`, `get_pipe_rowcount`, `clear_pipe`),
//! `context.options` (`sync_pipe`) or the top level (a plugin's own `sync`). All data is
//! synthetic; every test uses temporary directories.
use meerschaum_connector::{Connector, DataQuery, DateBound, InstanceConnector, SyncOptions};
use meerschaum_core::Pipe;
use meerschaum_kernel::Frame;
use meerschaum_plugin::{LoadedPlugin, discovery, serde_json::json};
use std::sync::Arc;
mod support {
    pub mod compile_single_file;
}

const SOURCE: &str = r##"//! Echoes the bounds each call receives.
fn show(begin: &Option<Bound>, end: &Option<Bound>) -> String {
    format!("{begin:?} {end:?}")
}

#[make_connector(instance)]
mod echo {
    use super::*;

    fn sync_pipe(data: Vec<Value>, begin: Option<Bound>, end: Option<Bound>) -> SuccessTuple {
        SuccessTuple::ok(format!("{} rows={}", show(&begin, &end), data.len()))
    }

    fn clear_pipe(begin: Option<Bound>, end: Option<Bound>) -> SuccessTuple {
        SuccessTuple::ok(show(&begin, &end))
    }

    fn get_pipe_data(begin: Option<Bound>, end: Option<Bound>) -> Vec<Value> {
        vec![json!({"bounds": show(&begin, &end)})]
    }

    fn get_pipe_rowcount(begin: Option<Bound>, end: Option<Bound>) -> Option<i64> {
        match (begin, end) {
            (Some(Bound::Int(b)), Some(Bound::Int(e))) => Some(e - b),
            _ => None,
        }
    }
}

#[make_connector]
mod source {
    use super::*;

    fn fetch(begin: Option<Bound>, end: Option<Bound>) -> Vec<Value> {
        vec![json!({"bounds": show(&begin, &end)})]
    }
}

fn fetch(begin: Option<Bound>, end: Option<Bound>) -> Vec<Value> {
    vec![json!({"bounds": show(&begin, &end)})]
}

fn sync(begin: Option<Bound>, end: Option<Bound>) -> SuccessTuple {
    SuccessTuple::ok(show(&begin, &end))
}
"##;

fn compiled(source: &str) -> (tempfile::TempDir, Arc<LoadedPlugin>) {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("bounds.rs"), source).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    support::compile_single_file::compile(&found.candidates[0]);
    // SAFETY: the fixture above is trusted code built with this crate's runtime.
    let plugin = unsafe { LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    (root, plugin)
}

fn pipe() -> Pipe {
    Pipe::builder("plugin:bounds", "readings")
        .instance("echo:abi")
        .build()
        .unwrap()
}

fn query() -> DataQuery {
    DataQuery {
        begin: Some(DateBound::Int(2)),
        end: Some(DateBound::Int(5)),
        ..Default::default()
    }
}

fn bounds_of(frame: Option<Frame>) -> String {
    match frame.unwrap().column("bounds").unwrap().data.get(0) {
        meerschaum_kernel::Value::Str(s) => s,
        other => panic!("{other:?}"),
    }
}

const EXPECTED: &str = "Some(Int(2)) Some(Int(5))";

#[test]
fn bounds_reach_instance_methods_fetch_and_sync_wherever_the_host_puts_them() {
    let (_root, plugin) = compiled(SOURCE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pipe = pipe();
    let instance = plugin.connector("echo", "abi", json!({})).unwrap();
    let source = plugin.connector("source", "abi", json!({})).unwrap();
    let plugin_source = plugin.source(json!({}));
    let options = SyncOptions {
        begin: Some(DateBound::Int(2)),
        end: Some(DateBound::Int(5)),
        ..Default::default()
    };
    let rows = || {
        Frame::from_object_columns(vec![("id".into(), vec![meerschaum_kernel::Value::Int(1)])])
            .unwrap()
    };
    rt.block_on(async {
        // `sync_pipe` carries its bounds in `context.options`.
        let synced = instance.sync_pipe(&pipe, rows(), &options).await;
        assert_eq!(synced.message, format!("{EXPECTED} rows=1"));
        // The query-carrying instance methods.
        let cleared = instance.clear_pipe(&pipe, &query()).await;
        assert_eq!(cleared.message, EXPECTED);
        let data = instance.get_pipe_data(&pipe, &query()).await.unwrap();
        assert_eq!(bounds_of(data), EXPECTED);
        assert_eq!(
            instance.get_pipe_rowcount(&pipe, &query()).await.unwrap(),
            Some(3)
        );
        // Unbounded calls stay unbounded.
        let unbounded = instance
            .sync_pipe(&pipe, rows(), &SyncOptions::default())
            .await;
        assert_eq!(unbounded.message, "None None rows=1");
        // A source connector's and the plugin's own `fetch`.
        assert_eq!(
            bounds_of(source.fetch(&pipe, &query()).await.unwrap()),
            EXPECTED
        );
        assert_eq!(
            bounds_of(plugin_source.fetch(&pipe, &query()).await.unwrap()),
            EXPECTED
        );
        // The plugin's own `sync`, reached as an instance `sync_pipe`.
        let synced = plugin_source.sync_pipe(&pipe, rows(), &options).await;
        assert_eq!(synced.message, EXPECTED);
    });

    // A plugin's own `sync` with flat (top-level) bounds, as `NativeSync` sends them.
    let request = meerschaum_plugin::Request {
        method: "sync".into(),
        connector_type: "plugin".into(),
        label: "bounds".into(),
        config: json!({}),
        context: json!({"pipe": {"keys": {"connector_keys": "plugin:bounds", "metric_key": "readings"}, "parameters": {}}, "begin": 2, "end": 5}),
    };
    assert_eq!(
        plugin.invoke(&request, None).unwrap().value,
        json!([true, EXPECTED])
    );
}
