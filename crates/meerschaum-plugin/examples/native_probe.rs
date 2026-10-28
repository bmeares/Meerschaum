//! Test plugin: a plain fetch source (no `sync()` override, so the built-in sync writes the rows
//! to the pipe's instance) whose sync hooks append a line to the file named by the
//! `NATIVE_PROBE_LOG` environment variable. Setting `NATIVE_PROBE_FAIL=pre` or `post` makes that
//! hook report a failure. All data is synthetic.
use meerschaum_plugin::{
    Error, Frame, Plugin, Reply, Request, Result,
    arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    serde_json::{Value, json},
};
use std::io::Write;
use std::sync::Arc;

#[derive(Default)]
struct Probe;

/// Append `line` to `NATIVE_PROBE_LOG`; report a failure when `NATIVE_PROBE_FAIL` names `phase`.
fn record(phase: &str, line: String) -> Reply {
    if let Some(path) = std::env::var_os("NATIVE_PROBE_LOG")
        && let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
    {
        let _ = writeln!(file, "{line}");
    }
    if std::env::var("NATIVE_PROBE_FAIL").is_ok_and(|v| v == phase) {
        Reply::json(json!([
            false,
            format!("PROBE_{}_FAILED", phase.to_uppercase())
        ]))
    } else {
        Reply::json(json!([true, format!("PROBE_{}_OK", phase.to_uppercase())]))
    }
}

impl Plugin for Probe {
    const NAME: &'static str = "native_probe";
    const VERSION: &'static str = "1.0.0";
    fn manifest(&self) -> Value {
        json!({"schema_version": meerschaum_plugin::MANIFEST_SCHEMA_VERSION, "meerschaum_version": "5.0.0-rc.17",
          "python_version": "", "fingerprint": null,
          "plugins": {"native_probe": {"name": "native_probe", "version": "1.0.0",
            "data_functions": ["fetch"],
            "actions": [{"name": "probe", "doc": "Return a synthetic greeting."}],
            "arguments": [], "pre_sync_hooks": ["before"], "post_sync_hooks": ["after"],
            "connectors": [], "api_plugins": [], "web_pages": []}},
          "connector_types": {}, "pre_sync_hooks": ["before"], "post_sync_hooks": ["after"]})
    }
    fn fetch(&self, request: &Request) -> Result<Option<Frame>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        // `context.kwargs.rows` (a sync's extra keyword arguments) sets the row count.
        let rows = request.context["kwargs"]["rows"].as_i64().unwrap_or(3);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(1..=rows))],
        )
        .map_err(|e| Error::Message(e.to_string()))?;
        Ok(Some(
            Frame::from_record_batch(&batch).map_err(|e| Error::Message(e.to_string()))?,
        ))
    }
    fn call(&self, r: &Request, _data: Option<Frame>) -> Result<Reply> {
        match r.method.as_str() {
            "fetch" => self.fetch(r).map(Reply::data),
            "action" => Ok(Reply::json(json!([true, "Hello from native_probe"]))),
            "pre_sync_hook" => Ok(record(
                "pre",
                format!("pre metric={}", r.context["pipe"]["metric_key"]),
            )),
            "post_sync_hook" => Ok(record(
                "post",
                format!(
                    "post metric={} success={}",
                    r.context["pipe"]["metric_key"], r.context["kwargs"]["success_tuple"][0]
                ),
            )),
            _ => Err(Error::Unsupported(format!(
                "unsupported method {}",
                r.method
            ))),
        }
    }
}
meerschaum_plugin::export_plugin!(Probe);
