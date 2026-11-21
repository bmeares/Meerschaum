# Executors and Streaming Connectors in Rust Plugins

A Rust plugin can provide a **job executor** (native counterpart of `@make_executor`) and **streaming connectors** for `mrsm sync pipes --listen` (counterpart of `IS_STREAMING` / `subscribe`). Both run in-process without starting a Python worker.

See the complete, tested examples: [`native_executor.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_executor.rs) (`procs:<label>`) and [`native_stream.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_stream.rs) (`ticker:<label>`).

## 👷 Executors

An executor manages [background jobs](../../background-jobs/executors.md) by name. Declare its type under `executor_types` in `manifest()` and return it from `Plugin::executor`:

```rust
use meerschaum_plugin::executor::{Executor, mrsm_command};
use meerschaum_plugin::prelude::*;

#[derive(Default)]
struct Procs;

impl Plugin for Procs {
    fn manifest(&self) -> Value {
        Manifest::new()
            .executor_type("procs", ["get_job_names", "get_job_metadata", "create_job"], "Run jobs as local processes.")
            .into()
    }

    fn executor(&self, request: &Request) -> Option<Box<dyn Executor>> {
        (request.connector_type == "procs").then(|| Box::new(ProcsExecutor) as _)
    }
}

struct ProcsExecutor;

impl Executor for ProcsExecutor {
    fn get_job_names(&self) -> Result<Vec<String>> { Ok(vec![]) }
    fn get_job_metadata(&self, _name: &str) -> Result<Value> { Ok(Value::Null) }
    fn create_job(&self, name: &str, _sysargs: &[String], _properties: &Value) -> Result<SuccessTuple> {
        let _cmd = mrsm_command(); // host's own `mrsm` command
        Ok(SuccessTuple::ok(format!("Started job '{name}'.")))
    }
}

meerschaum_plugin::export_plugin!(Procs);
```

Use it anywhere executor keys are accepted (CLI `-e`, shell `executor`, `meerschaum:executor` config, Compose, or `mrsm.Job(..., executor_keys='procs:main')`):

```bash
mrsm sync pipes -c plugin:noaa -d -e procs:main --name noaa
mrsm show jobs -e procs:main
mrsm show logs noaa -e procs:main
mrsm stop jobs noaa -e procs:main
```

Executor config comes from `meerschaum:connectors:procs:<label>` (or `MRSM_PROCS_<LABEL>`). Every method may make [host calls](advanced.md#talking-to-meerschaum) with the calling process's trust. The API server's `/jobs` routes, web console, and MCP job tools manage local server jobs only.

### The methods

`Executor` mirrors `meerschaum.jobs.Executor`. Unimplemented methods return "not supported" (or derive from other methods):

| Method | Returns | Default |
|---|---|---|
| `get_job_names()` | All job names (including hidden) | not supported |
| `get_job_exists(name)` | `bool` | `name` in `get_job_names()` |
| `get_jobs()` | `{name: metadata}` | `get_job_names()` + `get_job_metadata()` |
| `get_job_metadata(name)` | Metadata JSON, or `Value::Null` | not supported |
| `get_job_properties(name)` | Job properties JSON | `metadata.daemon.properties` |
| `get_job_status(name)` | `running`, `paused`, or `stopped` | `metadata.daemon.status` |
| `get_job_began` / `_ended` / `_paused(name)` | ISO timestamps | `properties.process.began` / `ended` / `paused` |
| `create_job(name, sysargs, properties)` | `SuccessTuple` (create **and** start) | not supported |
| `start_job` / `stop_job` / `pause_job` / `delete_job(name)` | `SuccessTuple` | not supported |
| `get_logs(name)` / `get_job_stop_time(name)` | Log text / ISO timestamp | not supported |
| `monitor_logs(name, options)` | `Box<dyn LogStream>` | not supported |
| `get_job_is_blocking_on_stdin(name)` | `bool` | not supported |
| `get_job_prompt_kwargs(name)` | Pending prompt kwargs | not supported |
| `write_stdin(name, data)` | Write one `\n`-terminated line | not supported |

Expected metadata shape:

```json
{"sysargs": ["sync", "pipes"], "result": [true, "Success"], "restart": false,
 "daemon": {"status": "running", "pid": 4242,
            "properties": {"process": {"began": "2026-01-01T00:00:00+00:00", "ended": null}}}}
```

Key behaviors:

- **Spawning & state:** Start jobs with `executor::mrsm_command()` + `sysargs` (`sysargs` never contains `-d` or `-e`). A fresh executor is built per call, so store state in files keyed by `request.context["root_dir"]` (not `MRSM_ROOT_DIR`, since Compose manages multiple roots), a DB, or a static map.
- **Following logs:** `monitor_logs` returns a `LogStream` whose `next_lines(timeout)` yields `LogEvent::Lines(vec)`, `LogEvent::Timeout`, or `LogEvent::End(Some(result))` (when `options.stop_on_exit` is set).
- **Prompts:** On each `LogEvent::Timeout` during interactive `show logs` / foreground `start job`, the host polls `get_job_is_blocking_on_stdin` and forwards answers via `write_stdin`.

## 📡 Streaming connectors

Mark a connector `"is_streaming": true` with `"subscribe"` in its `methods` (or list `subscribe` under `data_functions` for `plugin:<name>`) and implement `Plugin::subscribe`:

```rust
use meerschaum_plugin::subscribe::{Batch, ChannelSubscription, Subscription};

fn subscribe(&self, request: &Request) -> Result<Box<dyn Subscription>> {
    let topic = request.context["pipe"]["parameters"]["fetch"]["topic"].clone();
    let (sender, subscription) = ChannelSubscription::new(16);
    std::thread::spawn(move || {
        for frame in read_messages(topic) {
            if sender.send(frame).is_err() { return; }
        }
    });
    Ok(Box::new(subscription))
}
```

```bash
mrsm register pipe -c ticker:office -m readings -i sql:local
mrsm sync pipes -c ticker:office -i sql:local --listen
```

The host pulls batches via `next_batch(timeout)` and feeds the micro-batching sink (`parameters['stream']`: `idle_ms`, `max_latency_ms`, `max_rows`, `max_bytes`), naturally backpressuring slow instances:

| Return | Meaning |
|---|---|
| `Batch::data(frame)` | Rows to sync. |
| `Batch::with_ack(frame, token)` | Rows whose `token` is passed to `ack(tokens)` after commit (empty frames still ack on next flush). |
| `Batch::Timeout` | Nothing new before `timeout`. |
| `Batch::End` | Stream ended; engine resubscribes after backoff. |

Errors from `subscribe` or `next_batch` close the stream, emit `stream_restarted`, and resubscribe with exponential backoff. Ack tokens are per-subscription (safe to reuse across reconnects), `close()` runs on stop, and committed flushes trigger `--listen --children`. `next_batch`, `ack`, and `close` run concurrently with other plugin calls and may make [host calls](advanced.md#talking-to-meerschaum) on their own thread (producer threads of `ChannelSubscription` cannot).

## 📄 Single-file plugins

Declare executors with `#[make_executor]` (custom name via `#[make_executor(name = "...")]`; `local`, `systemd`, `api`, `none` are reserved) and streaming connectors with `fn subscribe` inside `#[make_connector]` (or top-level for `plugin:<name>`):

```rust
#[make_executor]
mod procs {
    fn get_job_names(executor: &ExecutorInfo) -> Vec<String> { vec![] }
    fn get_job_metadata(executor: &ExecutorInfo, name: &str) -> Value { Value::Null }
    fn create_job(executor: &ExecutorInfo, name: &str, sysargs: Vec<String>, properties: &Value) -> (bool, String) {
        (true, format!("Started job '{name}' under {}.", executor.root_dir.display()))
    }
    fn monitor_logs(name: &str, stop_on_exit: bool) -> impl FnMut(Duration) -> LogEvent {
        move |_timeout| LogEvent::End(None)
    }
}

#[make_connector]
mod ticker {
    fn subscribe(pipe: &Pipe, kwargs: &Value) -> Result<impl FnMut(Duration) -> Batch> {
        let mut next = 0;
        Ok(move |_timeout: Duration| {
            next += 1;
            Batch::RowsAck(vec![json!({"id": next})], next)
        })
    }
    fn ack(tokens: &[u64]) -> Result<()> { Ok(()) }
}
```

`ExecutorInfo` provides `executor_type`, `label`, `config`, `root_dir`, `mrsm`, and `keys()`. Single-file `Batch` variants are `Rows(rows)`, `RowsAck(rows, token)`, `Data { rows, ack, redelivered }`, `Timeout`, and `End`; you may also return `impl Subscription` or `Box<dyn Subscription>` (`ChannelSubscription`). Without `monitor_logs`, only `show logs --nopretty` (`get_logs`) displays logs. See [Single-file reference](advanced.md#single-file-reference) for general single-file rules.

## 🔌 The wire contract

Direct C ABI plugins answer these JSON methods (rows travel as Arrow C streams, or JSON `data` under the rows ABI):

| Method | Context | Reply |
|---|---|---|
| `executor.<method>` | `{name?, sysargs?, properties?, mrsm, root_dir?}` | Method JSON value (status `3` if unsupported) |
| `executor.write_stdin` | `{name, data, mrsm, root_dir?}` | `null` |
| `executor.monitor_logs` | `{name, strip_timestamps, stop_on_exit, mrsm, root_dir?}` | `{"handle": u64}` |
| `executor.next_logs` | `{handle, timeout_ms}` | `{"status": "lines" \| "timeout" \| "end", "lines": [...], "result": [bool, str] \| null}` |
| `executor.close_logs` | `{handle}` | `null` |
| `subscribe` | `{pipe, kwargs}` | `{"handle": u64}` |
| `next_batch` | `{handle, timeout_ms}` | `{"status": "batch" \| "timeout" \| "end", "ack": u64 \| null, "redelivered": bool}` + output stream |
| `ack_batches` / `close_subscription` | `{handle, tokens?}` | `null` |
