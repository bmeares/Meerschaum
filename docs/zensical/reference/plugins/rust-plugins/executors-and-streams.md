# Executors and Streaming Connectors in Rust Plugins

A Rust plugin can provide a **job executor** (the native counterpart of `@make_executor`) and
**streaming connectors** whose data `mrsm sync pipes --listen` lands as it arrives (the
counterpart of an `IS_STREAMING` connector's `subscribe`). Both run in-process; no Python worker
is started for them.

The complete, tested examples are
[`native_executor.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_executor.rs)
(a `procs:<label>` executor that runs each job as a local process) and
[`native_stream.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_stream.rs)
(a `ticker:<label>` streaming connector and a plugin-level `subscribe`).

## 👷 Executors

An executor manages [background jobs](../../background-jobs.md#executors) by name. Declare its
type under the manifest's top-level `executor_types` and return it from `Plugin::executor`:

```rust
use meerschaum_plugin::executor::{Executor, mrsm_command};
use meerschaum_plugin::prelude::*;

#[derive(Default)]
struct Procs;

impl Plugin for Procs {
    fn manifest(&self) -> Value {
        Manifest::new()
            .executor_type(
                "procs",
                ["get_job_names", "get_job_metadata", "create_job"],
                "Run jobs as local processes.",
            )
            .into()
    }

    fn executor(&self, request: &Request) -> Option<Box<dyn Executor>> {
        // `request.label` is the label of `procs:<label>`, `request.config` its configuration,
        // `request.context["root_dir"]` the Meerschaum root the jobs belong to.
        (request.connector_type == "procs").then(|| Box::new(ProcsExecutor) as _)
    }
}

struct ProcsExecutor;

impl Executor for ProcsExecutor {
    fn get_job_names(&self) -> Result<Vec<String>> { /* ... */ Ok(vec![]) }
    fn get_job_metadata(&self, name: &str) -> Result<Value> { /* ... */ Ok(Value::Null) }
    fn create_job(&self, name: &str, sysargs: &[String], properties: &Value) -> Result<SuccessTuple> {
        // Start `mrsm <sysargs>` with `mrsm_command()` (the host's own `mrsm`), then report.
        Ok(SuccessTuple::ok(format!("Started job '{name}'.")))
    }
    // start_job, stop_job, delete_job, get_logs, monitor_logs, ...
}

meerschaum_plugin::export_plugin!(Procs);
```

Then use `procs:<label>` wherever executor keys are accepted:

```bash
mrsm sync pipes -c plugin:noaa -d -e procs:main --name noaa
mrsm show jobs -e procs:main
mrsm show logs noaa -e procs:main
mrsm stop jobs noaa -e procs:main
```

The shell's `executor procs:main` command, `meerschaum:executor` in your configuration (also
in a compose project's `config`) and `mrsm.Job(..., executor_keys='procs:main')` in Python work
the same way; in Python, `job.executor` is a `NativeExecutor` with the methods of
`meerschaum.jobs.Executor`. Shell and command-line completion suggest the declared types
(`procs:main`, or the configured labels). The executor's configuration is the connector
configuration of `procs:<label>` (`meerschaum:connectors:procs:<label>` in `edit config`, or
`MRSM_PROCS_<LABEL>`). Every method may make
[host calls](advanced.md#talking-to-meerschaum) (`host::plugin_config()`, `host::pipe(...)`),
with the trust of the process that uses the executor.

The API server's `/jobs` routes (and the web console and MCP job tools) manage the server's
local jobs only, as in v4; plugin executors are reached from the command line, the shell,
configuration, compose and Python.

### The methods

`Executor` mirrors Python's `meerschaum.jobs.Executor`. Every method has a default, so you
implement only what your executor supports:

| Method | Returns | Default |
|---|---|---|
| `get_job_names()` | every job, hidden ones included | not supported |
| `get_job_exists(name)` | `bool` | `name` in `get_job_names()` |
| `get_jobs()` | `{name: metadata}` | `get_job_names()` + `get_job_metadata()` |
| `get_job_metadata(name)` | metadata, or `Value::Null` when the job does not exist | not supported |
| `get_job_properties(name)` | the job's properties | `metadata.daemon.properties` |
| `get_job_status(name)` | `running`, `paused` or `stopped` | `metadata.daemon.status` |
| `get_job_began` / `get_job_ended` / `get_job_paused(name)` | ISO timestamps | `properties.process.began` / `ended` / `paused` |
| `create_job(name, sysargs, properties)` | success tuple (create **and** start) | not supported |
| `start_job` / `stop_job` / `pause_job` / `delete_job(name)` | success tuple | not supported |
| `get_logs(name)` | the log text | not supported |
| `get_job_stop_time(name)` | ISO timestamp of a manual stop | not supported |
| `monitor_logs(name, options)` | a `LogStream` (see below) | not supported |
| `get_job_is_blocking_on_stdin(name)` | `bool` | not supported |
| `get_job_prompt_kwargs(name)` | the pending prompt's keyword arguments | not supported |
| `write_stdin(name, data)` | write one answer line to the job's stdin | not supported |

Metadata has the shape the local and `api:` executors report:

```json
{"sysargs": ["sync", "pipes"], "result": [true, "Success"], "restart": false,
 "daemon": {"status": "running", "pid": 4242,
            "properties": {"process": {"began": "2026-01-01T00:00:00+00:00", "ended": null}}}}
```

A method you leave out is reported to the user as
`` `pause_job` is not supported by the 'procs:main' executor. `` (actions fail with that message;
listings show such a job as `stopped` without timestamps). Any other error from a method (an I/O
error, a panic) is printed as a warning naming the executor and the method, so a broken executor
is never silently shown as a stopped job. The `sysargs` given to `create_job`
never contain `-d` or `-e` / `--executor-keys`.

**Starting a job.** A job runs `mrsm <sysargs>`. Use `executor::mrsm_command()` for the program:
it is the host's own command (the `mrsm` binary, or `python -P -m meerschaum` when Meerschaum runs
from the Python package), so the job uses the same installation and environment.

**State.** A new executor value is built for every call, so keep job state outside it: in files
(as the example does under `<root_dir>/procs_jobs/<label>/`), a database, or a static map keyed by
label. Key file state on `request.context["root_dir"]`, not on `MRSM_ROOT_DIR`: one process may
manage the jobs of several roots (`mrsm compose` runs in the host's root and manages the jobs of
each project's root).

**Following logs.** `monitor_logs` returns a `LogStream`; the host calls
`next_lines(timeout)` until you return `LogEvent::End(result)` (only when
`options.stop_on_exit` is set and the job stopped) or the user stops watching:

```rust
impl LogStream for Tail {
    fn next_lines(&mut self, timeout: Duration) -> Result<LogEvent> {
        // Return LogEvent::Lines(new_lines), LogEvent::Timeout after `timeout`,
        // or LogEvent::End(Some(result)).
    }
}
```

**Prompts.** While the host follows a job's log with input accepted (`show logs`, or `start job`
in the foreground), each `LogEvent::Timeout` makes it ask `get_job_is_blocking_on_stdin`; when
that is `true`, it reads the user's answer and calls `write_stdin(name, data)` (one line, ending
in `\n`). Executors without those two methods never receive input.

## 📡 Streaming connectors

A streaming connector pushes rows as they arrive instead of answering `fetch`. Mark the
connector type `"is_streaming": true` with `"subscribe"` in its `methods` (or list `subscribe`
under the plugin's `data_functions` for `plugin:<name>`) and implement `Plugin::subscribe`:

```rust
use meerschaum_plugin::subscribe::{Batch, ChannelSubscription, Subscription};

fn subscribe(&self, request: &Request) -> Result<Box<dyn Subscription>> {
    // `request.context["pipe"]` is the pipe, `request.context["kwargs"]` the sync's keyword
    // arguments (`begin`, `end`, `params`, `chunksize`, `check_existing`, plugin flags, ...),
    // the same ones a Python `subscribe(pipe, ..., **kwargs)` receives.
    let topic = request.context["pipe"]["parameters"]["fetch"]["topic"].clone();
    // A producer thread feeds a bounded channel: `send` blocks while the host is busy.
    let (sender, subscription) = ChannelSubscription::new(16);
    std::thread::spawn(move || {
        for frame in read_messages(topic) {
            if sender.send(frame).is_err() {
                return; // the host closed the subscription
            }
        }
    });
    Ok(Box::new(subscription))
}
```

Then listen:

```bash
mrsm register pipe -c ticker:office -m readings -i sql:local
mrsm sync pipes -c ticker:office -i sql:local --listen
```

The host **pulls**: it calls `next_batch(timeout)` on your subscription, hands each batch to the
engine's micro-batching sink (`parameters['stream']`: `idle_ms`, `max_latency_ms`, `max_rows`,
`max_bytes`) and asks again only once the sink accepted it, so a slow instance slows your source
down instead of filling memory. `next_batch` may block (on a channel, a socket, a sleep) up to the
timeout:

| Return | Meaning |
|---|---|
| `Batch::data(frame)` | rows to sync |
| `Batch::with_ack(frame, token)` | rows whose `token` your `ack(tokens)` receives once they committed (acknowledge-after-commit, like MQTT QoS 1 or Kafka offsets). An empty frame with a token (a filtered message) is acknowledged with the next committed flush |
| `Batch::Timeout` | nothing new yet |
| `Batch::End` | the source ended; the engine subscribes again after a backoff |

An error from `next_batch` (or `subscribe`) closes the subscription and the engine resubscribes
with exponential backoff, emitting `stream_restarted`. Tokens belong to the subscription that
returned them: `ack` only ever receives tokens of its own subscription. Batches still buffered when
a subscription ended are never acknowledged on the next one (their source redelivers them), so a
token may be reused across subscriptions (MQTT packet ids, per-session offsets). `close()` is called once when the listener
stops. Committed flushes trigger the stream's children (`--listen --children`) exactly like a
Python `subscribe` stream. A subscription may implement `Subscription` directly instead of using
`ChannelSubscription`; the example's `ticker` does, with no thread at all.

`next_batch`, `ack` and `close` are not serialized with your plugin's other calls (one blocking
`next_batch` never holds up a `fetch` or another subscription), so share state between them only
through synchronized types. `subscribe` and each of these calls may make
[host calls](advanced.md#talking-to-meerschaum) (`pipe.get_sync_time()`, `host::plugin_config()`,
...) on their own thread, so a source can resume from the pipe's newest row or keep a cursor in
its config; a producer thread of a `ChannelSubscription` cannot.

## 🔌 The wire contract

Executors and subscriptions are ordinary JSON methods of the plugin ABI; data crosses as an Arrow
C stream like `fetch`. The SDK implements them for you; a plugin written against the C ABI
directly answers:

| Method | Context | Reply |
|---|---|---|
| `executor.<method>` (every method above) | `{name?, sysargs?, properties?, mrsm, root_dir?}` | the method's JSON value; status 3 when unsupported |
| `executor.write_stdin` | `{name, data, mrsm, root_dir?}` | `null` |
| `executor.monitor_logs` | `{name, strip_timestamps, stop_on_exit, mrsm, root_dir?}` | `{"handle": u64}` |
| `executor.next_logs` | `{handle, timeout_ms}` | `{"status": "lines" \| "timeout" \| "end", "lines": [...], "result": [bool, str] \| null}` |
| `executor.close_logs` | `{handle}` | `null` |
| `subscribe` | `{pipe, kwargs}` | `{"handle": u64}` |
| `next_batch` | `{handle, timeout_ms}` | `{"status": "batch" \| "timeout" \| "end", "ack": u64 \| null, "redelivered": bool}`, rows in the output stream |
| `ack_batches` | `{handle, tokens}` | `null` |
| `close_subscription` | `{handle}` | `null` |

For `executor.*` calls, the request's `connector_type` and `label` are the executor keys and
`config` is the executor's configuration. `mrsm` is the host's command (see above) and `root_dir`
the root the jobs belong to. For `subscribe`, `kwargs` are the sync's keyword arguments. A
`next_batch` reply with status `batch`, an `ack` and no rows is an empty batch whose token is
still acknowledged.

## 📄 Single-file plugins

A single `.rs` file declares both without a Cargo project. An executor is a module marked
`#[make_executor]`, named after the executor type (`#[make_executor(name = "...")]` to choose
another). Its functions are the methods of the table above with the same names; a method you
leave out answers "not supported", and the derived ones (`get_job_exists`, `get_jobs`,
`get_job_properties`, `get_job_status`, `get_job_began` / `_ended` / `_paused`) fall back on the
others as in the SDK:

```rust
#[make_executor]
mod procs {
    // Any function may take `executor: &ExecutorInfo`: `executor.label`, `executor.config`,
    // `executor.root_dir` (the root the jobs belong to) and `executor.mrsm` (the command that
    // starts `mrsm <sysargs>`).
    fn get_job_names(executor: &ExecutorInfo) -> Vec<String> { /* ... */ }
    fn get_job_metadata(executor: &ExecutorInfo, name: &str) -> Value { /* null when missing */ }
    fn create_job(executor: &ExecutorInfo, name: &str, sysargs: Vec<String>, properties: &Value) -> (bool, String) { /* ... */ }
    fn stop_job(name: &str) -> (bool, String) { /* ... */ }
    fn get_logs(executor: &ExecutorInfo, name: &str) -> Option<String> { /* ... */ }
    fn write_stdin(name: &str, data: &str) -> Result<()> { /* ... */ }
    // Each call is one `next_logs`. `show logs` follows a job through this method; without
    // it, only `show logs --nopretty` (which reads `get_logs`) shows the logs.
    fn monitor_logs(name: &str, stop_on_exit: bool) -> impl FnMut(Duration) -> LogEvent { /* ... */ }
}
```

Every function may also return a `Result` of its type, and `SuccessTuple` is accepted wherever
`(bool, String)` is.

A streaming connector is a `fn subscribe` inside a `#[make_connector]` module (or at the top
level, for `plugin:<name>`). It receives the pipe, the connector and the sync's keyword arguments
and returns a closure the host calls for each `next_batch`, a value of a type implementing
`Subscription`, or the SDK's `Box<dyn Subscription>` (a `ChannelSubscription` fed by a producer
thread works too). A `subscribe` with other parameters or another return type is an ordinary
helper:

```rust
#[make_connector]
mod ticker {
    fn subscribe(pipe: &Pipe, kwargs: &Value) -> Result<impl FnMut(Duration) -> Batch> {
        let mut next = 0;
        Ok(move |_timeout: Duration| {
            next += 1;
            // Batch::Rows(rows), Batch::RowsAck(rows, token), Batch::Data { rows, ack,
            // redelivered }, Batch::Timeout or Batch::End.
            Batch::RowsAck(vec![json!({"id": next})], next)
        })
    }
    // Optional: called with the committed tokens.
    fn ack(tokens: &[u64]) -> Result<()> { Ok(()) }
}
```

Rows are JSON objects (`Vec<Value>`) as `fetch` returns them; the rows ABI carries them in the
`next_batch` reply's `data`, with the same `status`, `ack` and `redelivered` fields as the table
above. The closure may block up to its timeout. Dropping it closes the subscription. The
[single-file reference](advanced.md#executors) lists every parameter and return type.
