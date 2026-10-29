# meerschaum-worker

> **Internal crate.** `meerschaum-worker` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

How the Rust host runs Python (design `docs/design/v5.0.0.md` §4.2, D2, §4.3 "Plugin action
manifests", §4.4 "Adapter", §5.6 "Compatibility"):

- a **pool** of `python -m meerschaum.worker` processes, sized by `system.engine.python_workers`;
- the **wire protocol** they speak (this document is its specification);
- **`PythonConnector`**, which implements the `Connector` and `InstanceConnector` traits by
  forwarding to plugin connectors and custom `@make_connector` connectors;
- the **callback channel** that turns `pipe.sync(df)` inside plugin code into a native sync;
- **supervised streams** for v4-pattern streaming connectors and `subscribe()` connectors;
- plugin **actions, hooks and `setup()`**;
- the plugin **manifest**, its JSON schema and its fingerprinted cache.

The Python side is `meerschaum/worker/` (stdlib + `pyarrow`; `msgpack` when importable). It runs
the v4 plugin machinery unchanged (`get_connector`, `Plugin`, `Venv`, `import_plugins`,
`load_plugins`, `get_connector_plugin`, `filter_arguments` / `filter_keywords`), so plugin code
receives exactly the arguments it received on v4.

## Deviation from the spec: a separate crate

§4.1 places the worker pool under `meerschaum-cli`. The engine (`SyncExecutor` for Python
pipes, hooks, legacy streams), jobs, the server (manifest, actions over the API) and the CLI all
need it, and the CLI is a top-layer crate nothing may depend on, so the pool, protocol, adapter and
manifest live here instead. Normal dependencies: `core`, `kernel`, `connector` (no cycles; the
engine, jobs, server and CLI depend on this crate, never the reverse).

## Pool policy

- One pool per host process. Size: `system.engine.python_workers`; when the key is unset the
  fallback is the CPU count clamped to `1..=4` (`pool::default_python_workers`).
- Workers start lazily, on the first call that needs Python. Venvs are `sys.path` entries
  activated by `Venv`, so one worker serves every plugin.
- A call holds one worker exclusively. A worker runs one call at a time.
- **Crash isolation.** If the process dies (or the call times out, or the stream desyncs), only
  the running call fails (`WorkerError::Crashed` / `Timeout` / `Protocol`); the worker is
  discarded and the next call starts a fresh one. A Python exception is not fatal: the call fails
  with `WorkerError::Python` and the worker returns to the pool.
- **Backoff.** Consecutive fatal failures delay the next spawn exponentially
  (`backoff_initial`·2ⁿ, capped at `backoff_max`); a successful call resets it.
- **Timeouts** are per call (`CallOptions::timeout`, default `PoolConfig::call_timeout`). A
  timed-out worker is killed.
- **Recycling.** `WorkerPool::recycle()` replaces every worker (idle ones now, busy ones when
  their call returns) so changed plugin sources are re-imported; `ensure_manifest` calls it after
  a rebuild.
- Interpreter: `MRSM_PYTHON`, else `python3` (`python` on Windows). `PoolConfig::from_config`
  passes `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR` from the config registry.

## Protocol (version 1)

### Transport

The host binds a listener and starts the worker with `--socket ADDRESS`; the worker connects.

- **Unix:** a Unix domain socket `s.sock` in a fresh `0700` directory under the temp dir.
- **Windows:** a named pipe `\\.\pipe\mrsm-w-<pid>-<id>` (first instance, remote clients
  rejected). The Rust side compiles on Windows; the Python side opens the pipe as a file. Unix is
  the tested platform.

### Framing

Every frame is `u32 big-endian payload length | u8 kind | payload`.

| Kind | Payload |
|---|---|
| `1` control | one MessagePack map with string keys |
| `2` data | one self-contained Arrow IPC stream (schema, batches, end-of-stream) |

A control message carries `frames: n`; exactly `n` data frames follow it. Frames are limited to
4 GiB (`u32`).

### MessagePack extension types

| Code | Python type | Payload |
|---|---|---|
| 1 | `datetime` (aware or naive) | `isoformat()` as UTF-8 |
| 2 | `date` | `isoformat()` as UTF-8 |
| 3 | `Decimal` | `str()` as UTF-8 |
| 4 | `UUID` | 16 raw bytes |
| 5 | geometry (shapely) | WKB |
| 6 | `timedelta` | total seconds as a UTF-8 float string |

`bytes` travel as MessagePack `bin`; NA values (`None`, `pd.NA`, `NaT`) as `nil`; numpy scalars
as native numbers; anything else as `str()`. The stdlib fallback codec produces the same bytes as
the `msgpack` package (`tests/test_worker.py`).

### Arrow data

DataFrames (pandas, GeoPandas, Polars, pyarrow) cross as Arrow IPC. Special pandas columns carry
the kernel's field tags: `meerschaum:dtype` = `json` | `numeric` | `uuid` | `bytes` | `geometry`,
plus `ARROW:extension:name` = `arrow.json` / `geoarrow.wkb` (with the CRS in
`ARROW:extension:metadata`). Frames sent to Python (`sync_pipe(df)`) are rebuilt as pandas with
nullable integer/boolean dtypes and JSON / UUID / `Decimal` / shapely objects restored (a
GeoDataFrame when GeoPandas is importable).

### Messages

Host → worker:

| `t` | Fields | Meaning |
|---|---|---|
| `call` | `id: u64`, `op: str`, `args: map` | Run an operation. |
| `cb_result` | `cb: u64`, `value: [bool, str]`, `error?: str` | Answer to a `callback`. |

Worker → host:

| `t` | Fields | Meaning |
|---|---|---|
| `hello` | `protocol`, `pid`, `python`, `meerschaum`, `msgpack` (`msgpack` or `stdlib`) | First message after connecting. The host refuses another `protocol`. |
| `chunk` | `id`, `kind`, `value` (+ a data frame for `arrow`) | One fetch result chunk. |
| `result` | `id`, `value` (+ data frames) | The call succeeded. |
| `error` | `id`, `kind` (`NotImplemented` / `BadRequest` / `Exception`), `exception`, `message`, `traceback` | The call raised. |
| `callback` | `id` (the running call, `0` after it returned), `cb`, `op` (`sync` / `push`), `args: {pipe, kind, value, kwargs}` (+ a data frame for `arrow`) | Plugin code called `pipe.sync(df)` or a `subscribe()` sink. |

Calls are answered in order, one at a time. Callbacks may arrive at any time (from any plugin
thread) and are answered by `cb`.

A chunk or callback `kind` is `arrow` (data frame), `docs` (list of maps), `columns` (map of
lists), `text` (a JSON or simple-lines string) or `infer` (`pipe.sync()` without data). Non-Arrow
shapes stay typed so the kernel applies v4's own `parse_df_datetimes` / `enforce_dtypes` rules.

A pipe is sent as `Pipe::to_state()`: `{connector_keys, metric_key, location_key, instance_keys,
parameters}`. The worker rebuilds a v4 `Pipe(..., parameters=..., cache=False)`.

### Operations

| `op` | `args` | `result.value` |
|---|---|---|
| `ping` | `echo?` | `{pong, pid, echo}` |
| `shutdown` | — | `true`, then the worker exits |
| `describe_connector` | `connector` | `{type, label, class_name, is_instance, is_thread_safe, is_streaming, methods, has_fetch, has_sync, has_register, has_subscribe}` |
| `fetch` | `connector`, `pipe`, `begin`, `end`, `params?`, `kwargs` (`sync_chunks: true` provides a `chunk_hook` whose chunks become `sync` callbacks) | streams `chunk`s, then `{chunks, generator?, parallel, none?, hook_results}` |
| `sync` | `connector`, `pipe`, `begin`, `end`, `kwargs`, `keep_alive?` | `[success, message]` (exceptions become v4's failure tuple) |
| `subscribe` | `connector`, `pipe`, `kwargs` | the return value of `subscribe(pipe, sink)`; each `sink(df)` is a `push` callback |
| `register` | `connector`, `pipe` | `{parameters, valid, defined}` |
| `instance` | `connector`, `method`, `pipe?`, `args`, `kwargs`, `frame_arg?` (+ a data frame) | the method's return value (a DataFrame as `{kind: frame}` plus a data frame) |
| `setup` | `plugin`, `args`, `kwargs` | `[success, message]` or `bool` |
| `action` | `sysargs` or `kwargs` (`action` as words), `patch?` | `[success, message]`, as `entry_with_args` returns it; core actions are refused |
| `pre_sync_hook` / `post_sync_hook` | `pipe`, `kwargs` (v4's sync kwargs: `sync_timestamp`, `success_tuple`, `sync_duration`, `sync_complete_timestamp`, …), `plugins?` | `[{plugin, hook, success, message}, …]` in registration order |
| `build_manifest` | — | the manifest without fingerprints |
| `crash` | `code` | exits immediately; only when `MRSM_WORKER_ALLOW_CRASH` is set (tests) |

Fetch accepts everything v4 accepts: a DataFrame, a GeoDataFrame, a Polars frame, a list of dicts,
a dict of lists, a JSON or simple-lines string, `None`, `True` ("synced in parallel"), a list of
chunk-hook results, or a generator of any of these (nested generators flatten). Each generator item
is one `chunk`, sent as soon as it is produced.

## Callbacks

`CallbackHandler::handle(CallbackRequest)` receives every `pipe.sync()` made by plugin code during
a call (plugin `sync()`, `chunk_hook`, hooks) or by a long-lived stream. `InstanceSyncHandler` is
the standalone implementation: register if needed, `Pipe.enforce_dtypes` (kernel), persist newly
detected special columns, then the instance's `sync_pipe()`. The engine supplies its own handler
wrapping its native sync (autotime, ledger, observers).

## Streams (§5.6)

`PythonStream::start` runs a connector in a **dedicated** worker (outside the pool's permits):

- `StreamMode::V4Sync`: the connector's `sync()` (e.g. an MQTT plugin that subscribes and
  returns); the worker stays alive and its callback `pipe.sync()` calls reach the handler.
- `StreamMode::Subscribe`: `subscribe(pipe, sink)` for `IS_STREAMING = True` connectors; each sink
  call is a `push` callback.

The supervisor restarts the worker with exponential backoff when it exits or `sync()` fails
(`StreamEvent::{Started, Returned, Exited, Restarting, Stopped}`), until `stop()` or
`max_restarts`.

## Manifest

`schema/manifest.schema.json` (also `manifest::MANIFEST_JSON_SCHEMA`) defines the format. Per
plugin: `version`, `path`, `required`, `data_functions`, `actions` (with docstrings, `daemon` and
subactions), `arguments` (`add_plugin_argument`), `pre_sync_hooks`, `post_sync_hooks`,
`api_plugins`, `dash_plugins`, `web_pages`, `mcp_tools`, `mcp_resources`, `mcp_prompts`,
`connectors`, `import_error`, `venv_warnings` (static findings: a plugin calls `attempt_import('x')`
in the shared `mrsm` venv for a package that is neither in `required` nor declared by a Meerschaum
extra; D20) and `fingerprint`. Schema version 3 on both sides; `nargs`, `const`, `default`,
`choices` and `metavar` of an argument are optional; at the top level, `connector_types` and the plugin
lists the server's `HostManifest` reads (`api_plugins`, `dash_plugins`, `web_pages`, `mcp_tools`).

The cache is `<root>/cache/plugins_manifest.json`. `ensure_manifest` hashes each plugin source tree
(relative paths, sizes and modification times; `__pycache__` and `.pyc` excluded) and rebuilds in
a fresh worker when any plugin was added, removed or changed, or the schema version changed.
Plugins are discovered like v4: plugin directories in order, entries sorted, first name wins.

## Tests

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo test -p meerschaum-worker
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python -m pytest tests/test_worker.py
```

The integration tests (`tests/*.rs`) copy the synthetic plugins in `tests/plugins/` into a
temporary plugins directory, use a temporary root (with `venvs/mrsm` linked to the repo's
`test_root/venvs/mrsm` so v4 finds pandas and SQLAlchemy without installing), and compare results
with v4: every fetch shape, a plain and an instance custom connector, a plugin `sync()` with
callbacks and plugin actions are synced or run both ways and read back through v4's `get_data()`.
They skip when `python3` lacks `pyarrow` and `pandas`.

## Known gaps

- The Windows named-pipe transport is untested.
- `InstanceSyncHandler` covers dtype enforcement, special columns and `sync_pipe`; autotime,
  `filter_existing` observers and the ledger belong to the engine's handler.
- Users, plugins and tokens on a Python instance connector use the trait defaults (generic
  implementation over the connector's pipe methods); Python overrides are not forwarded.
- PARITY (deliberate): v4's CLI runs a module-named plugin action (`def <plugin_name>()`) only
  until the plugin's venv exists; the worker always runs it.

## Rust-host protocol extensions

The Rust binary embeds `python/host.py` and starts it with the selected SDK interpreter. It
registers `host_action` (plugin dispatch with native Pipe.sync callbacks), `parse_plugin_args`,
`complete` and an extended `build_manifest`. Every standard action runs natively in the binary, so there is no
Python-side operation for them (the former `core_action` is gone). Captured action
and completion replies contain `result`, `stdout`, and `stderr`; other worker diagnostics go to
stderr so MCP stdio stays clean. Interactive `python` is launched by the CLI directly.

Manifest schema version 2 adds `actions[].has_completer`; version 3 adds `web_pages[].mode` (`html` or `dash`, found by calling the page function once like v4) and `web_pages[].route`. An empty plugin tree is fingerprinted
and cached entirely in Rust, including when no Python interpreter is installed.

Manifest staleness is idempotent: the fingerprint covers the `.py` files of every plugin Rust
discovers plus every manifest-listed plugin outside the plugins directories that reported a
`path` (pip entry-point plugins), and it is taken after the worker imported the plugins, so an
unchanged tree is never stale whatever Python reported. `ensure_manifest` recycles the pool only
when it really rebuilt; `rebuild_manifest` forces a rebuild (the `reload` action).

Nested calls from native callback processing have a bounded reserve in the same pool, so a
plugin syncing into a Python instance cannot deadlock a size-one pool. Main-call concurrency
still uses `python_workers`; callback calls may temporarily use up to eight reserve workers
per configured worker, with at most eight nested levels. Reserve exhaustion fails immediately.
Idle workers are trimmed back to the configured pool size. Hosts relaying callbacks through
channels use `pool::callback_scope` around the receiving native sync operation.

CLI action calls request `stream_output`: console writes arrive immediately as text chunks
containing JSON `{stream, text}` and the reply's captured strings are empty. This keeps custom
loops from buffering unbounded output and shows prompts before actions finish. A CLI attached to
a terminal enables `WorkerOptions.inherit_stdin`; stdio servers and ordinary pool users leave it
disabled. Shell-patched action kwargs retain datetime/timedelta MessagePack extension types.
