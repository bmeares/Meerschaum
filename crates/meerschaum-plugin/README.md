# Native Rust plugins

> The native plugin SDK of [Meerschaum](https://meerschaum.io). Its C ABI (`mrsm_plugin_v1`) is versioned
> and stable independently of the Rust types; also re-exported by the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate behind the `plugin` feature.

`meerschaum-plugin` implements the native plugin boundary from design §9. It depends on
core, kernel and connector; it does not depend on the engine, CLI, worker or an instance
backend. `examples/native_demo.rs` is a complete source connector and in-memory instance,
with an action, arguments and sync hooks. No Python process is needed.

## Authoring and installation

Implement the safe `Plugin` trait and call `meerschaum_plugin::export_plugin!(MyPlugin)` once
in a `cdylib` crate. `fetch` returns `Option<Frame>`; `sync` accepts optional data and returns
JSON (normally a serialized `SuccessTuple`). Override `call` for instance methods, actions,
register, setup and hooks. `Request` contains the method, connector type, label, config and
context; every hook kwarg is forwarded unchanged inside `context.kwargs`. Constructors and
calls are enclosed in `catch_unwind`, including serialization and frame conversion. Build
with `panic = "unwind"`; an aborting panic, process exit or native memory fault cannot be
recovered by the host.

The SDK constructs `Default` for each call, so plugin structs are stateless handlers. Keep
shared state in a synchronized store keyed by connector label/config. The example's single
static frame is intentionally only a small demonstration, not a general instance backend.
The SDK imposes no async runtime. Native calls are synchronous; async applications should
schedule potentially blocking calls on their blocking executor.

`Request` is the low-level dispatch envelope. Its `pipe()` method provides a typed Pipe
view; the optional host-call ABI extension supports `get_sync_time()` and `sync(data)`
for the current Pipe during source fetch and sync calls. Host writes use the normal
sync engine with supplied input, avoiding recursive plugin dispatch. Arbitrary connector
access, other Pipe construction and Python helper imports are not exposed.

Build the example with `cargo build -p meerschaum-plugin --example native_demo`. Install the
result in a configured plugin directory:

```text
plugins/native_demo/plugin.toml
plugins/native_demo/libnative_demo.so
```

`plugin.toml` contains top-level `name` and `version` strings (see `examples/plugin.toml`). Instead
of installing the library you can install the crate and let Meerschaum build it (see "Source plugins").
On macOS use `libnative_demo.dylib`; on Windows install as `libnative_demo.dll` (rename Cargo's
`native_demo.dll`). `discover` accepts ordered configured directories; `environment_directories`
parses the OS path-list in `MRSM_PLUGINS_DIR`, without an implicit user-home fallback. Earlier
directories win; entries inside each directory are sorted. A directory containing both
`plugin.toml` and Python files is native. The caller must exclude discovered native names
from Python discovery. An invalid plugin (bad descriptor, missing or unloadable library, metadata mismatch) is skipped with a
`Warning: Skipping invalid native plugin at '<dir>': <reason>` on stderr and the rest keep loading
(PARITY: v4 warns about a broken Python plugin and loads the others); `discover_report` and
`cached_manifest_report` return the skipped plugins as data.

## Source plugins

A plugin directory that is a Cargo crate (`plugin.toml`, `Cargo.toml`, `src/`) and has no
up-to-date `lib<name>.<ext>` next to `plugin.toml` is built by the host (`source.rs`):
`cargo build --release --manifest-path <dir>/Cargo.toml --target-dir <build>/<name>` with
`CARGO_INCREMENTAL=0`, where `<build>` is `<root>/cache/native_build`. The plugins directory is
written only by Cargo itself, and only `Cargo.lock` when it is missing or stale (the lock written
during a build is included in the stamped fingerprint, so it does not cause a rebuild). `cargo` is `$CARGO` or the first one on `PATH`. The crate's package name must equal
the `plugin.toml` name and `[lib] crate-type` must contain `cdylib`; otherwise the plugin is
skipped as invalid. Without `cargo` the plugin is skipped with `Warning: Skipping native plugin
at '<dir>': it is a source crate and no `cargo` was found on PATH (install a Rust toolchain or
ship lib<name>.<ext>)`; a failed build is skipped with the last 20 lines of cargo's stderr. The
other plugins still load.

* **Fingerprint.** For a source plugin `Candidate::fingerprint` is the content hash of
  every file in the plugin directory except `target/`, hidden entries other than `.cargo/`, and shared
  libraries (so `include_str!` assets and `build.rs` inputs count; not mtimes,
  so a checkout does not trigger a build). After a successful build the hash is written to
  `<build>/<name>/mrsm-source-fingerprint`; a build is current when that stamp matches and the
  library exists. The manifest cache keys on the same fingerprint, so a cache hit builds nothing.
* **Library path.** `Candidate::library` is the file Cargo produced
  (`<build>/<name>/release/lib<libname>.<ext>`; hyphens in the package name become underscores);
  `Candidate::source_crate` is true and the manifest entry gets a `library` field with the path.
* **Failed builds are remembered.** A failed build writes `<build>/<name>/mrsm-build-failed` with the
  source fingerprint; until the sources change or the build directory is deleted the plugin is skipped
  with `the last build failed; edit the sources or delete '<dir>' to retry` and cargo is not run.
* **A shipped library.** If `lib<name>.<ext>` exists next to the sources it is used unless a build
  input is newer than it; when it is stale and there is no `cargo`, it is still used, with a warning
  (`Using native plugin at '<dir>': its library is older than the sources and was not rebuilt`).
* **API.** `discover_report` / `discover_report_in(dirs, build_dir)` only scan: a source plugin that
  needs building comes back with `Candidate::build` set and no library yet.
  `Discovery::build_pending` runs the builds (a failure moves the plugin to `skipped` with
  `Skipped::source_crate`). `discover`, `cached_manifest(_report)` and `NativePlugins::open` build
  before they read the manifest, using `native_build` next to the cache file. Without an explicit
  build directory the root comes from `meerschaum-core`'s `Paths::from_env` (`MRSM_ROOT_DIR`).
  Path dependencies outside the plugin directory are not fingerprinted.

## ABI v1 and ownership

Native plugins and Python workers share manifest schema 5, separately from ABI v1 and
worker protocol v1. Return `MANIFEST_SCHEMA_VERSION` from the SDK in your manifest rather
than a version literal. A prerelease native library exporting schema 3 must be rebuilt
against the current SDK (and its SDK dependency updated); refreshing the manifest cache
cannot rewrite the library's exported `describe` metadata. Aggregate caches from an older
schema are regenerated automatically. Unsupported manifest versions are rejected.

The only exported symbol is `mrsm_plugin_v1`, returning a static `#[repr(C)] MrsmPluginV1`.
The permanent prefix contains two `u32`s: ABI version and structure size. The host reads only
this prefix before rejecting incompatible or truncated tables. Breaking changes require a
new symbol (`mrsm_plugin_v2`) and version; extending the JSON method namespace is additive.
The host should keep the v1 loader for at least one major release after v2 is introduced.

We use a hand-written C ABI instead of `abi_stable`: a small set of C primitives, explicit
allocator callbacks and the standardized Arrow interface suffices, without coupling plugins
to Rust trait-object layouts, compiler versions, async runtimes or an additional ABI type
system. Host and plugin must still target the same OS and architecture. The vtable uses
`describe`, `invoke` and `free_bytes`. Metadata (name, version and ABI version) and the manifest
come from `describe`. `invoke` multiplexes method names instead of adding a function pointer
for every connector method; this deliberately refines the illustrative per-method vtable in
§9.1 while retaining versioned C calls and the same operations.

* Borrowed `Bytes` remain owned by the caller until the call returns. JSON is UTF-8.
* `CallResult` has status 0 (success JSON), 1 (error UTF-8), 2 (caught panic UTF-8),
  or 3 (unsupported method UTF-8, mapped to `ConnectorError::NotImplemented`).
  Every payload is an exporting-library allocation, released exactly once using `free_bytes`.
* The input and output `FFI_ArrowArrayStream` pointers are valid, exclusive and non-null.
  Output initially has no release callback. The receiver moves input, leaving an empty stream.
  On every outcome the caller releases any remaining input/output stream.
* Full SDK frame data never uses JSON: stream callbacks exchange `FFI_ArrowArray` and `FFI_ArrowSchema`, with
  their standard producer-owned release callbacks. SDK output is an eager, one-batch stream;
  arbitrary conforming C plugins may return multiple batches. SDK callbacks contain only
  Arrow-owned schema/array operations over already materialized frames, not author code.
* The host consumes streams and materializes kernel frames while the library is alive.
  No borrowed callback or imported Arrow allocation escapes the call. The loader retains a
  library handle in every adapter and serializes calls through its mutex (except the stream
  calls and the server calls `api_route` / `mcp_*`, which run concurrently). Plugins must not
  leave background threads or callbacks running after their last owner drops.
* `LoadedPlugin::load` and `cached_manifest` are unsafe because executing arbitrary native
  code requires trust and ABI conformance. All SDK author methods and loaded-plugin calls
  are safe. This is an in-process ABI, not a security sandbox.

`LoadedPlugin::source` returns the standard `plugin:<name>` source adapter, without requiring
a custom connector declaration. `LoadedPlugin::connector` returns a `NativeConnector` implementing both connector traits;
check `is_instance()` before registering it as an instance. Required and optional pipe,
maintenance and lease operations are dispatched over the ABI. Generic user/plugin/token
operations remain the connector trait's implementations over internal pipes. `call` exposes
additional operations; `LoadedPlugin::action` and `hook` offer typed `SuccessTuple` results.

The control context for pipe operations is `{ "pipe": { "keys": ..., "parameters": ...,
"temporary": ... } }`. Reads add `query` with every `DataQuery` field. Sync adds `options`
with every `SyncOptions` field. Bounds are integers, ISO timestamp strings or null. Other
method arguments retain their Rust/Python names as top-level context fields; lease TTL is
`ttl_seconds`. Action context is `{ "name": ..., "kwargs": ... }`; hook context adds the synced
`pipe` (`LoadedPlugin::sync_hook`, which also passes host services for the hook's Pipe calls). Results use ordinary
JSON, including `[bool, string]` success tuples, null missing values, list/dict pipe keys,
and integer/string IDs. Data results use only the output stream.

## Manifest and cache

The bundled schema is an exact copy of `meerschaum-worker/schema/manifest.schema.json`,
checked against that source by an integration test. Loading validates the full schema and
metadata identity. Actions include docstrings; arguments retain argparse's manifest fields;
hooks and connector types share the Python manifest's shape. Native manifests use empty
`python_version` and null `python_executable`.

`cached_manifest(directories, cache_path)` avoids loading code when fingerprints match.
Use a separate native cache file outside plugin trees; merge it into the worker manifest in
the caller. Fingerprints use the worker host's SHA-256 byte grammar: plugin name, sorted
relative file paths, sizes and nanosecond mtimes, excluding bytecode and hidden directories.
The aggregate uses schema version, names and per-plugin fingerprints in discovery order.
The cache is written through a unique sibling temporary file and rename. Native trees reject
symlinks (cycles and external files are not tracked). This is a native-only restriction;
Python plugin discovery is unchanged. Install by replacement when no library is loaded;
this API does not promise live reload of an already-open library.

## Validation and remaining work

Run with fresh temporary `MRSM_ROOT_DIR` and empty `MRSM_PLUGINS_DIR`:

```sh
cargo fmt -p meerschaum-plugin
cargo clippy -p meerschaum-plugin --all-targets -- -D warnings
cargo test -p meerschaum-plugin
```

Integration tests build real example cdylibs, load them, fetch and sync nullable Arrow data
into the in-memory instance, exercise actions/hooks and panic recovery, reject a deliberately
short incompatible ABI table, validate the worker JSON schema and test discovery/cache
invalidation. Test subprocesses receive their own temporary roots. No database or Python
reference is used: v4 had no native ABI; the reused connector return shapes and manifest
schema are the compatibility surfaces.

Host integration is done (see "Hosting" below). The `mrsm` binary tests in
`crates/meerschaum-cli/tests/native_plugins.rs` and `native_e2e.rs` drive the examples end to
end (actions, hooks around a real sync, routes served by `start api`, a native plugin replacing a
Python plugin of the same name, rejection of corrupt or wrong-ABI libraries, the manifest cache),
and `tests/console/test_console.py` renders a native page in Chromium. Examples: `native_demo` (source,
instance, action, hooks, routes, page), `native_probe` (a plain fetch source whose hooks append to
`NATIVE_PROBE_LOG`, for asserting the built-in sync path), `native_api` (`api_routes` and MCP
tools, resources and prompts; `tests/api_mcp.rs`, `meerschaum-cli/tests/native_api_mcp.rs`) and `bad_abi`. `tests/source.rs` covers source plugins with a stand-in `cargo`;
`crates/meerschaum-cli/tests/native_source_plugin.rs` builds a real crate. Not yet available: repository
artifact upload, target-triple selection and installation (the installer's job), and lazy
(multi-batch) `fetch` streaming. Continuous `subscribe` is the pull-model subscription below. The future wasm contract is sketched in
`wit/plugin.wit` (not compiled); it replaces native Arrow pointers with Arrow IPC bytes.

## Hosting (wave 6)

`NativePlugins::open(dirs, cache)` is the process-level entry for hosts: it discovers native
plugins, reads or rebuilds the cached manifest without loading code on a fingerprint hit, and
loads each library lazily (`plugin(name)`, `connector(type, label, config)`, `source(name)`,
`hooks(post)`). The CLI merges its manifest into the Python manifest (a native plugin shadows
a Python plugin of the same name), dispatches native actions in-process, resolves native
connectors and `sync()` for the engine, and runs native sync hooks.

Server extensions are declared in the manifest entry and served through call kinds of the one
`invoke` entry point (no ABI change; see `docs/zensical/reference/plugins/rust-plugins/routes-and-mcp.md`):

- `api_routes`: `[{"name", "method", "path", "scopes"?, "public"?}]`; any method or `*`,
  Starlette paths (one `{param}` per segment with an optional prefix, suffix and
  `int`/`float`/`uuid`/`str` converter, and a final `{*rest}` or `{rest:path}`); a public route
  may not declare scopes. Each request is an `api_route` call whose context is
  `api::RouteRequest` (method, path, `path_params`, `query_params`, headers without
  `authorization` or the console's session cookie,
  `body_base64`, `principal`, `scopes`); the reply is `api::RouteResponse`
  (`{status, headers, body_base64}`, or the `json` / `body` / `content_type` shorthands). The
  server (`meerschaum_server::native`) checks credentials and scopes first and refuses, at
  startup, routes that would shadow a core route or conflict with an earlier one. Typed
  `Plugin::api_route`; host side `LoadedPlugin::api_route`. Server calls run concurrently
  (`LoadedPlugin::call_json_concurrent`, not serialized with the plugin's other calls); the CLI
  bounds them at 32 per plugin with a 30 s queue that then answers `503`.
- `mcp_tools` / `mcp_resources` / `mcp_prompts` (`mcp::ToolSpec`, `ResourceSpec`, `PromptSpec`):
  served by `/mcp` and `mrsm start mcp` through `mcp_tool` (`mcp::ToolCall`, reply: any JSON),
  `mcp_resource` (`mcp::ResourceRead`, reply: `mcp::ResourceContents`) and `mcp_prompt`
  (`mcp::PromptGet`, reply: a string, messages, or `{description?, messages}`). The server
  (`meerschaum_server::mcp::NativeMcp`, implemented by the CLI) filters them by scope and
  read-only mode like built-in tools and validates tool arguments against `input_schema`.
- Plugins that override `Plugin::call` can forward these four methods to
  `sdk::call_server_extension`. Single-file plugins spell them `#[api_route(METHOD, "/path",
  scopes = [...], public = .., name = ..)]`, `#[mcp_tool(description = .., input_schema =
  r#"{...}"#, read_only = .., ..)]`, `#[mcp_resource(uri | uri_template = .., ..)]` and
  `#[mcp_prompt(arguments = [..], ..)]` on top-level functions; `single_file.rs` writes the
  same manifest entries (checking JSON options, schemas, public-with-scopes and the route path
  at generation; `api::check_route_path` mirrors the server's startup check, and
  `meerschaum-cli/tests/single_file_parity.rs` asserts that the two agree)
  and the runtime's `api` / `mcp` modules mirror `RouteRequest`, `RouteResponse`, `ToolCall`,
  `ResourceRead`, `ResourceContents`, `PromptGet` and the `Into*` return conversions.
- Legacy routes: a manifest naming an `api_plugins` entry and no `api_routes` is asked for
  `routes` (`[{"method", "path", "public"?}]`, static paths) and served by `route`
  (`{method, path, query, body, user}` with a text body; reply `{status, content_type, body}`,
  parsed leniently as before: an invalid status is 200, the content type defaults to text).
- Console pages are the manifest's `web_pages` entries; `page` receives
  `{path, function, query, username}` and returns `{html, status?}` (a trusted fragment). The Dash console of the Python package fetches the fragment
  from the server and frames it in its chrome; a standalone install shows no console, so the page is only reachable there.
  Single-file plugins spell a page `#[web_page("my/page", login_required = .., skip_navbar = ..,
  page_group = .., dark_theme = ..)]` on a top-level function taking `page: &PageRequest` and
  returning HTML (`String`, `&str`, `(u16, T)`, `Result`); the entry's `function` routes the
  `page` call, and the path follows Python's `@web_page` (`/dash/<path>`, `page_key`).

### Single-file source plugins

Place `readings.rs` directly in `MRSM_PLUGINS_DIR`:

```rust
fn fetch(pipe: &Pipe, end: Option<Bound>) -> Result<Vec<Value>> {
    let last = pipe.get_sync_time()?;
    Ok(vec![json!({"id": 1, "previous": last, "end": end})])
}
```

The generated adapter accepts any subset of named `pipe: &Pipe`, `begin: Option<Bound>`
and `end: Option<Bound>` arguments, in any order, including no arguments. `sync` additionally
accepts `data: Option<Frame>`. Leading underscores are allowed. Unsupported names and async,
unsafe, conditional or generic entry points produce discovery errors. `Request` is not part
of the single-file author interface.

Fetch returns rows (`Vec<Value>`), `Frame` or `Option<Frame>`, optionally wrapped in `Result`.
Sync returns `SuccessTuple` or `(bool, String)` / `(bool, &str)`, optionally wrapped in
`Result`. Both functions may exist; sync overrides fetch in the engine. `Pipe`, `Bound`,
`SuccessTuple`, JSON `Value`, `json!`, `Frame`, `Result` and `Error` are implicitly imported.

`pipe.get_sync_time()` reads the newest axis value; `pipe.sync(rows)` writes supplied rows
through the host's sync engine and returns `Result<SuccessTuple>`. A zero-argument Pipe sync
is not provided, to avoid recursively invoking the same plugin. Callback state is scoped
to the current call; Pipe views cannot move to other threads. Recursive plugin calls fail
instead of blocking on the library's serialized call lock.

Meerschaum generates the crate, manifest and ABI adapter under the root's build cache.
Cargo is required to compile it; unchanged builds are reused.

Actions need only an annotated function, with optional named `action: Vec<String>` and
`kwargs: &Value` parameters:

```rust
/// Greet the supplied names.
#[make_action]
fn greet(action: Vec<String>) -> (bool, String) {
    (true, format!("Hello, {}!", action.join(", ")))
}
```

`mrsm greet Alice Bob` invokes it. Actions may return `()`, a success tuple,
`SuccessTuple`, or `Result` of those. `#[make_action(name = "hello", daemon = false)]`
overrides the command name and daemon setting. The generator derives help from doc comments.

Source connectors use annotated modules:

```rust
#[make_connector]
mod readings {
    fn register() -> Value {
        json!({"columns": {"datetime": "id"}, "dtypes": {"id": "int"}})
    }
    fn fetch(connector: &Connector) -> Vec<Value> {
        vec![json!({"id": 1, "source": connector.label()})]
    }
}
```

This provides `readings:<label>`. The module may define `fetch`, `sync`, `register`, and
ordinary helpers. Named `connector: &Connector` and `kwargs: &Value` parameters are also
accepted by data handlers. The connector view exposes `label()`, `connector_type()`, and
`config()`. `register` returns a JSON object or null, optionally wrapped in `Option` or
`Result`; its default parameters are merged under explicit parameters. Top-level `register`
provides the same hook for `plugin:<filename>`. Multiple actions, connectors, and top-level
data functions may share a file. Source connector sync overrides are honored by the engine.
`#[make_connector(instance)]` (or `instance = true`) makes the module an instance connector
(`is_instance`): its functions named after `host.rs`'s instance methods (`INSTANCE_METHODS` in
`single_file.rs`) answer them, with parameters bound by name (`pipe`, `connector`, `data:
Option<Frame> | Vec<Value>`, `context`, and any field of the call's context, `query` or `options`,
read through the runtime's `Request::field`), success methods returning a success tuple,
`get_pipe_data` rows or a frame, and the others any serializable value. Routes, MCP entries,
console pages, executors and streams have single-file spellings too (below and in the executors
section).

Python's other plugin surfaces have single-file spellings too. `#[pre_sync_hook]` and
`#[post_sync_hook]` mark hook functions (named `pipe: &Pipe` and `kwargs: &Value`, plus
`success_tuple: SuccessTuple` and `sync_duration: f64` for post hooks; `()` returns
`(true, "Success")`); markers stack, so one function may be an action and both hooks. A
top-level `fn setup(kwargs: &Value)` answers `setup` with a bool or success tuple (a `setup`
with other parameters stays a helper). `#[add_plugin_argument("--flag", action = "store_true", help = "...")]` on a
top-level function (or `#![add_plugin_argument(...)]` on the file) records an argparse
manifest entry. `const VERSION: &str = "1.0.0";` sets the plugin version (otherwise
`0.0.0`) and `const REQUIRED: &[&str] = &[...];` the manifest's `required` list;
`source::single_file_metadata` reads both without building (constants of other types are
ordinary constants). The CLI enforces `type`, `choices` and `required` and skips (with a
warning) a flag whose option string the built-in CLI or another plugin already takes. Each marker has its own
recognizer in `single_file.rs` (`ITEM_MARKERS`, `Generated::hook`, `Generated::argument`).

An optional top-of-file doc-comment fence supplies normal Cargo dependencies:

```rust
//! ```cargo
//! [dependencies]
//! regex = "1"
//! ```
```

Versions, features, renamed dependencies, Git sources and local paths are passed to Cargo;
local paths resolve relative to the original file. Only `[dependencies]` is accepted.
The generated `Cargo.lock` stays in the cache. External path dependency edits require an
edit to the plugin or cache deletion to force rebuilding. The shorthand annotations are
processed by Meerschaum, not exported procedural macros for packaged plugins.

### Lightweight single-file builds

By default, the generated single-file crate embeds a small runtime with only Serde and
`serde_json` as direct dependencies; inline Cargo blocks may add dependencies. It uses the optional `mrsm_plugin_invoke_rows_v1` C ABI extension: request,
result and row JSON bytes, plus call-scoped host callbacks for Pipe operations. Host
conversion owns the full data kernel. No Polars, Arrow, connector, loader or async runtime
dependency is compiled into these plugins. The original v1 Arrow ABI is unchanged.

Single-file `Frame` is a JSON row container (`from_rows`, `rows`, `into_rows`, `height`),
not the full SDK's typed frame. JSON loses frame schemas and converts special scalar
values to their JSON representations; choose a full Cargo plugin for typed Arrow
interchange and optimized data processing. Simple builds use `opt-level = 0` and
share a Cargo target cache across plugins; the installed libraries and stamps remain
separate. Saved compiler diagnostics survive failure caching.

## Executors and subscriptions

Both are additive JSON methods over `invoke`; the SDK routes them before `Plugin::call` (they
never reach it), so `call` overrides keep working. User guide:
`docs/zensical/reference/plugins/rust-plugins/executors-and-streams.md`.

**Executors** (`executor.rs`). The manifest's top-level `executor_types`
(`{"<type>": {"type", "plugin", "class_name"?, "methods"?, "doc"?}}`, in both schema copies and
aggregated into the native manifest cache) declares `<type>:<label>` executor keys.
`Plugin::executor(&Request) -> Option<Box<dyn Executor>>` returns the implementation for
`request.connector_type` / `label` / `config`. `Executor` mirrors `meerschaum/jobs/_Executor.py`;
every method defaults to `Error::Unsupported` except those derived from others (`get_job_exists`,
`get_jobs`, `get_job_properties`, `get_job_status`, `get_job_began/ended/paused`). JSON methods
are `executor.<name>` with `{name, sysargs, properties}` contexts; `executor.monitor_logs`
returns a handle followed with `executor.next_logs {handle, timeout_ms}` (`{"status": "lines" |
"timeout" | "end", "lines", "result"}`) and `executor.close_logs`. Every executor context carries
`mrsm` (`meerschaum_core::self_exe::self_command()`), which `executor::mrsm_command()` returns
inside the call. Host side: `LoadedPlugin::executor` / `NativePlugins::executor` return a
`NativeExecutor` (`call`, `monitor_logs` -> `NativeLogStream`). The CLI adapts it to
`meerschaum_jobs::JobExecutor` (`meerschaum-cli/src/native_executors.rs`) and installs it in
every jobs context through `JobsContext::with_executors` (CLI actions, compose, the Python jobs
API); `parse_executor_keys` accepts the declared types. Unsupported methods become
``"`<method>` is not supported by the '<keys>' executor."``.

**Subscriptions** (`subscribe.rs`). A connector type with `is_streaming: true` and `subscribe` in
`methods` (or `plugin:<name>` with `subscribe` in `data_functions`) is a stream source
(`NativeConnector::is_streaming`). `Plugin::subscribe(&Request) -> Result<Box<dyn
Subscription>>`; `Subscription::next_batch(timeout) -> Batch` (`Data { frame, ack, redelivered }`,
`Timeout`, `End`), `ack(&[u64])`, `close()`. `ChannelSubscription::new(capacity)` wraps a bounded
`sync_channel` for producer threads. JSON methods: `subscribe {pipe, kwargs}` -> `{"handle"}`,
`next_batch {handle, timeout_ms}` -> `{"status": "batch" | "timeout" | "end", "ack",
"redelivered"}` with the rows in the output Arrow stream, `ack_batches {handle, tokens}`,
`close_subscription {handle}` (idempotent). Host side: `NativeConnector::subscribe` ->
`NativeSubscription` (`next_batch`, `ack`, `close`; closes on drop). The CLI's
`native_streams::NativeStreamSource` implements the engine's `StreamSource` over it (polling 250 ms
per `next_batch` on a blocking thread, sending each batch into the sink before pulling again) and
`NativeRegistry::stream_source` returns it, so `--listen` and the supervised streams treat native
sources exactly like Python `subscribe` sources: `subscribe` receives the same listen `kwargs`
(`NativeRegistry::set_stream_kwargs`, set next to `PythonRegistry::set_stream_kwargs`). The source
hands the sink its own ack tokens mapped to (subscription generation, plugin token), so a token
flushed after its subscription ended is dropped instead of acknowledged on the next subscription;
an empty batch that carries a token is still sent to the sink (acknowledged with the next flush).

**Serialization.** `subscribe` and `executor.*` calls take the plugin's call lock like any other
call. `next_batch`, `ack_batches`, `close_subscription`, `executor.next_logs` and
`executor.close_logs` go through `LoadedPlugin::invoke_stream`, which skips the lock (and has no
host services); the SDK locks each handle (`handles.rs`). A rows-ABI (single-file) runtime must
answer the same methods with the same JSON shapes (rows in `data`).

**Executor context.** Every `executor.*` context carries `mrsm` (the host's command) and, from the
CLI, compose and Python hosts, `root_dir` (the root the jobs belong to; compose manages project
roots from the host's process, so an executor keys file state on it, not on `MRSM_ROOT_DIR`).
`executor.write_stdin {name, data}` answers a prompt: `NativeJobExecutor::monitor_logs` asks
`get_job_is_blocking_on_stdin` on each idle poll while `accept_input` is set and sends the input
callback's line. Display getters warn on failures other than "not supported".

**Single-file spellings** (`single_file.rs`; user guide in the single-file reference of
`docs/zensical/reference/plugins/rust-plugins/advanced.md`): `#[make_executor] mod <type> { fn
start_job(name: &str) -> (bool, String) { ... } ... }` declares the executor type `<type>`
(`(name = "...")` overrides; `local`, `systemd`, `api`, `none` are reserved), each function being the
`Executor` method of the same name with parameters bound by name (`executor: &ExecutorInfo`,
`name`, `sysargs`, `properties`, `data`, `strip_timestamps`, `stop_on_exit`). Missing methods
are unsupported except the derived ones, which the runtime derives as the SDK does.
`monitor_logs` returns a `FnMut(Duration) -> LogEvent` closure, a `LogStream` or a
`Box<dyn LogStream>`. `fn subscribe(pipe, connector, kwargs)` inside a `#[make_connector] mod` (or
at the top level, for `plugin:<name>`) returns a `FnMut(Duration) -> Batch` closure, a
`Subscription`, a `Box<dyn Subscription>` (the runtime implements both traits for the boxes)
or a `ChannelSubscription` (`Batch::{Rows, RowsAck, Data, Timeout, End}`), with an optional
module-level `fn ack(tokens: &[u64])`; the generator adds `+ use<>` to an `impl` return type so
the closure captures no parameter lifetime. A `subscribe` is the streaming one only when its
parameters are among those three and its return type is syntactically a subscription
(`subscription_return`: `impl FnMut`/`impl Subscription`, `Box<dyn ...>`, or a type the file
implements `Subscription` for, optionally in `Result`); any other `subscribe` stays a helper, as
in rc12. The runtime keeps the subscription and log-monitor handle tables
(`streams` module, one lock per handle) and answers `next_batch`, `ack_batches`,
`close_subscription`, `executor.next_logs` and `executor.close_logs` before the plugin's
dispatch, since the host calls them without the plugin's call lock. Tests:
`tests/single_file_server.rs` (every attribute at the ABI, plus their diagnostics) and
`meerschaum-cli/tests/single_file_server.rs` (`start api` routes and `/mcp`, an executor running
a job, and a `subscribe` stream feeding `sync pipes --listen`). `tests/single_file_parity.rs` and
`meerschaum-cli/tests/single_file_parity.rs` cover instance connectors (an instance serving
`register` / `sync` / `show pipes` / `delete pipes`), console pages (served to the console's
`/_mrsm/native-pages`), a top-level `subscribe` under `--listen`, the SDK's boxed return types,
`ChannelSubscription`, `mcp::message` and the helpers named `subscribe`.

Tests: `tests/executor_subscribe.rs` (ABI level, over the `native_executor` and `native_stream`
examples), `meerschaum-jobs/tests/custom_executor.rs`,
`meerschaum-cli/tests/native_executor_stream.rs` (`mrsm` end to end: `-e procs:<label>` jobs, the
shell's `executor` command, a compose project on a plugin executor, and a `--listen` sync into
SQLite with listen kwargs, a filtered empty batch and its acknowledgement) and `tests/test_native_executor.py` (the Python `Job` façade).
