# Advanced Rust Plugins

Start with the [single-file Rust plugin guide](../rust-plugins.md) for actions,
data sources, connectors, and inline dependencies. This page covers Cargo packaging,
the full SDK, deployment, and detailed single-file behavior.

Single files cover every plugin feature a Cargo plugin has: actions, source and instance
connectors, sync hooks, `setup`, plugin command-line flags, a version, `required` packages,
HTTP routes, MCP tools, resources and prompts, console pages, job executors, and streaming
connectors. Use a Cargo plugin for multiple source files, typed Arrow data, or full control
of the manifest and the ABI. Rust plugins support
these native extensions; Python Dash callbacks and arbitrary FastAPI customization
remain Python-host capabilities.

Only build and load plugins you trust: native plugins run inside the Meerschaum process.

## 📁 Layout

A native plugin is a directory in `MRSM_PLUGINS_DIR` (the existing multi-directory path list works too) holding a descriptor and the library:

```text
plugins/
└── native_demo/
    ├── plugin.toml
    └── libnative_demo.so       # .dylib on macOS; install as libnative_demo.dll on Windows
```

`plugin.toml` has two required keys:

```toml
name = "native_demo"
version = "1.0.0"
```

The library name is always `lib<name>.<ext>`, with the `lib` prefix on Windows too. Names may contain letters, digits, `_` and `-`. Directories are scanned in order, earlier `MRSM_PLUGINS_DIR` entries win, and a plugin that is invalid (bad descriptor, missing library, wrong ABI) is skipped with a warning while the others still load.

### Rust before Python

A directory that contains both a `plugin.toml` and Python files is a Rust plugin. If a Rust plugin and a Python plugin have the same name, **the Rust plugin replaces the Python plugin entirely**: its actions, hooks, connectors, routes and pages disappear from the merged manifest, and nothing falls back to the Python module, even for functions the library does not implement. Python plugins with other names are unaffected and keep working beside the native one. To port a plugin gradually, split it: move one capability into a Rust plugin with a new name and keep the rest in Python (the two may depend on each other through `required`).

### Gradual migration

Keep a Python plugin such as `acme_ui` for `@dash_plugin`, custom login layouts, and FastAPI
handlers, and put the data capability into a differently named Rust plugin, `acme_data`.
For example, the native plugin can implement `fetch` and the existing Python page can read
its registered Pipe through the normal SDK:

```python
import meerschaum as mrsm

readings = mrsm.Pipe('plugin:acme_data', 'readings', instance='sql:local')
# The page continues to use its existing Dash callbacks and data formatting.
data = readings.get_data()
```

Use the native fetch example below as the starting implementation for `acme_data`. Register
and sync that Pipe before opening the page. Moving the fetch implementation does not require
rewriting the page or changing its session handling.

For a fetch plugin, first list the operations it performs on its Python `pipe` argument.
Single-file plugins provide `pipe.get_sync_time()` and `pipe.sync(data)` through the host.
Full SDK plugins can obtain the same typed view with `request.pipe()?` when invoked by
a host with the optional callback extension. Other operations, such as
`pipe.instance_connector.read()` or importing helpers from Python plugins, still need
an explicit replacement. Listing a Python plugin in `required` does not make its
functions callable from Rust.

Validate a port under a separate plugin name and scratch root before changing existing
Pipe connector keys. Compare its rows, nulls, dtypes, bound handling, empty responses, and
errors with the Python implementation using recorded or synthetic inputs. Then sync the
native plugin through `mrsm` into a disposable instance to check discovery, the Arrow
boundary, and repeated-sync behavior together.

Native routes take path parameters (Starlette syntax, converters included), any method, required
scopes, the caller's principal, request and response headers (cookies and redirects included) and
byte bodies, and serve requests concurrently; see
[Routes and MCP](routes-and-mcp.md). They are not FastAPI: there is no dependency injection,
OpenAPI entry or request validation beyond what your handler does. Same-name native plugins
replace the complete Python plugin rather than individual capabilities.

## 🔧 Writing one

Create a library crate:

```toml
# Cargo.toml
[package]
name = "native_demo"
version = "1.0.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
meerschaum-plugin = "5.0"

[profile.release]
panic = "unwind"      # required: the SDK catches panics at the boundary
```

Implement the `Plugin` trait and export it once:

```rust
use meerschaum_plugin::{
    Error, Frame, Plugin, Reply, Request, Result,
    arrow::{array::Int64Array, datatypes::{DataType, Field, Schema}, record_batch::RecordBatch},
    serde_json::{Value, json},
};
use std::sync::Arc;

#[derive(Default)]
struct Demo;

impl Plugin for Demo {
    const NAME: &'static str = "native_demo";
    const VERSION: &'static str = "1.0.0";

    /// Declares what this plugin provides (actions, arguments, hooks, connectors, pages).
    fn manifest(&self) -> Value { /* see "The manifest" below */ json!({}) }

    /// `fetch`: return the new rows as an Arrow-backed `Frame`.
    fn fetch(&self, request: &Request) -> Result<Option<Frame>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))])
            .map_err(|e| Error::Message(e.to_string()))?;
        Ok(Some(Frame::from_record_batch(&batch).map_err(|e| Error::Message(e.to_string()))?))
    }

    /// Everything else (actions, hooks, routes, pages, instance methods) arrives here.
    fn call(&self, request: &Request, data: Option<Frame>) -> Result<Reply> {
        match request.method.as_str() {
            "fetch" => self.fetch(request).map(Reply::data),
            "action" if request.context["name"] == "hello" => {
                Ok(Reply::json(json!([true, "Hello from native_demo"])))
            }
            _ => Err(Error::Unsupported(format!("{} is not implemented", request.method))),
        }
    }
}

meerschaum_plugin::export_plugin!(Demo);
```

The complete, tested version is [`crates/meerschaum-plugin/examples/native_demo.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_demo.rs). It is a source connector, an in-memory instance, an action with an argument, sync hooks, two routes and a console page, all on synthetic data.

Things to know:

- **Stateless handlers.** The SDK builds `Default::default()` for every call. Keep shared state in a synchronized `static` (a `Mutex`) or a database, keyed by connector label.
- **Synchronous calls.** The SDK imposes no async runtime. Calls into one loaded plugin are serialized by the host, except stream calls (`next_batch`, ...) and the server calls (`api_route`, `mcp_tool`, `mcp_resource`, `mcp_prompt`), which run concurrently and must be thread-safe (see [Routes and MCP](routes-and-mcp.md#how-calls-run)). Schedule blocking work on your own executor if you need one.
- **Panics are contained.** Constructors and calls run inside `catch_unwind` and surface as an error (`native plugin panicked`). An aborting panic, `process::exit` or memory fault cannot be recovered. Build with `panic = "unwind"`.
- **Same OS and architecture.** The host and the plugin do not need the same Rust compiler version, since only `extern "C"` and the Arrow C interfaces cross the boundary, but they must target the same OS and CPU.

### What `Request` contains

| Field | Meaning |
|---|---|
| `method` | The operation: `fetch`, `sync`, `action`, `pre_sync_hook`, `post_sync_hook`, `api_route`, `mcp_tool`, `mcp_resource`, `mcp_prompt`, `routes`, `route`, `page`, `register_pipe`, `get_pipe_data`, and so on |
| `connector_type`, `label`, `config` | Which connector instance the call is for, and its configuration |
| `context` | A JSON object. For pipe operations, `{"pipe": {"keys": ..., "parameters": ..., "temporary": ...}}`; reads add `query`, syncs add `options`; actions use `{"name": ..., "kwargs": ...}`, and hooks `{"name": ..., "kwargs": ..., "pipe": ...}` |

Bounds (`begin`, `end`) are integers, ISO timestamp strings or `null`. Results are plain JSON: success tuples as `[true, "message"]`, missing values as `null`.

## 📜 The manifest

`manifest()` returns the same JSON document the Python plugin manifest uses, as a whole: the top-level keys `schema_version` (use the SDK constant `meerschaum_plugin::MANIFEST_SCHEMA_VERSION`; native plugins and Python workers share manifest schema `5`), `meerschaum_version`, `python_version` (`""`), `fingerprint` (`null`) and `plugins`, plus `connector_types`, `pre_sync_hooks` and `post_sync_hooks` when you provide them (the shipped example is the reference). The host validates it against the bundled JSON schema (`crates/meerschaum-plugin/schema/manifest.schema.json`) at load time, so `--help`, tab completion and the server can learn what your plugin offers **without loading the library**: it is cached by a fingerprint of the plugin directory and rebuilt when a file changes.

Native libraries built with a 5.0 pre-release SDK (manifest schema `3`) must be rebuilt with the current SDK. Stale manifest caches are regenerated automatically. The native ABI and worker protocol remain at version `1`.

The part for your plugin lives under `plugins.<name>`:

```json
{
  "name": "native_demo",
  "version": "1.0.0",
  "data_functions": ["fetch", "sync"],
  "actions": [{"name": "hello", "doc": "Return a synthetic greeting."}],
  "arguments": [{"option_strings": ["--native-demo"], "action": "store_true", "help": "Enable demo."}],
  "pre_sync_hooks": ["before"],
  "post_sync_hooks": ["after"],
  "connectors": ["native_demo"],
  "api_plugins": ["routes"],
  "web_pages": [{"path": "/dash/native-demo", "page_group": "Native demo",
                 "page_key": "native-demo", "login_required": true,
                 "function": "page", "mode": "html"}]
}
```

Arguments reach `context.kwargs` exactly as they reach a Python plugin's `**kwargs`, which is v4's rule: an argument that was not given is **absent** (never `false` or `null`), a flag that was given is `true`, and a `store_false` flag is present (as `true`) only while it is not given. Declare `"default": "None"` to receive an explicit `null`. Read flags with a fallback (`kwargs.get("native_demo").and_then(Value::as_bool).unwrap_or(false)`).

### Equivalents of `@make_action` and `@make_connector`

Single-file plugins use `#[make_action]` on a function and `#[make_connector]` on a
module, as shown in the [single-file guide](../rust-plugins.md), `#[pre_sync_hook]`,
`#[post_sync_hook]` and `#[add_plugin_argument(...)]` for hooks and flags, `#[api_route]`,
`#[mcp_tool]`, `#[mcp_resource]` and `#[mcp_prompt]` for server extensions,
`#[make_connector(instance)]` for instances, `#[web_page]` for console pages,
`#[make_executor]` for executors, and `fn subscribe` for streams (see the
[single-file reference](#single-file-reference)). The generator writes their manifests and dispatchers for you.

For packaged Cargo plugins, use declarations in `manifest()` and handlers in
`Plugin::call()`, as described here.

For Python's `@make_action`, add an entry to the plugin's `actions` array:

```json
"actions": [{"name": "hello", "doc": "Return a greeting."}]
```

Then handle the action in `call()`:

```rust
"action" if request.context["name"] == "hello" => {
    Ok(Reply::json(json!([true, "Hello!"])))
}
```

This registers `mrsm hello`; parsed options arrive in `request.context["kwargs"]`.

For Python's `@make_connector`, list the type in the plugin entry's `connectors`
array and declare it in the manifest's top-level `connector_types` object:

```json
"connector_types": {
  "my_source": {
    "type": "my_source", "plugin": "native_demo", "class_name": "Demo",
    "is_instance": false, "is_thread_safe": false,
    "methods": ["fetch", "register"]
  }
}
```

With `"connectors": ["my_source"]` in `plugins.native_demo`, this provides
`my_source:<label>` connectors. Dispatch by `request.method`; the connector type,
label and configuration are in `request.connector_type`, `request.label` and
`request.config`. Set `is_instance` to `true` and implement instance operations
when the connector should also be usable with `-i`.

The optional source `register` handler returns a JSON object of default Pipe
parameters (or `null`), with the Pipe in `request.context["pipe"]`. For
`plugin:<name>`, also list `register` under `data_functions`. Bootstrap uses these
defaults; ordinary registration merges explicitly supplied parameters over them.
This source hook differs from `register_pipe`, which persists a Pipe on an instance.

Rust plugins cover actions, arguments, hooks, fetch/sync, custom connectors and
instances, supported HTTP routes, and console page fragments. They do **not** have
complete Python plugin parity: arbitrary Python helper imports and connector
access, Dash callbacks, unrestricted FastAPI customization and live library reload are not
available.
The Pipe helper surface is described in the [single-file reference](#single-file-reference), and HTTP adapter limits are described above; see the
status note below for further limits.

## 🧩 What a plugin can provide

| You want | Do this |
|---|---|
| A **fetch source** for `plugin:<name>` pipes | Implement `fetch`, list `fetch` under `data_functions`. The engine passes `begin`, `end`, `params` and the pipe's keys and parameters in `context`. |
| A **sync override** | Implement `sync`; it receives optional data and returns a success tuple, like a Python `sync(pipe, **kw)`. |
| An **action** (`mrsm hello`) | Add it to `actions` and answer the `action` method (`context.name`, `context.kwargs`). Add flags with `arguments`. |
| **Hooks** | List names under `pre_sync_hooks` / `post_sync_hooks`; they receive the same kwargs as Python's `@pre_sync_hook` and `@post_sync_hook` (`sync_timestamp`, plus `success_tuple`, `sync_duration` and `sync_complete_timestamp` after the sync, ...), and the synced Pipe in `context.pipe` (Python passes it positionally, so it is not among the kwargs). |
| A whole **connector** or **instance** | List it under `connectors` / `connector_types` (set `is_instance`), and answer the instance methods (`register_pipe`, `sync_pipe`, `get_pipe_data`, `fetch_pipes_keys`, ...). A method you do not implement returns `Error::Unsupported`, which the host maps to "not implemented" and lets the engine fall back (for example to file-based leases). |
| **HTTP routes** | Declare `api_routes` (`{name, method, path, scopes?, public?}`, with `{param}` path segments) and implement `api_route`, which receives the whole request (path and query parameters, headers, body bytes, principal, scopes) and returns a status, headers and body bytes. Routes require the caller's credentials and declared scopes unless `"public": true` or the server runs with `--no-auth`. A route that would shadow a core route is refused at startup. See [Routes and MCP](routes-and-mcp.md). |
| **MCP tools, resources, prompts** | Declare `mcp_tools`, `mcp_resources`, `mcp_prompts` and implement `mcp_tool`, `mcp_resource`, `mcp_prompt`. They are served by `/mcp` and `mrsm start mcp` with the same scope and read-only filtering as built-in tools; tool arguments are validated against your input schema. See [Routes and MCP](routes-and-mcp.md). |
| A **job executor** (`-e <type>:<label>`) | Declare it under the top-level `executor_types` and return an `Executor` from `Plugin::executor`; methods you leave out answer "not supported". See [Executors and Streams](executors-and-streams.md). |
| A **streaming connector** (`sync pipes --listen`) | Mark the connector type `is_streaming` with `subscribe` in its `methods` (or list `subscribe` under `data_functions`) and return a `Subscription` from `Plugin::subscribe`; the host pulls Arrow batches with `next_batch` and acknowledges them after they commit. See [Executors and Streams](executors-and-streams.md). |
| A **console page** | List it under `web_pages`; answer `page` with `{html, status?}`. The Python web console (`meerschaum[dash]`) fetches your fragment from the server (`/_mrsm/native-pages`) and frames it in its chrome; a standalone install without the console does not show it. See the [page contract](/reference/migrating-to-v5/#option-b-the-page-contract-recommended-for-new-pages). |

Data always crosses the boundary as an **Arrow C stream** (`FFI_ArrowArrayStream`), never JSON. The SDK handles that for you: you return a `Frame`, and the host imports it without copying through Python.

Hooks run around every sync that goes through the sync engine (`sync pipes`, jobs, compose). As with Python hooks, `sync pipes` prints every hook's success tuple (successful or not) in order, and a failing hook never fails the sync. A hook that returns an error (instead of a tuple) is reported as `Failed to execute sync hook '<hook>' from plugin '<plugin>':` followed by the error, both as a warning and as a failed tuple, like a Python hook that raised; the remaining hooks still run. Native hooks run before Python hooks.

## 🏗️ Build, install, load

Build the shipped example from a checkout (the prebuilt route, which needs no toolchain at runtime):

```bash
cargo build --release -p meerschaum-plugin --example native_demo
```

Install it into a plugins directory:

```bash
mkdir -p "$MRSM_PLUGINS_DIR/native_demo"
cp crates/meerschaum-plugin/examples/plugin.toml "$MRSM_PLUGINS_DIR/native_demo/"
cp target/release/examples/libnative_demo.so "$MRSM_PLUGINS_DIR/native_demo/"
```

### Source plugins

You do not have to build and copy the library yourself. A plugin directory that is a **Cargo crate** is built by Meerschaum when `cargo` is on `PATH`:

```text
plugins/
└── native_demo/
    ├── plugin.toml
    ├── Cargo.toml              # [package] name = "native_demo", [lib] crate-type = ["cdylib"]
    ├── Cargo.lock              # optional, recommended
    └── src/lib.rs
```

A directory counts as a source plugin when it has a `plugin.toml` and a `Cargo.toml` and no up-to-date `lib<name>.<ext>` next to `plugin.toml`. (A library shipped next to the sources is used as is, unless a source file is newer than it.) The crate's `[package] name` must equal the `name` in `plugin.toml` and the crate must build a `cdylib` with `panic = "unwind"`; otherwise the plugin is skipped with a warning like any other invalid plugin.

**When it builds.** The first time `mrsm` discovers the plugin, and again whenever its source fingerprint changes: a hash of the contents of every file in the plugin directory except `target/`, hidden directories (but `.cargo/` is included) and shared libraries, so assets read by `include_str!` and files read by `build.rs` count, as do `Cargo.toml`, `Cargo.lock` and `plugin.toml`. Meerschaum runs `cargo build --release` (the `CARGO` environment variable names the executable if it is set) with `--manifest-path`, `--target-dir` and `CARGO_INCREMENTAL=0`, prints one line on stderr (`Building native plugin 'native_demo' (cargo build --release)...`) and is silent afterwards. The first build compiles the SDK's dependencies and takes minutes. A build whose sources have not changed is never repeated, and `--help` and tab completion read the cached manifest without building. Path dependencies outside the plugin directory are not part of the fingerprint, and the hash follows file contents, not modification times (touching a file does nothing): after changing such a dependency, edit a file in the plugin directory or delete `<root>/cache/native_build/<name>` to force a rebuild.

!!! warning "Trust: building runs code"

    `cargo build` runs the crate's build scripts and procedural macros and may download dependencies from crates.io, with no timeout. It runs whenever plugins are opened and a build is needed, including `mrsm start api`, and it is the same trust as loading the library: only put crates you trust in `MRSM_PLUGINS_DIR`. A build failure, including a transient one such as being offline, is remembered until the sources change or `<root>/cache/native_build/<name>` is deleted.

**Where the artifact lives.** Under `<root>/cache/native_build/<name>/` (the root is `MRSM_ROOT_DIR`); the library is loaded from `<root>/cache/native_build/<name>/release/lib<name>.<ext>` and its path is recorded in the plugin's entry of `<root>/cache/native_plugins_manifest.json`. The only file Cargo may write into the plugin directory is `Cargo.lock`, when it is missing or out of date, so ship an up-to-date `Cargo.lock` and a read-only plugins directory works. Delete the `native_build` directory to force a rebuild.

**No toolchain.** Without `cargo` the plugin is skipped and the rest still load:

```text
Warning: Skipping native plugin at '/plugins/native_demo': it is a source crate and no `cargo` was found on PATH (install a Rust toolchain or ship libnative_demo.so)
```

A failed build prints the last lines of cargo's stderr in the same kind of warning and skips that plugin only. The failure is remembered: later runs skip the plugin with `the last build failed; edit the sources or delete '<build dir>' to retry` and do not run cargo again until you edit the sources or delete the build directory. If a library is shipped next to the sources but is older than them and there is no `cargo`, the shipped library is loaded with a warning that it was not rebuilt.

!!! note "The slim image ships no toolchain"

    `bmeares/meerschaum:slim` has no Rust toolchain, so there only the prebuilt contract works: put `plugin.toml` and `lib<name>.so` in the plugin directory, built for the image's OS and architecture.

Point `MRSM_PLUGINS_DIR` at the directory (use a temporary `MRSM_ROOT_DIR` while experimenting). The action and its argument are discovered immediately:

```text
$ mrsm hello
 🎉 Hello from native_demo

$ mrsm --help | grep -A1 '^hello'
hello
Return a synthetic greeting.
```

The plugin's flags (`--native-demo`) are listed in `--help` after its last action, and `mrsm hello --native-demo` is accepted.

Register a pipe that uses it as a source:

```bash
mrsm register pipe -c plugin:native_demo -m demo -i sql:local --params '{"columns": {"id": "id"}}'
mrsm sync pipes -i sql:local --nopretty
```

```text
{"pipe": {"connector_keys": "plugin:native_demo", "metric_key": "demo", ...}, "success": true, "message": "Success"}
```

The example declares both `fetch` **and** `sync`, so (exactly like a Python plugin that defines `sync()`) its own `sync` replaces the built-in sync process: it keeps the rows in memory and reports `Success` instead of writing a table. Remove `sync` from `data_functions` to let Meerschaum write the fetched frame to your instance.

## 🧪 Testing your plugin

Everything below runs against a throwaway root, so it never touches your real configuration:

```bash
export MRSM_ROOT_DIR="$(mktemp -d)"
export MRSM_PLUGINS_DIR="$(mktemp -d)"      # install your plugin directory here
mrsm show plugins                           # native plugins are listed as `name (native)`
mrsm show plugins --nopretty                # bare names, for scripts
mrsm show actions --nopretty                # your actions appear here
mrsm --help                                 # ...and here, with their documentation and flags
```

A plugin that is missing from these listings was skipped: look for `Warning: Skipping invalid native plugin at '<dir>': <reason>` on stderr. The reasons are a bad `plugin.toml`, a missing or unloadable library, an incompatible ABI (for example `unsupported native plugin ABI 99; expected 1`), and, for a source crate, a package name that differs from `plugin.toml` or a crate that is not a `cdylib`. A valid source crate that could not be built (no `cargo`, a compile error) is reported as `Warning: Skipping native plugin at '<dir>': <reason>`. The other plugins still load. A panic inside your code does not stop `mrsm`: the call returns a failure (`native plugin panicked`) and the next command works normally. The host caches the manifest under `$MRSM_ROOT_DIR/cache/native_plugins_manifest.json`, keyed by a fingerprint of the plugin directory (the source fingerprint for a source crate), so replacing or removing a plugin takes effect on the next run (do it while `mrsm` is not running).

To check a source and its hooks, register a pipe on a SQLite instance and sync it:

```bash
mrsm register pipe -c plugin:my_plugin -m demo -i sql:local --params '{"columns": {"id": "id"}}'
mrsm sync pipes -c plugin:my_plugin -m demo -i sql:local --nopretty
mrsm plan pipes -c plugin:my_plugin -m demo -i sql:local --nopretty    # the plugin as a graph source
mrsm start api -i sql:local --host 127.0.0.1 -p 8224 --no-dash        # your routes are served next to the core API
```

### What the repository tests

These suites build the example plugins in `crates/meerschaum-plugin/examples/` (`native_demo`, `native_probe`, `native_api`, `bad_abi`) and load them as real shared libraries; all data is synthetic. Run each with a temporary `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR`:

```bash
cargo test -p meerschaum-plugin                       # the SDK and loader
cargo test -p meerschaum-cli --test native_plugins    # the `mrsm` binary: actions, flags, a native source
cargo test -p meerschaum-cli --test native_e2e        # hooks, routes, shadowing, rejection, cache
cargo test -p meerschaum-cli --test native_api_mcp    # routes with parameters, scopes and bytes; MCP over HTTP and stdio
cargo test -p meerschaum-cli --test native_source_plugin   # a Cargo crate built, cached and rebuilt by mrsm (compiles the SDK: slow the first time)
MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console -k native_page   # a native console page in Chromium
```

| Capability | Covered by |
|---|---|
| Fetch source, sync override, in-memory instance, actions, hooks, panic containment (library level) | `meerschaum-plugin/tests/native.rs` |
| Action dispatch, `show plugins`/`show actions`, flags, panic containment, a sync from a native source | `native_plugins.rs` |
| Native hooks around real syncs, the built-in sync writing a native fetch, failing hooks as warnings | `native_e2e.rs` |
| Routes (public, authenticated, `--no-auth`) served by `mrsm start api` | `native_e2e.rs` |
| `api_routes` (path parameters, headers, bytes, scopes, a refused shadowing route) and MCP tools, resources and prompts over `/mcp` and stdio | `native_api_mcp.rs`; the server side in `meerschaum-server/tests/native_extensions.rs`, the ABI in `meerschaum-plugin/tests/api_mcp.rs` |
| A native plugin replacing a Python plugin of the same name | `native_e2e.rs` (needs a Python environment; skipped without one) |
| A custom instance (`-i native_demo:memory`): pipes listed and synced, file-lock fallback for leases | `native_e2e.rs` |
| Corrupt library and wrong ABI skipped, manifest cache follows the directory | `native_e2e.rs`; discovery details in `native.rs` |
| `plan pipes` / `show graph` with a native source | `native_e2e.rs` |
| A source crate built by `mrsm`, reused on the next run, rebuilt after an edit; a mismatched crate name and a missing `cargo` skipped | `native_source_plugin.rs` (real `cargo`); stamp, fingerprint and failure handling in `meerschaum-plugin/tests/source.rs` |
| A console page rendered in the Dash console's frame, identical to the Python page | `tests/console/test_console.py` |

## 🔬 The ABI in one page

The library exports a single symbol, `mrsm_plugin_v1`, which returns a static `#[repr(C)]` table:

| Field | Purpose |
|---|---|
| `abi_version`, `struct_size` | A permanent prefix. The host reads **only** these two `u32`s before rejecting an incompatible or truncated table. |
| `describe()` | Returns metadata and the manifest as JSON. |
| `invoke(request_json, input_stream, output_stream)` | One entry point for every method; `request.method` selects the operation. Input and output are Arrow streams. |
| `free_bytes(bytes)` | Releases every payload the plugin allocated. |

Every result carries a status: `0` success (JSON), `1` error (UTF-8 message), `2` caught panic, `3` unsupported method. Memory a plugin allocates is always released by that plugin's `free_bytes`; the host never frees it.

The host serializes a plugin's calls, except the stream methods (`next_batch`, `ack_batches`,
`close_subscription`, `executor.next_logs`, `executor.close_logs`): those may run while another
call is in progress, so one blocking `next_batch` never holds up the rest of the plugin. The SDK
locks each open handle itself.

Single-file plugins additionally export `mrsm_plugin_invoke_rows_v1`. This extension
exchanges JSON request, result and row bytes, with call-scoped host callbacks; it does
not change the original Arrow ABI. Host and plugin always free their own allocations.
JSON rows trade conversion overhead and schema fidelity for small builds; full Cargo
plugins use the Arrow path described above.

The optional `mrsm_plugin_invoke_host_v1` symbol accepts the original request and Arrow
streams plus a versioned host callback table. Its context is valid only during the call;
result bytes are freed by the host that allocated them. Older libraries use the original
v1 entry point unchanged. New plugins loaded by an older host can still run, but attempting
a Pipe host operation returns an unavailable-services error.

The ABI version is checked at load. A breaking change would ship as a new `mrsm_plugin_v2` symbol, and the host keeps loading v1 for at least one major release. The same interface is also written as WIT (`crates/meerschaum-plugin/wit/plugin.wit`) for the future WebAssembly host.

## 📦 Distribution

`mrsm install plugins readings -r api:repo` accepts a repository tar.gz archive containing
`readings.rs`, or a `readings/` directory with `plugin.toml` and either Cargo sources or
a prebuilt library for the host OS and CPU. Source plugins are built and validated during
installation. Native installs skip Python virtual environments and Python dependency
installation, run an optional native setup hook, record `.mrsm_origins.json`, and restore
the previous plugin if compilation, loading or setup fails. `uninstall plugins` removes
single-file Rust sources as well as native directories. A descriptor supplies the version
for normal up-to-date checks; single `.rs` files have no author version and are replaced
on each install. `--skip-deps` does not skip the Rust build needed to validate a source plugin.

Upload either format with `mrsm register plugins readings -r api:repo`. Single-file uploads
retain the inline dependency block. Packaged uploads retain Cargo sources, manifests,
lockfiles, assets, `.cargo` configuration, and any shipped library; build output, other
hidden files, symlinks, and files matched by `.gitignore` are excluded. Local path dependencies
must be included inside a packaged plugin or replaced with registry or Git dependencies
before sharing; uploading a single `.rs` file does not upload its neighboring files.

You may also copy these files directly into `MRSM_PLUGINS_DIR`. Automatic target-triple
selection is not implemented; a prebuilt library must match the host OS and CPU.
`mrsm edit plugins readings` opens the `.rs` file or the Cargo library entry point
(`[lib].path`, default `src/lib.rs`). Binary-only packages must include sources to be edited.

!!! note "Status in 5.0"

    Source plugins (a Cargo crate built by Meerschaum into the root's cache) work when `cargo` is on `PATH`; the prebuilt `lib<name>.<ext>` remains the contract for the slim image. Discovery, the manifest cache, actions, arguments, hooks, source `fetch`/`sync`, custom connectors, routes and pages are implemented, and each is exercised through the `mrsm` binary by the tests listed under [Testing your plugin](#testing-your-plugin). The deepest instance operations (reads, rowcounts, maintenance) are exercised only at the library level; through the binary the example instance proves resolution, `fetch_pipes_keys`, `sync_pipe` and the lease fallback. Job executors and streaming connectors (`subscribe`, pulled batch by batch) are covered in [Executors and Streams](executors-and-streams.md). Not yet available: target-triple selection by `install plugin`, and live reload of an already-open library (replace a plugin while `mrsm` is not running).

## Single-file reference

These details apply to the [single-file format](../rust-plugins.md), without a Cargo project.

### Accepted signatures

For data sources, define `fetch`, `sync`, or both. Each function may accept any subset of these **named**
parameters, in any order:

| Parameter | Type | Meaning |
|---|---|---|
| `pipe` | `&Pipe` | Pipe metadata and operations on its host instance |
| `connector` | `&Connector` | Current connector type, label, and resolved configuration |
| `kwargs` | `&Value` | Options passed to the handler |
| `begin` | `Option<Bound>` | Inclusive lower bound, or `None` when not supplied / unbounded |
| `end` | `Option<Bound>` | Exclusive upper bound, or `None` when unbounded |
| `data` (sync only) | `Option<Frame>` | Input frame if the caller supplied one |

For example, `fetch(pipe: &Pipe)`, `fetch(end: Option<Bound>)`, and
`fetch(pipe: &Pipe, begin: Option<Bound>, end: Option<Bound>)` are all valid.
Leading underscores are allowed (`_pipe`, `_begin`); use a named parameter rather than
`_`. Names distinguish `begin` from `end`, whose types are identical. Functions must be
synchronous, safe, non-generic Rust functions, declared at the file's top level or inside a marked connector module.
`Request` is an internal dispatch envelope, not an accepted single-file parameter.

`Pipe`, `Connector`, `Bound`, `SuccessTuple`, JSON `Value`, `json!`, `Frame`, `Result`, and `Error`
are available without imports. `Bound` is either `Bound::Int(i64)` or
`Bound::Datetime(String)` (an ISO timestamp). Fetch receives the engine's resolved
bounds; an unspecified default begin in a sync override is `None`.

Fetch may return `Vec<Value>` containing JSON objects, `Frame`, `Option<Frame>`, or
any of those wrapped in `Result`. Missing row fields become null cells; an empty vector
means no data. Single-file plugins exchange JSON rows over a versioned C ABI; the host
converts them to its native frames. `Frame` here is a lightweight row container with
`from_rows`, `rows`, `into_rows` and `height`, not the full SDK's Arrow-backed frame.
Use a Cargo plugin for typed Arrow data, preserved frame schemas, or optimized bulk processing.
Sync may return `SuccessTuple`, `(bool, String)`, `(bool, &str)`, or a `Result` of those.
If both functions exist, the engine uses `sync` as the override, as with Python plugins.
A normal engine-invoked sync override receives `data = None`; explicit input is written
by the engine without invoking the override again.

### Sync hooks

Mark a top-level function with `#[pre_sync_hook]` or `#[post_sync_hook]`, the
equivalents of Python's `@pre_sync_hook` and `@post_sync_hook`. The function name is
the hook's name; a file may define several hooks of each kind, which run in file order.

```rust
#[pre_sync_hook]
fn announce(pipe: &Pipe) -> (bool, String) {
    (true, format!("Syncing {}", pipe.metric_key()))
}

#[post_sync_hook]
fn report(pipe: &Pipe, success_tuple: SuccessTuple, sync_duration: f64) -> (bool, String) {
    (true, format!("{} took {sync_duration:.1}s: {}", pipe.metric_key(), success_tuple.message))
}
```

| Parameter | Type | Meaning |
|---|---|---|
| `pipe` | `&Pipe` | The synced Pipe, with its [operations](#pipe-operations) (`get_sync_time`, `sync`) |
| `kwargs` | `&Value` | The keyword arguments Python hooks receive (`begin`, `end`, `params`, `sync_timestamp`, and after the sync `success_tuple`, `sync_duration`, `sync_complete_timestamp`, ...) |
| `success_tuple` (post only) | `SuccessTuple` | The sync's result |
| `sync_duration` (post only) | `f64` | Seconds the sync took |

A hook may return `()`, `SuccessTuple`, `(bool, String)` / `(bool, &str)`, or a `Result`
of those. `()` counts as `(true, "Success")`, as a Python hook returning `None` does.
The results are printed by `sync pipes` and never fail the sync (see
[What a plugin can provide](#what-a-plugin-can-provide)). As in Python, a `pipe.sync(...)`
inside a hook runs no hooks.

Markers stack like Python's decorators: one function may carry `#[make_action]`,
`#[pre_sync_hook]` and `#[post_sync_hook]` together, and is registered once for each
(its parameters must suit every role, so `kwargs: &Value` is the usual choice).

### Setup

A top-level `fn setup` runs during `mrsm setup plugins <name>` and after
`mrsm install plugins`, like Python's `setup(**kwargs)`. It may accept `kwargs: &Value`
and returns `bool`, `SuccessTuple`, `(bool, String)`, or a `Result` of those (an `Err`
is a failed setup). A bare `true` or `false` gets the message "Setup for Plugin '<name>'
did not return a message." A setup function must return a value: Python fails a
`setup()` that returns `None`, so `fn setup()` without a return type is rejected. A
top-level `setup` with parameters other than `kwargs` is an ordinary helper function,
as it was before single-file plugins had a setup.

```rust
fn setup(kwargs: &Value) -> (bool, String) {
    (true, format!("Ready (force = {})", kwargs["force"]))
}
```

### Plugin arguments

`#[add_plugin_argument(...)]` takes the arguments of Python's `add_plugin_argument`
(argparse's `add_argument`): the quoted flags first, then `key = value` options. Put it
on a top-level function, or at the top of the file as an inner attribute. Flags belong
to the whole plugin either way, as in Python, and every action receives them in `kwargs`.

```rust
#![add_plugin_argument("--times", "-n", type = int, default = 1, help = "Repeat the greeting.")]

#[add_plugin_argument("--loud", action = "store_true", help = "Shout.")]
#[make_action]
fn greet(action: Vec<String>, kwargs: &Value) -> (bool, String) {
    let mut text = format!("Hello, {}!", action.join(", "));
    if kwargs["loud"] == true {
        text = text.to_uppercase();
    }
    (true, text.repeat(kwargs["times"].as_u64().unwrap_or(1) as usize))
}
```

`mrsm greet Alice --loud -n 2` prints `HELLO, ALICE!HELLO, ALICE!`. Supported options:

| Option | Values |
|---|---|
| `action` | `"store"` (default), `"store_true"`, `"store_false"`, `"store_const"`, `"append"`, `"append_const"`, `"count"` |
| `type` | `int`, `float` or `str` (bare or quoted), for `store` and `append` |
| `default`, `const` | A string, number or bool literal (`const` only with `store_const` / `append_const`) |
| `choices` | An array of literals, e.g. `choices = ["north", "south"]` |
| `help`, `metavar`, `dest` | Strings. `dest` defaults to the first long flag with dashes as underscores (`--dry-run` is `kwargs["dry_run"]`) |
| `required` | `true` or `false` |

`nargs` is not supported; use `action = "append"` to collect a flag given several times.
As with Python plugins, a flag that was not given (or is `false`) is absent from `kwargs`,
and a declared `default` is filled in. Values are checked as argparse checks them: a value
that is not a valid `int` / `float` or not one of the `choices` fails the command with
argparse's message (`argument --times/-n: invalid int value: 'x'`). A `required` flag is
required when one of the plugin's own actions runs (other commands are unaffected).

A flag may not reuse an option string that is already taken, by the built-in CLI (`-t`
is `--tags`, `-d` is `--debug`, ...) or by another plugin: as Python's
`add_plugin_argument` does, Meerschaum warns (`conflicting option string`) and ignores
that flag. Run `mrsm --help` to see the built-in flags.

### Version and requirements

Declare a version and requirements with top-level constants, the Rust spelling of
Python's `__version__` and `required`:

```rust
const VERSION: &str = "1.2.0";
const REQUIRED: &[&str] = &["plugin:noaa", "requests>=2"];
```

Both must be literals with exactly these types (`&str` and `&[&str]`); Meerschaum reads
them from the source without building the plugin. A `VERSION` or `REQUIRED` constant of
another type (say `const VERSION: u32`) is an ordinary constant and declares nothing. `VERSION` is the plugin's version (`0.0.0` when omitted): `register plugins`
uploads it, and `install plugins` skips a repository plugin whose version matches the
installed one, as it does for Python plugins. `REQUIRED` lists Python packages and
`plugin:` requirements that `install required` (and `install plugins`) install into the
plugin's virtual environment, exactly like a Python plugin's `required` list. Rust crates
belong in the [Cargo block](#dependencies), not in `REQUIRED`.

### Pipe operations

`pipe.connector_keys()`, `metric_key()`, `location_key()`, `instance_keys()`, `keys()`,
and `parameters()` expose the current Pipe's metadata. Host operations are fallible:

- `pipe.get_sync_time()?` returns `Option<Bound>`, reading the newest axis value from
  this Pipe's instance. An empty target returns `None`.
- `pipe.sync(rows)?` or `pipe.sync(frame)?` writes supplied data through the host's normal
  sync engine, including dtype enforcement and existing-row checks. It returns a
  `SuccessTuple` and inherits the surrounding sync options. Empty rows are a successful
  no-op. Supply data explicitly; a zero-argument sync would recursively invoke the plugin.

An integer-axis sync plugin can therefore be just:

```rust
fn sync(pipe: &Pipe) -> Result<SuccessTuple> {
    let next = match pipe.get_sync_time()? {
        Some(Bound::Int(last)) => last + 1,
        None => 1,
        _ => return Err(Error::Message("Expected an integer axis".into())),
    };
    pipe.sync(vec![json!({"id": next})])
}
```

Register an integer axis for this example:

```bash
mrsm register pipe -c plugin:readings -m readings -i sql:local --params '{"columns":{"datetime":"id"},"dtypes":{"id":"int"}}'
mrsm sync pipes -c plugin:readings -i sql:local
```

Host operations are synchronous and scoped to the current invocation. A `Pipe` cannot
be sent to background threads. Re-entering the same native library through a callback
returns an error instead of waiting on its own call lock. These operations apply to the
current Pipe; arbitrary connector access, other Pipe construction and Python helper
imports are not exposed.

### API routes

`#[api_route(...)]` on a top-level function serves it from `mrsm start api`, the native
counterpart of an `@api_plugin` route (the [routes guide](routes-and-mcp.md#routes) has the
server's rules). The method comes first (an identifier such as `GET` or a string such as
`"post"`; `"*"` is any method, and it defaults to `GET` when only the path is given),
then the path, then options:

```rust
/// One item; the doc comment is the route's description.
#[api_route(GET, "/readings/items/{id:int}", scopes = ["pipes:read"])]
fn item(request: &RouteRequest) -> Result<Value> {
    let id: i64 = request.path_param("id").unwrap_or("0").parse().map_err(|e| Error::Message(format!("{e}")))?;
    Ok(json!({"id": id, "q": request.query_param("q"), "user": request.principal}))
}

#[api_route(POST, "/readings/upload/{*name}", scopes = ["pipes:write"], name = "upload")]
fn upload_file(request: &RouteRequest) -> (u16, String) {
    (201, format!("{} bytes for {}", request.body.len(), request.path_param("name").unwrap_or_default()))
}

#[api_route("/readings/health", public = true)]
fn health() -> RouteResponse {
    RouteResponse::text("ok").header("cache-control", "no-store")
}
```

| Option | Meaning |
|---|---|
| `scopes = ["..."]` | Scopes the caller must hold (with the API's credentials) |
| `public = true` | Served without credentials; a public route may not list scopes |
| `name = "..."` | The route's name (default: the function's name) |
| `description = "..."` | Overrides the doc comment |

Paths use the [Starlette syntax](routes-and-mcp.md#paths) (`{id}`, `{name}.json`,
`{n:int}`, a final `{*rest}` or `{rest:path}`). The function may take
`request: &RouteRequest`: `method`, `path`, `route_path`, `path_params`, `query`,
`query_params`, `headers` (lowercase names, without `authorization`), `body` (bytes),
`principal` (`Option<Principal>` with `kind`, `username`, `user_id`, `user_type`,
`token_id`, `label`) and `scopes`, with the helpers `path_param`, `query_param`, `header`,
`cookie`, `text`, `json::<T>()` and `has_scope`. It returns a `RouteResponse` (`json`,
`text`, `bytes`, `empty`, `error`, then `with_status`, `header`, `with_body`), a JSON
`Value` (`application/json`), a `String` or `&str` (`text/plain`), `Vec<u8>`
(`application/octet-stream`), `(u16, T)` to set the status, or any of these in a `Result`
(an `Err` is a `500 {"detail": message}`). Route functions run concurrently, so they may not
use `pipe`, and shared state needs a `Mutex`.

The path is checked when the plugin is built, with the rules `mrsm start api` applies: bad
syntax (`"/a/{x:float32}"`, a `{*rest}` that is not last) or a path that would shadow a core
route (a parameter in the first segment, a core route group such as `/pipes`, `/dash`,
`/_pages`, `/static`, `/_mrsm`) is an error at the path's line, not a notice at server
startup.

### MCP tools, resources and prompts

`#[mcp_tool]`, `#[mcp_resource]` and `#[mcp_prompt]` on top-level functions expose them
through `/mcp` and `mrsm start mcp` (see the [MCP guide](routes-and-mcp.md#mcp-tools-resources-and-prompts)).
The doc comment is the description unless `description = "..."` is given; tools and
resources need one. Every entry takes `name = "..."` (default: the function's name),
`title = "..."` and `scopes = ["..."]`. JSON options are string literals, usually raw
strings.

```rust
/// Count the rows of a synthetic table.
#[mcp_tool(read_only = true, input_schema = r#"{"type": "object", "properties": {"table": {"type": "string"}}, "required": ["table"]}"#)]
fn count_rows(call: &ToolCall) -> Result<Value> {
    let table = call.argument("table").and_then(Value::as_str).unwrap_or_default();
    Ok(json!({"table": table, "rows": 0, "caller": call.principal}))
}

/// One reading.
#[mcp_resource(uri_template = "readings://items/{id}", scopes = ["pipes:read"])]
fn reading(resource: &ResourceRead) -> ResourceContents {
    ResourceContents::json(&json!({"id": resource.variable("id")}))
}

/// Summarize a station.
#[mcp_prompt(arguments = ["station"])]
fn summarize(prompt: &PromptGet) -> String {
    format!("Summarize the readings of {}.", prompt.argument("station").unwrap_or("?"))
}
```

| Attribute | Options | Parameters | Returns |
|---|---|---|---|
| `#[mcp_tool]` | `input_schema` (a JSON Schema object, checked when the plugin is built; default: no arguments), `output_schema`, `read_only = true` (`readOnlyHint`, so read-only servers still offer it), `annotations` (a JSON object) | `call: &ToolCall` (`name`, `arguments`, `principal`, `scopes`, `argument(name)`, `arguments::<T>()`), `arguments: &Value` | any serializable value in a `Result`, a JSON `Value` or a `String` |
| `#[mcp_resource]` | `uri` or `uri_template` (exactly one), `mime_type` | `resource: &ResourceRead` (`name`, `uri`, `variables`, `variable(name)`, `principal`, `scopes`) | `ResourceContents` (`text`, `json`, `blob`), a `String`, a JSON `Value`, or a `Result` of those |
| `#[mcp_prompt]` | `arguments`: `["name", ...]` (required arguments) or a JSON array of `{"name", "description"?, "required"?}` | `prompt: &PromptGet` (`name`, `arguments`, `argument(name)`, `principal`, `scopes`), `arguments: &Value` | a `String` (one user message), `Vec<Value>` messages, a JSON `Value` `{"description"?, "messages"}`, or a `Result` of those |

Tool arguments are validated against `input_schema` before your function runs. Like
routes, these functions run concurrently and without `pipe`. A prompt message with another
role is `meerschaum_plugin::mcp::message("assistant", text)`, as in the Cargo SDK.

### Executors

`#[make_executor]` on a top-level inline module declares the executor type named after
the module (`#[make_executor(name = "procs")]` to choose another; `local`, `systemd`, `api`
and `none` are reserved). Its functions named after the [executor methods](executors-and-streams.md#the-methods)
answer them; others are helpers. A method you leave out answers "not supported", except
those derived from others, exactly as in the Cargo SDK: `get_job_exists` and `get_jobs`
from `get_job_names` and `get_job_metadata`, and `get_job_properties`, `get_job_status`
and `get_job_began` / `_ended` / `_paused` from the metadata.

```rust
/// Run each job as a local process.
#[make_executor]
mod procs {
    fn get_job_names(executor: &ExecutorInfo) -> Vec<String> { /* list the jobs' directories */ vec![] }
    fn get_job_metadata(executor: &ExecutorInfo, name: &str) -> Result<Value> { /* null when missing */ Ok(Value::Null) }
    fn create_job(executor: &ExecutorInfo, name: &str, sysargs: Vec<String>, properties: &Value) -> Result<SuccessTuple> {
        // Start `executor.mrsm` + `sysargs` under `executor.root_dir`, then report it.
        Ok(SuccessTuple::ok(format!("Started job '{name}'.")))
    }
    fn stop_job(name: &str) -> (bool, String) { (true, format!("Stopped job '{name}'.")) }
    fn monitor_logs(name: &str, stop_on_exit: bool) -> impl FnMut(Duration) -> LogEvent {
        let mut done = false;
        move |_timeout| if std::mem::replace(&mut done, true) { LogEvent::End(None) } else { LogEvent::Lines(vec!["started".into()]) }
    }
}
```

Every method may take `executor: &ExecutorInfo` (`executor_type`, `label`, `config` (the
connector configuration of `<type>:<label>`), `root_dir` (the root the jobs belong to, a
compose project's root for its jobs; key file state on it), `mrsm` (the command that
runs this Meerschaum: start a job as `mrsm` followed by its `sysargs`) and `keys()`), and
`name: &str` where the method has a job. `create_job` also takes `sysargs: Vec<String>` (or
`&[String]`, without `-d` / `-e`) and `properties: &Value` (or `Value`), `write_stdin` takes
`data: &str`, and `monitor_logs` takes `strip_timestamps: bool` and `stop_on_exit: bool`.

| Method | Returns |
|---|---|
| `get_job_names` | `Vec<String>` |
| `get_job_exists`, `get_job_is_blocking_on_stdin` | `bool` |
| `get_jobs`, `get_job_metadata`, `get_job_properties`, `get_job_prompt_kwargs` | `Value` |
| `get_job_status` | `String` or `&str` (`running`, `paused` or `stopped`) |
| `get_job_began`, `_ended`, `_paused`, `get_logs`, `get_job_stop_time` | `Option<String>` |
| `create_job`, `start_job`, `stop_job`, `pause_job`, `delete_job` | `(bool, String)`, `(bool, &str)` or `SuccessTuple` |
| `write_stdin` | `()` |
| `monitor_logs` | a closure `FnMut(Duration) -> LogEvent` (or `-> Result<LogEvent>`) called for each poll, a type implementing `LogStream` (`next_lines`, optional `close`), or `Box<dyn LogStream>` (the Cargo SDK's type) |

Any of these may be wrapped in `Result` (an `Err` is reported as the executor's failure).
`LogEvent` is `Lines(Vec<String>)`, `Timeout` or `End(Option<SuccessTuple>)`; the closure may
block up to the timeout it receives, and it is dropped when the host stops following.
`show logs` follows a job through `monitor_logs`; an executor without it shows its logs only
with `show logs --nopretty`, which reads `get_logs`.

### Streaming connectors

`fn subscribe` inside a `#[make_connector]` module makes the connector type a stream source
for `sync pipes --listen` and the engine's supervised streams, the counterpart of an
`IS_STREAMING` connector's `subscribe`. A top-level `fn subscribe` does the same for
`plugin:<name>`. `subscribe` may take `pipe: &Pipe` (with its
[operations](#pipe-operations)), `connector: &Connector` and `kwargs: &Value` (the sync's
keyword arguments, as Python's `subscribe` receives them) and returns, optionally in a
`Result`, a closure `impl FnMut(Duration) -> Batch` (or `-> Result<Batch>`), an
`impl Subscription`, a `Box<dyn Subscription>` (the Cargo SDK's type), or a type the file
implements `Subscription` for. A function named `subscribe` with other parameters or another
return type is an ordinary helper, at the top level and in a connector module, as it was
before streaming connectors (a connector module whose only function is such a `subscribe`
is an error that says why).

```rust
/// Readings as they arrive.
#[make_connector]
mod ticker {
    fn subscribe(pipe: &Pipe, kwargs: &Value) -> Result<impl FnMut(Duration) -> Batch> {
        let station = pipe.parameters()["fetch"]["station"].as_str().unwrap_or("a").to_owned();
        let mut next = 0;
        // Inside `Ok(...)`, Rust needs the closure's parameter type spelled out.
        Ok(move |timeout: Duration| {
            std::thread::sleep(timeout.min(Duration::from_secs(1)));
            next += 1;
            Batch::RowsAck(vec![json!({"id": next, "station": station})], next)
        })
    }

    // Optional: the tokens of batches whose rows committed.
    fn ack(tokens: &[u64]) -> Result<()> {
        Ok(())
    }
}
```

The host calls the closure for each batch, with the longest time it may block, and only
after the previous batch reached the sink, so a slow sink slows the source down. `Batch` is
`Rows(rows)`, `RowsAck(rows, token)` (acknowledge `token` once the rows committed; empty rows
still get their token acknowledged), `Data { rows, ack, redelivered }` (`redelivered` forces
`check_existing`), `Timeout` or `End` (the host closes the subscription and subscribes again
after a backoff). Rows are JSON objects, as `fetch` returns them. The optional module-level
`fn ack(tokens: &[u64])` (it may also take `connector: &Connector`) runs after the
subscription's own `ack`; a type implementing `Subscription` may instead override
`ack(&mut self, tokens)` and `close(&mut self)`. The returned closure lives beyond the call,
so it cannot borrow `pipe` or `kwargs`: copy what it needs, as `station` above (Meerschaum adds
`+ use<>` to an `impl` return type for you, so a borrow is reported at your line).

A producer thread can feed a `ChannelSubscription`, as in the Cargo SDK: a full channel blocks
the producer, and the subscription ends when every sender is dropped.

```rust
#[make_connector]
mod feed {
    fn subscribe(pipe: &Pipe) -> Result<Box<dyn Subscription>> {
        let count = pipe.parameters()["fetch"]["count"].as_u64().unwrap_or(3);
        let (sender, subscription) = ChannelSubscription::new(1);
        std::thread::spawn(move || {
            for id in 0..count {
                let rows = Frame::from_rows(vec![json!({"id": id})]).unwrap();
                if sender.send(rows).is_err() {
                    return;
                }
            }
        });
        Ok(Box::new(subscription))
    }
}
```

### Instance connectors

`#[make_connector(instance)]` (or `instance = true`) makes the module's connector type an
instance, the counterpart of a Python connector with `IS_INSTANCE = True`: `-i <type>:<label>`
then registers, syncs, lists and deletes pipes through it. Its functions named after the
instance methods answer them; others are helpers. A method you leave out is "not
implemented", so the host falls back where it can (file-based leases, for example). The module
may also define `fetch`, `sync` and `subscribe`, as a source connector does.

```rust
/// Pipes and rows in memory.
#[make_connector(instance)]
mod memory {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    static ROWS: Mutex<BTreeMap<String, Vec<Value>>> = Mutex::new(BTreeMap::new());

    fn key(pipe: &Pipe) -> String { format!("{}|{}", pipe.connector_keys(), pipe.metric_key()) }

    fn register_pipe(pipe: &Pipe) -> SuccessTuple {
        ROWS.lock().unwrap().entry(key(pipe)).or_default();
        SuccessTuple::success()
    }
    fn pipe_exists(pipe: &Pipe) -> bool { ROWS.lock().unwrap().contains_key(&key(pipe)) }
    fn sync_pipe(pipe: &Pipe, data: Vec<Value>) -> (bool, String) {
        let count = data.len();
        ROWS.lock().unwrap().entry(key(pipe)).or_default().extend(data);
        (true, format!("Inserted {count} rows."))
    }
    fn get_pipe_data(pipe: &Pipe, begin: Option<Bound>, params: &Value) -> Vec<Value> {
        ROWS.lock().unwrap().get(&key(pipe)).cloned().unwrap_or_default()
    }
    fn get_sync_time(pipe: &Pipe, newest: bool) -> Option<i64> { None }
    fn fetch_pipes_keys(query: &Value) -> Value {
        Value::Array(ROWS.lock().unwrap().keys().map(|k| {
            let (c, m) = k.split_once('|').unwrap();
            json!([c, m, null])
        }).collect())
    }
}
```

| Method | Returns |
|---|---|
| `register_pipe`, `delete_pipe`, `drop_pipe`, `edit_pipe`, `sync_pipe`, `clear_pipe`, `deduplicate_pipe`, `drop_pipe_indices`, `create_pipe_indices`, `compress_pipe`, `decompress_pipe`, `analyze_pipe`, `vacuum_pipe`, `partition_pipe` | `SuccessTuple`, `(bool, String)` or `(bool, &str)` |
| `get_pipe_data` | `Vec<Value>` rows, `Frame` or `Option<Frame>` |
| `test_connection`, `pipe_exists`, `acquire_pipe_lease`, `release_pipe_lease` | `bool` |
| `fetch_pipes_keys` | `[[connector_keys, metric_key, location_key], ...]` (or a map of pipe IDs to those lists) |
| `get_pipe_id`, `get_pipe_rowcount`, `get_pipe_size` | `Option<i64>` (or `i64`) |
| `get_sync_time` | an integer, an ISO timestamp string, or `None` |
| `get_pipe_attributes`, `get_pipe_columns_types`, `get_pipe_index_names`, `get_pipe_columns_indices` | a JSON object (`Value`, or any serializable map) |

Any of these may be wrapped in `Result`; the value methods may return any serializable type.
Parameters bind by name: `pipe: &Pipe`, `connector: &Connector`, `data: Option<Frame>` or
`data: Vec<Value>` (the rows of `sync_pipe`), `context: &Value` (the call's whole JSON context,
as `NativeConnector` sends it), and any field of the call: `query`, `options`
and `params` (`&Value`), `begin` / `end` (`Option<Bound>`), `select_columns`, `omit_columns`,
`limit`, `order`, `check_existing`, `chunksize`, `extra`, `newest`, `patch`, `no_policy`,
`full`, `chunk_minutes`, `holder` (`&str`) and `ttl_seconds`. A field is read from the context,
then from its `query`, then from its `options`; declare it as `&Value`, `&str`, or any owned
type it deserializes into (`bool`, `f64`, `Option<i64>`, `Option<Vec<String>>`, ...).

### Console pages

`#[web_page]` on a top-level function serves it as a page of the web console
(`meerschaum[dash]`), the counterpart of Python's `@web_page`. The path defaults to the
function's name and is served under `/dash/`; the options are Python's:

```rust
/// A report of the readings.
#[web_page("readings/report", login_required = false, page_group = "Readings")]
fn report(page: &PageRequest) -> String {
    let station = page.query_param("station").unwrap_or_default();
    format!("<h3>Readings for {station}</h3><p>Viewed by {}.</p>", page.username.as_deref().unwrap_or("a guest"))
}

#[web_page]
fn status() -> (u16, String) {
    (503, "<p>Not ready yet.</p>".into())
}
```

| Option | Meaning |
|---|---|
| a path (first) | `"readings/report"` is served at `/dash/readings/report` (a leading `/dash/` is accepted); default: the function's name |
| `login_required = false` | Served without a console session (default `true`) |
| `skip_navbar = true` | Hide the console's navigation bar (default `false`) |
| `page_group = "..."` | The navigation group (default: the plugin's name) |
| `dark_theme = false` | Opt out of the console's dark component theme (default `true`) |

The function may take `page: &PageRequest` (`path`, the raw `query`, `query_param(name)`, and
`username`, `None` for a guest) and returns the page's HTML as a `String` or `&str`, `(u16, T)`
to set the status, or a `Result` of those (an `Err` is a `500` page showing its message). The HTML
follows the [page contract](/reference/migrating-to-v5/#option-b-the-page-contract-recommended-for-new-pages)
(`data-mrsm-figure` / `data-mrsm-map` elements). The path is checked when the plugin is built:
relative segments of letters, digits, `-` and `_`, no wildcards. Pages are fixed when the
server starts.

### Dependencies

The standard library and shorthand types need no declarations. To use an external
crate, add one optional Cargo block at the **top of the same file**:

```rust
//! ```cargo
//! [dependencies]
//! regex = "1"
//! ```

use regex::Regex;

#[make_action]
fn count_words(action: Vec<String>) -> (bool, String) {
    let words = Regex::new(r"\w+").unwrap();
    let count = words.find_iter(&action.join(" ")).count();
    (true, format!("{count} words"))
}
```

Run `mrsm count words hello world`. Cargo versions, feature lists, renamed dependencies,
Git dependencies, and local `path` dependencies use their normal Cargo syntax.
Local dependency paths resolve relative to the original `.rs` file, rather than the cache.
You can enable Serde features, for example `serde = { version = "1", features = ["derive"] }`;
`serde_json` keeps the features required by the runtime.

Meerschaum generates the Cargo project and keeps its `Cargo.lock` in the build cache.
Unchanged plugins reuse the built library; editing a dependency declaration triggers a
rebuild. Dependencies are compiled into the plugin, so Cargo is needed for builds but
not to use an already-built library. First builds may download crates. Nothing is
installed into a Python virtual environment.

Only `[dependencies]` is supported in the inline block. Use a packaged Cargo plugin for
workspace settings, build dependencies, custom profiles, or publishing a lockfile.
Changes to external path dependencies are not fingerprinted: edit the `.rs` file or
remove its build cache after changing such a dependency. Relative `include_str!` and
other source paths still resolve from the cached source; use a Cargo plugin for assets.

### Single-file build cache

The filename supplies the plugin name; the version is `VERSION` (`0.0.0` when omitted).
Sources and build output stay under `<root>/cache/native_build/<name>/`; nothing is
written beside the original `.rs` file. Unchanged builds are reused, edits trigger
recompilation, and failed builds are remembered until an edit or cache deletion.
The small runtime is embedded by the running host; there is no published SDK dependency.
Shared Cargo dependencies live in `<root>/cache/native_build/.single_file_target/`; each
plugin has its own copied library and build stamp. Compiler diagnostics are saved in
`<root>/cache/native_build/<name>/mrsm-build-error.txt` and shown again for failed builds.

Rust returns the final expression of a function. In the example above, do **not** put
a semicolon after `vec![...]`: that would discard the rows and return `()` instead of
`Vec<Value>`.

Use the full crate layout above for multiple source files, relative included assets, typed
Arrow frames, or a hand-written manifest. Relative paths in a
shorthand file resolve from its generated copy in the cache.

