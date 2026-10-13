# 🦀 Rust Plugins

> New in v5. A Rust plugin is a native shared library that Meerschaum loads in-process. It can do everything a [Python plugin](/reference/plugins/types-of-plugins/) can: fetch or sync data, add actions and arguments, run hooks around syncs, provide a whole connector, serve HTTP routes and add console pages. Python plugins are unchanged, and the two kinds live side by side in the same plugins directory.

Choose a Rust plugin when you want no Python interpreter at runtime (the `bmeares/meerschaum:slim` image), predictable performance on large frames, or a single binary artifact. Keep writing Python for everything else.

!!! warning "Trust"

    A plugin is arbitrary native code running inside the `mrsm` process. The ABI is a compatibility contract, **not a sandbox**. Install only plugins you trust. (A sandboxed WebAssembly host for the same interface is planned for v5.x.)

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

A directory that contains both a `plugin.toml` and Python files is a Rust plugin. If a Rust plugin and a Python plugin have the same name, **the Rust entry points take precedence**. This lets you port a plugin one function at a time: ship the Rust library, keep the Python module as the fallback for anything the library does not implement.

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
meerschaum-plugin = "5"   # or a path/git dependency on crates/meerschaum-plugin until it is published

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
- **Synchronous calls.** The SDK imposes no async runtime. Calls into one loaded plugin are serialized by the host. Schedule blocking work on your own executor if you need one.
- **Panics are contained.** Constructors and calls run inside `catch_unwind` and surface as an error (`native plugin panicked`). An aborting panic, `process::exit` or memory fault cannot be recovered. Build with `panic = "unwind"`.
- **Same OS and architecture.** The host and the plugin do not need the same Rust compiler version, since only `extern "C"` and the Arrow C interfaces cross the boundary, but they must target the same OS and CPU.

### What `Request` contains

| Field | Meaning |
|---|---|
| `method` | The operation: `fetch`, `sync`, `action`, `pre_sync_hook`, `post_sync_hook`, `routes`, `route`, `page`, `register_pipe`, `get_pipe_data`, and so on |
| `connector_type`, `label`, `config` | Which connector instance the call is for, and its configuration |
| `context` | A JSON object. For pipe operations, `{"pipe": {"keys": ..., "parameters": ..., "temporary": ...}}`; reads add `query`, syncs add `options`; actions and hooks use `{"name": ..., "kwargs": ...}` |

Bounds (`begin`, `end`) are integers, ISO timestamp strings or `null`. Results are plain JSON: success tuples as `[true, "message"]`, missing values as `null`.

## 📜 The manifest

`manifest()` returns the same JSON document the Python plugin manifest uses (schema version 3). The host validates it at load time, so `--help`, tab completion and the server can learn what your plugin offers **without loading the library**: it is cached by a fingerprint of the plugin directory and rebuilt when a file changes.

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

## 🧩 What a plugin can provide

| You want | Do this |
|---|---|
| A **fetch source** for `plugin:<name>` pipes | Implement `fetch`, list `fetch` under `data_functions`. The engine passes `begin`, `end`, `params` and the pipe's keys and parameters in `context`. |
| A **sync override** | Implement `sync`; it receives optional data and returns a success tuple, like a Python `sync(pipe, **kw)`. |
| An **action** (`mrsm hello`) | Add it to `actions` and answer the `action` method (`context.name`, `context.kwargs`). Add flags with `arguments`. |
| **Hooks** | List names under `pre_sync_hooks` / `post_sync_hooks`; they receive the same kwargs as Python's `@pre_sync_hook` and `@post_sync_hook` (`success_tuple`, `sync_timestamp`, `sync_duration`, ...). |
| A whole **connector** or **instance** | List it under `connectors` / `connector_types` (set `is_instance`), and answer the instance methods (`register_pipe`, `sync_pipe`, `get_pipe_data`, `fetch_pipes_keys`, ...). A method you do not implement returns `Error::Unsupported`, which the host maps to "not implemented" and lets the engine fall back (for example to file-based leases). |
| **HTTP routes** | List `api_plugins`; answer `routes` with `[{"method", "path", "public"?}]` and `route` with `{status, content_type, body}`. Routes require the caller's credentials unless `"public": true` or the server runs with `--no-auth`. Paths under `/dash`, `/webterm` or a core route group are rejected. |
| A **console page** | List it under `web_pages`; answer `page` with `{html, status?}`. The console wraps your fragment in its chrome. See the [page contract](/reference/migrating-to-v5/#option-b-the-page-contract-recommended-for-new-pages). |

Data always crosses the boundary as an **Arrow C stream** (`FFI_ArrowArrayStream`), never JSON. The SDK handles that for you: you return a `Frame`, and the host imports it without copying through Python.

## 🏗️ Build, install, load

Build the shipped example from a checkout:

```bash
cargo build --release -p meerschaum-plugin --example native_demo
```

Install it into a plugins directory:

```bash
mkdir -p "$MRSM_PLUGINS_DIR/native_demo"
cp crates/meerschaum-plugin/examples/plugin.toml "$MRSM_PLUGINS_DIR/native_demo/"
cp target/release/examples/libnative_demo.so "$MRSM_PLUGINS_DIR/native_demo/"
```

Point `MRSM_PLUGINS_DIR` at the directory (use a temporary `MRSM_ROOT_DIR` while experimenting). The action and its argument are discovered immediately:

```text
$ mrsm hello
 🎉 Hello from native_demo

$ mrsm --help | grep -A1 hello
hello
  --native-demo  Enable demo.
```

Register a pipe that uses it as a source:

```bash
mrsm register pipe -c plugin:native_demo -m demo -i sql:local --params '{"columns": {"id": "id"}}'
mrsm sync pipes -i sql:local --nopretty
```

```text
{"pipe": {"connector_keys": "plugin:native_demo", "metric_key": "demo", ...}, "success": true, "message": "Success"}
```

The example declares both `fetch` **and** `sync`, so (exactly like a Python plugin that defines `sync()`) its own `sync` replaces the built-in sync process: it keeps the rows in memory and reports `Success` instead of writing a table. Remove `sync` from `data_functions` to let Meerschaum write the fetched frame to your instance.

## 🔬 The ABI in one page

The library exports a single symbol, `mrsm_plugin_v1`, which returns a static `#[repr(C)]` table:

| Field | Purpose |
|---|---|
| `abi_version`, `struct_size` | A permanent prefix. The host reads **only** these two `u32`s before rejecting an incompatible or truncated table. |
| `describe()` | Returns metadata and the manifest as JSON. |
| `invoke(request_json, input_stream, output_stream)` | One entry point for every method; `request.method` selects the operation. Input and output are Arrow streams. |
| `free_bytes(bytes)` | Releases every payload the plugin allocated. |

Every result carries a status: `0` success (JSON), `1` error (UTF-8 message), `2` caught panic, `3` unsupported method. Memory a plugin allocates is always released by that plugin's `free_bytes`; the host never frees it.

The ABI version is checked at load. A breaking change would ship as a new `mrsm_plugin_v2` symbol, and the host keeps loading v1 for at least one major release. The same interface is also written as WIT (`crates/meerschaum-plugin/wit/plugin.wit`) for the future WebAssembly host.

## 📦 Distribution

The plugin repository stores **per-target-triple artifacts** plus a source tarball. On install, the matching triple is downloaded; if none matches and a Rust toolchain is available, the plugin is built with `cargo build --release`. Python plugins keep using `required`, `plugin:name@api:repo` entries and `.mrsm_origins.json` unchanged, and a Python plugin may depend on a Rust plugin and vice versa.

!!! note "Status in this pre-release"

    Discovery, the manifest cache, actions, arguments, hooks, source `fetch`/`sync`, custom connectors, routes and pages are implemented and tested. Not yet available: `subscribe` for continuous streams from a native plugin (it needs a reader/sink SDK beyond the current eager `Frame` API), repository upload and target-triple selection by `install plugin`, and live reload of an already-open library (replace a plugin while `mrsm` is not running).
