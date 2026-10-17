# 🦀 Rust Plugins

!!! info "v5 is still a Python package"

    Meerschaum 5 is installed the same way as before (`pip install meerschaum`), and most users keep writing Python plugins, which work unchanged. Rust plugins are an option, not a requirement: use one for the native performance path (the Rust core, kernel and Syncing Engine) or to run on the Python-free `slim` image. Everything on this page is about that option.

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

A directory that contains both a `plugin.toml` and Python files is a Rust plugin. If a Rust plugin and a Python plugin have the same name, **the Rust plugin replaces the Python plugin entirely**: its actions, hooks, connectors, routes and pages disappear from the merged manifest, and nothing falls back to the Python module, even for functions the library does not implement. Python plugins with other names are unaffected and keep working beside the native one. To port a plugin gradually, split it: move one capability into a Rust plugin with a new name and keep the rest in Python (the two may depend on each other through `required`).

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

`manifest()` returns the same JSON document the Python plugin manifest uses, as a whole: the top-level keys `schema_version` (use the SDK constant `meerschaum_plugin::MANIFEST_SCHEMA_VERSION`; it is `3`, the native manifest's own schema, not the Python worker's), `meerschaum_version`, `python_version` (`""`), `fingerprint` (`null`) and `plugins`, plus `connector_types`, `pre_sync_hooks` and `post_sync_hooks` when you provide them (the shipped example is the reference). The host validates it against the bundled JSON schema (`crates/meerschaum-plugin/schema/manifest.schema.json`) at load time, so `--help`, tab completion and the server can learn what your plugin offers **without loading the library**: it is cached by a fingerprint of the plugin directory and rebuilt when a file changes.

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

Hooks run around every sync that goes through the sync engine (`sync pipes`, jobs, compose). A failing hook never fails the sync: the host prints `Warning: Native plugin '<plugin>' hook failed: <hook>: <message>` on stderr and carries on with the remaining hooks. A hook that succeeds prints nothing, whereas the messages of Python hooks are shown in the sync output.

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

These suites build the example plugins in `crates/meerschaum-plugin/examples/` (`native_demo`, `native_probe`, `bad_abi`) and load them as real shared libraries; all data is synthetic. Run each with a temporary `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR`:

```bash
cargo test -p meerschaum-plugin                       # the SDK and loader
cargo test -p meerschaum-cli --test native_plugins    # the `mrsm` binary: actions, flags, a native source
cargo test -p meerschaum-cli --test native_e2e        # hooks, routes, shadowing, rejection, cache
cargo test -p meerschaum-cli --test native_source_plugin   # a Cargo crate built, cached and rebuilt by mrsm (compiles the SDK: slow the first time)
MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console -k rust_pages   # a native console page in Chromium
```

| Capability | Covered by |
|---|---|
| Fetch source, sync override, in-memory instance, actions, hooks, panic containment (library level) | `meerschaum-plugin/tests/native.rs` |
| Action dispatch, `show plugins`/`show actions`, flags, panic containment, a sync from a native source | `native_plugins.rs` |
| Native hooks around real syncs, the built-in sync writing a native fetch, failing hooks as warnings | `native_e2e.rs` |
| Routes (public, authenticated, `--no-auth`) served by `mrsm start api` | `native_e2e.rs` |
| A native plugin replacing a Python plugin of the same name | `native_e2e.rs` (needs a Python environment; skipped without one) |
| A custom instance (`-i native_demo:memory`): pipes listed and synced, file-lock fallback for leases | `native_e2e.rs` |
| Corrupt library and wrong ABI skipped, manifest cache follows the directory | `native_e2e.rs`; discovery details in `native.rs` |
| `plan pipes` / `show graph` with a native source | `native_e2e.rs` |
| A source crate built by `mrsm`, reused on the next run, rebuilt after an edit; a mismatched crate name and a missing `cargo` skipped | `native_source_plugin.rs` (real `cargo`); stamp, fingerprint and failure handling in `meerschaum-plugin/tests/source.rs` |
| A console page rendered in the browser, identical to the Python page | `tests/console/test_console.py` |

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

Today a native plugin is distributed as a directory you copy into `MRSM_PLUGINS_DIR` (a prebuilt `plugin.toml` + `lib<name>.<ext>` for the target OS and CPU, or a source crate that Meerschaum builds, see [Source plugins](#source-plugins)). Python plugins keep using `required`, `plugin:name@api:repo` entries and `.mrsm_origins.json` unchanged, and a Python plugin may depend on a Rust plugin and vice versa. Planned: a plugin repository that stores per-target-triple artifacts plus a source tarball, with `install plugin` choosing the matching triple and falling back to `cargo build --release` when none matches.

!!! note "Status in this pre-release"

    Source plugins (a Cargo crate built by Meerschaum into the root's cache) work when `cargo` is on `PATH`; the prebuilt `lib<name>.<ext>` remains the contract for the slim image. Discovery, the manifest cache, actions, arguments, hooks, source `fetch`/`sync`, custom connectors, routes and pages are implemented, and each is exercised through the `mrsm` binary by the tests listed under [Testing your plugin](#testing-your-plugin). The deepest instance operations (reads, rowcounts, maintenance) are exercised only at the library level; through the binary the example instance proves resolution, `fetch_pipes_keys`, `sync_pipe` and the lease fallback. Not yet available: `subscribe` for continuous streams from a native plugin (it needs a reader/sink SDK beyond the current eager `Frame` API), repository upload and target-triple selection by `install plugin`, and live reload of an already-open library (replace a plugin while `mrsm` is not running).
