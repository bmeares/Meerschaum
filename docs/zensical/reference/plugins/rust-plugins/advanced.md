# Advanced Rust Plugins

Start with the [single-file Rust plugin guide](../rust-plugins.md) for quick actions, data sources, and inline dependencies. This page covers packaged Cargo plugins, the host-call API, the C ABI, and the complete single-file reference.

Single-file `.rs` plugins support every native feature — actions, connectors, instances, sync hooks, `setup`, CLI flags, HTTP routes, MCP entries, console pages, executors, and streams. Use a Cargo crate when you want multiple source files, typed Arrow frames, bundled assets, or full manifest control.

!!! warning "Trust"
    Native plugins compile and run inside the Meerschaum process. Only load plugins you trust.

## 📁 Layout & Precedence

A packaged plugin lives in `MRSM_PLUGINS_DIR` with a `plugin.toml` descriptor and either a prebuilt library or a Cargo crate:

```text
plugins/
└── native_demo/
    ├── plugin.toml          # name = "native_demo", version = "1.0.0"
    ├── libnative_demo.so    # .dylib on macOS; libnative_demo.dll on Windows (always `lib` prefix)
    └── Cargo.toml           # optional: auto-built by mrsm when cargo is on PATH
```

Names allow letters, digits, `_`, and `-`. Earlier `MRSM_PLUGINS_DIR` entries win, and invalid plugins are skipped with a stderr warning while others still load.

**Rust before Python:** If a Rust plugin and a Python plugin share a name, the Rust plugin replaces the Python plugin completely — nothing falls back to Python. To migrate gradually (for example keeping `@dash_plugin` callbacks or custom FastAPI apps in Python), split them into separate names (e.g. `acme_data` in Rust and `acme_ui` in Python) and read the Rust pipe from Python via `mrsm.Pipe('plugin:acme_data', ...)`.

## 🔧 Writing a Cargo Plugin

Configure a `cdylib` crate with `panic = "unwind"` so the SDK can catch panics across the FFI boundary:

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
panic = "unwind"
```

Implement `Plugin` and export it with `export_plugin!` (name and version come from `Cargo.toml` unless overridden via `const NAME` / `const VERSION`):

```rust
use meerschaum_plugin::prelude::*;

#[derive(Default)]
struct Demo;

impl Plugin for Demo {
    fn manifest(&self) -> Value {
        Manifest::new()
            .fetch()
            .action("hello", "Return a synthetic greeting.")
            .into()
    }

    fn fetch(&self, _request: &Request) -> Result<Option<Frame>> {
        let rows = vec![
            json!({"ts": "2024-01-01T00:00:00Z", "id": 1, "value": 1.5}),
            json!({"ts": "2024-01-01T01:00:00Z", "id": 2, "value": 2.5}),
        ];
        Frame::from_rows(rows, &[("ts", "datetime"), ("id", "int"), ("value", "float")]).map(Some)
    }

    fn call(&self, request: &Request, _data: Option<Frame>) -> Result<Reply> {
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

`Frame::from_rows(rows, dtypes)` builds a typed Arrow `Frame` using Meerschaum dtypes (`datetime`, `date`, `int`, `float`, `string`, `bool`, `json`, `numeric`, `uuid`, `bytes`, `geometry[...]`); `frame.to_rows()` converts back, and `Frame::from_record_batch` accepts Arrow batches (`arrow`, `chrono`, `serde_json` are re-exported). Examples: [`native_demo.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_demo.rs), [`native_host.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_host.rs).

| `Request` field | Meaning |
|---|---|
| `method` | `fetch`, `sync`, `action`, `pre_sync_hook`, `post_sync_hook`, `api_route`, `mcp_*`, `page`, `register_pipe`, `get_pipe_data`, etc. |
| `connector_type`, `label`, `config` | Target connector keys and configuration. |
| `context` | Payload (`pipe`, `query`, `options`, `name`, `kwargs`). Bounds (`begin`, `end`) are ints, ISO strings, or `null`. |

Handlers are stateless (`Default::default()` per call; use a `Mutex` static keyed by label for state), synchronous (host-serialized except concurrent stream and server calls — see [Routes and MCP](routes-and-mcp.md#how-calls-run)), panic-contained (`catch_unwind`), and must match the host OS and CPU.

## 📜 Manifest & Capabilities

`manifest()` lets `--help`, completions, and servers discover capabilities without loading the library (cached in `<root>/cache/native_plugins_manifest.json`, schema `5` / `MANIFEST_SCHEMA_VERSION`):

```rust
fn manifest(&self) -> Value {
    Manifest::new()
        .fetch()
        .action("hello", "Return a greeting.")
        .flag("--loud", "Shout.")
        .pre_sync_hook("before").post_sync_hook("after")
        .connector(ConnectorType::new("memory", ["fetch", "register_pipe", "sync_pipe"]).instance())
        .executor_type("procs", ["get_job_names", "create_job"], "Run jobs as local processes.")
        .api_route(RouteSpec::new("item", "GET", "/my/items/{id}").scopes(["pipes:read"]))
        .mcp_tool(ToolSpec::new("add", "Add two integers."))
        .web_page("my/page", "page", true)
        .required(["plugin:noaa"])
        .into()
}
```

| Capability | Cargo manifest / method | Notes |
|---|---|---|
| Data & setup | `.fetch()`, `.sync()`, `.register()`, `.setup()` | `sync` overrides built-in sync; `register` returns default pipe params. |
| Actions & flags | `.action(name, doc)`, `.flag(...)` / `.argument(...)` | Unset flags are absent from `context.kwargs`. |
| Sync hooks | `.pre_sync_hook(name)`, `.post_sync_hook(name)` | Run before Python hooks; failures warn and never fail the sync. |
| Connectors & instances | `.connector_type(...)` / `ConnectorType::new(...).instance()` | Return `Error::Unsupported` for unimplemented instance methods. |
| HTTP routes & MCP | `.api_route(...)`, `.mcp_tool/resource/prompt(...)` | See [Routes and MCP](routes-and-mcp.md). |
| Executors & streams | `.executor_type(...)`, `is_streaming` + `subscribe` | See [Executors and Streams](executors-and-streams.md). |
| Console pages | `.web_page(path, fn_name, login_required)` | Answer `page` with `{html, status?}` for `meerschaum[dash]`. |

## 🏗️ Building, Distributing & Testing

Prebuilt libraries (required for `bmeares/meerschaum:slim`, which has no Rust toolchain):

```bash
cargo build --release -p meerschaum-plugin --example native_demo
mkdir -p "$MRSM_PLUGINS_DIR/native_demo"
cp crates/meerschaum-plugin/examples/plugin.toml target/release/examples/libnative_demo.so \
   "$MRSM_PLUGINS_DIR/native_demo/"
```

**Source auto-build:** Directories with `plugin.toml` + `Cargo.toml` (`cdylib`, `panic = "unwind"`, matching name) and no newer prebuilt library are auto-built via `cargo build --release` (`CARGO_INCREMENTAL=0`) into `<root>/cache/native_build/<name>/`. Ship `Cargo.lock` for read-only directories; delete the cache dir to retry a failed build.

**Repositories & testing:** `mrsm register plugins` / `install plugins` / `uninstall plugins` / `edit plugins` support both `.rs` files and packaged crates (skipping Python venvs and rolling back on build errors; prebuilt binaries must match host OS/CPU, and live hot-reload is unsupported). Test on a scratch root:

```bash
export MRSM_ROOT_DIR="$(mktemp -d)" MRSM_PLUGINS_DIR="$(mktemp -d)"
mrsm show plugins && mrsm sync pipes -c plugin:my_plugin -m demo -i sql:local --nopretty
```

## 🔬 The C ABI

Libraries export `mrsm_plugin_v1` returning a static `#[repr(C)]` vtable (status codes: `0` success JSON, `1` error, `2` caught panic, `3` unsupported):

| Field | Purpose |
|---|---|
| `abi_version`, `struct_size` | Fixed prefix verified before reading the rest of the table (currently ABI `1`). |
| `describe()` | Returns plugin metadata and manifest JSON. |
| `invoke(request_json, in_stream, out_stream)` | Single entry point for all methods; tabular data crosses as Arrow C streams (`FFI_ArrowArrayStream`). |
| `free_bytes(bytes)` | Releases plugin-allocated buffers (host and plugin always free their own allocations). |

Optional extensions: `mrsm_plugin_invoke_host_v1` passes a call-scoped `HostV1` callback table for host calls (older hosts degrade gracefully on newer methods), and single-file plugins additionally export `mrsm_plugin_invoke_rows_v1` to exchange lightweight JSON rows without compiling Arrow.

## Talking to Meerschaum

Plugins interact with the host process through **host calls** on `Pipe` and `host` (pre-imported in single-file plugins; `meerschaum_plugin::{host, Query}` in Cargo crates):

```rust
fn sync(pipe: &Pipe) -> Result<SuccessTuple> {
    let readings = host::pipe("plugin:readings", "readings", None, None)?;
    let daily = host::read_sql(
        pipe.instance_keys(),
        &format!(
            "SELECT date(ts) AS day, station, AVG(temperature) AS temperature FROM {} GROUP BY 1, 2",
            readings.target()
        ),
    )?;
    pipe.replace(daily, None, None)
}
```

| Call | Returns | Python equivalent |
|---|---|---|
| `host::pipe(conn, metric, loc, instance)` | `Pipe` | `mrsm.Pipe(conn, metric, loc, instance=...)` |
| `pipe.get_data(begin, end, params)` | Rows (`Vec<Value>` or `Frame`) | `pipe.get_data(begin=, end=, params=)` |
| `pipe.get_data_with(&Query::new()...)` | Rows | `pipe.get_data(select_columns=, omit_columns=, limit=, order=, ...)` |
| `pipe.get_rowcount(begin, end, params)` | `i64` | `pipe.get_rowcount(...)` (`0` if no table) |
| `pipe.get_sync_time()` / `_with(params, newest)` | `Option<Bound>` | `pipe.get_sync_time(params=, newest=)` |
| `pipe.sync(rows)` | `SuccessTuple` | `pipe.sync(docs)` |
| `pipe.replace(rows, begin, end)` | `SuccessTuple` | `pipe.sync(docs, begin=, end=, replace=True)` |
| `pipe.clear(begin, end, params)` | `SuccessTuple` | `pipe.clear(begin=, end=, params=)` |
| `pipe.exists()`, `id()`, `is_registered()`, `get_columns_types()`, `register()` | Metadata / `SuccessTuple` | Matching `Pipe` methods |
| `pipe.target()`, `datetime_column()`, `parameters()`, `with_parameters(...)` | Local accessors | `pipe.target`, `pipe.columns['datetime']`, `pipe.parameters` |
| `host::read_sql(conn, sql)` / `read_sql_with(conn, sql, &params)` | Rows | `conn.read(sql, params=...)` |
| `host::exec_sql(conn, sql)` / `exec_sql_with(conn, sql, &params)` | `SuccessTuple` | `conn.exec(sql, params)` |
| `host::transaction(conn, lock_key, \|tx\| ...)` | `Result<T>` | Atomic SQL transaction (`tx.read_sql_with`, `tx.exec_sql_with`) |
| `host::root_dir()` | `PathBuf` | Resolved `MRSM_ROOT_DIR` of the current call |
| `host::plugin_config()` / `write_plugin_config(&val)` | `Value` / `SuccessTuple` | `get_plugin_config()` / `write_plugin_config(...)` |
| `host::config(&["meerschaum", "instance"])` | `Value` | `get_config('meerschaum', 'instance')` |
| `host::mint_token(...)`, `revoke_token(id)`, `draining()` | `MintedToken` / `SuccessTuple` / `bool` | Route token lifecycle & shutdown ([rules](routes-and-mcp.md#tokens-and-draining)) |

**Parameterized SQL & transactions:** Named `:param` binds accept JSON scalars, `null`, arrays (native PostgreSQL `= ANY(:ids)`), and typed helpers (`Bound::to_sql_param()`, `host::sql_timestamptz`, `sql_timestamp`, `sql_date`, `sql_bytes`, `sql_json`). With `lock_key: Some(key)`, `host::transaction` acquires `pg_advisory_xact_lock` on Postgres or `BEGIN IMMEDIATE` on SQLite, commits on `Ok`, and rolls back on `Err` or panic:

```rust
let next_id: i64 = host::transaction("sql:main", Some("cmdq:1"), |tx| {
    let rows = tx.read_sql_with(
        "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM cmd_queue WHERE created_at >= :since",
        &json!({"since": host::sql_timestamptz("2026-09-25T00:00:00+00:00")}),
    )?;
    let id = rows[0]["next_id"].as_i64().unwrap_or(1);
    let result = tx.exec_sql_with("INSERT INTO cmd_queue (id) VALUES (:id)", &json!({"id": id}))?;
    if !result.success {
        return Err(Error::Message(result.message));
    }
    Ok(id)
})?;
```

Key host-call rules:

- **Default instance:** `host::pipe` with `instance = None` uses the synced pipe's instance, the action's `-i` / `meerschaum:instance`, or the API server's instance.
- **Writes & safety:** `pipe.sync` runs normal dtype checks/upserts (never recursively calling plugin `fetch`/`sync`), `pipe.replace` swaps `[begin, end)` atomically, and writes to `protected` instances fail unless `--allow-protected` / `MRSM_ALLOW_PROTECTED=1` is set.
- **Thread-bound:** Host calls work synchronously on the calling thread during plugin callbacks (`Pipe` is not `Send`, and re-entering the same native library returns an error). There is no Rust equivalent of `from_plugin_import`.

### Permissions in routes, MCP, and pages

Inside HTTP routes, MCP entries, and console pages, host calls enforce the caller's live scopes, pipe visibility, edit rights, and `api:permissions:instances` (under `--no-auth` and MCP stdio, all calls are allowed):

| Host calls | Required scope / rule |
|---|---|
| `host::pipe`, `get_data`, `get_rowcount`, `get_sync_time`, `exists`, `id`, `is_registered`, `get_columns_types` | `pipes:read` |
| `pipe.sync`, `pipe.replace` | `pipes:write` (plus `pipes:register` if creating) |
| `pipe.clear` / `pipe.register` | `pipes:delete` / `pipes:register` |
| `read_sql*`, `transaction` + `tx.read_sql*` | `sql:read`; served `sql:` connectors only; refuses hidden pipe targets, `mrsm_*` tables, `_mrsm_internal`, and dynamic SQL functions (`dblink`, `query_to_xml`, etc.) |
| `exec_sql*`, `tx.exec_sql*` | `actions:execute` + caller allowed to run actions (admins, or `non_admin: true` with `jobs:execute`; never under `--secure`) |
| `host::config` | `*` (admin or `--no-auth`) |
| `plugin_config`, `write_plugin_config`, `root_dir`, `draining` | None (scoped to plugin settings & call root) |
| `mint_token`, `revoke_token` | Signed-in caller holding each custom plugin scope ([rules](routes-and-mcp.md#tokens-and-draining)) |

Public routes and unauthenticated console pages can grant non-admin scopes for host calls via `host_scopes` (preserving Everyone visibility and edit rules):

```rust
#[api_route(GET, "/status/{metric}", public = true, host_scopes = ["pipes:read"])]
fn status(request: &RouteRequest) -> Result<Value> {
    let pipe = host::pipe("plugin:readings", request.path_param("metric").unwrap_or(""), None, None)?;
    Ok(json!({"rows": pipe.get_rowcount(None, None, None)?}))
}
```

## Single-file reference

Single-file `.rs` plugins need no Cargo project. Pre-imported types: `Pipe`, `Connector`, `Bound` (`Int(i64)` / `Datetime(String)` with `as_int()`, `timestamp()`, `from_timestamp()`), `SuccessTuple`, `Value`, `json!`, `Frame` (lightweight JSON row container), `Result`, `Error`, `host`, and `Query`.

### Macros & attributes

| Attribute / Function | Target | Purpose |
|---|---|---|
| `fn fetch(...)` / `fn sync(...)` | Top-level or connector `mod` | Data source fetch or sync override for `plugin:<name>` or `<type>:<label>`. |
| `fn setup(...)` | Top-level | Runs on `mrsm setup plugins` and after `install plugins`. |
| `#[make_action]` | Top-level `fn` | Registers a CLI action (stackable with hook attributes). |
| `#[pre_sync_hook]` / `#[post_sync_hook]` | Top-level `fn` | Sync lifecycle hooks (run in file order). |
| `#[add_plugin_argument(...)]` | File (`#![...]`) or top-level `fn` | Declares CLI flags received in `kwargs`. |
| `#[make_connector]` / `#[make_connector(instance)]` | Inline `mod` | Source connector or `-i` instance connector; use `#[method]` inside for custom Python-callable methods. |
| `#[web_page(...)]` | Top-level `fn` | HTML fragment served under `/dash/` in `meerschaum[dash]`. |
| `#[api_route]`, `#[mcp_tool]`, `#[mcp_resource]`, `#[mcp_prompt]` | Top-level `fn` | HTTP routes and MCP entries — see [Routes and MCP](routes-and-mcp.md#single-file-plugins). |
| `#[make_executor]`, `fn subscribe` | Inline `mod` / `fn` | Job executors and streaming connectors — see [Executors and Streams](executors-and-streams.md#single-file-plugins). |

### Data functions & `setup`

`fetch` and `sync` accept any subset of these **named** parameters in any order (leading `_` allowed; must be synchronous, non-generic):

| Parameter | Type | Meaning |
|---|---|---|
| `pipe` | `&Pipe` | Target pipe metadata and host calls. |
| `connector` | `&Connector` | Connector type, label, and config. |
| `kwargs` | `&Value` | Sync options and plugin flags. |
| `begin` / `end` | `Option<Bound>` | Inclusive lower / exclusive upper bound (`None` if unbounded). |
| `data` *(sync only)* | `Option<Frame>` | Input rows if caller supplied them. |

`fetch` returns `Vec<Value>`, `Option<Vec<Value>>`, `Frame`, `Option<Frame>`, or a `Result` of those (`vec![]` succeeds with 0 new rows; `None` fails with "No data were fetched"). `sync` returns `SuccessTuple`, `(bool, String)`, `(bool, &str)`, or a `Result`.

`setup` optionally takes `kwargs: &Value` and must return `bool`, `SuccessTuple`, `(bool, String)`, or a `Result`:

```rust
fn setup() -> Result<SuccessTuple> {
    if host::plugin_config()?.is_null() {
        host::write_plugin_config(&json!({"stations": ["north"]}))?;
    }
    Ok(SuccessTuple::ok("Ready."))
}
```

### Sync hooks

Hooks accept any subset of `pipe: &Pipe`, `kwargs: &Value`, and (post-sync only) `success_tuple: SuccessTuple` and `sync_duration: f64`, returning `()`, `SuccessTuple`, `(bool, String)`/`&str`, or `Result`:

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

### Plugin arguments, `VERSION`, and `REQUIRED`

Declare plugin version/Python requirements with exact literal constants and CLI flags with `#[add_plugin_argument(...)]`:

```rust
#![add_plugin_argument("--times", "-n", type = int, default = 1, help = "Repeat count.")]

const VERSION: &str = "1.2.0";
const REQUIRED: &[&str] = &["plugin:noaa", "requests>=2"];

#[add_plugin_argument("--loud", action = "store_true", help = "Shout.")]
#[make_action]
fn greet(action: Vec<String>, kwargs: &Value) -> (bool, String) {
    let mut text = format!("Hello, {}!", action.join(", "));
    if kwargs["loud"] == true { text = text.to_uppercase(); }
    (true, text.repeat(kwargs["times"].as_u64().unwrap_or(1) as usize))
}
```

| Option | Values |
|---|---|
| `action` | `"store"` (default), `"store_true"`, `"store_false"`, `"store_const"`, `"append"`, `"append_const"`, `"count"` |
| `type` | `int`, `float`, or `str` (for `store` / `append`) |
| `default`, `const`, `choices` | Literals (`choices = ["north", "south"]`) |
| `help`, `metavar`, `dest`, `required` | Strings / `bool` (`dest` defaults to snake_case of first long flag; `nargs` is unsupported) |

Flags conflicting with built-in or other plugin options emit a warning and are ignored.

### Custom connector methods & instance connectors

Mark extra connector methods with `#[method]` to expose them directly to Python (`mrsm.get_connector('kuma:main').push(status='up')`), and use `#[make_connector(instance)]` to build a custom `-i` instance:

```rust
#[make_connector]
mod kuma {
    #[method]
    pub fn push(connector: &Connector, kwargs: &Value) -> Result<SuccessTuple> {
        Ok(SuccessTuple::ok(format!("Pushed {} to {}", kwargs["status"], connector.label())))
    }
}

#[make_connector(instance)]
mod memory {
    fn register_pipe(pipe: &Pipe) -> SuccessTuple { SuccessTuple::success() }
    fn pipe_exists(pipe: &Pipe) -> bool { true }
    fn sync_pipe(pipe: &Pipe, data: Vec<Value>) -> (bool, String) { (true, format!("Inserted {}.", data.len())) }
    fn get_pipe_data(pipe: &Pipe, begin: Option<Bound>, params: &Value) -> Vec<Value> { vec![] }
    fn fetch_pipes_keys(query: &Value) -> Value { json!([]) }
}
```

| Instance methods | Return type (all accept `Result<T>`) |
|---|---|
| `register_pipe`, `delete_pipe`, `drop_pipe`, `edit_pipe`, `sync_pipe`, `clear_pipe`, `deduplicate_pipe`, `drop_pipe_indices`, `create_pipe_indices`, `compress_pipe`, `decompress_pipe`, `analyze_pipe`, `vacuum_pipe`, `partition_pipe` | `SuccessTuple`, `(bool, String)`, or `(bool, &str)` |
| `get_pipe_data` | `Vec<Value>`, `Frame`, or `Option<Frame>` |
| `test_connection`, `pipe_exists`, `acquire_pipe_lease`, `release_pipe_lease` | `bool` |
| `fetch_pipes_keys` | `[[connector_keys, metric_key, location_key], ...]` |
| `get_pipe_id`, `get_pipe_rowcount`, `get_pipe_size` | `Option<i64>` or `i64` |
| `get_sync_time` | `Option<i64>`, ISO timestamp string, or `None` |
| `get_pipe_attributes`, `get_pipe_columns_types`, `get_pipe_index_names`, `get_pipe_columns_indices` | JSON object (`Value` or serializable map) |

Instance method parameters bind by name (`pipe`, `connector`, `data`, `context`, `query`, `options`, `params`, `begin`, `end`, `select_columns`, `omit_columns`, `limit`, `order`, `check_existing`, `chunksize`, `extra`, `newest`, `patch`, `no_policy`, `full`, `chunk_minutes`, `holder`, `ttl_seconds`).

### Console pages

Serve HTML fragments under `/dash/` with `#[web_page]` (following the [page contract](/reference/migrating-to-v5/#option-b-the-page-contract-recommended-for-new-pages)):

```rust
#[web_page("readings/report", login_required = false, page_group = "Readings")]
fn report(page: &PageRequest) -> String {
    format!("<h3>Station {}</h3>", page.query_param("station").unwrap_or_default())
}
```

Options: leading path string (default function name), `login_required` (default `true`), `skip_navbar` (`false`), `page_group` (plugin name), `dark_theme` (`true`), `host_scopes`. Accepts optional `page: &PageRequest` (`path`, `query`, `query_param`, `username`) and returns `String`/`&str`, `(u16, T)`, or `Result`.

### Inline dependencies & build cache

Add external Rust crates in a single doc block at the **top of the `.rs` file** (only `[dependencies]` is supported; local `path` deps resolve relative to the `.rs` file):

```rust
//! ```cargo
//! [dependencies]
//! regex = "1"
//! ```

use regex::Regex;

#[make_action]
fn count_words(action: Vec<String>) -> (bool, String) {
    let count = Regex::new(r"\w+").unwrap().find_iter(&action.join(" ")).count();
    (true, format!("{count} words"))
}
```

Generated Cargo projects and stamps live under `<root>/cache/native_build/<name>/` (with shared target artifacts in `.single_file_target/` and build errors in `mrsm-build-error.txt`); nothing is written next to your `.rs` file. External path dependencies are not fingerprinted, relative `include_str!` resolves from the cache copy, and remember Rust returns the final expression without a trailing semicolon.
