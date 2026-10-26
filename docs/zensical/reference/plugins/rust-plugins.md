# 🦀 Rust Plugins

A Rust plugin can be one `.rs` file. Add a command, return rows of data, or create a
connector with a few functions. Meerschaum handles building and loading it.

Save your files in the plugins directory (normally `~/.config/meerschaum/plugins`;
see [custom paths](../environment.md#mrsm_plugins_dir)). You need a Rust toolchain
with `cargo` available for the first build and after edits. The first build may
download dependencies; later runs reuse the compiled plugin.

To create a starter file, run `mrsm bootstrap plugins greetings` and choose **Rust**
when the wizard asks which language to use. If you pick an action, say `sing song`,
the starter defines `#[make_action] fn sing_song(...)`, which `mrsm sing song` runs.
Use `mrsm edit plugins greetings` to reopen it. Upload it to a repository with
`mrsm register plugins greetings -r api:your-repo`, just as you would a Python plugin.

## Add a command

Save this as `greetings.rs`:

```rust
/// Say hello.
#[make_action]
fn greet() {
    println!("Hello from Rust!");
}
```

Run it:

```bash
mrsm greet
```

The function name becomes the command. The `///` comment becomes its help text.
To accept names from the command line, replace the function with:

```rust
#[make_action]
fn greet(action: Vec<String>) -> (bool, String) {
    (true, format!("Hello, {}!", action.join(", ")))
}
```

`mrsm greet Alice Bob` returns `Hello, Alice, Bob!`. The returned pair contains
whether the command succeeded (`true` or `false`) and a message.

## Provide data

Save this as `readings.rs`. Each `json!` object is one row:

```rust
fn fetch() -> Vec<Value> {
    vec![json!({"id": 1, "temperature": 21.5})]
}

fn register() -> Value {
    json!({"columns": {"id": "id"}})
}
```

`fetch` supplies the data. The optional `register` function supplies Pipe settings;
here, `id` identifies each row. Register a Pipe, then sync it:

```bash
mrsm register pipe -c plugin:readings -m readings -i sql:local
mrsm sync pipes -c plugin:readings -i sql:local
```

The filename supplies the plugin name: `readings.rs` becomes `plugin:readings`.
`Value` and `json!` are available without imports. Return the final expression
without a trailing semicolon, as shown above.

## Create a connector

To provide your own connector type, put its functions in a marked module:

```rust
#[make_connector]
mod readings {
    fn fetch(connector: &Connector) -> Vec<Value> {
        vec![json!({"id": 1, "source": connector.label()})]
    }
}
```

This creates `readings:<label>` connectors. For example:

```bash
mrsm register pipe -c readings:office -m readings -i sql:local
mrsm sync pipes -c readings:office -i sql:local
```

The label is `office` in this example. Use `connector.config()` to read its
configuration. You can add `register` inside the module to supply Pipe settings.
One file may contain several actions, connector modules, and helper functions.

Python sees the same types: `mrsm.get_connector('readings:office')` returns a connector
whose `fetch()`, `register()` and other module functions run in Rust, and
`mrsm.Pipe('readings:office', 'readings', instance='sql:local')` syncs from it.

## Run code around syncs

Mark functions with `#[pre_sync_hook]` or `#[post_sync_hook]` to run them before or
after every sync, like Python's sync hooks:

```rust
#[post_sync_hook]
fn report(pipe: &Pipe, success_tuple: SuccessTuple, sync_duration: f64) -> (bool, String) {
    (true, format!("{} took {sync_duration:.1}s: {}", pipe.metric_key(), success_tuple.message))
}
```

`mrsm sync pipes` prints each hook's message. A failing hook never stops the sync.

## Add flags, a version, and setup

Give your commands their own flags with `#[add_plugin_argument(...)]`, which takes the
same arguments as Python's `add_plugin_argument` (except `nargs`; pick flags the built-in
CLI does not already use, see `mrsm --help`):

```rust
const VERSION: &str = "1.0.0";

#[add_plugin_argument("--loud", action = "store_true", help = "Shout.")]
#[make_action]
fn greet(kwargs: &Value) -> (bool, String) {
    let text = if kwargs["loud"] == true { "HELLO!" } else { "Hello!" };
    (true, text.to_string())
}

fn setup() -> (bool, String) {
    (true, "Ready.".into())
}
```

`mrsm greet --loud` returns `HELLO!`. `VERSION` is the plugin's version when you
share it through a repository, and `setup` runs during `mrsm setup plugins greetings`.
List Python packages or other plugins your plugin needs in
`const REQUIRED: &[&str] = &["plugin:other"];`, like Python's `required`.

## Serve HTTP routes and MCP tools

Mark a function with `#[api_route(...)]` to serve it from `mrsm start api`, next to the
core API, like Python's `@api_plugin`:

```rust
/// One item.
#[api_route(GET, "/greetings/items/{id}", scopes = ["pipes:read"])]
fn item(request: &RouteRequest) -> Value {
    json!({"id": request.path_param("id"), "user": request.principal})
}
```

Callers need the API's credentials and every listed scope; add `public = true` (and no
scopes) for a route anyone may call. Return JSON, text, bytes or a `RouteResponse`.

Mark functions with `#[mcp_tool]`, `#[mcp_resource]` or `#[mcp_prompt]` to offer them
to AI agents through `/mcp` and `mrsm start mcp`:

```rust
/// Add two integers.
#[mcp_tool(input_schema = r#"{"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}}"#)]
fn add(arguments: &Value) -> Value {
    json!({"sum": arguments["a"].as_i64().unwrap_or(0) + arguments["b"].as_i64().unwrap_or(0)})
}
```

The `///` comment is the tool's description.

## Run jobs and stream data

Mark a module with `#[make_executor]` to run Meerschaum jobs your own way: the jobs of
`-e procs:<label>` (for example `mrsm sync pipes -d -e procs:main`) go to its functions,
which are the executor's methods. Leave out the ones you do not need:

```rust
#[make_executor]
mod procs {
    fn get_job_names() -> Vec<String> {
        vec![]
    }
    fn create_job(name: &str, sysargs: Vec<String>) -> (bool, String) {
        (true, format!("Would run `mrsm {}` as '{name}'.", sysargs.join(" ")))
    }
}
```

Add `fn subscribe` to a connector module to stream rows into `mrsm sync pipes --listen`
as they arrive. It returns a closure the host calls for each batch:

```rust
#[make_connector]
mod ticker {
    fn subscribe() -> impl FnMut(Duration) -> Batch {
        let mut next = 0;
        move |_timeout| {
            next += 1;
            std::thread::sleep(Duration::from_secs(1));
            Batch::Rows(vec![json!({"id": next})])
        }
    }
}
```

## Store pipes and add console pages

Add `instance` to a connector module to make it a Meerschaum instance: `-i memory:main`
then registers, syncs and lists pipes through the functions named after the instance
methods (`register_pipe`, `sync_pipe`, `get_pipe_data`, `fetch_pipes_keys`, ...):

```rust
#[make_connector(instance)]
mod memory {
    fn register_pipe(pipe: &Pipe) -> SuccessTuple {
        SuccessTuple::ok(format!("Registered {}.", pipe.metric_key()))
    }
    fn sync_pipe(pipe: &Pipe, data: Vec<Value>) -> (bool, String) {
        (true, format!("Received {} rows.", data.len()))
    }
}
```

From Python, pass `instance='memory:main'` to `mrsm.Pipe()` or `mrsm.get_pipes()`.

Mark a function with `#[web_page]` to add a page to the web console
(`pip install meerschaum[dash]`), as Python's `@web_page` does:

```rust
#[web_page("readings/report", login_required = false)]
fn report(page: &PageRequest) -> String {
    format!("<h3>Readings</h3><p>Hello, {}.</p>", page.username.as_deref().unwrap_or("guest"))
}
```

## Every attribute

| Attribute or function | Python equivalent | Example |
|---|---|---|
| `#[make_action]` | `@make_action` | `#[make_action] fn greet(action: Vec<String>) -> (bool, String)` |
| `#[make_connector]` module | `@make_connector` | `#[make_connector] mod readings { fn fetch() -> Vec<Value> { ... } }` |
| `#[make_connector(instance)]` module | `@make_connector` with `IS_INSTANCE = True` | `#[make_connector(instance)] mod memory { fn sync_pipe(pipe: &Pipe, data: Vec<Value>) -> SuccessTuple { ... } }` |
| `fn fetch` / `fn sync` / `fn register` | `fetch` / `sync` / `register` | `fn fetch(pipe: &Pipe) -> Vec<Value>` |
| `fn subscribe` (top level or in a connector) | an `IS_STREAMING` connector's `subscribe` | `fn subscribe(pipe: &Pipe) -> impl FnMut(Duration) -> Batch` (or `Box<dyn Subscription>`) |
| `fn setup` | `setup` | `fn setup() -> bool { true }` |
| `#[pre_sync_hook]` / `#[post_sync_hook]` | `@pre_sync_hook` / `@post_sync_hook` | `#[post_sync_hook] fn report(success_tuple: SuccessTuple)` |
| `#[add_plugin_argument(...)]` | `add_plugin_argument` | `#[add_plugin_argument("--loud", action = "store_true")]` |
| `const VERSION` / `const REQUIRED` | `__version__` / `required` | `const VERSION: &str = "1.0.0";` |
| `#[api_route(...)]` | `@api_plugin` routes | `#[api_route(POST, "/my/items", scopes = ["pipes:write"])]` |
| `#[mcp_tool(...)]` | MCP tools | `#[mcp_tool(description = "Count the items.", read_only = true)] fn count() -> Value` |
| `#[mcp_resource(...)]` | MCP resources | `#[mcp_resource(uri_template = "my://items/{id}", description = "One item.")] fn item(resource: &ResourceRead) -> String` |
| `#[mcp_prompt(...)]` | MCP prompts | `#[mcp_prompt(arguments = ["who"])] fn greet(prompt: &PromptGet) -> String` |
| `#[make_executor]` module | `@make_executor` | `#[make_executor] mod procs { fn start_job(name: &str) -> (bool, String) { ... } }` |
| `#[web_page(...)]` | `@web_page` | `#[web_page("my/page", login_required = false)] fn page(page: &PageRequest) -> String` |

The [single-file reference](rust-plugins/advanced.md#single-file-reference) lists every
parameter, option and return type.

## Add dependencies

To use another Rust crate, put a Cargo dependency block at the top of your file:

```rust
//! ```cargo
//! [dependencies]
//! regex = "1"
//! ```
```

Then import it normally, for example `use regex::Regex;`. Versions and features
use standard Cargo syntax. Meerschaum builds the dependencies and keeps the
lockfile in its cache; no separate `Cargo.toml` is needed.

## Learn more

Run `mrsm show plugins` to check discovery, or `mrsm greet --help` for an action's
help. Editing a file triggers a rebuild on the next command that loads plugins.
Only run plugins you trust.

See the [single-file reference](rust-plugins/advanced.md#single-file-reference)
for optional arguments, hooks, flags, setup, routes, MCP entries, executors, streams,
Pipe operations, sync overrides, and dependency details.
The [advanced guide](rust-plugins/advanced.md) covers Cargo packaging, the full SDK,
web extensions, testing, and distribution.
