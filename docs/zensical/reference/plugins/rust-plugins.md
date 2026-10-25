# 🦀 Rust Plugins

A Rust plugin can be one `.rs` file. Add a command, return rows of data, or create a
connector with a few functions. Meerschaum handles building and loading it.

Save your files in the plugins directory (normally `~/.config/meerschaum/plugins`;
see [custom paths](../environment.md#mrsm_plugins_dir)). You need a Rust toolchain
with `cargo` available for the first build and after edits. The first build may
download dependencies; later runs reuse the compiled plugin.

To create a starter file, run `mrsm bootstrap plugins greetings` and choose **Rust**
when the wizard asks which language to use.
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
for optional arguments, Pipe operations, sync overrides, and dependency details.
The [advanced guide](rust-plugins/advanced.md) covers Cargo packaging, the full SDK,
hooks, web extensions, testing, and distribution.
