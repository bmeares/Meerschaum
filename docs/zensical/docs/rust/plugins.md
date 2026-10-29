# Rust plugins

A single-file plugin needs no Cargo project. Save `greetings.rs` in your plugins directory:

```rust
/// Say hello.
#[make_action]
fn greet() {
    println!("Hello from Rust!");
}
```

Run `mrsm greet`. Meerschaum builds the plugin when needed; a Rust toolchain is required for the first build and after edits.

## Supply rows

Save `readings.rs`:

```rust
fn register() -> Value {
    json!({"columns": {"id": "id"}})
}

fn fetch() -> Vec<Value> {
    vec![json!({"id": 1, "temperature": 21.5})]
}
```

```bash
mrsm register pipe -c plugin:readings -m readings -i sql:local
mrsm sync pipes -c plugin:readings -i sql:local
```

Single-file plugins receive `Value`, `json!`, and the marker macros from their host. For connectors, hooks, packaged plugins, or host calls, continue to the [Rust plugin guide](../../reference/plugins/rust-plugins.md) and [SDK reference](https://docs.rs/meerschaum-plugin/latest/meerschaum_plugin/).
