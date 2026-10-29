# Embed Meerschaum

Library applications disable default features to avoid building the full CLI:

```toml
[dependencies]
meerschaum = { version = "5", default-features = false, features = ["sql", "engine"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

| Feature | Adds |
|---|---|
| `sql` | Native SQL connectors |
| `engine` | Pipelines, plans, runs, and execution history |
| `api` | Client for a Meerschaum HTTP instance |
| `valkey` | Valkey connector |
| `plugin` | Native plugin SDK |
| `server` | HTTP server |

The default `cli` feature builds the standalone `mrsm` command. Keep it off for a library unless you need that command-line surface.

## Construct a Pipe

```rust
use meerschaum::Pipe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipe = Pipe::builder("plugin:manual", "readings")
        .instance("sql:demo")
        .build()?;
    println!("{pipe}");
    Ok(())
}
```

Continue with [syncing rows](pipes.md). For engine configuration and run options, browse [the engine module](https://docs.rs/meerschaum/latest/meerschaum/engine/).
