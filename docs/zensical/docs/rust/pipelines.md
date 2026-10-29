# Pipelines in Rust

The engine takes a connector registry, a home instance, and run options. Its selection chooses targets and how far to traverse their dependencies.

This helper runs registered Pipes on an existing SQL connection:

```rust
use std::sync::Arc;
use meerschaum::{Engine, RunOptions, SqlConnector};
use meerschaum::engine::{Depth, EngineConfig, PipeRef, Selection, StaticRegistry};

async fn run_reporting(conn: Arc<SqlConnector>) {
    let registry = Arc::new(
        StaticRegistry::new()
            .with_instance("sql:demo", conn.clone())
            .with_source("sql:demo", conn),
    );
    let engine = Engine::new(registry, EngineConfig {
        home_instance: "sql:demo".into(),
        ..Default::default()
    });
    let target = PipeRef::new("sql:demo", "sql:demo", "reporting", None);
    let options = RunOptions {
        selection: Some(Selection::refs([target]).with_parents(Some(Depth::All))),
        ..Default::default()
    };
    println!("{:?}", engine.plan(&options).await);
    let result = engine.run(options).await;
    println!("{:?}", result);
}
```

Register the sources and transformations before calling the helper. If definitions use `{{ Pipe(...) }}` SQL templates, supply the registry's `with_table_reference` resolver for the connector's identifier rules.

## Plan, run, inspect

`Engine::plan` reads the graph and current state. `Engine::run` executes independent branches concurrently and persists run history by default. `RunOptions` controls bounds, workers, rebuild approval, and selection.

The root-level reexports are the stable Rust interface. The implementation module used for registry and selection helpers may change in a minor release, so pin the crate version when depending on those details.

See [Engine](https://docs.rs/meerschaum/latest/meerschaum/struct.Engine.html), [RunOptions](https://docs.rs/meerschaum/latest/meerschaum/struct.RunOptions.html), and [engine internals](https://docs.rs/meerschaum-engine/latest/meerschaum_engine/) for full signatures. The [operational Pipeline guide](../../reference/pipelines/index.md) explains planning, backfills, and recovery.
