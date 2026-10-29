# Pipes in Rust

This local example provides rows directly and writes them through the SQL connector. Use the dependencies from [Embedding](embedding.md).

```rust
use meerschaum::{Frame, InstanceConnector, Pipe, SqlConnector, SyncOptions};
use meerschaum::kernel::value::Value as Cell;
use meerschaum::sql::uri::ConnectionSpec;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spec = ConnectionSpec::from_uri("sqlite:///demo.db", Some("demo"), None)?;
    let conn = SqlConnector::connect("demo", spec).await?;
    let pipe = Pipe::builder("plugin:manual", "readings")
        .instance("sql:demo")
        .build()?;
    let rows = Frame::from_object_columns(vec![
        ("id".into(), vec![Cell::Int(1)]),
        ("temperature".into(), vec![Cell::Float(21.5)]),
    ])?;
    let result = InstanceConnector::sync_pipe(
        &conn, &pipe, rows, &SyncOptions::default(),
    ).await;
    println!("{result:?}");
    Ok(())
}
```

`Frame` accepts Arrow record batches too. `InstanceConnector` exposes the same operations across SQL, API, and Valkey instances; select the matching Cargo feature.

See [Frame](https://docs.rs/meerschaum/latest/meerschaum/struct.Frame.html), [InstanceConnector](https://docs.rs/meerschaum/latest/meerschaum/trait.InstanceConnector.html), and [SqlConnector](https://docs.rs/meerschaum/latest/meerschaum/struct.SqlConnector.html) for method signatures.
