# meerschaum

The Rust API of [Meerschaum](https://meerschaum.io): *pipes* sync data between databases, APIs and
files, incrementally and idempotently. This is the crate to depend on; it re-exports a curated,
semver-stable surface and keeps the implementation crates (`meerschaum-core`, `-kernel`,
`-connector`, `-connector-sql`, `-connector-valkey`, `-connector-api`, `-engine`, `-server`,
`-plugin`, ...) behind cargo features, the way `polars` fronts the `polars-*` crates.

```toml
[dependencies]
# Use the exact pre-release version (e.g. "5.0.0-rc.1") until 5.0.0 is out: a bare "5" does not match one.
meerschaum = { version = "5", features = ["sql", "engine"] }
```

| Feature | Adds |
|---|---|
| `sql` (default) | `SqlConnector` (PostgreSQL, MySQL, SQLite, MSSQL); `duckdb` / `oracle` add those flavors |
| `valkey` | `ValkeyConnector` |
| `api` | `APIConnector`, the client of a Meerschaum API server |
| `engine` | the dependency-aware Syncing Engine (`Pipeline`, `Plan`, `Engine`) |
| `server` | the `axum` HTTP API server (implies `api`) |
| `plugin` | the native plugin SDK |
| `full` | all of the above except `duckdb` / `oracle` |

The `lib.rs` documentation lists exactly which items are semver-stable. Every other
`meerschaum-*` crate is **internal and may change in any minor release**; depend on `meerschaum`
(or on `meerschaum-plugin` to write native plugins). The `mrsm` command line is the
`meerschaum-cli` crate (`cargo install meerschaum-cli`), also published on PyPI as the `meerschaum-cli` wheel.
