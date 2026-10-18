# meerschaum

The Rust API of [Meerschaum](https://meerschaum.io): *pipes* sync data between databases, APIs and
files, incrementally and idempotently. This is the crate to depend on; it re-exports a curated,
semver-stable surface and keeps the implementation crates (`meerschaum-core`, `-kernel`,
`-connector`, `-connector-sql`, `-connector-valkey`, `-connector-api`, `-engine`, `-server`,
`-plugin`, ...) behind cargo features, the way `polars` fronts the `polars-*` crates.

```toml
[dependencies]
# Use the exact pre-release version (e.g. "5.0.0-rc.1") until 5.0.0 is out: a bare "5" does not match one.
meerschaum = { version = "5", default-features = false, features = ["sql", "engine"] }
```

A library takes `default-features = false`: the default `cli` feature compiles the whole command line
(shell, API server, jobs) for the `mrsm` binary, which a library does not need.

## The `mrsm` command line

```bash
cargo install meerschaum        # builds the standalone `mrsm` (default `cli` feature)
pip install meerschaum          # the same CLI, run from the Python wheel's extension (`mrsm`, `python -m meerschaum`)
```

Both install Meerschaum: one product per ecosystem, like `polars`. The standalone binary starts in
about 0.01 s and runs without Python as long as no Python plugin or connector is used (it asks for an
interpreter, `MRSM_PYTHON`, the first time Python is needed); the `pip` console script loads the
extension first (see the migration guide for numbers).

| Feature | Adds |
|---|---|
| `sql` (default) | `SqlConnector` (PostgreSQL, MySQL, SQLite, MSSQL, DuckDB); `oracle` adds that flavor |
| `cli` (default) | the standalone `mrsm` binary (`cargo install meerschaum`); implies `sql`. Libraries use `default-features = false` |
| `valkey` | `ValkeyConnector` |
| `api` | `APIConnector`, the client of a Meerschaum API server |
| `engine` | the dependency-aware Syncing Engine (`Pipeline`, `Plan`, `Engine`) |
| `server` | the `axum` HTTP API server (implies `api`) |
| `plugin` | the native plugin SDK |
| `full` | all of the above except `oracle` |

The `lib.rs` documentation lists exactly which items are semver-stable. Every other
`meerschaum-*` crate is **internal and may change in any minor release**; depend on `meerschaum`
(or on `meerschaum-plugin` to write native plugins). `meerschaum-cli` is the internal library
behind `mrsm`; the PyPI package is the single `meerschaum` wheel.
