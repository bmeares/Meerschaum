# meerschaum

The Rust API of [Meerschaum](https://meerschaum.io): *pipes* sync data between databases, APIs and
files, incrementally and idempotently. This is the crate to depend on; it re-exports a curated,
semver-stable surface and keeps the implementation crates (`meerschaum-core`, `-kernel`,
`-connector`, `-connector-sql`, `-connector-valkey`, `-connector-api`, `-engine`, `-server`,
`-plugin`, ...) behind cargo features, the way `polars` fronts the `polars-*` crates.

```toml
[dependencies]
# Use the exact pre-release version (e.g. "5.0.0-rc.9") until 5.0.0 is out: a bare "5" does not match one.
meerschaum = { version = "5", default-features = false, features = ["sql", "engine"] }
```

A library takes `default-features = false`: the default `cli` feature compiles the whole command line
(shell, API server, jobs) for the `mrsm` binary, which a library does not need.

## The `mrsm` command line

```bash
cargo install meerschaum --locked   # builds the standalone `mrsm` (default `cli` feature); needs Rust 1.95 or newer
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

## Known issue: sqlx 0.9.0 and TCP latency (crates.io builds)

Meerschaum's workspace pins a fixed sqlx, but crates.io ignores a `[patch]`, so `cargo install meerschaum`
and library users build against sqlx 0.9.0, which does not set `TCP_NODELAY` (sqlx #4335). The PyPI wheel and
the Docker images use the pinned build and are not affected. Over a network connection to MySQL or MariaDB,
the unanswered statement-close message followed by the next small write waited about 40 ms for the server's
delayed ACK and kept the pooled connection busy, so the pool opened extra connections (up to `Too many
connections`). The SQL connector now prepares small statements once per connection, which brings the
connection counts back to those of the fixed build (instance suites on MySQL: 493 connections down to 67;
the fixed build opens 67). What remains: each empty read (zero rows) on MySQL/MariaDB still waits ~40 ms once,
as do statements over 8 KiB or 128 parameters, so a read-heavy sync of mostly empty ranges is slower than on
the fixed build (about 1.5x on small test suites). PostgreSQL shows no extra connections and only small
latency differences. 5.0.0 ships on sqlx 0.9.0; a 5.0.x release moves to sqlx 0.9.1 once it is published.

The `lib.rs` documentation lists exactly which items are semver-stable. Every other
`meerschaum-*` crate is **internal and may change in any minor release**; depend on `meerschaum`
(or on `meerschaum-plugin` to write native plugins). `meerschaum-cli` is the internal library
behind `mrsm`; the PyPI package is the single `meerschaum` wheel.
