# Meerschaum v5 Rust workspace

The design is `docs/design/v5.0.0.md`. Read it before touching a crate. The crate map is §4.1; the
port table with line counts per Python area is §6.1; the compatibility contract (release gate) is §11.

## Conventions (binding for humans and agents)

- **One crate per directory under `crates/`.** Work inside your crate. Do not edit another crate or
  the root `Cargo.toml` except to add a `[workspace.dependencies]` entry when two or more crates need
  the same dependency; crate-private dependencies go in the crate's own `Cargo.toml`.
- **Shared target dir** (`.cargo/config.toml`). Never set `CARGO_TARGET_DIR`; disk on the self-hosted
  runner is limited. Builds from concurrent sessions may block on the directory lock; batch your builds.
- **Parity is measured against the Python implementation in this repo** (v4.1.4 on this branch). To
  produce golden fixtures, run the Python code directly:

  ```bash
  PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins python -c '...'
  ```

  Always set `PYTHONPATH=$(pwd)`: a stale `meerschaum` is installed under `~/.local` and shadows the
  working tree otherwise. Check `meerschaum.__file__` if a result looks impossible.
  Commit fixtures under `crates/<crate>/tests/fixtures/` as JSON with the generating script next to them.
- **Rules of the port:** reproduce observable behavior, including quirks the corpora depend on (§4.4
  "What the façade must preserve", §6.2, §6.3). Where v4 behavior is a bug, keep it and leave a
  `// PARITY:` comment pointing at the Python line, so the parity gate can carry a snapshot explanation.
- **Python stays runnable throughout.** The Python package on this branch keeps working as v4.1 until a
  façade module is switched to `_core` behind tests. Do not delete Python code in this phase.
- **Before you finish:** `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test -p <crate>`.
  No `unsafe` outside `meerschaum-plugin` and `meerschaum-py` without a `// SAFETY:` comment.
- **Do not commit.** The orchestrating session commits each wave.
- **Never name the private corpora or their organizations** in code, comments, fixtures or docs.
  Fixture data must be synthetic.

## Crate layering

Dependencies point down only. No cycles; `meerschaum-core` is a leaf.

```
  cli ◀── py           top layer: depend on every crate below; nothing depends on them
   │      │
   ▼      ▼
  ┌─────────────────────────────┐
  │ sql ──▶ kernel ──▶ core     │
  │  └────────────────▲         │
  └─────────────────────────────┘
```

| Crate | Normal workspace dependencies |
|---|---|
| `meerschaum-core` | none |
| `meerschaum-kernel` | core |
| `meerschaum-sql` | core, kernel (Arrow conversion of query results, dtype mapping) |
| `meerschaum-cli` | core, kernel, sql |
| `meerschaum-py` | core, kernel, sql, cli |

- **core** owns every type shared by two or more crates: `Value` / `Map` (parameters, config and
  `params`), the Python text helpers (`pyfmt`: `str()`/`repr()`/`json.dumps`, float repr,
  `strip`/`splitlines`/`dedent`/`indent`, `str.format`, `PyDateTime`), the dtype vocabulary
  (`dtypes`: aliases, `are_dtypes_equal`, `GeometrySpec`/`Srid`, precision units, the
  database → pandas table `get_pd_type_from_db_type`, and the Arrow field-tag keys
  `dtypes::field_meta`), `Pipe` and its attributes (`columns`, `dtypes`, `indices`, `precision`,
  `get_chunk_interval`, `get_backtrack_interval`, `target`), the config registry, `static.toml`
  and `SuccessTuple` / `SyncResult`.
- **kernel** owns the in-memory table: `Frame` is the table type for every crate. It owns the
  pandas / Polars / Arrow dtype mapping, `enforce_dtypes`, `filter_existing`, `query_df`, and the
  cell scalar `kernel::Value` (with `pd.NA`, `NaT`, decimals, geometries), which converts to and
  from `core::Value`.
- **sql** owns flavor-specific SQL: query builders, the per-flavor pandas → database tables,
  drivers. Query results are Arrow batches tagged with `meerschaum:dtype` / `meerschaum:db_type`;
  `SqlDriver::read_frame` / `driver::batches_to_frame` turn them into a kernel `Frame` whose
  columns keep the tags, so `enforce_dtypes` reads the declared dtypes instead of detecting them.
  A pipe reaches the query builders as `SqlPipe`: the core `Pipe` plus the flavor-resolved target.
- **cli** and **py** are the top layer and depend on everything below (py also on cli). In this
  wave they call only into core (`SuccessTuple`, the text helpers, `static.toml`); the kernel and
  sql edges are in place for the actions and bindings that come next. Nothing may depend on cli
  or py.
- Crates added later (valkey, api, engine, compose, jobs, server, web, plugin) follow the same
  rule: depend on core (and kernel / sql as needed), never on cli or py, and never re-define a
  type core owns. Need something shared that core lacks? Add it to core (additively) instead of a
  local stand-in.

Check the edges with:

```bash
cargo tree -p meerschaum-sql -e normal --depth 1 | grep meerschaum   # core, kernel
cargo tree -p meerschaum-kernel -e normal --depth 1 | grep meerschaum # core
cargo tree -p meerschaum-core -e normal --depth 1 | grep meerschaum   # (only itself)
cargo tree -p meerschaum-py -e normal --depth 1 | grep meerschaum     # core, kernel, sql, cli
```

## Building

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build                 # whole workspace
cargo test -p meerschaum-core
cargo run -p meerschaum-cli -- --help
```

The PyO3 extension (`crates/meerschaum-py`) is built with maturin into `meerschaum/_core`; see that
crate's README once it exists.
