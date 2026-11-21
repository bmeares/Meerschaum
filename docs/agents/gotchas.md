# Verified gotchas

Traps that cost time before and that no config or error message reveals. Read the section for the
area you are changing.

## SQL connector (`crates/meerschaum-connector-sql`)

- **`SqlDriver::ready()` must stay tiny**: its slow path is `Box::pin(...)`; an inline body
  overflowed the 2 MiB tokio stack in debug builds. `driver::future_size_tests` guards it.
- **Partitioned tables fold the datetime column into the primary key**, so an upsert's conflict
  target must include `dt_col` there.
- **MariaDB `get_sync_time(params=...)` on a partitioned table** uses `MAX`/`MIN`: the optimizer
  returns zero rows with `ORDER BY ... LIMIT 1` (`instance/data.rs`).
- **`get_chunk_bounds(align=True)`** (verify, ledger grid) anchors edges to the Unix epoch so they
  match partition boundaries; elsewhere it defaults to `align=False`.
- **`clean()` in `meerschaum.utils.sql` is only a keyword blocklist**: bind user values; use
  `clean()` for internal names.

## Python boundary (`crates/meerschaum-py`)

- **Empty frames export no Arrow batches**: the boundary must import an empty batch carrying the
  schema.
- **Polars at the Arrow boundary**: `Null` columns export `n_buffers = 1` (repaired in
  `ffi_import.rs`); Polars inputs go through `DataFrame.to_arrow()` when pyarrow is present;
  `ffi_import.rs` calls the private C stream callbacks through a `#[repr(C)]` mirror (see
  `crates/meerschaum-py/README.md`).
- **Dtype hints restore pandas backends** (`pandas_bridge.rs`, table in `PARITY.md`): the kernel
  erases `int64` vs `Int64` vs `int64[pyarrow]`; special columns stay `object`.
- **Forked job daemons and Tokio**: the extension's runtime is keyed by PID; never `block_on` an
  inherited runtime.
- **`get_config` is hot** (~35 calls per sync chunk): the binding reads the live Python dict and
  caches nothing so `set_config` / env patches are visible at once. Keep it that way.

## Server and API connector

- **The API server caches `Pipe` objects** per instance (`AppState::pipes` in
  `crates/meerschaum-server/src/state.rs`); the `APIConnector` keeps short-lived client caches of
  attributes, existence and column types (`crates/meerschaum-connector-api/src/client/pipes.rs`).
  The server refuses `mrsm` connector keys on the generic routes.

## Jobs

- **Jobs start through the admission gate** ([ADR 0005](../adr/0005-job-startup-admission-gate.md)):
  a job "stuck at starting" for 10 to 30 s is the gate. Tests that wait for jobs started by
  `compose up` or a bulk start pass `--no-stagger` (or set `jobs.startup.enabled: false`).

## CI

- **CI keeps `target/` on the self-hosted runners**; `scripts/ci/changes.py` decides which jobs a
  push runs, and a dispatch or `v5*` tag runs everything (`docs/design/v5-build.md`
  "CI: what runs when").
