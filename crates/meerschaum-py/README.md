# meerschaum-py: the `meerschaum._core` extension

PyO3 bindings that expose the Rust crates to Python as `meerschaum._core` (design §4.2, D3, D12).
The Python façade calls these bindings directly (the v4 Python core was removed in wave 16, D14);
every discrepancy found while the two implementations ran side by side is tracked in
[`PARITY.md`](PARITY.md).

| Symbol | What it does |
|---|---|
| `__version__` | Cargo version (`5.0.0-rc.22`); `build_info()['version_pep440']` is the wheel form (`5.0.0rc22`) |
| `build_info()` | rustc version, profile, opt level, target/host, enabled features, Python ABI (`abi3-py311`) |
| `static_config()` | `crates/meerschaum-core/static.toml` (D15) as a dict, parsed by `meerschaum_core::static_config` (which embeds it at build time). The file's `{ __null__ = true }` and `{ __cpu_count_or__ = N }` markers are resolved the way `STATIC_CONFIG` does, with Python's own `os.cpu_count()` |
| `roundtrip_arrow(obj)` | Imports anything with `__arrow_c_stream__` / `__arrow_c_array__` into Rust `RecordBatch`es and returns an `ArrowStream`, a pyarrow-free object that exports the data back through `__arrow_c_stream__` / `__arrow_c_schema__` (`ffi_export.rs`, honoring `requested_schema`) |
| `polars_roundtrip(df)` | `polars.DataFrame` → `polars::DataFrame` → `polars.DataFrame` over the Arrow C stream, read and written with Polars' own Arrow FFI (`polars_bridge.rs`; no pyarrow, every Polars type including `Enum`) |
| `HAS_STATIC_CONFIG` | Build-time flag |

Kernel and core functions (each takes exactly the arguments its dispatch guard passes, so the
Python signature is the v4 one):

| Symbol | Python function it replaces | Rust |
|---|---|---|
| `enforce_dtypes(df, dtypes, explicit_dtypes, safe_copy, coerce_numeric, coerce_timezone, strip_timezone, as_polars, debug)` | `meerschaum.utils.dataframe.enforce_dtypes` | `kernel::enforce::enforce_dtypes_with_path` |
| `filter_unseen_df(old_df, new_df, safe_copy, dtypes, include_unchanged_columns, coerce_mixed_numerics, debug)` | `...dataframe.filter_unseen_df` | `kernel::diff::filter_unseen_df` |
| `query_df(df, params, begin, end, datetime_column, select_columns, omit_columns, inplace, reset_index, coerce_types, debug)` | `...dataframe.query_df` | `kernel::query::query_df` |
| `get_json_cols`, `get_numeric_cols`, `get_uuid_cols`, `get_bytes_cols` `(df)`; `get_datetime_cols(df, timezone_aware, timezone_naive, with_tz_precision)`; `get_geometry_cols(df, with_types_srids)` | `...dataframe.get_*_cols` | `kernel::detect` |
| `are_dtypes_equal(ldtype, rdtype)`, `to_pandas_dtype(dtype)`, `is_dtype_numeric(dtype)`, `get_geometry_type_srid(dtype, default_type, default_srid)` | `meerschaum.utils.dtypes.*` | `core::dtypes`, `kernel::dtypes` |
| `get_pd_type_from_db_type(db_type, allow_custom_dtypes)` | `meerschaum.utils.dtypes.sql.*` | `core::dtypes` |
| `get_config(*keys, patch, substitute, sync_files, write_missing, as_tuple, warn, debug)` | `meerschaum.config.get_config` | `core::config::Config` |
| `replace_pipes_syntax(text, _pipe)` | `meerschaum.utils.pipes.replace_pipes_syntax` | `core::template` with a resolver that reads live Python `Pipe`s |
| `hash_password(password, salt, rounds)`, `verify_password(password, password_hash)` | `meerschaum.core.User._User.*` | `core::users` |

### The SQL instance connector (`_core.SqlConnector`, wave 4)

`SqlConnector(label, attributes)` wraps `meerschaum_connector_sql::SqlConnector` (the instance layer). It is
built lazily, once per Python `SQLConnector`, by `SQLConnector._rust_method()` from the attributes
v4 already resolved (`flavor`, `uri`, `host`, `port`, `username`, `password`, `database` and
`self.schema`), so `connectors.yaml`, keyword arguments and `MRSM_SQL_<LABEL>` URIs all work
unchanged. The pool connects on first use. Every method takes the v4 method's arguments (extra
keywords such as `debug` are accepted and ignored) and reads pipes, users and plugins by
attribute only (duck typing, design §4.4). Database I/O runs on the extension's multi-threaded
Tokio runtime (`src/runtime.rs`) with the GIL released; frames cross as Arrow through
`pandas_bridge` with the pipe's dtypes as hints (`read(dtype=...)` in v4), Polars with
`as_polars=True`.

| Group | Methods (v4 names) |
|---|---|
| Registration | `register_pipe`, `edit_pipe`, `delete_pipe`, `get_pipe_id`, `get_pipe_attributes`, `fetch_pipes_keys` |
| Data | `sync_pipe`, `get_pipe_data` (also `as_iterator`), `get_pipe_docs`, `get_pipe_data_query`, `get_sync_time`, `get_pipe_rowcount`, `pipe_exists`, `get_pipe_columns_types`, `get_pipe_columns_indices`, `clear_pipe`, `drop_pipe`, `deduplicate_pipe` |
| Indices | `create_indices`, `drop_indices`, `create_pipe_indices`, `drop_pipe_indices`, `get_create_index_queries`, `get_drop_index_queries`, `get_pipe_index_names` |
| Maintenance | `get_pipe_size`, `compress_pipe`, `decompress_pipe`, `apply_compression_policy`, `vacuum_pipe`, `analyze_pipe`, `partition_pipe`, `get_partition_info` |
| Users | `register_user`, `edit_user`, `delete_user`, `get_user_id`, `get_users`, `get_user_password_hash`, `get_user_type`, `get_user_attributes` |
| Plugins | `register_plugin`, `delete_plugin`, `get_plugin_id`, `get_plugin_version`, `get_plugin_user_id`, `get_plugin_username`, `get_plugin_attributes`, `get_plugins` |

Tokens need no binding: the SQL instance stores them in the tokens pipe through the pipe methods
above. Raw SQL (`read`, `exec`, `exec_queries`, `value`, `to_sql`, rc15 Q53) goes through
`sql_query`, `sql_execute`, `sql_execute_many`, `sql_begin` and `sql_to_sql` (`src/sql_raw_api.rs`:
`SqlFrame` batches keep what each flavor's DBAPI returns, `SqlFrame.to_pandas()` is the frame
`pd.read_sql_query()` builds from those values, `SqlTx` is an open transaction) for every flavor
in `meerschaum/connectors/sql/_native.py::NATIVE_SQL_FLAVORS`; only `.engine` and `get_tables()`
(SQLAlchemy objects) stay SQLAlchemy (D16); `sync_pipe_inplace` (SQL-to-SQL syncs on the same instance) stays Python for now
and reaches the routed methods through `self`. `sync_pipe` writes the parameter changes v4's
`sync_pipe` makes (`autoincrement`, numeric `dtypes`) back into the Python pipe's in-memory
parameters (`update_parameters(..., persist=False)`, Rust already persisted them) and clears the
pipe's `_exists` / column-type / index caches.

All supported SQL flavors route Pipe methods through the Rust instance layer, including
MSSQL, Oracle, DuckDB, and memory SQLite. In a Python host, DuckDB uses a lazy public Python
driver transport when `duckdb >= 1.5.6` is installed and `MRSM_DUCKDB_LIB` is unset. A shared
anchor and duplicate connections make Rust Pipe methods and SQLAlchemy (`.engine`) see the
same database while retaining separate transactions. Standalone / base-only installs use the
native C API. An explicit override pins native mode and refuses mixed SQLAlchemy access;
unset it, install `meerschaum[sql-duckdb]`, and restart to use both. Transport selection does
not install packages or move Pipe algorithms into Python. The tested public-driver minimums are `duckdb >= 1.5.6` and `duckdb-engine >= 0.17.0`. Fork guards cover Meerschaum-owned handles and engine access; the public driver also cannot safely close/finalize inherited worker threads during normal child interpreter exit. Fork children must exec a fresh interpreter or use immediate process exit; prefer spawn. Direct use of previously held raw SQLAlchemy/DBAPI connections after fork remains unsupported. Conflicting configuration and
unsupported custom connection routing fail before database access. `src/pipe_bridge.rs` converts the duck-typed objects;
`meerschaum_connector_sql::facade` (`crates/meerschaum-connector-sql/src/facade.rs`) holds the two additive helpers
the bindings need (`sync_pipe_mut`, `get_pipe_data_and_state`).

DataFrames cross as Arrow (`src/pandas_bridge.rs`): typed pandas columns through
`__arrow_c_stream__`, `object` / `geometry` columns value by value (`src/convert.rs`, with one
vectorized `shapely.to_wkb` / `from_wkb` call per geometry column), Polars frames directly, Dask
frames computed first. On the way out the boundary restores the pandas dtypes v4 returns
(`int64[pyarrow]` vs `Int64` vs `int64`, Arrow-backed columns where v4 went through Polars,
GeoDataFrames with their CRS); `PARITY.md` explains the rules. Columns the kernel returns
unchanged are copies of the input Series, and `enforce_dtypes` passes untyped pandas columns as
placeholders (checked after the call, redone with full conversion if one changed).
[`PERF.md`](PERF.md) has the benchmark record, the profile and the optimizations; `MRSM_CORE_TRACE=1` prints per-stage timings of each call to stderr.

## Layout

```
crates/meerschaum-py/
  Cargo.toml        [lib] name = "_native", crate-type = ["cdylib", "rlib"]
  build.rs          records build_info
  pyproject.toml    maturin config: module-name = "meerschaum._core._native", python-source = "../.."
  src/              lib.rs (#[pymodule] _native), arrow_bridge.rs, ffi_import.rs, ffi_export.rs,
                    polars_bridge.rs,
                    convert.rs (Python <-> Value), pandas_bridge.rs (frames), kernel_api.rs,
                    core_api.rs, trace.rs (MRSM_CORE_TRACE stage timers), runtime.rs (Tokio),
                    pipe_bridge.rs (duck-typed pipes/users/plugins), sql_api.rs (SqlConnector)
  tests/            arrow_ffi.rs (cargo test), test_core.py (pytest; v4 results recorded in
                    ../../tests/oracle/test_core.json)
meerschaum/_core/
  __init__.py       lazy loader (stdlib only); `import meerschaum` fails with the build
                    instructions when the extension file is missing
  _native.abi3.so     the built extension (git-ignored)
scripts/rust/
  build_py.sh       build and place the extension
  clean_py.sh       remove it
```

**Why `meerschaum._core._native` with a loader package:** the compiled module sits inside a
pure-Python package, `meerschaum/_core/`, so that:

- the package always imports, built or not. Its `__init__.py` reports `AVAILABLE`, `LOAD_ERROR` and
  `EXTENSION_PATH`, and raises a clear `CoreNotBuiltError` (an `ImportError`) only when a symbol is
  actually used;
- the extension loads lazily on first attribute access (PEP 562). `import meerschaum` never pays
  for it: the loader adds about 0.4 ms, and loading the release `.so` (about 100 MB, 85 MB stripped) takes about 3 ms;
- the same dotted name works unchanged at 5.0, when the extension ships inside the `meerschaum`
  wheel.

**Why the maturin project lives in the crate and points `python-source` at the repo root:** the
root `pyproject.toml` stays setuptools this wave, so the v4 pytest suite and `scripts/test.sh` keep
working. Because the crate's pyproject is a mixed project rooted at the repository,
`maturin develop --skip-install` copies `_native.abi3.so` straight into `meerschaum/_core/` in the
working tree. Nothing is installed into any environment, and `PYTHONPATH=$(pwd)` picks it up.
`maturin build` from here produces an extension-only wheel (`meerschaum/_core/_native.abi3.so`,
distribution `meerschaum-core-dev`), because maturin does not package Python sources outside the
project root. The distribution is deliberately not called `meerschaum`, so installing it can never
replace an installed v4 package. Do not upload it.

## Build and import

```bash
uv tool install maturin                  # once (maturin 1.15 tested)
scripts/rust/build_py.sh                 # release (default): ~42 MB .so
scripts/rust/build_py.sh --debug         # unoptimized, stripped (~34 MB; ~1 GB unstripped)
scripts/rust/clean_py.sh [--cargo]       # remove the .so (and this crate's cargo artifacts)

PYTHONPATH=$(pwd) python -c 'import meerschaum._core as c; print(c.AVAILABLE, c.build_info())'
```

`build_py.sh` runs
`maturin develop --skip-install --manifest-path crates/meerschaum-py/Cargo.toml [--release | --strip]
--features pyo3/abi3-py311,extension-module,oracle` with `PYO3_PYTHON` set to `$PYTHON`. A
command-line `--features` *replaces* the pyproject `features` list, so the script always passes the
full set. Without `extension-module`, the module links whichever libpython pyo3 found, and the
script refuses a `.so` that has a `NEEDED libpython` entry. The script runs (default `python3`, any version 3.11 or newer works because
the module is abi3). It first removes stale builds, then imports the result to check it, using a
throwaway `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR`. Keep
`PYTHON` stable between runs: a different interpreter changes pyo3's build fingerprint and rebuilds
the Polars crates. The first build compiles Arrow and Polars into the shared `target/` (about
2.5 minutes on the dev machine), and later builds take seconds.

Always put the working tree first on `sys.path` (`PYTHONPATH=$(pwd)`): a stale v4 `meerschaum` under
`~/.local` shadows it otherwise (see `CLAUDE.md`).

## Tests

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo test -p meerschaum-py              # pure Rust: build info, static.toml, Arrow FFI stream round trip
scripts/rust/build_py.sh
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python -m pytest crates/meerschaum-py/tests/test_core.py -q
```

`cargo test` and `cargo clippy` build without the `extension-module` feature, so the test binaries
link libpython. `test_core.py` needs pyarrow,
polars and pandas (test-only). Without a built extension it runs only the loader tests and
skips the rest.

## Dependency pins

`pyo3`, `arrow` and `polars` are workspace dependencies (`pyo3 = { workspace = true }`), upgraded
together: RecordBatches cross between the kernel, the plugin C ABI and this crate, and pyo3's
`links = "python"` allows only one pyo3 in the lockfile. Since 17b the crate depends on no binding
crate that pins that triple: the Python <-> Arrow boundary is our own PyCapsule code
(`ffi_import.rs` imports, `ffi_export.rs` exports, both over arrow-rs FFI), and
`polars_roundtrip` uses Polars' own Arrow FFI (`polars-arrow`, kept on the `polars` version).
So pyo3, arrow-rs and Polars can each move to their latest release independently (pyo3 0.30,
arrow 60, polars 0.55 today; arrow 60 needs Rust 1.88).

`num-bigint` (0.5, shared with arrow-buffer) never crosses into pyo3: Python ints beyond `i64`
travel as decimal text (`config_api.rs`, `yaml_api.rs`), so pyo3's `num-bigint` feature is off.

## Sharing the Polars build with the kernel

The crate depends on `polars = { workspace = true }` (the kernel's features, used for column
types at the pandas boundary) and nothing that changes Polars' features, so
`cargo build/test/clippy -p meerschaum-py` and the extension reuse the kernel's Polars build.

## Findings at the Arrow boundary

- **Polars `Null` columns.** Polars exports `Null`-typed arrays (an all-`None` column) with
  `n_buffers = 1`, but the C Data Interface specifies 0. arrow-rs (56 through at least 60) rejects
  them. `ffi_import.rs` repairs this at import by setting `n_buffers = 0` on `Null` nodes before
  `from_ffi`. Producers that follow the spec are untouched. Any other Rust code that imports
  polars-produced streams with arrow-rs needs the same repair.
- **Polars frames into Rust Polars.** `polars_bridge.rs` reads the capsule with `polars-arrow`'s
  `ArrowArrayStreamReader` (not arrow-rs), so Polars' own layouts and field metadata (`Enum`
  categories, `Categorical`, extension types) come through without the `Null` repair or pyarrow,
  and writes back with `export_iterator` at `CompatLevel::newest()`.
- **Metadata survives.** Schema-level metadata (pandas' `b"pandas"` block, so `to_pandas()`
  restores `Int64`, `category` and tz-aware dtypes) and field-level metadata
  (`ARROW:extension:name`, for example GeoArrow WKB) round-trip. Every imported batch carries the
  stream's full schema.
- **Empty streams have no batches.** A zero-row pandas or Polars frame exports a stream with no
  batches at all; `pandas_bridge::frame_from_arrow` imports an empty batch carrying the stream's
  schema so the columns survive.
- **Polars `Extension` string columns.** Polars 1.36 exported them as `Utf8View` with a buffer
  layout arrow-rs 56 panicked on (`byte_view_array.rs`). With Polars 2.0 and arrow-rs 59/60 the
  capsule imports and validates (`validate_full`), but the kernel bindings still import Polars
  frames through `DataFrame.to_arrow()` (non-view layout, extension metadata kept) when pyarrow is
  available, for the older Polars releases the `polars` extra allows, and fall back to the
  capsule otherwise.
- **Arrow 60 hides the stream callbacks.** `FFI_ArrowArrayStream`'s fields became private in
  arrow 60; `ffi_import.rs` calls `get_schema` / `get_next` / `get_last_error` through a
  `#[repr(C)]` mirror of the C struct (layout asserted at compile time), the same pattern as its
  `RawArrowArray` mirror.
- **Zero copy.** Imported buffers are the producer's allocations, kept alive by the C Data Interface
  release callbacks. The tests assert that buffer addresses are unchanged.

## How the façade calls the extension

Façade functions call the bound symbol directly (`_core.filter_unseen_df(...)`,
`_core.pipe_get_data(...)`, ...) and the `SQLConnector` methods delegate to the
`_core.SqlConnector` built for the connector (`SQLConnector._core_connector()`, one per process:
a forked child builds its own). `SQLConnector._rust_method(name)` returns the bound method (an
`AttributeError` names a method the extension does not define, such as `read`: raw SQL goes
through `_native` and the `sql_*` bindings) and raises `RuntimeError` when no Rust driver can be built (an unknown flavor, the Oracle client
library `libclntsh` missing, a failed pool). The extension is loaded lazily, on the first
attribute read of `meerschaum._core`; a missing file is an `ImportError` (`CoreNotBuiltError`) at
`import meerschaum`. The results the parity tests compare against are recorded in
`tests/oracle/*.json` (`tests/_oracle.py`; recordings of the parity-phase Python mode, not v4.1.4
output, not re-recordable); the parity oracle itself is v4.1.x in a separate venv (§10.2).

## The 5.0 wheels

**`meerschaum` (D9, D20).** The root `pyproject.toml` becomes a maturin mixed project and this
crate's `pyproject.toml` is deleted:

```toml
[build-system]
requires = ["maturin>=1.9,<2.0"]
build-backend = "maturin"

[project]
name = "meerschaum"
requires-python = ">=3.11"
dynamic = ["version"]                      # from the workspace Cargo version, PEP 440-normalized
dependencies = []                          # D8 revised, D20: one wheel, no dependencies

[project.scripts]
mrsm = "_meerschaum_mrsm:main"             # `_native.main(argv)`: the CLI runs inside the extension
meerschaum = "_meerschaum_mrsm:main"

[project.optional-dependencies]            # §12.1
pandas = ["pandas", "pyarrow"]
polars = ["polars"]
# geo, sql, sql-mssql, sql-oracle, sql-duckdb, sql-cockroachdb, api, dash, shell, cli-tools, gui, full

[tool.maturin]
bindings = "pyo3"
manifest-path = "crates/meerschaum-py/Cargo.toml"
module-name = "meerschaum._core._native"
python-source = "."
python-packages = ["meerschaum"]
features = ["pyo3/abi3-py311", "extension-module"]
exclude = ["**/__pycache__/**", "**/*.pyc", "meerschaum/_core/_native*.so"]
```

- One `cp311-abi3` wheel per platform (manylinux x86_64/aarch64, musllinux, macOS x86_64/arm64,
  Windows x86_64) covers CPython 3.11 through 3.15. Free-threaded builds (3.14t/3.15t, R8)
  cannot load abi3 modules. They need separate `cp314t`/`cp315t` wheels built without the abi3
  feature. The module already declares `gil_used = false`.
- The extension statically links Arrow and Polars, so the base wheel needs no Python data library.
  pandas and pyarrow come with `[pandas]`, polars with `[polars]`.
- The `[project.scripts] mrsm` / `meerschaum` entry points call `_meerschaum_mrsm:main` (a top-level
  module in the wheel that marks the process as the CLI before `meerschaum` is imported), which runs
  `_native.main(argv)` (`src/main_api.rs`): the whole `mrsm` CLI inside the extension, the same
  `meerschaum_cli::run_main` the standalone binary runs. It writes to the real file descriptors, resets
  SIGINT to its default action for the run (and restores Python's handler when it returns to a
  Python caller), releases the GIL for the whole run and sets `MRSM_PYTHON` to `sys.executable` unless
  set. Everything the CLI re-launches (job supervisors and workers, compose, `systemd` units, the web
  terminal) runs `<sys.executable> -m meerschaum` (`meerschaum_core::self_exe`), or the binary named by
  `MRSM_BIN`. `import meerschaum` is eager (as it always was) except in the CLI process (`mrsm`, and
  `python -m meerschaum`, detected from `sys.orig_argv`), where the façade is imported on first use. From the
  main thread only; an empty `sys.executable` is an error.
- The `rust_impl` guards and the replaced Python bodies are gone (D14).
- The sdist must carry `Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`, `rust-toolchain.toml`
  and the `crates/` the extension depends on (maturin's sdist collects the path dependencies of
  `manifest-path`).

**One wheel (D8, revised 2026-10-17).** There is no `meerschaum-cli` wheel: the extension holds the one
copy of the core and the CLI. The standalone `mrsm` binary is `cargo install meerschaum` (the front
crate's default `cli` feature). The standalone binary locates its Python environment as §4.3 describes.
