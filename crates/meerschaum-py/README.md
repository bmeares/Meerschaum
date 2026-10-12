# meerschaum-py: the `meerschaum._core` extension

PyO3 bindings that expose the Rust crates to Python as `meerschaum._core` (design §4.2, D3, D12).
Wave 3 binds the kernel and the core behind the parity-phase `MRSM_CORE` gate; every discrepancy
the sqlite suite found under `MRSM_CORE=rust` is tracked in [`PARITY.md`](PARITY.md).

| Symbol | What it does |
|---|---|
| `__version__` | Cargo version (`5.0.0-a1`); `build_info()['version_pep440']` is the wheel form (`5.0.0a1`) |
| `build_info()` | rustc version, profile, opt level, target/host, enabled features, Python ABI (`abi3-py311`) |
| `static_config()` | `crates/meerschaum-core/static.toml` (D15) as a dict, parsed by `meerschaum_core::static_config` (which embeds it at build time). The file's `{ __null__ = true }` and `{ __cpu_count_or__ = N }` markers are resolved the way `STATIC_CONFIG` does, with Python's own `os.cpu_count()` |
| `roundtrip_arrow(obj)` | Imports anything with `__arrow_c_stream__` / `__arrow_c_array__` into Rust `RecordBatch`es and returns an `ArrowStream`, a pyarrow-free object that exports the data back through `__arrow_c_stream__` / `__arrow_c_schema__` |
| `polars_roundtrip(df)` | `polars.DataFrame` → `polars::DataFrame` → `polars.DataFrame` through `pyo3-polars` (crate feature `polars`: off for plain cargo builds, on by default in `build_py.sh`) |
| `HAS_STATIC_CONFIG`, `HAS_POLARS` | Build-time flags |

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

DataFrames cross as Arrow (`src/pandas_bridge.rs`): typed pandas columns through
`__arrow_c_stream__`, `object` / `geometry` columns value by value (`src/convert.rs`, with one
vectorized `shapely.to_wkb` / `from_wkb` call per geometry column), Polars frames directly, Dask
frames computed first. On the way out the boundary restores the pandas dtypes v4 returns
(`int64[pyarrow]` vs `Int64` vs `int64`, Arrow-backed columns where v4 went through Polars,
GeoDataFrames with their CRS); `PARITY.md` explains the rules. Columns the kernel returns
unchanged are copies of the input Series, and `enforce_dtypes` passes untyped pandas columns as
placeholders (checked after the call, redone with full conversion if one changed).
[`PERF.md`](PERF.md) has the benchmark (`bench/bench_boundary.py`), the profile and the
optimizations; `MRSM_CORE_TRACE=1` prints per-stage timings of each call to stderr.

## Layout

```
crates/meerschaum-py/
  Cargo.toml        [lib] name = "_core", crate-type = ["cdylib", "rlib"]
  build.rs          records build_info
  pyproject.toml    maturin config: module-name = "meerschaum._core._core", python-source = "../.."
  src/              lib.rs (#[pymodule] _core), arrow_bridge.rs, ffi_import.rs, polars_bridge.rs,
                    convert.rs (Python <-> Value), pandas_bridge.rs (frames), kernel_api.rs,
                    core_api.rs, trace.rs (MRSM_CORE_TRACE stage timers)
  tests/            arrow_ffi.rs (cargo test), test_core.py (pytest)
  bench/            bench_boundary.py (python vs rust timings, see PERF.md)
meerschaum/_core/
  __init__.py       lazy loader (stdlib only)
  _gate.py          MRSM_CORE parity gate (development only, see below)
  _core.abi3.so     the built extension (git-ignored)
scripts/rust/
  build_py.sh       build and place the extension
  clean_py.sh       remove it
```

**Why `meerschaum._core._core` with a loader package:** the compiled module sits inside a
pure-Python package, `meerschaum/_core/`, so that:

- the package always imports, built or not. Its `__init__.py` reports `AVAILABLE`, `LOAD_ERROR` and
  `EXTENSION_PATH`, and raises a clear `CoreNotBuiltError` (an `ImportError`) only when a symbol is
  actually used;
- the extension loads lazily on first attribute access (PEP 562). `import meerschaum` never pays
  for it: the loader and gate add about 0.4 ms, and loading the 42 MB release `.so` takes about 2 ms;
- the same dotted name works unchanged at 5.0, when the extension ships inside the `meerschaum`
  wheel.

**Why the maturin project lives in the crate and points `python-source` at the repo root:** the
root `pyproject.toml` stays setuptools this wave, so the v4 pytest suite and `scripts/test.sh` keep
working. Because the crate's pyproject is a mixed project rooted at the repository,
`maturin develop --skip-install` copies `_core.abi3.so` straight into `meerschaum/_core/` in the
working tree. Nothing is installed into any environment, and `PYTHONPATH=$(pwd)` picks it up.
`maturin build` from here produces an extension-only wheel (`meerschaum/_core/_core.abi3.so`,
distribution `meerschaum-core-dev`), because maturin does not package Python sources outside the
project root. The distribution is deliberately not called `meerschaum`, so installing it can never
replace an installed v4 package. Do not upload it.

## Build and import

```bash
uv tool install maturin                  # once (maturin 1.15 tested)
scripts/rust/build_py.sh                 # release (default): ~42 MB .so
scripts/rust/build_py.sh --debug         # unoptimized, stripped (~34 MB; ~1 GB unstripped)
scripts/rust/build_py.sh --no-polars     # skip pyo3-polars and Polars (~12 MB)
scripts/rust/clean_py.sh [--cargo]       # remove the .so (and this crate's cargo artifacts)

PYTHONPATH=$(pwd) python -c 'import meerschaum._core as c; print(c.AVAILABLE, c.build_info())'
```

`build_py.sh` runs
`maturin develop --skip-install --manifest-path crates/meerschaum-py/Cargo.toml [--release | --strip]
--features pyo3/abi3-py311,extension-module[,polars]` with `PYO3_PYTHON` set to `$PYTHON`. A
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
link libpython. Add `--features polars` to lint and test `polars_bridge.rs`. maturin turns the feature on through `pyproject.toml`. `test_core.py` needs pyarrow,
polars and pandas (test-only). Without a built extension it runs only the loader and gate tests and
skips the rest.

## Dependency pins

pyo3 is declared here as **0.25**, not through the workspace's `pyo3 = 0.26`. `pyo3-polars` for the
workspace's `polars = 0.51` (which the kernel uses) is 0.24, and 0.24 requires pyo3 0.25. Both
`pyo3-arrow` 0.11 (pyo3 0.25) and 0.12 (pyo3 0.26) target `arrow = 56`, so pyo3 0.25 is the only
version consistent with both workspace data pins. Moving to pyo3 0.26 needs polars 0.52
(`pyo3-polars` 0.25). The latest consistent set is pyo3 0.29, arrow 59, polars 0.55,
`pyo3-arrow` 0.19 and `pyo3-polars` 0.28. Upgrade all three together. pyo3's `links = "python"` means
only one pyo3 version can exist in the workspace lockfile, so the workspace entry should be set to
match whichever version this crate uses before a second crate depends on pyo3.

## Sharing the Polars build with the kernel

The crate depends on `polars = { workspace = true }` (the kernel's features, used for column
types at the pandas boundary), so `cargo build/test/clippy -p meerschaum-py` reuses the kernel's
Polars build. `pyo3-polars` 0.24, however, enables `polars-error/python`, which pulls pyo3 into
`polars-error`, and `polars-core/allow_unused`. Every Polars crate depends on `polars-error`, so
whenever the `polars` feature (which now only adds `pyo3-polars`) is on, the whole Polars stack is
compiled with a different fingerprint from the kernel's. For this reason the `polars` feature is
**opt-in**: `build_py.sh` turns it on (release by default, so it does not duplicate the kernel's
dev build) and `--no-polars` leaves it off.

`polars_roundtrip` should move to the stream path (Arrow C stream → `polars::DataFrame` via
polars-arrow FFI), so `pyo3-polars` can be dropped and the extension links exactly the kernel's
Polars. The kernel bindings already take streams.

## Findings at the Arrow boundary

- **Polars `Null` columns.** Polars exports `Null`-typed arrays (an all-`None` column) with
  `n_buffers = 1`, but the C Data Interface specifies 0. arrow-rs (56 through at least 60) rejects
  them. `ffi_import.rs` repairs this at import by setting `n_buffers = 0` on `Null` nodes before
  `from_ffi`. Producers that follow the spec are untouched. Any other Rust code that imports
  polars-produced streams with arrow-rs needs the same repair.
- **pyo3-polars needs pyarrow at call time.** `pyo3-polars` 0.24 extracts each column through
  `Series.to_arrow()`, which returns a pyarrow array. It also drops field metadata, so an `Enum`
  comes back as `Categorical`. The `__arrow_c_stream__` path (`roundtrip_arrow`) needs no pyarrow
  and keeps `Enum`, so kernel bindings should take streams unless they truly need a
  `polars::DataFrame`.
- **Metadata survives.** Schema-level metadata (pandas' `b"pandas"` block, so `to_pandas()`
  restores `Int64`, `category` and tz-aware dtypes) and field-level metadata
  (`ARROW:extension:name`, for example GeoArrow WKB) round-trip. Every imported batch carries the
  stream's full schema.
- **Empty streams have no batches.** A zero-row pandas or Polars frame exports a stream with no
  batches at all; `pandas_bridge::frame_from_arrow` imports an empty batch carrying the stream's
  schema so the columns survive.
- **Polars `Extension` string columns.** Polars 1.36 exports them as `Utf8View` with a buffer
  layout arrow-rs 56 panics on (`byte_view_array.rs`). The kernel bindings import Polars frames
  through `DataFrame.to_arrow()` (non-view layout, extension metadata kept) when pyarrow is
  available, and fall back to the capsule otherwise.
- **Zero copy.** Imported buffers are the producer's allocations, kept alive by the C Data Interface
  release callbacks. The tests assert that buffer addresses are unchanged.

## The `MRSM_CORE` gate (parity phase only)

`meerschaum/_core/_gate.py` reads `MRSM_CORE` once at import: `python` (default), `rust`, or
`rust:feature_a,feature_b`. The functions the kernel and core crates replace first start with a
guard:

```python
if (_rust := _rust_impl('filter_unseen_df')) is not None:
    return _rust(old_df, new_df=new_df, ...)
```

`rust_impl()` returns `None` (run Python) unless the gate selects the feature and `_core` binds a
symbol of that name. An unbound symbol falls through to Python. An error raised inside a bound
function propagates. With `MRSM_CORE=rust` and no build at all, `CoreNotBuiltError` is raised.
Gated today, listed in `_gate.GATED_FEATURES`: `meerschaum.utils.dataframe.filter_unseen_df`,
`enforce_dtypes`, `query_df` and the six detectors; `meerschaum.utils.dtypes.are_dtypes_equal`,
`to_pandas_dtype`, `is_dtype_numeric` and `get_geometry_type_srid`;
`meerschaum.utils.dtypes.sql.get_pd_type_from_db_type`; `meerschaum.utils.pipes.replace_pipes_syntax`;
`meerschaum.config.get_config`; `meerschaum.core.User._User.hash_password` and `verify_password`.
Every gated feature is bound.

**This gate is development-only.** D14 removes the Python core in 5.0, and the shipped package has
no `MRSM_CORE` switch. The parity oracle is v4.1.x in a separate venv (§10.2). The gate, its guards
and `GATED_FEATURES` are deleted when each façade function switches to `_core` for good. Never
document `MRSM_CORE` for users.

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
dependencies = ["meerschaum-cli==5.0.0"]   # D8: the binary; nothing else (D20)

[project.optional-dependencies]            # §12.1
pandas = ["pandas", "pyarrow"]
polars = ["polars"]
# geo, sql, sql-mssql, sql-oracle, sql-duckdb, sql-cockroachdb, api, dash, shell, cli-tools, gui, full

[tool.maturin]
bindings = "pyo3"
manifest-path = "crates/meerschaum-py/Cargo.toml"
module-name = "meerschaum._core._core"
python-source = "."
python-packages = ["meerschaum"]
features = ["pyo3/abi3-py311", "extension-module"]
exclude = ["**/__pycache__/**", "**/*.pyc", "meerschaum/_core/_core*.so"]
```

- One `cp311-abi3` wheel per platform (manylinux x86_64/aarch64, musllinux, macOS x86_64/arm64,
  Windows x86_64) covers CPython 3.11 through 3.15. Free-threaded builds (3.14t/3.15t, R8)
  cannot load abi3 modules. They need separate `cp314t`/`cp315t` wheels built without the abi3
  feature. The module already declares `gil_used = false`.
- The extension statically links Arrow and Polars, so the base wheel needs no Python data library.
  pandas and pyarrow come with `[pandas]`, polars with `[polars]`.
- The `[project.scripts] mrsm` / `meerschaum` entry points are **removed** from this wheel: the
  `mrsm` command comes from `meerschaum-cli`, and two packages must not install the same script.
  `python -m meerschaum` stays.
- `_gate.py`, the `rust_impl` guards and the replaced Python bodies are gone (D14).
- The sdist must carry `Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`, `rust-toolchain.toml`
  and the `crates/` the extension depends on (maturin's sdist collects the path dependencies of
  `manifest-path`).

**`meerschaum-cli` (D8).** A separate project at `crates/meerschaum-cli/pyproject.toml` with
`[tool.maturin] bindings = "bin"` (the uv/ruff pattern). The wheel is tagged `py3-none-<platform>`,
has no Python dependency, and installs the `mrsm` executable into the environment's scripts
directory. CI builds both wheels from the same workspace version and publishes them together.
`meerschaum` pins `meerschaum-cli==<same version>`. The binary locates its Python environment as
§4.3 describes.
