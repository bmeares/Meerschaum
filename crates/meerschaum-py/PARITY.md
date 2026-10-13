# Parity gate: `MRSM_CORE=python` vs `MRSM_CORE=rust`

The first façade switch-over (wave 3) binds the kernel and core functions listed in
[`README.md`](README.md) and runs the v4 pytest suite twice on the sqlite flavor. Wave 4 routes
the `SQLConnector` pipe, user and plugin methods through Rust and runs both modes on five flavors
(section "Wave 4" below):

```bash
scripts/rust/build_py.sh
for mode in python rust; do
  MRSM_CORE=$mode MRSM_TEST_FLAVORS=sqlite PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root \
    MRSM_PLUGINS_DIR=./tests/plugins python3 -m pytest tests/ -q -p no:cacheprovider \
    --ignore=tests/test_jobs.py --ignore=tests/test_mcp.py \
    --ignore=tests/test_listen_shutdown.py --ignore=tests/test_api_config.py \
    $DESELECT   # rust mode only, see "Deselected under rust" below
done
```

Results are at the bottom of this file. Every discrepancy found on the way is in the table.

## How the boundary restores pandas dtypes

The kernel's `Frame` erases pandas backends: `int64`, `Int64` and `int64[pyarrow]` are all one
Polars `Int64`, and `str` object columns, `string` and `large_string[pyarrow]` are all `String`
(the kernel's own fixtures compare dtypes "by logical class" for this reason, see
`crates/meerschaum-kernel/tests/common/mod.rs::logical_dtype`). v4's callers and tests observe the
exact pandas dtype, so `src/pandas_bridge.rs` restores it from *hints* computed per function from
what v4 does:

| Function | Hint for a column | Arrow-backed (`ArrowDtype`) columns | Index |
|---|---|---|---|
| `enforce_dtypes` | `to_pandas_dtype(dtypes[col])` for typed columns, the input dtype for untyped columns | every native column when v4 took the whole-frame Polars path (`to_pandas(use_pyarrow_extension_array=True)`); the native subset when v4 enforced it through Polars before the object path | reset on the Polars path, else kept |
| `filter_unseen_df` | `to_pandas_dtype()` of `dtypes` (default: the old frame's dtypes, so `int64` becomes `int64[pyarrow]`) for columns in both frames, else the new frame's dtype | none | always reset |
| `query_df` | the input dtypes (v4 ends with `enforce_dtypes(result, df.dtypes)`) | as for `enforce_dtypes` with those dtypes | kept (`index.take(rows)`), reset with `reset_index` or on the Polars path |

A hint applies only when its logical class (int, float, bool, string, datetime + timezone, date,
bytes, numeric, category, geometry, object) matches the kernel column's class; an all-null column
matches any hint, and an `object` hint matches any object column. Otherwise (for example a mixed
int/float column that the kernel turned into `Decimal`s) the default mapping applies: NumPy
dtypes for integers / floats / booleans without nulls, nullable `Int64` / `boolean` with nulls,
`datetime64[unit, tz]`, `object` for strings, decimals and nulls, and the Arrow-backed column for
dates, binary and nested types. When the `astype()` to the hint fails, the default is used.

Geometry columns come back as a `GeoSeries` (and the frame as a `GeoDataFrame` with the CRS) when
the input was a GeoDataFrame or `enforce_dtypes` declared a `geometry[...]` dtype, as v4 does.
Polars results carry `geoarrow.wkb` / `arrow.json` as Polars `Extension` dtypes and UUIDs as
`Object`, as v4's `to_polars()` builds them. Dask inputs are computed, run through the kernel, and
returned with `dask.dataframe.from_pandas(npartitions=...)` (v4 keeps them lazy).

## `get_config` and the Python `config` dictionary

During the parity phase the v4 Python package keeps mutating its module-level
`meerschaum.config.config` dictionary (`set_config`, `replace_config`, the environment patches,
callers editing returned dicts). The Rust binding therefore uses that dictionary as the shared
in-memory store: each call loads it (only the requested top-level key and `_symlinks` when the path
is already loaded) into the `meerschaum_core::config::Config` registry, runs the Rust lookup (file
reads, defaults, `MRSM{}` substitution, writing missing files, `_allow_write_missing` honored),
writes back only the top-level keys the lookup changed, and returns the dictionary's own object
when the keys were already loaded, so callers that mutate the result keep working. `sync_files`
still calls v4's `meerschaum.config._sync.sync_files` (no Rust port yet). At 5.0 the registry owns
the state and the Python dictionary becomes a proxy (design §4.4).

## Discrepancies

| # | Test(s) | Symptom under `rust` | Root cause | Resolution |
|---|---|---|---|---|
| 1 | 55 of the first run's 62 failures: most of `test_sync.py`, `test_pipes_dtypes.py`, `test_maintenance.py`, `test_verify.py`, `test_deduplicate.py`, `test_filter_existing_all_new` | `TypeError: missing column dt` from every binding on an empty frame | A zero-row pandas frame exports an Arrow C stream with **no batches**; `Frame::from_record_batches(&[])` returns a frame without columns | Boundary: import an empty batch carrying the stream's schema (`frame_from_arrow`) |
| 2 | smoke check, `test_filter_unseen_df` parity | `filter_unseen_df` returned `int64` where v4 returns `int64[pyarrow]` | v4 casts the new frame to `to_pandas_dtype()` of the old frame's dtypes, and `int64` is a Meerschaum alias (`int64[pyarrow]`) | Boundary hint: `to_pandas_dtype()` of `dtypes` / the old dtypes for columns in both frames |
| 3 | smoke check, `query_df` / `enforce_dtypes` on mixed frames | native columns came back NumPy-backed (`datetime64[ns, UTC]`, `float64`) where v4 has `timestamp[ns, tz=UTC][pyarrow]`, `double[pyarrow]`; `None` vs `NaN` in nullable floats | With fallback-typed (`object`, `uuid`, `numeric`, `geometry`) or untyped columns, v4 enforces the native subset through Polars and assigns the Arrow-backed arrays back; the kernel's `enforce_dtypes_with_path` only reports the whole-frame path | Boundary: re-run the subset decision (`native_subset_columns`, the same `enforce_dtypes_with_path` on the subset) and keep those columns Arrow-backed; `query_df` applies the same rule with `df.dtypes` |
| 4 | `tests/utils/test_dataframe.py::test_polars_geometry_uses_geoarrow_wkb` | `as_polars=True` geometry column was plain `Binary`, later `large_binary` without `extension<geoarrow.wkb>` in `to_arrow()` | Polars only keeps extension dtypes it is told about (unregistered extensions load as storage with a warning); v4 builds `pl.Extension('geoarrow.wkb', ...)` and calls `geoarrow.types.type_pyarrow.register_extension_types()` | Boundary: strip the extension metadata on export, re-apply `pl.Extension(...)` with `Series.ext.to`, register the GeoArrow pyarrow types like v4 |
| 5 | `test_polars_enforce_mixed_special_dtypes` | UUIDs came back as 16-byte binary from `to_pandas()` | The kernel exports UUIDs as `arrow.uuid` `FixedSizeBinary(16)`; v4's `to_polars()` keeps them as Polars `Object` (`strict=False`) | Boundary: `arrow.uuid` columns become `pl.Object` series of `uuid.UUID` |
| 6 | `test_sync_polars_dataframe`, `test_sync_polars_upsert` | `AttributeError: 'list' object has no attribute 'items'` in `get_bool_cols` | `enforce_dtypes(polars_df)` returns **pandas** in v4 unless `as_polars=True`; the binding returned Polars | Boundary: Polars input, pandas output (untyped columns Arrow-backed, as `to_pandas()` makes them) |
| 7 | `test_filter_existing_polars_parity_json`, `test_sync_polars_dataframe` | `PanicException: index out of bounds` importing a Polars frame | Polars 1.36 exports `Extension` string columns as `Utf8View` with a buffer layout arrow-rs 56 rejects (`byte_view_array.rs:871`) | Boundary: Polars inputs go through `DataFrame.to_arrow()` (non-view layout, extension metadata kept), the capsule only as a fallback. Open upstream: report to Polars / arrow-rs |
| 8 | `test_sync_polars_dataframe` | dtypes differed for a Polars frame with a `json` column of plain strings (Arrow-backed vs `datetime64[us, UTC]`) | v4's `_enforce_dtypes_with_polars` gives up when a declared `json` column of a **Polars** frame is not an `arrow.json` extension and takes the object path; the kernel treats a Polars `String` like a pandas `string` column (which v4 does enforce natively) and goes native | Boundary: for Polars inputs, a declared `json` column that is still native forces the object-path output. Open (kernel): the kernel cannot see the frame's origin; recorded as a known divergence of `enforce_dtypes_with_path` |
| 9 | `test_sync_polars_dataframe` (`get_data(as_polars=True, as_iterator=True)`) | an all-null chunk's `payload` column was `Null`, not `Extension('arrow.json')` | v4's `to_polars(json_cols=..., geometry_cols_types_srids=...)` types declared JSON / geometry columns even when every value is null; the kernel infers the extension from the first valid value | Boundary: `frame_to_polars` takes the declared dtypes and casts all-null declared columns to the extension |
| 10 | `test_sync_bytes_inplace` | `df['bytes_col'][2]` was `<NA>` (`binary[pyarrow]`), v4 has `None` (`object`) | In v4's object path, `bytes` / `json` / `uuid` / `numeric` columns are cast value by value and the final `astype` loop only casts columns whose current dtype is **not** `are_dtypes_equal` to the target (`object` ≡ `binary`) | Boundary hint rule for the object path: special columns stay `object`; other typed columns keep the input dtype when it is `are_dtypes_equal` to `to_pandas_dtype(typ)` (so `int32[pyarrow]` stays for `int`) |
| 11 | `tests/utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order` | `IndexError` on `native_results[0]` | The test monkeypatches v4's private `_filter_unseen_df_with_polars` and `_POLARS_FILTER_MIN_ROWS` and asserts the private helper was called; the Rust binding never calls v4 internals (D12) | **Deselected under rust.** The behavior it protects (source order, duplicate null rows) is covered by `test_filter_unseen_df_polars_parity`, which passes. Open: the test should assert on the result only (needs a change in `tests/`, outside this crate) |
| 12 | (performance, all tests) | `get_config` cost 78 µs/call vs 17 µs in Python | Each call converted the whole config dictionary | Boundary: convert only the first key and `_symlinks` when the path is already loaded. Still ~64 µs vs ~17 µs per call (converting the requested top-level subtree, e.g. `system`, dominates). **Open**: goes away when the registry owns the state (5.0) |
| 13 | `test_rust_mode_falls_through_when_symbol_missing` (`crates/meerschaum-py/tests/test_core.py`) | asserted that some gated symbol is unbound | every gated feature is bound now | Test rewritten: delete one symbol with `monkeypatch` to prove the fall-through; added `test_every_gated_feature_is_bound` and `test_rust_mode_matches_python` |
| 14 | `test_deduplicate_without_instance_method`, `test_filter_existing_all_new`, `test_mixed_timezone_aware_and_naive`, `test_no_null_indices` (regressions of fix 10) | downstream `TypeError: Cannot use .astype to convert from timezone-naive dtype to timezone-aware dtype` | Fix 10 kept the input dtype for `are_dtypes_equal` targets, but `datetime64[us]` is "equal" to `datetime64[us, UTC]` and v4 coerces datetime columns explicitly | Boundary: datetime targets always take `to_pandas_dtype(typ)` |
| 15 | `test_sync_dask_dataframe` | `NotImplementedError: Divisions calculation failed for non-numeric column 'dt'` | The binding computes Dask inputs and rebuilds them with `dd.from_pandas`, which sorts the index by default | Boundary: `from_pandas(..., sort=False)`. Open: v4 keeps Dask frames lazy; the kernel has no partitioned path (Dask is deleted in 5.0, D20) |
| 16 | (server report; `test_out_of_range_floats_do_not_saturate`, `crates/meerschaum-kernel/tests/big_floats.rs`) | `1e20` in a float column came back as `9.22e18`; `enforce_dtypes(..., {'a': 'int'})` on `[1e20, -1e20, 2**63, 2**64]` returned saturated `int64[pyarrow]` values | `as i64` saturates: `docs::convert_dtypes` made integral floats `Int64` without a range check, `pandas_astype` cast out-of-range floats / decimals to int | Kernel: `cast::float_fits_i64` (`[-2**63, 2**63)`, like pandas' `astype(int) == arr` check and Arrow's safe cast); out-of-range values stay float / the cast fails as in v4 |
| 17 | (found by `bench/bench_boundary.py`, not in the suite) | `filter_unseen_df(..., dtypes={'blob': 'bytes'})` on `object` bytes columns returns `binary[pyarrow]`; v4 returns `object` | v4 casts only the new frame to `binary[pyarrow]`; its merge with the old frame's `object` column yields `object` | **Open**: the hints would have to follow pandas' merge dtype rules |
| 18 | (found while checking 16) | `filter_unseen_df` on `object` columns of floats, or with `dtypes={'a': 'numeric'}` on float columns, returns `object`; v4 returns `float64` | v4's `normalize_nulls` calls `infer_objects()`; the kernel does not | **Open** (kernel) |

## Results

| Mode | Command | Result |
|---|---|---|
| `python` (baseline, before this wave's guards) | as above | 583 passed, 2 skipped (309.7 s) |
| `rust`, first pass | as above | 62 failed, 521 passed, 2 skipped |
| `rust`, final | as above with `--deselect tests/utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order` | **582 passed, 2 skipped, 1 deselected** (227.6 s) |
| `rust`, after the boundary optimizations (PERF.md) | same | **582 passed, 2 skipped, 1 deselected** (168 s) |
| `python`, final (same build and guards) | as above | **583 passed, 2 skipped** (239.4 s) |
| crate tests | `python3 -m pytest crates/meerschaum-py/tests/test_core.py` | 127 passed, 1 xfailed (128 passed with the row 16 regression test) |

Skipped by `--ignore` in both modes (they need services): `test_jobs.py`, `test_mcp.py`,
`test_listen_shutdown.py`, `test_api_config.py`.

## Wave 4: `SQLConnector` pipe, user and plugin methods through `_core.SqlConnector`

The v4 `SQLConnector` methods listed in [`README.md`](README.md) ("The SQL instance connector")
start with a `_rust_method(self, name)` guard; under `MRSM_CORE=rust` they run
`meerschaum_sql::SqlConnector` on the extension's Tokio runtime, with the GIL released. The v4
`Pipe` methods are unchanged and reach Rust through the instance connector and the kernel
bindings. Run per flavor (sequentially; flavor keys from `tests/connectors.py`):

```bash
for flavor in sqlite postgis timescaledb mariadb mysql; do
  for mode in python rust; do
    MRSM_CORE=$mode MRSM_TEST_FLAVORS=$flavor PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root \
      MRSM_PLUGINS_DIR=./tests/plugins python3 -m pytest tests/ -q -p no:cacheprovider \
      --ignore=tests/test_jobs.py --ignore=tests/test_mcp.py \
      --ignore=tests/test_listen_shutdown.py --ignore=tests/test_api_config.py \
      $DESELECT   # rust only: row 11 above and rows 19-20 below
  done
done
```

`DESELECT` under rust: `tests/utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order`,
`tests/test_pipe_attributes.py::test_sql_attribute_errors_are_not_unregistered`,
`tests/test_pipe_attributes.py::test_sql_no_row_is_still_empty`.

### Discrepancies

| # | Test(s) | Symptom | Root cause | Resolution |
|---|---|---|---|---|
| 19 | `tests/test_pipe_attributes.py::test_sql_attribute_errors_are_not_unregistered`, `::test_sql_no_row_is_still_empty` | under rust, the mocked `exec` is never called (no `RuntimeError`; a real row instead of `{}`) | both tests monkeypatch `SQLConnector.exec` (SQLAlchemy) to fake the metadata query; the routed `get_pipe_attributes` reads through the Rust driver | **Deselected under rust.** The behavior they protect (a failed metadata read raises instead of reporting "unregistered") is covered with a real failure by `tests/test_facade_sql.py::test_attribute_read_errors_propagate` (Rust errors surface as `RuntimeError`) |
| 20 | `tests/test_index_queries.py::test_postgresql_upsert_creates_one_unique_constraint` (and every test calling a v4 SQL function with a stub `self`) | **python mode**: `AttributeError: 'SimpleNamespace' object has no attribute '_rust_method'` | the first guards called `self._rust_method(...)`; tests (and plugins) call these functions with `SimpleNamespace` stand-ins for the connector | Guards call `_gate.rust_method(self, name)`, which returns `None` in `python` mode before touching `self`, and for objects without `_rust_method` (duck typing, §4.4). Regression test `tests/test_facade_sql.py::test_stub_self_runs_python`, `test_core.py::test_rust_method_runs_python_for_stubs_and_python_mode` |
| 21 | `tests/test_partition.py::test_repartition_roundtrip[postgis]` | `pipe.parameters['verify']['chunk_minutes']` kept the old width after `partition_pipe` | v4's `partition_pipe` calls `pipe.update_parameters({'verify': {'chunk_minutes': n}})` on the Python pipe; Rust persisted the new width to `mrsm_pipes` but cannot mutate the Python object | Wave 5: `partition_pipe_with_parameters` returns the parameter patch and the binding applies it in memory (`persist=False`), including changes preceding a later repartition failure. The existing Python guard still performs its redundant registered-parameter refresh. |
| 22 | (review, `sync_pipe`) | the Python pipe's in-memory `autoincrement` / numeric `dtypes` and its caches went stale after a Rust sync | v4's `sync_pipe` mutates the Python pipe (`update_parameters`, `pipe.dtypes = ...`) and clears `_columns_types` / `_columns_indices` | `facade::sync_pipe_mut` returns the mutated core pipe; the binding patches the changed top-level parameters into the Python pipe (`persist=False`, Rust persisted them) and the guard clears `_exists`, `_columns_types(_timestamp)`, `_columns_indices(_timestamp)` |
| 23 | (review, `create_indices`) | the guard ran before v4's `_skip_check_indices` early return | ordering | guard moved below the early return (likewise `get_pipe_columns_indices`) |
| 24 | (found by a smoke check, `get_pipe_docs`) | NULL cells came back as `nan` | `Column::values()` gives the pandas null flavor of the column type (`NaN` for floats and strings) | Binding: `NULL` → `None` through the null mask, as `result.mappings()` gives |
| 25 | (found by a smoke check, `get_pipe_docs` on SQLite) | `datetime` cells are `datetime` objects under rust, `'2024-01-01 00:00:00.000000'` strings under v4 | v4 returns the raw `sqlite3` values (SQLite stores the timestamps as text); the Rust driver types the column from its declared `TIMESTAMP` type | Resolved in wave 5: `SqlDriver::read_docs_frame` decodes SQLite storage classes without applying declared timestamp types; the binding uses this raw read path. `get_data()` retains typed timestamps. |
| 26 | (review, `get_pipe_data(order=None)`) | rows sorted ascending | the binding mapped `order=None` to `'asc'` | `facade::get_pipe_data_and_state(..., no_order)`: no `ORDER BY` and no sort, as v4 |
| 27 | (review, jobs) | `block_on` would hang in a forked child (v4 job daemons fork) | a forked child inherits the Tokio runtime object but none of its threads, and the parent's pool sockets | the runtime is keyed by PID (a child builds its own; the parent's is leaked, not dropped) and `SQLConnector._rust_method` rebuilds `_core.SqlConnector` when `os.getpid()` changed |
| 28 | (perf, all PostgreSQL tests) | small calls 3-4x slower than v4 (`get_sync_time` 13.8 ms vs 3.3 ms) | every Rust data method re-read the table state (`table_exists` + a columns-types catalog query), where v4 reads the pipe's cached `get_columns_types()` | the bindings pass `pipe.get_columns_types()` (v4's own 60 s cache) into `facade::pipe_state_cached`; see `PERF.md` |
| 29 | (design §4.4, not a test) | `pipe.parameters['tags'] = [...]` then `pipe.edit()` does **not** persist, in both modes | v4.1.4's `parameters` property returns a resolved copy (`pipe.parameters is pipe.parameters` is `False`); attribute setters (`pipe.tags = ...`) write `_attributes['parameters']` | Unchanged in `python` mode (parity). Resolved in wave 6 for `rust` mode (row 30); `tests/test_facade_sql.py::test_attribute_aliases_persist` pins the setter path |

Not routed (stay v4 Python on purpose): `read`, `exec`, `value`, `to_sql`, `.engine`,
`get_tables()` (D16); `sync_pipe_inplace` (it reaches the routed methods through `self`);
`get_add_columns_queries` / `get_alter_columns_queries` / `get_to_sql_dtype` /
`create_pipe_table_from_df` (only used by the Python `sync_pipe` and `sync_pipe_inplace`); the
`mssql`, `oracle` and `duckdb` flavors and in-memory SQLite databases (their SQLAlchemy and Rust
connections would see different databases).

### Results (wave 4)

Final runs (same build and guards for both modes; 609 tests collected per flavor, including
this wave's `tests/test_facade_sql.py`). "2 failed" in every row are the two `test_worker.py`
tests explained below.

| Flavor | `python` | `rust` |
|---|---|---|
| `sqlite` | 604 passed, 3 skipped, 2 failed (192 s) | **602 passed, 2 skipped, 3 deselected**, 2 failed (84 s) |
| `postgis` | 604 passed, 3 skipped, 2 failed (231 s) | **602 passed, 2 skipped, 3 deselected**, 2 failed (100 s) |
| `timescaledb` | 604 passed, 3 skipped, 2 failed (219 s) | **602 passed, 2 skipped, 3 deselected**, 2 failed (109 s) |
| `mariadb` | 604 passed, 3 skipped, 2 failed (223 s) | **602 passed, 2 skipped, 3 deselected**, 2 failed (90 s) |
| `mysql` | 604 passed, 3 skipped, 2 failed (227 s) | **602 passed, 2 skipped, 3 deselected**, 2 failed (93 s) |
| crate tests (`crates/meerschaum-py/tests/test_core.py`) | 132 passed, 1 xfailed | |

The extra `python` skip is `test_facade_sql.py::test_attribute_read_errors_propagate` (rust only).
First rust pass on sqlite (before rows 19-29): 2 failed (row 19); on postgis: 1 more (row 21).
Under `python`, before row 20, the stub-`self` tests (`test_index_queries.py`) failed in the guard itself.

`tests/test_worker.py::test_fallback_codec_roundtrip` and `::test_fallback_codec_matches_msgpack`
belong to the concurrent `meerschaum-worker` work: they fail in **both** modes when run after the
rest of the suite and pass alone (order-dependent `msgpack` fallback state), so they are not
façade discrepancies.

## Performance

Optimized after this wave; see [`PERF.md`](PERF.md) for the benchmark, the profile and the
current numbers (every scenario is now faster under `rust` than under `python`). The table below
is the state at the end of the parity wave.

### At the end of the parity wave (1M-row synthetic frame, no optimization yet)

Release build, best of three, same machine and process; frames have a UTC `datetime` column,
a random `int` id, a random `float` and a pandas `string` label (half of `new` overlaps `old` on the
datetime axis).

| Call | `python` | `rust` |
|---|---|---|
| `filter_unseen_df(old, new)` | 2.29 s | 7.41 s |
| `enforce_dtypes(all-string frame, {dt: datetime, id: int, value: float})` (whole-frame Polars path) | 1.07 s | 2.03 s |
| `enforce_dtypes(typed frame, {id: int})` (mixed: native subset + untyped columns) | 0.06 s | 3.30 s |
| `get_config('system', 'connectors', 'sql', 'chunksize')` | 17 µs | 64 µs |

Not profiled yet. The likely costs are at the boundary rather than in the kernel algorithms:
`object` columns cross value by value, the kernel converts between Polars and Arrow
through IPC buffers in both directions, string columns come back to pandas as Python `str`
objects, and `enforce_dtypes` on a mixed frame runs the native-subset decision a second time to
reproduce v4's output dtypes. None of this was optimized in this wave.

## Wave 5 SQL verification

The documented SQLite command (the four ignores and three deselections above), after the
cached sync-state/read-options changes, raw SQLite document reads, and UTC sync-time fix:
**606 passed, 2 skipped, 3 deselected** in 84.53 s on the final release build
(earlier runs under concurrent builds took 92.92 s and 165.43 s). This used the release extension with
`MRSM_CORE=rust`, `PYTHONPATH=$(pwd)`, `MRSM_ROOT_DIR=./test_root`, and
`MRSM_PLUGINS_DIR=./tests/plugins`. An initial run with an empty plugin directory and a minimal
Python environment failed fixture/dependency setup; the count above is the complete rerun
with the documented test environment. Large SQLite/PostGIS benchmark results and the
remaining millisecond-scale performance gaps are recorded in `PERF.md`.

## Wave 6: live `Pipe.parameters` (design §4.4)

| # | Test(s) | Symptom | Root cause | Resolution |
|---|---|---|---|---|
| 30 | `tests/test_live_parameters.py` | `pipe.parameters['fetch']['backtrack_minutes'] = 5` then `pipe.edit()` did not persist (row 29) | v4's `parameters` property returns a resolved deep copy | Under `MRSM_CORE=rust` (feature name `pipe`, so `rust:pipe` works) the property returns `meerschaum/core/Pipe/_live_parameters.py::LiveDict`: a real `dict` subclass holding the **resolved** values (references, `{{ Pipe() }}`, `MRSM{}`, as v4 reads them) whose mutators (`__setitem__`, `__delitem__`, `update`, `pop`, `popitem`, `setdefault`, `clear`, `|=`) write through to the **raw** parameters; nested dicts and lists are live (`LiveList` replays `append`/`extend`/`insert`/`sort`/... on the raw list). Only touched keys are written, so a template that was merely read stays a template in the raw map; a write under an inherited (reference) key creates the key in the raw map only. Every write drops the resolved-parameters memo. `isinstance(dict)`, `json.dumps`, `**`, `deepcopy` and `pickle` give plain dicts. The `parameters` setter unwraps live views. `python` mode is unchanged (still a copy; pinned by `test_python_mode_parameters_are_a_copy`). |

## Wave 6b: `_core.Pipe` behind `meerschaum.Pipe` (design §4.4)

New pyclass `meerschaum._core.Pipe` (`src/pipe_api.rs`, wrapping `meerschaum_core::Pipe`). It maps
constructor arguments only and never touches an instance. Surface: `connector_keys`, `metric_key`,
`location_key`, `instance_keys`; `raw_parameters` (copy), `set_raw_parameters`,
`update_raw_parameters`, `get_path` / `set_path` / `del_path` (keys and list indices, negative
indices, missing dicts created on `set_path`); `resolved_parameters(pipe=None)` (core
`resolve_parameters_with`; with the Python `Pipe` it uses `PyResolver`, the Python-side resolver
callback for cross-pipe templates); `target(sql_flavor=None)`; `__eq__` / `__hash__` by keys;
`__str__` / `__repr__` (core `Display`, no ANSI); `__getstate__` / `__setstate__` / `__reduce__`.

Under `MRSM_CORE=rust` (feature `pipe`) `meerschaum.Pipe`:

- creates `self._core_pipe` in `__init__` once the instance keys are known; `connector_keys`,
  `metric_key`, `location_key`, `instance_keys` (and `metric` / `location`) read it;
- uses `_live_parameters.Attrs` as `_attributes`: assigning `'parameters'` adopts the value as a
  `RawDict` whose nested dicts and lists (`RawDict` / `RawList`) push the whole raw map into
  `_core_pipe` after every mutation (and drop the resolved-parameters memo). Every path that wrote
  `_attributes['parameters']` (init, the `parameters` setter, `update_parameters`, `columns` /
  `tags` / `upsert` / ... setters, direct nested writes by connectors) therefore reaches Rust.
  `_cache_value` / `_get_cached_value` re-adopt a replaced `'attributes'` dict (a metadata load from
  the instance or the disk cache), so Rust holds the loaded raw parameters too;
- `LiveDict` / `LiveList` (the resolved `pipe.parameters` view) write through to that Rust-owned raw
  map (`_raw_node()` resolves to the `RawDict` tree).

`python` mode is untouched. Tests: `tests/test_core_pipe.py` (7, rust-only), the existing
`tests/test_live_parameters.py` (adjusted setter assertion).

| # | Deviation | Reason |
|---|---|---|
| 31 | v4 stored the dict passed as `parameters=` by reference (later edits of the caller's dict were visible through the pipe); rust mode copies it into the Rust-owned map | the raw map is owned by `_core.Pipe`; the live `pipe.parameters` view is the supported way to mutate |
| 32 | Python keeps reading the raw parameters from a Python-side `RawDict` mirror; Rust is written on every change (and verified equal by `test_every_raw_parameter_write_reaches_rust`), not read back on each access | `get_parameters` / `_symlinks` / reference resolution are v4 Python and read `_attributes['parameters']` on hot paths; converting from Rust on each read would cost a clone per access. Moving reads over comes with the Rust resolution of `get_parameters` |
| 33 | `get_parameters()` resolution (references, `{{ Pipe() }}` with `_symlinks` capture used by `edit()`) is still v4 Python; `_core.Pipe.resolved_parameters(pipe)` exists and is tested on self templates only | the Python `_symlinks` bookkeeping and reference caching have no Rust counterpart yet |
| 34 | `Pipe.target`, `__eq__`, `__hash__`, `__str__` (ANSI styling) stay on the Python implementations (they read the keys from Rust); `_core.Pipe.target/__eq__/__hash__/__str__` are bound but not routed | `target` warns and writes back through Python state; `__eq__` must stay duck-typed; ANSI `pipe_repr` is a Python formatter |

Known gaps: `pipe.columns` / `dtypes` return plain copies (assignment works, in-place mutation of the
returned dict does not); `pipe.tags` returns the live list in `rust` mode. The resolved view is a
snapshot at read time. Whole-map pushes are O(size of parameters) per mutation.

Results (sqlite, ignores as above plus `--ignore=tests/api_host`, release extension rebuilt with
`scripts/rust/build_py.sh`): `rust` **623 passed, 3 skipped, 3 deselected** (134 s); `python`
**613 passed, 16 skipped** (286 s; the skips are rust-only tests); crate tests
`crates/meerschaum-py/tests/test_core.py` 132 passed, 1 xfailed; `cargo test -p meerschaum-py` all
passing; `cargo clippy -p meerschaum-py --all-targets --no-deps -- -D warnings` clean (without
`--no-deps` it stops on an unrelated warning in `meerschaum-cli/src/native_plugins.rs:114`, another
task's file).
