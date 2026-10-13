# Performance of the pandas boundary (`MRSM_CORE=rust` vs `python`)

The kernel bindings (`enforce_dtypes`, `filter_unseen_df`) were slower than v4 on large frames
after the first parity wave. This file records the measurements, where the time went, what was
changed, and what is left.

## How to measure

```bash
scripts/rust/build_py.sh                      # release build (required: debug is ~10x slower)
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python3 crates/meerschaum-py/bench/bench_boundary.py            # the table below
# One scenario in a loop, for a profiler:
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python3 crates/meerschaum-py/bench/bench_boundary.py --profile enforce_mixed --loops 20
# Per-stage timings of the boundary (stderr), see src/trace.rs:
MRSM_CORE_TRACE=1 ... bench_boundary.py --profile filter_unseen_df --loops 1
```

`bench_boundary.py` runs both modes in one process (it flips the gate with `_gate.set_mode()`,
so the Rust path goes through the same guard as the test suite), times each call best of three
after a warm-up call, and compares the two results (values with `assert_frame_equal(check_exact=
True)`, dtypes, index, class). Scenarios:

| Scenario | Call | Rows |
|---|---|---|
| `filter_unseen_df` | `filter_unseen_df(old, new)`: UTC `datetime64[us]`, `int64` id, `float64`, pandas `string` label; half of `new` overlaps `old` | 1,000,000 each |
| `enforce_all_string` | `enforce_dtypes(df, {dt: datetime, id: int, value: float})` on `object` columns of `str` (whole-frame Polars path) | 1,000,000 |
| `enforce_mixed` | `enforce_dtypes(old, {id: int})` (native subset + three untyped columns) | 1,000,000 |
| `enforce_special` | `enforce_dtypes` with `json` (dicts), `uuid`, `numeric` (`Decimal`), `bytes`, `geometry` (shapely points) object columns plus `datetime` / `int` | 100,000 |
| `filter_special` | `filter_unseen_df` on the same columns, with those `dtypes` | 75,000 each |

Machine: 40 cores, Linux 7.2, CPython 3.14.8, pandas 2.3.3, pyarrow 22.0.0, polars 1.36.1,
numpy 2.2.6, shapely 2.1.2 (in the `mrsm` venv), release build of `_core` (Rust 1.99).

## Results

| Scenario | `python` | `rust` before | `rust` after | after / python |
|---|---|---|---|---|
| `filter_unseen_df` | 2.73 s | 5.59 s | **1.06 s** | 0.39x (2.6x faster) |
| `enforce_all_string` | 0.88 s | 1.86 s | **0.70 s** | 0.79x |
| `enforce_mixed` | 0.068 s | 3.43 s | **0.050 s** | 0.74x |
| `enforce_special` | 1.01 s | 3.36 s | **0.79 s** | 0.78x |
| `filter_special` | 2.12 s | 3.18 s | **1.32 s** | 0.62x |

Every result is identical in both modes, except `filter_special`'s `blob` column, which was
already different before this work (see "Known differences" below). The sqlite suite under
`MRSM_CORE=rust` still passes (582 passed, 2 skipped, 1 deselected) and got faster as a whole:
229 s before, 168 s after.

## Where the time went (before)

Stage timers (`MRSM_CORE_TRACE=1`, one call), temporary timers inside the kernel functions, and
`py-spy record --native` (see "Profiling notes"). "Export" is pandas → Arrow (`pa.array` per
column), "import" Arrow → kernel `Frame`, "restore" Arrow → pandas with the dtype hints.

**`filter_unseen_df` (5.6 s)**

| Stage | Time |
|---|---|
| export + import, both frames (`pa.array`, `Table.from_arrays`, C stream, Arrow → Polars through IPC) | 105 ms |
| kernel `filter_unseen_df` | **3,529 ms** |
| &nbsp;&nbsp;`astype` of the non-datetime columns (value by value, even when the dtype already matches) | 697 ms |
| &nbsp;&nbsp;datetime casts (same, plus timezone coercion) | 385 ms |
| &nbsp;&nbsp;detectors (`get_numeric_cols`, `get_json_cols`, uuid/bytes/geometry): each built a full null mask element by element to find the first valid value | 372 ms |
| &nbsp;&nbsp;`normalize_nulls` (regex on every string of both frames, columns rebuilt as objects) | 219 ms |
| &nbsp;&nbsp;anti-join: `Vec<Vec<Value>>` row keys for both frames, `HashSet` of rows | 1,917 ms |
| &nbsp;&nbsp;`take` + deserialization | 180 ms |
| kernel → Arrow (Polars IPC) | 12 ms |
| restore `dt` (`ArrowExtensionArray.astype('datetime64[us, UTC]')` goes through one `Timestamp` per value) | **1,727 ms** |
| restore `label` (`object` → `string`) | 146 ms |

**`enforce_all_string` (1.9 s)**: `object` columns value by value 300 ms (3 × 100 ms); kernel
1,328 ms (the whole input deep-copied before the native path, `infer_native_series` cloning every
string into the Polars builder 100-157 ms per column, `str_to_datetime` parsing every string twice
with chrono's format parser 519 ms); export 37 ms.

**`enforce_mixed` (3.4 s)**: export + import 106 ms; kernel 40 ms; native-subset re-derivation
6 ms; kernel → Arrow 112 ms; restore `dt` **3,094 ms** (the `Timestamp` path again). The three
untyped columns, which v4 never touches, were converted in and out.

**`enforce_special` (3.4 s, 100k rows)**: geometry in **1,028 ms** (one `shapely.to_wkb` call per
value), JSON 64 ms, UUID 58 ms, `Decimal` 37 ms; kernel 622 ms (the native subset dumps every JSON
value to text and parses it back, plus three deep copies of the object columns); the boundary
re-ran the native-subset enforcement to learn which columns v4 enforces through Polars
(**467 ms**); geometry out **565 ms** (one `shapely.from_wkb` per value), UUID out 158 ms (parsing
hyphenated text), JSON out 109 ms, `Decimal` out 70 ms.

**`filter_special` (3.2 s)**: geometry in 2 × 750 ms; other object columns 2 × 120 ms; kernel
972 ms; restore 0.3 s.

So the kernel itself was the largest cost for `filter_unseen_df` and `enforce_all_string`, and
the boundary's datetime restoration and per-value geometry calls dominated the rest. Polars
frames going through `to_arrow()` and the empty-batch handling were not measurable here.

## What changed

Kernel (`crates/meerschaum-kernel`, behavior unchanged; its fixtures pass):

| Change | Where | Effect |
|---|---|---|
| `first_valid_index` scans with early exit (and answers in O(1) without nulls) instead of building a full null mask through `Series::get` | `frame.rs` | with the next three rows: kernel `filter_unseen_df` 3,529 → 490 ms |
| `pandas_astype` returns the column as is when the cast is an identity (same dtype; floats without `NaN`, since the value path turns `NaN` into null; no `UInt64`; datetimes naive or UTC and inside `DateTimeValue`'s range) | `enforce.rs` | (see first row) |
| Same-unit datetime casts relabel the timezone without rebuilding the column | `enforce.rs` (`native_cast`) | part of the above |
| Anti-join on typed keys: per column, both frames' values as `i64` / float bits / `&str` / `&[u8]` when the dtypes match (equivalent to `Value`'s key equality within one dtype), `Value`s otherwise; row hashes column-wise, chained buckets, exact comparison on hash hits; `normalize_nulls` applied on the fly for keys and only to the delta rows (`normalize(new).take(keep)` = `normalize(new.take(keep))`) | `diff.rs` | (see first row) |
| `enforce_dtypes_with_report` returns the `EnforcePath` (`Native`, `Subset(columns)`, `Object`) it took; `enforce_dtypes_with_path` wraps it | `enforce.rs` | the boundary no longer re-runs the subset (467 ms → 0) |
| Native path reads columns by reference (no deep copy of the frame or the selected subset); a declared `json` value that already is a JSON container skips the dump/parse round trip (an identity on the cell) | `enforce.rs` | `enforce_special` kernel 622 → 313 ms |
| `values_to_series` borrows strings and bytes instead of cloning them into the builder | `frame.rs` | 100-157 → 40 ms per 1M-string column |
| Datetime strings are parsed once (shape and value together), `YYYY-MM-DD` without chrono's format parser (chrono still handles every other form), and the column is parsed in contiguous chunks on scoped threads (`std::thread::scope`, no new dependency; a failure in any chunk fails the column, as before) | `enforce.rs`, `value.rs` | `dt` cast 519 → 160 ms single-threaded; with the threads and the two rows above, the `enforce_all_string` kernel call 1,328 → 236 ms |

Boundary (`crates/meerschaum-py`):

| Change | Where | Effect |
|---|---|---|
| Datetime restoration through `ChunkedArray.to_pandas()` (plus a lossless Arrow cast when the target unit is finer) when the timezone matches and the unit does not get coarser; other cases keep the pandas path | `pandas_bridge.rs` (`datetime_series`) | 1.7-3.1 s → 1-8 ms per column |
| Columns the kernel returns unchanged (bit for bit, `cells_identical` / `column_identical`) are copies of the input Series (`Series.copy()`, what v4's `df.copy()` returns for columns it does not cast) when the output dtype is the input dtype; they are left out of the Arrow export | `pandas_bridge.rs` (`unchanged_columns`, `OutSpec::reuse`) | JSON/UUID/`Decimal` out 337 ms → 4 ms; export shrinks |
| Untyped pandas columns cross `enforce_dtypes` as all-null placeholders (v4 never reads or casts them); the call checks that every placeholder came back unchanged and redoes the call with full conversion otherwise; the output copies the input Series | `pandas_bridge.rs` (`frame_from_py_with`), `kernel_api.rs` | `enforce_mixed` 226 → 48 ms |
| Geometry columns converted with one vectorized `shapely.to_wkb` / `shapely.from_wkb` per column (shapely classes recognized by a per-type cache) | `convert.rs` (`pylist_to_cells`, `cells_to_pylist`) | in 1,028 → 69 ms, out 565 → 66 ms |
| UUIDs built with `UUID(int=...)` (positionally) instead of parsing hyphenated text | `convert.rs` | ~40% less per UUID |
| No deep copies of the input frames to cross `allow_threads` (the kernel borrows them) | `kernel_api.rs` | object columns no longer copied before the kernel call |
| `MRSM_CORE_TRACE=1` prints per-stage timings to stderr | `trace.rs` | (diagnostics; off by default) |

The GIL was already released around the kernel calls (`py.allow_threads`).

### Tried and reverted

- **All-`str` object columns through `pa.array(series, from_pandas=True)`.** `pa.array` alone
  costs about what the per-value conversion costs (75-86 ms per 1M strings), and it tried
  (and failed) type inference on `Decimal` / `dict` / `UUID` columns first (up to 240 ms). Net
  loss; removed.

## After: where the time goes now

| Scenario | Stages (one call) |
|---|---|
| `filter_unseen_df` 1.06 s | export + import 2 × 60-85 ms (the `string[python]` label column's `__arrow_array__` converts every `str`, the rest is the Arrow → Polars IPC copy); kernel 490 ms; kernel → Arrow 19 ms; restore `label` 157 ms (500k Python `str` objects + `astype('string')`), other columns < 2 ms |
| `enforce_all_string` 0.70 s | `object` columns in 3 × 115 ms; kernel 236 ms; kernel → Arrow 36 ms |
| `enforce_mixed` 0.05 s | `id` in 12 ms; kernel < 1 ms; `id` out 12 ms; three reused columns 3 ms |
| `enforce_special` 0.79 s | object columns in 240 ms (JSON 64, UUID 58, geometry 70, `Decimal` 38); kernel 313 ms; geometry out 66 ms; `GeoDataFrame` 21 ms |
| `filter_special` 1.32 s | object columns in 2 × 175 ms; kernel 804 ms; object columns out 86 ms |

What would still be needed for more:

- **Object columns cross value by value under the GIL** (about 100 ns per `str`, 600 ns per
  JSON dict, 600 ns per UUID). This is the floor for `enforce_all_string` (345 of 700 ms) and a
  quarter of the special scenarios. Going lower needs either the kernel to accept Python-owned
  string data without allocating a `String` per value (a borrowed string column type in
  `Frame`), or pandas inputs that are already Arrow-backed (`string[pyarrow]`, which cross
  zero-copy today).
- **Arrow ↔ Polars goes through IPC buffers** in the kernel (`Frame::from_record_batches`,
  `Frame::to_record_batch`): 40-85 ms per 1M-row frame each way. `polars-arrow`'s C Data
  Interface (`import_array_from_c` + `Series::try_from((&ArrowField, ArrayRef))`, the same
  construction Polars' IPC reader uses) would make it zero-copy except for the string view
  conversion. Not done here: it adds `polars-arrow` as a direct kernel dependency (already in the
  build graph) and touches the path the SQL drivers use.
- **`filter_special`'s kernel time** (800 ms for 75k rows) is the special columns: JSON
  serialization with sorted keys and decimal formatting for both frames, then deserialization of
  the delta, all on `Value`s. Typed key columns for JSON text would avoid the second round trip.
- **Restoring `string` (`StringDtype('python')`) columns** needs one Python `str` per value plus
  pandas' validation in `astype('string')` (157 ms per 500k).

## Correctness work done on the same path

**Integral floats outside `int64` no longer saturate** (reported by the server work: `1e20` came
back as `9.22e18`). Two kernel spots used `f as i64`, which saturates:

- `docs::convert_dtypes` (`pd.DataFrame(docs).convert_dtypes()`): an integral float column became
  `Int64` without a range check. pandas converts only when `arr.astype(int) == arr`, i.e. inside
  `[-2**63, 2**63)`; `1e20`, `-1e20`, `2**63`, `2**64` now stay `Float64`, and `-2**63` still
  becomes `Int64`.
- `enforce::pandas_astype` to an integer dtype from a float or `Decimal`: now raises for
  out-of-range values like Arrow's safe cast ("Float value ... was truncated converting to
  int64"), so `enforce_dtypes(..., {'a': 'int'})` leaves the column as it was and
  `filter_unseen_df` against an `int64` frame falls back to the numeric rule (`Decimal('1E+20')`
  style values), as v4 does.

Both use `cast::float_fits_i64`. Regression tests: `crates/meerschaum-kernel/tests/big_floats.rs`
(kernel, values from `MRSM_CORE=python`) and `test_out_of_range_floats_do_not_saturate` in
`tests/test_core.py` (both modes, `float` / `numeric` / `int` / untyped, float64 and object
columns, `filter_unseen_df`, `query_df`).

## Known differences (not caused by this work, not fixed)

- `filter_unseen_df(..., dtypes={'blob': 'bytes'})` on `object` bytes columns returns
  `binary[pyarrow]` under Rust, `object` under v4: v4 casts only the new frame to
  `binary[pyarrow]`, and the pandas merge with the old frame's `object` column yields `object`.
  Matching it means emulating pandas' merge dtype rules in the hints.
- `filter_unseen_df` on `object` columns of floats (or with `dtypes={'a': 'numeric'}` on float
  columns) returns `object` under Rust and `float64` under v4: v4's `normalize_nulls` calls
  `infer_objects()`, which the kernel does not reproduce.
- The suite's deselect path is `tests/utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order`
  (PARITY.md, row 11).

## Profiling notes

- `py-spy record --native` at the default or 200 Hz never finished on the 51 MB release `.so`
  (the unwinder stalls); 10-50 Hz works (`-r 50`). Rust frames show as mangled `_RNv...`
  symbols; filter by crate name.
- `perf` and `cargo flamegraph` are not installed on the dev machine; the kernel breakdowns above
  come from temporary `Instant` timers inside the kernel functions (removed), the boundary ones
  from `MRSM_CORE_TRACE`.
- Never rebuild the extension while a test run has it loaded: `maturin` replaces the `.so` in
  place.

## Wave 4: end-to-end `Pipe` operations through the `SQLConnector` façade

`bench/bench_sync.py` times real `Pipe` calls (the v4 `Pipe` methods, with the instance methods
routed through `_core.SqlConnector` under `rust`). Both modes run in one process on separate pipes;
frames have a UTC `datetime`, a random `int` id, a `float` and a string label. `sync (half
overlap)` syncs 1M rows of which 500k are already stored (backtrack read + `filter_existing` +
insert); `sync (update)` changes the value of `--update-rows` stored rows spread over the whole
table (so the backtrack read covers all 1.5M rows). Release build, same machine as above, the
`get_data()` results are compared (identical in every run).

```bash
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python3 crates/meerschaum-py/bench/bench_sync.py --rows 1000000 --update-rows 10000 --flavor postgis
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
    python3 crates/meerschaum-py/bench/bench_sync.py --rows 1000000 --update-rows 100 --flavor sqlite
```

**PostGIS** (test container, `--update-rows 10000`):

| Step | `python` | `rust` | rust/python |
|---|---|---|---|
| `sync` (new table, 1M rows) | 22.74 s | 5.69 s | 0.25x |
| `sync` (1M rows, half overlap) | 15.68 s | 11.61 s | 0.74x |
| `sync` (update 10k rows) | 18.04 s | 6.62 s | 0.37x |
| `get_data()` (1.5M rows) | 12.91 s | 2.76 s | 0.21x |
| `get_sync_time()` | 3 ms | 4 ms | (noise) |
| `get_rowcount()` | 71 ms | 43 ms | 0.61x |

**SQLite** (`--update-rows 100`):

| Step | `python` | `rust` | rust/python |
|---|---|---|---|
| `sync` (new table, 1M rows) | 61.40 s | 5.84 s | 0.10x |
| `sync` (1M rows, half overlap) | 37.41 s | 11.76 s | 0.31x |
| `sync` (update 100 rows) | 21.45 s | 25.92 s | **1.21x** |
| `get_data()` (1.5M rows) | 7.97 s | 7.09 s | 0.89x |
| `get_sync_time()` | 3 ms | 3 ms | 1.09x |
| `get_rowcount()` | 7 ms | 9 ms | (noise) |

Explained regressions:

- **SQLite reads.** `MRSM_CORE_TRACE=1` splits a Rust `get_pipe_data` into `pipe_state`,
  `build_data_query`, `read`, `select + enforce_dtypes` and `sort + limit`
  (`meerschaum_sql::facade`). On 150k rows the driver's `read` is 972 ms of the 1,007 ms
  (enforce < 1 ms, sort 34 ms, frame → pandas 25 ms), about 6.5 µs per row, where v4's
  `pandas.read_sql` through `sqlite3` takes about 630 ms in total: `sqlx`'s SQLite driver decodes
  row by row into `SqlValue` cells (one `String` per text cell) on its worker thread. At 1M rows
  the rest of the pipeline makes up for it (`get_data` 0.89x), but the `sync (update)` step, whose
  backtrack read covers all 1.5M rows, is 21% slower. Request filed with `meerschaum-sql`: decode
  SQLite results straight into Arrow builders (e.g. `rusqlite` column access).
- **SQLite updates are slow in both modes.** v4's `sqlite_delete_insert` update
  (`DELETE ... WHERE ROWID IN (SELECT ... INNER JOIN patch ON <COALESCE'd join>)`) cannot use the
  index, so 1,000 updated rows on a 150k-row table take about 21 s under v4 and 25 s under Rust
  (same query text, kept for parity). Hence `--update-rows 100` on SQLite.
- **Per-call catalog queries (fixed).** Every Rust data method starts with `pipe_state`
  (`table_exists` + the columns-types catalog query), where v4 reads the Python pipe's
  `get_columns_types()` cache (60 s). On PostGIS that made small calls 3-4x slower
  (`get_sync_time` 13.8 ms vs 3.3 ms, `get_rowcount` 10.9 vs 2.0 ms, a 28-row `get_data` 26 vs
  10 ms). The bindings now pass `pipe.get_columns_types()` (v4's own cache, which v4's
  `get_sync_time` reads too) to `facade::pipe_state_cached`, and the numbers are at parity
  (6.1 vs 6.0 ms, 2.9 vs 3.0 ms, 9.1 vs 10.4 ms). `sync_pipe` still reads the table state itself
  (a 1-row sync is 89 ms vs 80 ms on PostGIS, 26 ms vs 62 ms on SQLite); passing the cached state
  into `sync_pipe_impl` needs a change in `meerschaum-sql` (requested).
- `get_pipe_columns_types()` called directly (uncached in both modes) is slower on PostgreSQL
  (8.5 ms vs 3.2 ms): the Rust connector runs `table_exists` before the columns query and its
  catalog query is heavier. Reported to `meerschaum-sql`.

The sqlite pytest suite takes 87 s under `rust` and 271 s under `python`; postgis 116 s vs 267 s.

`bench_boundary.py` re-run after this wave (unchanged code paths, same machine under load from
concurrent builds): `filter_unseen_df` 2.68 s / 1.12 s, `enforce_all_string` 0.94 / 0.72,
`enforce_mixed` 0.067 / 0.049, `enforce_special` 1.00 / 0.81, `filter_special` 2.11 / 1.33
(python / rust), with the known `blob` dtype difference.

## Wave 5 SQL driver measurements

The before run used the existing release extension, 1M rows, `--repeat 1`, 100 updated rows
on SQLite and 10,000 on PostGIS. Python used the pinned reference data libraries from an
isolated temporary environment, with a fresh `MRSM_ROOT_DIR`, empty `MRSM_PLUGINS_DIR`, and
`w5sql_bench` pipe keys. Rust builds from the other wave agents were active during these runs,
so these are single-run observations under shared CPU load. Both modes returned identical
1.5M-row frames and row counts.

| Flavor / step | Python before | Rust before |
|---|---:|---:|
| SQLite new table | 74.886 s | 7.581 s |
| SQLite half overlap | 37.270 s | 7.440 s |
| SQLite update | 27.769 s | 25.977 s |
| SQLite read | 14.212 s | 7.106 s |
| SQLite sync time | 4 ms | 3 ms |
| SQLite row count | 10 ms | 8 ms |
| PostGIS new table | 31.804 s | 6.801 s |
| PostGIS half overlap | 14.527 s | 11.239 s |
| PostGIS update | 14.240 s | 10.762 s |
| PostGIS read | 10.126 s | 2.447 s |
| PostGIS sync time | 3 ms | 5 ms |
| PostGIS row count | 72 ms | 55 ms |

The SQL façade now passes cached column metadata into sync, shares its data-read implementation
with the instance layer, and uses the driver's raw-document path for SQLite `get_docs`.

After the Arrow builder path, metadata reuse, and SQLite staging expression index, with the
same row counts and shared-load caveat:

| Flavor / step | Python after | Rust after |
|---|---:|---:|
| SQLite new table | 100.244 s | 5.886 s |
| SQLite half overlap | 41.767 s | 9.360 s |
| SQLite update | 23.408 s | 15.551 s |
| SQLite read | 9.075 s | 7.175 s |
| SQLite sync time | 2 ms | 6 ms |
| SQLite row count | 7 ms | 13 ms |
| PostGIS new table | 25.007 s | 5.515 s |
| PostGIS half overlap | 13.571 s | 6.937 s |
| PostGIS update | 13.628 s | 6.248 s |
| PostGIS read | 9.180 s | 1.782 s |
| PostGIS sync time | 3 ms | 3 ms |
| PostGIS row count | 77 ms | 55 ms |

Both final 1.5M-row frames match. The SQLite update trace attributes 1.518 s to filtering and
13.995 s to executing the SQL update. An intermediate run without the staging expression
index took 79.525 s for the Rust update (Python 28.180 s); the final implementation adds an
index matching the existing COALESCE comparisons on the temporary patch table. These runs
are not a controlled attribution study: concurrent builds varied CPU load considerably.

A separate PostGIS test of 20 warm one-row updates (integer primary key and value) measured
median Python / Rust sync times of 122.76 / 46.36 ms, and uncached column-type lookups of
5.15 / 4.50 ms. This is a new small-write scenario, not a reproduction of the earlier 89 ms
measurement. The remaining millisecond-scale sync-time / row-count overhead means the
single-run benchmark does **not** establish v4 parity on every step, despite faster large
sync and read operations. All runs used isolated roots, empty plugin directories, the
disposable PostGIS port 5449, and `w5sql_` pipe keys.
