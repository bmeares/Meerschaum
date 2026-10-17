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

## Bug and behavior review ledger

This ledger supplements the historical discrepancy table below. Keep inherited v4 bugs,
v5 regressions, implementation gaps and test-environment issues distinct. An intentional
departure from observable v4 behavior requires maintainer review, including any reference
fixture or expectation rewrite. Do not treat a proposed resolution as approved or weaken
parity assertions to make a failure disappear. Preserve evidence for both implementations
and record the decision here. Existing historical resolutions are not reclassified by this table.

| ID | Classification and trigger | Evidence / current behavior | Proposed or implemented handling | Review / status |
|---|---|---|---|---|
| V5-001 | v5 initialization race: independent cold connectors first write engine state | A table becomes visible before its unique index; PostgreSQL `ON CONFLICT` fails. Distinct targets can also race creating their common schema. `internal_initialization` tests reproduce both cases | Coordinate cold PostgreSQL initialization, recover missing indexes, fail uncached on initialization failure; skip ordinary indexes already present | Compatibility repair implemented; SQL suite (940 tests) and focused engine regressions pass. CockroachDB coordination remains open |
| V5-002 | v5 regression introduced while repairing V5-001: a second process writes existing engine tables | Repeated ordinary-index creation caused run persistence to fail while sync still reported success; browser polling saw only the first run | Skip existing ordinary indexes during internal readiness checks; test independent SQLite connectors and successive runs/nodes/events | Fixed in working tree; all 11 browser tests pass |
| V5-003 | v5 pandas boundary: old and newly cast string columns have different pandas string dtypes | pandas 3 inferred `str` versus nullable `string`, or differing string storage backends, merges to `object` in Python; Rust erased the distinction and restored `string` | Restore dtype based on original pandas metadata and retain `pd.NA` when the merge becomes object; strict frame and sentinel assertions cover null, non-null and empty deltas | Fixed; expanded core suites pass with pandas 2.3.3 and 3.0.3 in both modes (194 passed, one existing xfail per run); assertions retained |
| V5-004 | v5 pandas boundary: explicitly `object` string/null columns | With pandas 3, Python `normalize_nulls` may infer `str`, while Rust preserves `object`. Object nulls may also return `None` where Python uses `pd.NA`; pandas 2 comparisons can warn without failing | Restore `pd.NA` for ordinary string-bearing object merges, confirmed against released v4.1.4 with the same pandas version; exclude special JSON/Decimal/UUID/bytes handling and avoid copying input columns | Object inference now follows both complete merge inputs and pandas inference settings, including partial dtype maps and empty deltas; all four pandas 2.3.3/3.0.3 × Python/Rust core-and-boundary runs pass (300 passed, one existing xfail per run) |
| V5-005 | v5 sync-hook output omits v4 result lines | Python `actions/sync.py` prints each pre/post-hook success tuple; the Rust engine previously only logged failed hook results | An optional engine reporter queues ordered results for the native CLI formatter, including continuous/live paths; manifests without hooks still avoid starting Python | Fixed; success, failure, default-result suppression and exceptions compared with released v4 CLI output; hook invocation semantics retained |
| V5-006 | v5 `show partitions` labels datetime intervals as integer axes | Timestamp range partitions with a one-day interval displayed `1,440 (int axis)` because datetime minutes and integer-axis units shared one representation | A typed native metadata accessor preserves duration seconds for the CLI, using the same query implementation; public SQL metadata and Python timedelta conversion retain their contracts | Fixed; unit tests and a disposable PostgreSQL probe assert `1 day` versus `1,440 (int axis)` without rewriting existing metadata expectations |
| V5-007 | v5 native dataframe binding accepts Polars `Object` columns but loses non-null object values | Polars exports these objects as opaque `fixed_size_binary[8]`; Rust returned Binary values instead of the objects. Released v4.1.4 public `filter_unseen_df` rejects Polars DataFrames | Import supported Object scalars through the kernel cell model, retaining original Object-column metadata; reject unsupported arbitrary objects before Arrow export | **Maintainer approved supported Polars Object values on 2026-10-15.** Strings, UUIDs, bytes and Decimals are retained within the existing native Polars input path; arbitrary objects are rejected. This broader input contract is an approved v5 change; all four pandas 2.3.3/3.0.3 × Python/Rust runs pass (300 passed, one existing xfail per run) |
| V5-008 | v5 `filter_unseen_df` null sentinels in object UUID/bytes delta columns | Released v4.1.4 with pandas 3.0.3 returns `pd.NA` for these nulls; the Rust boundary returns `None`. Synthetic UUID and byte-string probes reproduce the mismatch. JSON `None` and Decimal NaN controls agree | Add focused special-type sentinel restoration and strict identity tests without changing JSON/Decimal handling | Repair restores the reference sentinel using the first valid special value in the delta, including mixed UUID/bytes and ordinary-string values; all four pandas 2.3.3/3.0.3 × Python/Rust core-and-boundary runs pass (300 passed, one existing xfail per run) |
| V5-009 | v5 JSON null-only delta sentinels | Released v4 normalization produces `pd.NA` for an all-null JSON input, but serialization of a non-null JSON input preserves `None` even when its final delta is null-only | Restore sentinels using the full input columns, with JSON/UUID/bytes/Decimal controls and partial dtype maps | Fixed; all four pandas 2.3.3/3.0.3 × Python/Rust runs pass (300 passed, one existing xfail per run) |
| V5-010 | v5 plugin pages registered during Dash initialization disappear | Page collection ran before `@dash_plugin` functions; functions declaring `@web_page` inside initialization produced apps with no routed pages. Private host replays exposed the same trigger in both deployments | Initialize plugin callbacks once, then collect and attach Dash pages and refresh HTML page routes; preserve page-only plugins, authentication and session stores | Fixed; all nine API-host tests pass, including dynamic Dash/HTML registration, shared sessions and idempotent initialization. Private page-route replay confirms the repair |
| V5-011 | Native password verification rejects supported legacy digests and mishandles excessive iteration counts | Moving the public password facade unconditionally to Rust exposed unsupported digest verification and differing overflow exceptions; released v4.1.4 verifies the legacy fixtures and rejects excessive rounds before computing | Support the 16 digests accepted by the released-reference runtime with Rust implementations and validate Python integer bounds before native PBKDF2; retain malformed-hash warnings and fixed reference vectors | Compatibility repair implemented. Final password facade tests pass in both modes; 30 invalid-digest cases exactly match installed v4 exception classes/messages and warnings. Unsupported-name lookup uses stdlib error metadata only; all password hashing stays Rust. Installed-package evidence is in the build work order |
| V4-001 | Inherited Python SQL engine creation mutates shared static defaults | Released v4.1.4 and the legacy branch bind `_create_engine_args` directly to `STATIC_CONFIG` and update it in place. A released-reference SQLite `create_engine(pool_size=3, max_overflow=4)` probe changed defaults for eleven SQL flavors without connecting to a database. This also makes `test_static_config_matches_python` order-dependent when collected after connector tests | Deep-copy flavor defaults before applying connector overrides. Tests cover precedence, cross-flavor isolation, nested dialect mutations and unchanged static configuration | **Maintainer approved the isolation fix on 2026-10-15. Implemented.** Connector overrides no longer affect other connectors. Original reference assertions retained; the regression followed by the static-config comparison passes |
| V4-002 | Dependency-version prechecks bypass `MRSM_NO_AUTO_INSTALL` | Installed released v4.1.4 and v5 both disable the local `attempt_import` installation branch, but `is_installed` calls `need_update`, which directly calls `pip_install` for missing version parsers; non-dry-run `pip_install` has no environment guard | Honor the opt-out for implicit dependency checks while preserving explicit SDK installation semantics; define and compare the missing-parser result before changing it | Confirmed inherited bug, not introduced by Wave 11. Offline image hooks intercepted the `semver` request; no download was performed. The direct installation path is established by source inspection. **Fixed 2026-10-16 (wave 14 K) as an intentional, documented behavior change (maintainer decision; listed in the migration guide).** One guard, `meerschaum.utils.packages._auto_install_allowed()`, reads `MRSM_NO_AUTO_INSTALL`. *Implicit* = reached from an import or precheck (`attempt_import`, `manually_import_module`, `is_installed` -> `need_update`, `ensure_readline`; these pass `pip_install(_implicit=True)`, which returns `False` without taking the lock or running an installer when the variable is set); *explicit* = `pip_install` called directly, `mrsm install packages`, `install required`, which keep working but, with the variable set, never bootstrap `uv`, `pip`, `wheel`, `setuptools` or `get-pip.py` and skip the PyPI `pip` self-update check. `need_update` also ignores `check_pypi` when disabled. Without an importable `semver` the comparison uses `_core.version_satisfies` (PEP 440, `pep440_rs`), then `packaging`, then reports 'no update needed' (`dprint` under debug). `init_venv` omits `uv venv --seed`. The API package no longer imports `uv` at import time (`shutil.which('uv')` only). With the variable unset every previous install attempt still happens (`tests/utils/test_packages.py`, `test_unset_variable_keeps_the_v4_install_attempts`) |
| PERF-001 | Serial per-pipe metadata reads in `show partitions` and related actions | CLI waits for each pipe's metadata before starting the next; wall time accumulates network latency | Bounded concurrent reads for capable native SQL connectors; default four workers capped by pool capacity, explicit single-worker mode and serial fallback | Implemented for native `show partitions`, `show rowcounts` and `show sizes`; overlap/bounds/error tests and exact one-versus-four-worker output comparisons pass |
| GAP-001 | Engine state targets use the instance schema rather than the specified internal schema | Moving fixed targets from multiple instance schemas into one internal schema can merge previously separate state | Design instance/schema namespacing and migration conflict handling before changing placement | Open design work; no migration or behavior change approved |
| GAP-002 | Cross-instance cursor cleanup on pipe removal | Current cleanup removes state on the local instance; dependent cursor rows on other instances remain | Establish discovery and cleanup semantics for remote state and unreachable instances | Open; no intentional deviation approved |
| GAP-003 | Compose execution still depends on Python core logic | Native dispatch now covers `ps`, `logs`, `down` and non-dry `up`, with project-scoped jobs, worker environments, temporary pipe sync and ownership-aware cleanup | Six execution tests exercise real jobs, plugin environment/config, SQLite sync, shared data protection and project reuse; existing Compose parity tests retained | Lifecycle migration implemented. Fresh plugin roots still reach Python-backed `install required` / `setup plugins`; existing non-local executor gaps remain. End-to-end D14 independence is **not** complete |
| GAP-004 | Installed base wheel still needs undeclared Python dependencies for public operations | Offline, read-only slim image with only the two Meerschaum distributions passes native CLI/config lookup/password checks, but public config writes reach missing PyYAML and dict-based Pipe sync reaches SQLAlchemy and typing/formatting dependencies | Port the remaining config, Pipe and SDK dependencies through native bindings; retain an offline public-operation smoke that rejects install requests | Open D14/D20 release blocker. Meeting the approved 250 MB image budget does not establish dependency-free facade operation; clean base-wheel checks reproduce the failures, and full-image Pipe/API-host startup attempts an implicit `semver>=3.0.2` install. Final sizes pass (200.38/844.05 MB); functional failures remain open in the build work order |
| TEST-001 | Generic Arrow roundtrip fixture collides with the registered GeoArrow extension | Combined Rust-mode collection first registers `geoarrow.wkb`; PyArrow then promotes the generic fixture's raw binary field during C-stream import, failing three exact-schema comparisons. A pure-PyArrow control reproduces this without Rust | Give opaque test metadata a deliberately unregistered synthetic name; retain exact schema/data assertions and the actual GeoArrow integration test | Test isolation repaired; same-process geometry plus core tests pass (195 passed, one existing xfail), and Python-mode core passes (194 passed, one existing xfail). Final combined Rust-mode suite passes 1,755 tests, with 21 skips and one existing xfail. No product behavior or v4 reference expectation changed |
| ENV-001 | Python geometry test order dependence (`test_polars_geometry_uses_geoarrow_wkb`) | Root cause (2026-10-16): `utils/dataframe.py::to_polars` registered the GeoArrow extension with a bare `from geoarrow.types.type_pyarrow import ...` inside `try/except: pass`; the import only succeeds while the `mrsm` venv is on `sys.path`, and `mrsm.entry()` deactivates every venv on exit (`_internal/entry.py`), so after `tests/test_compose.py::test_compose_stamps_the_project_name_onto_started_jobs` the registration silently failed and Arrow exported `large_binary` | Fixed: the registration goes through `attempt_import('geoarrow.types.type_pyarrow', lazy=False, warn=False, install=False)`, which activates the venv around the import; the assertion is unchanged | **Closed**. Not a Polars/PyArrow/geoarrow version issue (the earlier note was wrong); CI keeps the check |

The current execution plan is in the [hardening work order](../../docs/design/v5-build.md#active-v5-hardening-work-order-2026-10-15).

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
| 11 | `tests/utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order` | `IndexError` on `native_results[0]` | The test monkeypatches v4's private `_filter_unseen_df_with_polars` and `_POLARS_FILTER_MIN_ROWS` and asserts the private helper was called; the Rust binding never calls v4 internals (D12) | **Resolved (wave 7):** the private-helper assertion runs only when the Python path is active; the result assertion runs in both modes. No deselect needed |
| 12 | (performance, all tests) | `get_config` cost 78 µs/call vs 17 µs in Python (wave 8: up to ~0.27 ms on the façade sync path, ~35 calls per chunk) | Each call converted the whole config dictionary, then the requested top-level subtree | **Closed** (wave 9): a read-only fast path in `core_api.rs::fast_lookup`. When every key is already in the live `meerschaum.config.config` dict and (with `substitute=True`) the first key's subtree holds no `MRSM{` string (v4's own `'MRSM{' in json.dumps(config[keys[0]])` test, done by walking the dicts in Rust), the full lookup would only return the dict's own object, so the binding walks the dict and returns it. Nothing is cached, so `set_config`, `replace_config`, environment patches, `write_config` + reload and callers mutating returned dicts are seen on the next call (`tests/test_w9_get_config.py`, both modes compared). Every other call (missing keys, symlinks to substitute, defaults, the `_symlinks` key, non-plain values) takes the unchanged full path. 5 to 9 µs per call vs 18 to 26 µs in Python on the same host (PERF.md, "Wave 9") |
| 13 | `test_rust_mode_falls_through_when_symbol_missing` (`crates/meerschaum-py/tests/test_core.py`) | asserted that some gated symbol is unbound | every gated feature is bound now | Test rewritten: delete one symbol with `monkeypatch` to prove the fall-through; added `test_every_gated_feature_is_bound` and `test_rust_mode_matches_python` |
| 14 | `test_deduplicate_without_instance_method`, `test_filter_existing_all_new`, `test_mixed_timezone_aware_and_naive`, `test_no_null_indices` (regressions of fix 10) | downstream `TypeError: Cannot use .astype to convert from timezone-naive dtype to timezone-aware dtype` | Fix 10 kept the input dtype for `are_dtypes_equal` targets, but `datetime64[us]` is "equal" to `datetime64[us, UTC]` and v4 coerces datetime columns explicitly | Boundary: datetime targets always take `to_pandas_dtype(typ)` |
| 15 | `test_sync_dask_dataframe` | `NotImplementedError: Divisions calculation failed for non-numeric column 'dt'` | The binding computes Dask inputs and rebuilds them with `dd.from_pandas`, which sorts the index by default | Boundary: `from_pandas(..., sort=False)`. Open: v4 keeps Dask frames lazy; the kernel has no partitioned path (Dask is deleted in 5.0, D20) |
| 16 | (server report; `test_out_of_range_floats_do_not_saturate`, `crates/meerschaum-kernel/tests/big_floats.rs`) | `1e20` in a float column came back as `9.22e18`; `enforce_dtypes(..., {'a': 'int'})` on `[1e20, -1e20, 2**63, 2**64]` returned saturated `int64[pyarrow]` values | `as i64` saturates: `docs::convert_dtypes` made integral floats `Int64` without a range check, `pandas_astype` cast out-of-range floats / decimals to int | Kernel: `cast::float_fits_i64` (`[-2**63, 2**63)`, like pandas' `astype(int) == arr` check and Arrow's safe cast); out-of-range values stay float / the cast fails as in v4 |
| 17 | (found by `bench/bench_boundary.py`, not in the suite; `crates/meerschaum-kernel/tests/w9_merge_dtypes.rs`) | `filter_unseen_df(..., dtypes={'blob': 'bytes'})` on `object` bytes columns returns `binary[pyarrow]`; v4 returns `object` | v4 casts only the new frame to `binary[pyarrow]`; its merge with the old frame's `object` column yields `object` | **Closed in the kernel** (wave 9): the output dtype of every column follows pandas' merge rules (table below); a native/`object` pair merges to an `object` column. **Façade fixed too**: `kernel_api.rs::filter_unseen_df` keeps the `object` hint for delta columns the kernel leaves as `object` holding bytes or dates, so the `bytes` / `date` hint no longer recasts them (`tests/test_w9_filter_unseen_dtypes.py`, rust mode against python mode) |
| 18 | (found while checking 16; `crates/meerschaum-kernel/tests/w9_merge_dtypes.rs`) | `filter_unseen_df` on `object` columns of floats, or with `dtypes={'a': 'numeric'}` on float columns, returns `object`; v4 returns `float64` | v4's `normalize_nulls` calls `infer_objects()` (and every `Series.apply` of the special-column steps infers like it); the kernel did not | **Closed** (wave 9): `merge_dtype::infer_objects` reproduces pandas 3's inference on the whole old and new columns before the merge, and after each `apply` step (JSON, numeric, UUID, bytes); native columns come out native, merge-to-`object` columns come out as object columns. An `object` column cast to an Arrow integer type now truncates floats and decimals toward zero like v4 (a NumPy float column still raises) |

### Rows 17 and 18: the dtype of a `filter_unseen_df` result column

Enumerated against v4.1.4 (pandas 3.0.0, pyarrow 23) by `crates/meerschaum-kernel/tests/fixtures/gen_w9_merge_dtypes.py`
(285 non-raising old/new/hint combinations, `w9_merge_dtypes.json`; v4 raises `ValueError` for another 46 % of the full
33 x 33 x 12 matrix, e.g. an `int64` column against a `str` one, and the kernel does not raise there). The result column has the
dtype pandas gives `merge(new, old, how='left')` after `normalize_nulls`' `infer_objects()`:

| Step | v4 | Kernel (`merge_dtype.rs`, `diff.rs`) |
|---|---|---|
| `infer_objects()` of an `object` column | `int`s: `int64` (`float64` with `None` / `NaN`; `object` with `pd.NA`); `float`s: `float64` (`object` with `pd.NA`); `bool`s: `bool` (`object` with any null); `str`s: `str` (nulls allowed); datetimes of one timezone: `datetime64[us(, tz)]` (`object` with `pd.NA`, or mixed timezones); bytes, decimals, `date`s, UUIDs, dicts, all-null, empty and mixed columns stay `object` | `infer_objects` (same table), computed on the whole column, not on the delta rows |
| merge of two columns | equal dtypes keep it; numeric pairs keep the new (left) frame's dtype; datetime and string pairs of one class keep the left's; every other pair is `object`, or raises | `merge_kind`: equal keeps; numeric/numeric, datetime/datetime and the string/string, bool/bool pairs keep the new column's kind; any other pair is `object` (it never raises) |
| special-column `apply` (JSON, numeric, UUID, bytes) | `Series.apply` infers like `infer_objects()` (a `numeric` column of booleans comes back `bool`, of strings `str`) | `applied_column` |
| arrow vs NumPy backends | `bool[pyarrow]` or `string[pyarrow]` (what a hint casts the new frame to) against a NumPy `bool` / `str` (what `infer_objects()` makes) merges to `object` | modeled: columns that came out of `infer_objects()` are NumPy-backed, native columns Arrow-backed |

Not modeled (the kernel does not distinguish pandas backends of native columns; every one involves a NumPy-backed or nullable *old*
column, which an instance never returns): `int64[pyarrow]` against `float64` (raises), `Int64` against `double[pyarrow]` (`object`),
`str` against `string` (`object`), `bool` (NumPy) against `bool[pyarrow]` when the old frame is a native NumPy column, and
nanosecond datetimes (`datetime64[ns]` stays `ns` in v4, the kernel's values carry no unit and give `us`).

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
| 19 | `tests/test_pipe_attributes.py::test_sql_attribute_errors_are_not_unregistered`, `::test_sql_no_row_is_still_empty` | under rust, the mocked `exec` is never called (no `RuntimeError`; a real row instead of `{}`) | both tests monkeypatch `SQLConnector.exec` (SQLAlchemy) to fake the metadata query; the routed `get_pipe_attributes` reads through the Rust driver | **Skipped under rust (wave 7, `python_sql_only` marker; no deselect needed).** The behavior they protect (a failed metadata read raises instead of reporting "unregistered") is covered with a real failure by `tests/test_facade_sql.py::test_attribute_read_errors_propagate` (Rust errors surface as `RuntimeError`) |
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

## Wave 7 (task K): `_core.Pipe` is the only parameter store

`pipe._attributes['parameters']` under `MRSM_CORE=rust` is no longer a Python mirror pushed into Rust
after every change. `meerschaum/core/Pipe/_live_parameters.py::RawDict` / `RawList` are
**read-through caches** of one node of the Rust raw map, addressed by path:

- every write goes to `_core.Pipe` first (`set_path` / `del_path` / `set_raw_parameters`);
- every read compares the node's generation with `_core.Pipe.generation` (a mutation counter bumped
  by each Rust-side change) and reloads the node with `snapshot(path)` when they differ, so a held
  view observes writes made through other views, `update_parameters`, the setters, a metadata load
  or `_core_pipe.set_path` directly. Reloading keeps the identity of child containers whose path
  still holds a container of the same kind (so `pipe._attributes['parameters']['columns']` stays the
  same object across reads and writes);
- the root view is one object per pipe (`pipe._raw_root`); assigning `_attributes['parameters']`
  stores the value in Rust and keeps the root;
- the views stay `dict` / `list` subclasses (`isinstance`, `json.dumps`, `**`, `dict()`, `deepcopy`,
  pickle give plain containers), as v4 code and corpora rely on them;
- the resolved-parameters memo of `get_parameters` is keyed on the Rust generation, not on the
  identity of a Python dict. Parameters that contain a cross-pipe `{{ Pipe(...) }}` template are
  **not** memoized and re-resolve on every access (see row 36).

| # | Deviation / change | Reason |
|---|---|---|
| 35 | Rows 32 and 34 are closed: reads come from Rust; `Pipe.target` computes the name with the Rust rules (`_core.Pipe.target(flavor, resolved_params)`, `target_legacy()`), `__eq__` / `__hash__` use the core (`__eq__` still duck typed: only another façade pipe with a `_core_pipe` is compared in Rust), `__str__` / non-ANSI `__repr__` use the core `Display` (`str(pipe)` 490 us -> 1.3 us) | single store; the Python `target` keeps its side effects (warning on truncation, write-back of a non-default `target_name`, memoization rules) because they act on façade state |
| 36 | Parameters with a cross-pipe `{{ Pipe(...) }}` template re-resolve on every access (**superseded by row 39**, wave 9 U) | v4.1.4 memoizes `get_parameters` by raw identity, which keeps a referenced pipe's old `columns` forever; re-resolving matches the "per access" rule of the task and is the fresher behavior. Cost: pipes with such templates pay the Python resolution per access (plain pipes keep the memo) |
| 37 | A held `RawDict` / `RawList` addresses its node by **path**: after `lst.insert(0, x)` a view of the old first element now addresses the new first element | v4 held the Python object; unavoidable when Rust owns the data. Dict keys are stable, so nothing changes for the usual `pipe.parameters['a']['b'] = ...` |
| 38 | `SqlConnector.sync_pipe` receives the **raw** parameters (`ParamsView::Raw`); `meerschaum_sql::SqlConnector::sync_pipe_mut_cached` resolves cross-pipe templates through the instance on every call (`prepare_pipe`), syncs the resolved copy and applies only the changed raw top-level keys (`autoincrement`, numeric / special `dtypes`) to the caller's pipe | wave 6 handed the binding Python-resolved parameters, so the resolver never ran and the templates were a snapshot taken by Python |

Hot-path cost, rust mode (release extension, `sql:memory` pipe with 2 columns, 3 dtypes; median of
min-of-5, microseconds; baseline = wave 6): `pipe.columns` 85 -> 108 (python mode 42), `pipe.dtypes`
737 -> 726, `pipe.tags` 85 -> 82, `get_parameters()` 78 -> 64, `str(pipe)` 1500 -> 1.3,
`hash(pipe)` 3.3 -> 1.2, a nested write through `pipe.parameters[...]` 948 -> 1368 (a write reloads the
touched path from Rust before the next read). Everything is inside the 2x budget.

Tests: `tests/test_core_pipe.py` (+5: views read from Rust, dict/list duck typing, memo follows the
generation, target / str / eq / hash, cross-pipe template re-resolution),
`crates/meerschaum-sql/tests/w6b_templates.rs::sync_pipe_mut_resolves_templates` (all flavors).

### D14 runway: full suite under `MRSM_CORE=rust`

`PYTHONPATH=$(pwd) MRSM_CORE=rust MRSM_TEST_FLAVORS=<flavor> python -m pytest tests/` with the
ignores `tests/test_jobs.py`, `test_mcp.py`, `test_listen_shutdown.py`, `test_api_config.py`,
`tests/api_host` and `tests/console` (task J's), `MRSM_PLUGINS_DIR=./tests/plugins`, a scratch
`MRSM_ROOT_DIR`. See the report below for the per-flavor results. The only failures on every flavor are
the three tests that monkeypatch Python internals (rows 11 and 19): `test_pipe_attributes.py::
test_sql_attribute_errors_are_not_unregistered`, `::test_sql_no_row_is_still_empty` and
`utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order`. Running with
`MRSM_PLUGINS_DIR` pointing at an empty directory makes `test_maintenance.py`, `test_verify.py`,
`test_sync_engine` and `test_worker.py` fail (the `stress` test plugin is missing): that is a harness
mistake, not a façade failure.

Results (wave 7, release extension rebuilt, 636 passed per flavor; python mode on sqlite 621 passed,
21 skipped): `rust` on sqlite, postgis, timescaledb, mariadb, mysql and mssql each ended with the same
3 failures above and nothing else (`3 failed, 636 passed, 3 skipped` on the six server and file flavors).

### D14 removal plan (what goes away with the Python core, and what replaces it)

| Python module(s) | Replaced by (façade call) | State |
|---|---|---|
| `connectors/sql/_pipes.py`, `_users.py`, `_plugins.py`, `_compress.py`, `_maintenance.py`, `_partition.py` (all `_rust_method` guards) | `_core.SqlConnector.<same name>` (all bound) | Bound and exercised by the whole suite. Delete the Python bodies, keep the guard-free delegations |
| `connectors/sql/_pipes.py::sync_pipe_inplace`, `get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype`, `create_pipe_table_from_df` | `_core.sync_pipe_inplace(rust_connector, pipe, params, begin, end, check_existing)` (`src/d14_api.rs`; gate feature `sync_pipe_inplace`) | **Bound and routed** (wave 8). `get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype`, `create_pipe_table_from_df` are only called by the Python `sync_pipe`/`inplace` bodies and go with them |
| `connectors/sql/_sql.py` (`read`, `exec`, `value`, `to_sql`), `_create_engine.py`, `_uri.py`, `tables` | stay (D16: SQLAlchemy for user SQL); `_uri.py` could use the Rust URI parser | stays |
| `connectors/sql/_fetch.py` (`fetch`, `get_pipe_metadef`) | `_core.get_pipe_metadef(rust_connector, pipe, params, begin, end, check_existing, sync_time, parent)` (gate feature `get_pipe_metadef`). `fetch` keeps `self.read(meta_def, as_iterator=True)`: `read` stays SQLAlchemy (D16), so there is nothing left to bind | **`get_pipe_metadef` bound and routed** (wave 8; the experimental `join_fetch` shape stays Python). `fetch` is a two-line wrapper over the routed metadef |
| `core/Pipe/_attributes.py` (`get_parameters`, `parameters`, `columns`, `dtypes`, `indices`, `get_precision`, `update_parameters`, `target`) | `_core.Pipe` keys / raw parameters / `target` (bound); `_core.get_parameters(core_pipe, pipe)` (gate feature `get_parameters`) resolves references and `{{ Pipe() }}` through `_core.Pipe.resolved_parameters` and returns the `_symlinks` map; `MRSM{}` symlinks resolve in the same call against the live config (`config_api::substitute_parameters`) | **Routed** (wave 8, top-level call under `MRSM_CORE=rust`); `MRSM{}` in Rust: **closed** (wave 15, O) |
| `core/Pipe/_cache.py`, `_edit.py`, `_register.py`, `_delete.py`, `_drop.py`, `_clear.py`, `_index.py`, `_copy.py`, `_show.py`, `_bootstrap.py` | thin: instance calls (bound) plus Python-side caches and interactive prompts | Caches move into Rust with the engine; prompts stay SDK |
| `core/Pipe/_sync.py`, `_fetch.py`, `_verify.py`, `_deduplicate.py`, `_data.py`, `_dtypes.py`, `_compress.py`, `_maintenance.py` | `meerschaum-engine` / `meerschaum-worker` (sync, verify, fetch, filter_existing, get_chunk_bounds) and the kernel; `Pipe.verify` → `_core.verify_pipe(pipe, connectors, ...)` (`src/verify_api.rs`, gate feature `verify_pipe`); `Pipe.sync` → `_core.sync_pipe` (`src/sync_api.rs`, gate feature `sync_pipe`; routed in waves 13-15, task L); `Pipe.get_data` / `filter_existing` / `enforce_dtypes` / `fetch` / `deduplicate` → `_core.pipe_*` (`src/pipe_methods_api.rs`, wave 15, L2) | `Pipe.get_chunk_interval` and `get_chunk_bounds` are routed (wave 8). **`verify` is routed** (wave 9): `_core.verify_pipe` runs `meerschaum_cli::native::verify::verify_pipe_with` (the CLI's verify, every chunk synced by the Rust engine) on a registered-only, worker-less façade `Session` holding the `_core.SqlConnector`s of the pipe's instance and SQL source (`Session::register_sql`), so ad-hoc connectors built from Python work and the config registry is never read for connectors. Output goes to `sys.stdout` / `sys.stderr`; afterwards the Python pipe drops its table caches (`_invalidate_cache(hard=True)`, `_attributes_sync_time`). **Stays Python** (`_verify.py::_rust_verify_connectors` returns `None`): `debug=True`; any `**kwargs` other than `chunksize` / `check_existing` (they go to `pipe.sync()`, e.g. `force`, `retries`, `_inplace`, plugin arguments); a source that is not `sql:` (plugins, `@make_connector` connectors, API/Valkey sources: they need the worker or Python objects); an instance or source without a Rust connector (`mssql`, `oracle`, `duckdb`, in-memory SQLite, non-SQL instances); parameters with `reference` / `references`, any `MRSM{` symlink, a `{{ }}` template in `columns` / `dtypes` / `verify` / `precision`, or a cross-instance `{{ Pipe(..., instance=...) }}` template (the verification reads those before the engine resolves them). Fixed on the way (CLI too): remote sync times and row-counts are asked of the source connector, not the instance (v4 `self.connector if remote`); `check_rowcounts_only` of a cross-connector pipe reported 0 out-of-sync chunks before. Parity: `tests/test_w9_verify.py` (sqlite and postgis schema `w9q`, in-place and cross-connector, nine verify variants each: tuples, chunk bounds and rows equal in both modes). The rows below (wave 15, L2) list what each module loses in wave 16 and what stays. The `verify` exceptions above were closed in wave 15 (task L: `debug`, forwarded keywords, plugin / API / Valkey sources, references, templates and `MRSM{}` symlinks run natively). | **Wave 15, L2: the instance-wrapping methods of `_data.py`, `_dtypes.py`, `_fetch.py`, `_deduplicate.py` and `_sync.py` are routed; the rows below list what each module loses in wave 16 and what stays.**
| `core/Pipe/_data.py` | `_core.pipe_get_data`, `pipe_get_data_chunks` (`PipeChunks`), `pipe_get_backtrack_data`, `pipe_get_rowcount`; chunk interval / bounds were routed in wave 8 (`src/pipe_methods.rs`, `src/pipe_methods_api.rs`) | **Routed** (wave 15, L2). Deleted in wave 16: the bodies of `get_data` (incl. `_sort_df`, the Dask path), `_get_data_as_iterator`, `get_backtrack_data`, `get_rowcount`, and the Python `get_chunk_interval` / `get_chunk_bounds` / `_scale_delta_to_int_axis` bodies. Stays as façade: signatures and docstrings, `_rust_context`, `_iterate_chunks`, `parse_date_bounds` (text bounds through `dateutil`; the coercion rules exist in `engine::sync::parse_date_bound`), `get_doc` / `get_docs` / `get_value`, `get_size`, `get_chunk_bounds_batches`. Nothing stays on the v4 body but `as_dask` (a `TypeError`, D20) and an unbuildable instance (neutral v4 result) |
| `core/Pipe/_dtypes.py` | `_core.pipe_enforce_dtypes` (frames through the kernel binding with the pipe's context; documents through `pipe_enforce_dtypes` of the kernel), `pipe_infer_dtypes` | **Routed** (wave 15, L2). Deleted in wave 16: both bodies. Stays: the signatures and the `persist` tail of `infer_dtypes` (`self.dtypes = ...; self.edit()`). Input is the boundary of `Pipe.sync` (a `TypeError` naming the accepted types otherwise) |
| `core/Pipe/_fetch.py` | `_core.pipe_fetch` (`determine_begin`, the registry's source: `sql:` metadefinition, `api:`, `valkey:`, `plugin:` / custom through the worker), `pipe_get_backtrack_interval` (`Pipe::get_backtrack_interval`) | **Routed** (wave 15, L2). Deleted in wave 16: `fetch`'s body, `_determine_begin`, `get_backtrack_interval`'s body. `sync_chunks` / `chunk_hook` are native (module notes above, W15L2-003) |
| `core/Pipe/_deduplicate.py` | `_core.pipe_deduplicate` (the instance's `deduplicate_pipe`, then the generic chunk loop of `pipe_methods.rs` for instances without one) | **Routed** (wave 15, L2). Deleted in wave 16: the whole body (thread pool, `process_chunk_bounds`, retry pass). Stays: signature and docstring |
| `core/Pipe/_sync.py` (`get_sync_time`, `filter_existing`, `_filter_existing_polars`, `_get_delta_bounds`, `_is_polars_fallback_dtype`, `_polars_dtypes_reconcilable`, `_has_polars_object_cols`) | `_core.pipe_get_sync_time`, `pipe_filter_existing` (kernel `filter_existing` over the Rust `get_data`) | **Routed** (wave 15, L2). Deleted in wave 16: all of them but the two public signatures. `remote=True` reads go to any source. `exists`, `_get_chunk_label`, `get_num_workers` (caches and labels) stay as façade (the rest of `_sync.py` is task L's) |
| `connectors/api/` (`APIConnector`: `_pipes.py`, `_users.py`, `_plugins.py`, `_tokens.py`, `_jobs.py`, `_actions.py`, `_login.py`, `_misc.py`, `_fetch.py`, `_request.py`) | the Rust client `meerschaum_api::APIConnector` (the native engine and `Pipe` methods already use it for `api:` instances: `facade.rs`) | **Remaining, wave 16 (agent S)**: the SDK class still runs its own `requests` code. Methods to bind to the client: pipes `register_pipe` `edit_pipe` `fetch_pipes_keys` `sync_pipe` `delete_pipe` `delete_pipe_cache` `get_pipe_data` `get_pipe_docs` `get_pipe_id` `get_pipe_attributes` `get_sync_time` `pipe_exists` `get_pipe_rowcount` `drop_pipe` `clear_pipe` `get_pipe_columns_types` `get_pipe_columns_indices` `get_pipe_size` `compress_pipe` `decompress_pipe` `vacuum_pipe` `partition_pipe` `analyze_pipe` `get_pipe_index_names` `create_metadata`; users `get_users` `edit_user` `register_user` `get_user_id` `delete_user` `get_user_attributes` `get_user_password_hash` `get_user_type`; plugins `register_plugin` `install_plugin` `get_plugins` `get_plugin_attributes` `get_plugin_version` `delete_plugin`; tokens `register_token` `get_token_model` `get_tokens` `edit_token` `invalidate_token` `get_token_scopes` `token_exists` `delete_token`; jobs `get_jobs` `get_job` `get_job_metadata` `get_job_properties` `get_job_status` `get_job_began` `get_job_ended` `get_job_paused` `get_job_exists` `delete_job` `start_job` `create_job` `stop_job` `pause_job` `get_logs` `get_job_stop_time` `monitor_logs` `get_job_is_blocking_on_stdin` `get_job_prompt_kwargs`; `get_actions` `do_action` `do_action_legacy`; `login` `test_connection` `get_mrsm_version` `get_chaining_status` `fetch`; `make_request` `get` `post` `patch` `put` `delete` `wget`. Not implemented in this slice |
| `connectors/valkey/` (`ValkeyConnector`) | the Rust client `meerschaum_valkey::ValkeyConnector` (used by the native engine and the `Pipe` methods for `valkey:` instances) | **Remaining, wave 16 (agent S)**: the SDK class (needs the `valkey` package, absent from the base wheel) still runs Python. To bind: `client` `URI` `set` `get` `test_connection` `quote_table` `get_counter_key` `push_df` `push_docs` `get_datetime_column_key` `read` `read_docs` `drop_table` `get_entity_key`, pipes `register_pipe` `get_pipe_id` `get_pipe_attributes` `edit_pipe` `pipe_exists` `drop_pipe` `delete_pipe` `get_pipe_data` `sync_pipe` `get_pipe_columns_types` `clear_pipe` `get_sync_time` `get_pipe_rowcount` `fetch_pipes_keys` `fetch`, users `register_user` `get_user_id` `edit_user` `get_user_attributes` `delete_user` `get_users` `get_user_password_hash` `get_user_type`, plugins `register_plugin` `get_plugin_id` `get_plugin_version` `get_plugin_user_id` `get_plugin_username` `get_plugin_attributes` `delete_plugin`. Not implemented in this slice |
| `jobs/_Job.py`, `_Executor.py`, `systemd.py` (`meerschaum.jobs.Job`, the SDK class) | no `_core` binding: the native jobs live in `meerschaum-jobs` / `meerschaum-cli` (wave 15, M added the `api:` and `systemd` executors) | **Remaining, wave 16 (agent S)**: `Job` methods `from_pid` `start` `stop` `pause` `delete` `is_running` `exists` `get_logs` `monitor_logs` `is_blocking_on_stdin` `get_prompt_kwargs` `write_stdin` `restart` `check_restart` `get_daemon_properties`, properties `executor` `status` `pid` `result` `sysargs` `daemon` `began` `ended` `paused` `stop_time` `hidden` `label` `env` `delete_after_completion`; `Executor` / `SystemdExecutor` `get_job_*` `create_job` `start_job` `stop_job` `pause_job` `delete_job` `get_logs` `monitor_logs` `run_command` and the service-file helpers. Not implemented in this slice |
| `utils/dataframe.py`, `utils/dtypes/__init__.py`, `utils/dtypes/sql.py` (the guarded helpers) | `_core.enforce_dtypes`, `filter_unseen_df`, `query_df`, `get_*_cols`, `are_dtypes_equal`, `to_pandas_dtype`, `is_dtype_numeric`, `get_geometry_type_srid`, `get_pd_type_from_db_type` (bound) | Bound; the unguarded rest of the modules (`parse_df_datetimes`, `to_polars`, `serialize_*`, ...) stays as SDK |
| `utils/sql.py` (query builders, `build_where`, `clean`, `dateadd_str`, `sql_item_name`, ...) | `meerschaum_sql::queries` / `where_clause` / `ident`; `_core.sql_item_name` and `_core.build_where` bound (gate features of the same names) | **Routed** (wave 8). `get_create_table_query` is only reached from the Python sync bodies; the rest is removed with them |
| `utils/pipes.py::replace_pipes_syntax`, `core/User/_User.py` hashing, `config/__init__.py::get_config` | `_core.replace_pipes_syntax`, `hash_password` / `verify_password`, `get_config` (bound) | Password hashing and verification now delegate unconditionally to Rust (Wave 11), with the Python algorithm and gate entries removed. Config registry ownership and other guards remain open (PARITY "get_config") |
| `utils/daemon/`, `jobs/` executors, `actions/*`, `api/routes/*`, `api/dash/callbacks/*` | `meerschaum-jobs`, `meerschaum-cli` actions, `meerschaum-api` / `-server` (other waves) | Out of this task's scope |

Tests that still exercise only the Python path (they would have to be rewritten against the façade
or deleted): `tests/test_pipe_attributes.py::test_sql_attribute_errors_are_not_unregistered` and
`::test_sql_no_row_is_still_empty` (monkeypatch `SQLConnector.exec`),
`tests/utils/test_dataframe.py::test_filter_unseen_df_polars_preserves_source_order` (patches a private
helper); the tests that import Python SQL internals directly (`tests/test_index_queries.py`,
`test_partition.py`, `test_maintenance.py`, `test_deduplicate.py`, `test_pipes_dtypes.py`,
`test_filter_existing.py`, `test_sql.py`, `tests/utils/test_sql.py`, `tests/utils/test_dataframe.py`:
`meerschaum.connectors.sql._pipes` / `_partition` / `_maintenance` / `_compress`, `meerschaum.utils.sql`)
and run the Python query builders or stub-`self` calls; and the python-mode-only assertions in
`tests/test_live_parameters.py` (`test_python_mode_parameters_are_a_copy`). `Pipe.sync`, `verify`,
`fetch`, `get_data`, `filter_existing`, `enforce_dtypes` and `deduplicate` run in Rust under `rust`
(waves 13-15); their tests compare the two modes (`test_native_sync.py`, `test_w9_verify.py`,
`test_pipe_methods_native.py`) and need no Python body.

## Wave 9 (task U): server-flavor façade latency

| # | Deviation / change | Reason |
|---|---|---|
| 39 | Row 36 is replaced: under `MRSM_CORE=rust`, the resolved parameters of a pipe with a cross-pipe `{{ Pipe(...) }}` template are memoized like v4's, keyed on the `_core.Pipe` generation **and** on the process's metadata writes (a hard `_invalidate_cache()` from `edit` / `delete` / `verify` / `refresh=True`, `register()`, and any `_core.SqlConnector` register / edit / delete, a sync's dtype edits included, via `_core.pipe_metadata_epoch()`). The memo expires after `pipes:attributes:local_cache_timeout_seconds` (v4's staleness window for a pipe's own attributes), so another process's edit shows up within it | re-resolving on every access cost one instance round trip per reference per access, recursively along a derived chain: a 48-pipe replay ran its final `get_data` + `get_rowcount` dump in 41 s instead of 1.3 s. v4 keeps the first resolution forever; this keeps it until something that could change it happens |
| 40 | The template resolver (`PyResolver`) builds one Python `Pipe` per referenced pipe per resolution and reuses it for the table name, the raw parameters and attribute chains | the second `Pipe(...)` doubled the instance metadata reads of every resolution; v4 builds one `Pipe` per template |
| 41 | `get_rowcount` / `get_sync_time` / `clear_pipe` / `deduplicate_pipe` / `get_pipe_data_query` take pandas from `sys.modules` instead of importing it (`PyCtx::scalars`) | the first `get_rowcount()` of a process imported pandas (about 250 ms); v4's SQLAlchemy path never imports it there. With pandas unimported no argument can be a pandas object, so the conversions are the same |



## Wave 11: boundary, lifecycle and package hardening

The maintainer approved V4-001 (connector default isolation) and V5-007 (supported native
Polars Object values) on 2026-10-15. All other behavior changes in this batch are compatibility
repairs. Existing reference expectations remain intact; new password fixtures come from the
isolated released-v4 generator, including legacy digests, aliases and the prefix-parsing control.
The default SQL engine settings are now copied before overrides; combined collection can retain
the original static-config assertion instead of relying on separate test processes.

Native Compose now executes project jobs and temporary pipe sync with isolated worker config
and environment. It retains ownership-aware cleanup and shared-data protection. The former
routing test for Python Compose dispatch now asserts native execution with an unavailable Python
interpreter; this is an implementation-route expectation, not a changed v4 behavioral fixture.
Fresh plugin setup and non-local executors remain open migration work.

D14 progress in this batch removes the Python password algorithm and its gate entries. D20
progress removes synchronous SQLite's async-driver request, installer-based distribution metadata
lookup and redundant bootstrap/import requests. Public config read/write/sync, Pipe algorithms,
action dispatch fallbacks and remaining formatting/job dependencies still require migration.
The offline package/image smoke exercises these public operations and fails when they request
missing dependencies; importing the package and meeting image sizes do not close these gaps.

Private page, secure-server and synthetic export replays are described in the
[build work order](../../docs/design/v5-build.md#wave-11-continuation-2026-10-15). They use isolated
roots and synthetic data; original deployment data, URLs and raw reports are not public fixtures.

Final dataframe validation: 300 passed with one existing xfail in each pandas 2.3.3/3.0.3 ×
Python/Rust combination, including 106 strict boundary regressions. All 11 browser tests and
nine API-host tests passed after the dynamic-page repair. The expanded password checks and
whole-workspace/server/image gates are recorded in the build work order.

## Wave 13 (task H): native `Pipe.sync` for explicit data and document input/output

`Pipe.sync(list[dict] | dict[str, list] | DataFrame)` into a SQL instance with a Rust driver now runs
`_core.sync_pipe` (`src/sync_api.rs`, the engine's ordinary one-pipe flow `meerschaum_engine::sync::sync_pipe`)
under `MRSM_CORE=rust`. It is **not** a graph run: no ledger, no run history, no `mrsm:engine_*` pipe. The
session is the registered-only, worker-less one of `verify_pipe`'s façade (one connector, no config read,
no Python process). Documents and dictionaries of lists cross as kernel `PipeInput::{Docs,Columns}` straight
from the Python objects (`PyCtx::scalars` + `py_to_cell`): no pandas/Polars/pyarrow/JSON round trip, so UUID,
`bytes`, `Decimal`, nested JSON and timezone-aware datetimes keep their types and an interpreter without any
dataframe package can sync. `get_pipe_docs` (`as_docs=True`) builds no dataframe either. `Pipe.sync` returns a
`SyncResult` (a `tuple` subclass, `(success, message)`-compatible, `meerschaum.core.Pipe._sync.SyncResult`) on every
path; the native path fills `inserted`, `updated`, `upserted`, `deleted`, `duration`, `begin`, `end`; the other
paths (and python mode) return the same class with those `None`.

| # | Deviation / change | Reason |
|---|---|---|
| H1 | `frame_from_docs` fills a key a document lacks with `NaN`, not `None`; `convert_dtypes` turns a column that holds only `None` and `NaN` into `Int64` (`[None, None]` and `pd.NA` stay `object`) | `pd.DataFrame(docs)` fills missing keys with `NaN`, so a column that is absent from some documents and `None` in the others is `float64` and `convert_dtypes()` makes it `Int64`; the native path must give the same table column types (`crates/meerschaum-kernel/tests/docs_nulls.rs`, values read from pandas 2.3) |
| H2 | `SqlConnector.get_pipe_docs` takes `PyCtx::scalars` instead of `PyCtx::new` | the first `get_data(as_docs=True)` of a process imported pandas (or asked `attempt_import` to install it) |
| H3 | Data pandas would reject (`{'a': [1, 2], 'b': [1]}`: unequal lengths) is answered by the binding with the same warning and `(False, "DataFrame is None. Cannot sync <pipe>.")` as the Python path | the shape check is the binding's, so the façade does not need pandas to find out |
| H4 | `Pipe.sync` registers the pipe and runs `validate_indices` in the façade (a plugin connector's `register()` is Python) before `_core.sync_pipe`; newly detected special columns and the `autoincrement` flag the sync persisted are applied to the in-memory pipe's parameters (`update_parameters(persist=False)`) | v4 mutates the Python pipe the same way (`SQLConnector.sync_pipe`) |
| H5 | `parse_date_bounds` imports `dateutil` only for a text bound | the integer, `None` and datetime bounds of `sync` / `get_data` needed it (and, offline, its installation) |
| H6 | `SQLConnector.parse_uri` is `_core.parse_sql_uri` under the gate (the Rust URL grammar is SQLAlchemy 2.0's, golden-tested against `fixtures/uris.json`). SQLAlchemy's grammar is the reference and Rust is authoritative: a malformed URI raises `ValueError` with SQLAlchemy's message text (`Could not parse SQLAlchemy URL from given URL string`) instead of `sqlalchemy.exc.ArgumentError`, and there is no silent fallback to SQLAlchemy | `MRSM_SQL_<LABEL>` URI connectors needed SQLAlchemy only to split the URI; a second grammar behind a `try` would make the result depend on whether SQLAlchemy is installed |
| H7 | `tests/test_pipeline.py` asserts `type(result) is SyncResult` where it asserted `is tuple` | the intent (a bare `sync()` is not a graph `RunResult`) is unchanged; the spec makes the return a tuple subclass |
| H8 | Integers beyond 64 bits (`2**70`) sync natively as a `numeric` column (and register `dtypes: numeric`); v4 fails with `DataFrame is None. Cannot sync ...` because pandas cannot build the frame | a v4 limitation of the pandas boundary, not a contract: the value is valid and the kernel represents it exactly (`test_integers_beyond_64_bits_sync_as_numeric_only_natively`) |
| H9 | **Fixed.** Pipes with `enforce: False` and `sync(enforce_dtypes=False)` take the native sync again. The Rust instance now follows v4's non-enforcing rules: (1) a SQLite/GeoPackage table that does not enforce is read back with its `TIMESTAMP` columns as *text* (v4's `get_pipe_data` passes no `dtype` to `read()`) and the `enforce_dtypes` `astype` then reads them as UTC, so a UTC-aware row matches its backtrack and is updated, not inserted twice (`instance/data.rs`, `sqlite_datetimes_as_text`); (2) the kernel's object-path `astype` casts integer and float columns to a datetime dtype as ticks of the target's unit (`Series.astype('datetime64[us, UTC]')`), so an integer datetime axis is created as a datetime column and later integer rows are cast to it, instead of reaching the partition/insert code as integers (`enforce.rs`, `pandas_astype`; `fixtures/enforce_dtypes.json` carries the v4.1.4 oracle rows); (3) `filter_existing` raises pandas' merge `ValueError` for a timezone-naive key against a UTC-aware one (and a datetime key against a numeric/string/bool/bytes one) where v4 lets it escape from `Pipe.filter_existing()`, and the façade re-raises it from the failed native result (`// PARITY:` in `diff.rs`, `_sync_native`; `fixtures/filter_existing.json` oracle cases `no_enforce_*`) | a naive datetime axis raising on its second sync is a v4 quirk kept on purpose (the same rows with `enforce: True` are coerced to UTC and update); only the datetime merge-key rules are modeled, the `object`-column rules of pandas are not. The MySQL family stores `datetime` without a timezone, so a non-enforcing pipe there raises for UTC rows as well; the v4 oracle could not reach those containers, so it is covered by no test. Tests: `tests/test_native_sync.py::test_pipes_that_do_not_enforce_match_v4` (and `..._are_synced_natively`, `test_syncs_with_enforce_dtypes_false_match_v4`, `test_a_naive_axis_raises_the_pandas_merge_error_in_both_modes`), `crates/meerschaum-sql/tests/instance_enforce_false.rs`, `crates/meerschaum-kernel/tests/enforce_no_coerce.rs` |
| H10 | Documents with no column at all (`[{}, {}]`) into a new table raise `ValueError("Expecting columns for table '...'.")` from `Pipe.sync`, as v4 does (`utils/sql.py:2329`); the façade raises it from the native result (matched on that message) | v4 raises rather than returning a failed tuple |
| H11 | A static pipe that receives a new column fails in both implementations; the native message is `Failed to insert rows into "<table>".`, v4's is the SQLAlchemy traceback text. Rows and `success` are identical | v4's message is an accidental traceback; only the failure and the table contents are contract (`test_a_static_pipe_refuses_new_columns_in_both_modes`) |

**Not routed yet (remaining work; they keep the current Python body, selected by `_rust_sync_eligible(...)`, which
returns the reason and logs it under `debug`).** None of this is a new Python fallback and none of it is complete:
- no data given (the connector's `sync()` / `fetch()`), `None`, strings (`parse_simple_lines`), generators and other
  iterables (chunk generators, `sync_chunks`), Dask, Polars `LazyFrame`, tuples/lists of non-dicts, dicts with scalar
  or `Series` values or non-string keys (declined by `_core.sync_pipe` itself: the call has not read or written anything);
- `force` (the retry loop), `workers`, non-blocking syncs with `callback` / `error_callback`, `debug=True`, plugin
  keyword arguments (any other `**kw`);
- temporary pipes, pipes with `reference` / `references`, `{{ }}` templates or `MRSM{}` symlinks in their raw parameters;
- instances that are not a SQL connector with a Rust driver: `mssql`, `oracle`, `duckdb`, in-memory SQLite, `api:`,
  `valkey:` and custom `@make_connector` instances;
- `SyncResult.changed_intervals` is `None` on every path (the one-pipe flow does not report intervals yet);
  `SyncResult` is not exported as `mrsm.SyncResult`;
- `Pipe(...)`, `register()`, attribute lookup and `get_data(as_docs=True)` no longer need SQLAlchemy, pandas or
  Polars (asserted with those modules blocked in `tests/test_native_sync.py::test_construct_register_sync_and_read_docs_need_no_sqlalchemy_or_dataframes`),
  but their Python bodies still exist behind the gate; D14 deletes them. Everything D16 (`read/exec/value/to_sql/.engine`)
  still creates the SQLAlchemy engine on first use, as intended.

GAP-004 progress: with the config round trip on Agent G's native YAML, `scripts/docker/smoke_facade.py`
(`Pipe.sync(list[dict])` to a SQLite instance, `get_data(as_docs=True)`, `SyncResult` fields, no `sqlalchemy` /
`pandas` / `polars` / `numpy` imported) passes in a fresh venv that holds only the `meerschaum` distribution with
the network and installs disabled; see the final report of this wave for the commands.

## Wave 13 (task G): native lossless YAML codec (D14/D20 slice 1)

`meerschaum._core.yaml` (`load`, `safe_load`, `dump`, PyYAML's exception classes) is a port of PyYAML's
reader, scanner, parser, composer, constructor, representer, serializer and emitter
(`meerschaum_core::yaml::{scanner,parser,loader,node,emitter,dumper}`) with a lossless node tree: ordered
mappings with typed keys, arbitrary-size integers, `date`/`datetime` (naive and offset, microseconds), `bytes`,
tuples, sets and shared (aliased) nodes. `src/yaml_api.rs` builds real Python `datetime.date`/`datetime.datetime`/
`bytes`/`int`/`bool`/`None`/`tuple`/`set`/`dict` objects, so `{1: a, True: b}` collapses like a Python dict. It never
goes through `meerschaum_core::Value`; the JSON-like `meerschaum_core::yaml::{load,dump}` projection is kept for Rust
consumers whose data model is JSON (config registry, compose) and is documented as lossy. `meerschaum/utils/yaml.py`
routes `yaml.load` / `safe_load` / `dump` to it when the extension is present and imports no YAML library and no
`packaging` (PyYAML/ruamel stay as the fallback of an installation without `_core`, which is not a supported v5
configuration). Every `import yaml` / `attempt_import('yaml')` site of the package (`config/_read_config.py`, `_edit.py`,
`_sync.py`, `config/__init__.py`, `core/Pipe/_edit.py`, `actions/edit.py`, the dash callbacks) already went through
`utils/yaml.py`; `utils/packages/is_uv_enabled` now reads the config instead of probing `import yaml` when the codec is
native, so a `uv_pip: false` opt-out is honored without PyYAML in either gate mode.

Recorded with released `meerschaum==4.1.4` + PyYAML 6.0.3 by `crates/meerschaum-core/tests/fixtures/gen_yaml.py`
(`yaml_parity.json`: 250 load cases x `load`/`safe_load`, 3536 dump cases, 29 `read_config`/`write_config` cases),
consumed by `crates/meerschaum-core/tests/yaml_parity.rs` and `tests/test_yaml.py` (which also compares `_core` with
PyYAML in one process, and replays the config cases in both `MRSM_CORE` modes in a subprocess).

| # | Deviation | Reason |
|---|---|---|
| G1 | `yaml.load` (PyYAML's full `Loader`) refuses every `!!python/*` tag except `!!python/tuple` with the `SafeLoader` `ConstructorError` ("could not determine a constructor for the tag ..."); `!!python/object`, `/apply`, `/name`, `/module`, `/complex`, `/str`, `/dict`, `/list` are not constructed | those tags import modules and call arbitrary callables; a config file or a pipe's parameters must not execute code. `!!python/tuple` is what the Python dumper itself writes for tuples. 8 corpus cases (`PYTHON_TAG_DEVIATIONS` in `tests/test_yaml.py`) are compared as "same error as `safe_load`" |
| G2 | A lone surrogate escape (`"\ud800"`) loads as U+FFFD | a Rust `String` cannot hold one (`tests/test_yaml.py::test_lone_surrogate_escape_deviation`) |
| G3 | A self-referential anchor (`&a [*a]`) raises `RecursionError("maximum recursion depth exceeded")`; dumping a recursive Python structure raises `yaml.RepresenterError`, not a self-anchored document | an `Rc` node tree cannot hold a cycle; PyYAML builds the cycle and fails later in nearly every consumer (`loader.rs`, `yaml_api.rs`) |
| G4 | `dump()` ignores keywords the codec does not know (as `filter_keywords` did before PyYAML) and has no `Dumper` class hook; `get_dumper_class()` returns `None` | the v4 `CustomDumper` behavior (blank line between top-level keys, `|`-style multi-line strings) is built in and byte-compared with the reference on all 3536 dump cases |
| G5 | In `MRSM_CORE=rust`, the rust-mode default config carries `system:engine` inside the stack's `MRSM_CONFIG` default (v5 key; v4.1.4 lacks it); `tests/test_yaml.py` strips it before comparing the config cases | v5-only config section, not a codec difference |
| G6 | Root-level multi-line or whitespace-edged strings written by `yaml.dump` do not round-trip through any loader | v4's `CustomDumper` doubles the line breaks at indent depth 1; the reference writes the same text, kept byte for byte (`dump_cases`); `tests/test_yaml.py::test_pyyaml_reads_what_native_writes` skips exactly those |
| G7 | A date inside config YAML (`when: 2024-01-01`) is a `datetime.date` through `read_config(substitute=False)` (lossless), but `get_config` fails in python mode (`json.dumps` in `search_and_substitute_config`, as v4.1.4 does) and returns the string `'2024-01-01'` in rust mode (the registry is JSON-valued) | v4 quirk kept in python mode; the rust registry stores `meerschaum_core::Value`. **Closed in rust mode (wave 15, O):** the registry is lossless, `get_config` returns the `datetime.date` (python mode keeps the v4 failure) |

Fixture hygiene found by this work: `scripts/parity/run.sh` exports `MRSM_PARITY_REFERENCE`, which v4 reads as a
`parity:reference` connector the moment `meerschaum` is imported. `gen_yaml.py` therefore removes every `MRSM_*`
variable before the import and keeps the version for its own guard (the first recording had leaked the connector into
the recorded default config).

**Config ownership (deliverable 5): stopped at a clean boundary; nothing is duplicated. Closed in wave 15, task O (see "Wave 15 (task O)" below).** `read_config`,
`write_config`, `search_and_substitute_config`, `get_keyfile_path` and the `_sync` helpers are still the v4 Python
bodies, now running on the native lossless codec (no PyYAML), and `_core.get_config` still serves the live Python
`config` dict (one registry; `set_config` / `replace_config` / env patches stay visible, nothing cached). The Rust
`meerschaum_core::config::Config` (`read_config` / `write_config` / `search_and_substitute`) is a second, JSON-valued
registry used by the `mrsm` binary and `meerschaum-server`. Remaining work, in order:
1. make `meerschaum_core::config` hold the lossless node (or a `Value` with `Date`/typed keys) so a `datetime.date`
   in `config.yaml` survives `get_config` (G7) and `write_config` of a value the JSON projection cannot represent;
2. bind `read_config` / `write_config` / `search_and_substitute_config` / `get_keyfile_path` in `_core` over that
   registry (with `MRSM{}` symlink restoration, backups/rollback of `config/_backup.py`, `_allow_write_missing`,
   `with_filenames`, the malformed-file warning text) and gate them as `GATED_FEATURES` entries, then delete the
   Python bodies (D14); the 29 recorded config cases are the acceptance fixture;
3. make `get_config` read that registry instead of the Python dict (keeps "get_config is hot": read-only fast path).
`meerschaum.utils.packages._packages['core']['yaml']` (PyYAML) and `packaging` remain listed for the fallback and for
`pip_install` of plugin venvs; removing them is a `check_packaging.py` change owned by the packaging slice.

GAP-004 status: the config write/read half is closed in a fresh venv without PyYAML (commands in the wave 13 report of
task G); the Pipe-sync half is task H's. The Python CLI (`python -m meerschaum ...`) is not a supported entry point of
the base wheel (it needs prompt_toolkit/humanfriendly); the `mrsm` binary (`meerschaum-cli` wheel) is.

## Wave 13 (task I): native plugin lifecycle (`setup plugins`, `install required|plugins|packages`)

The four actions run in Rust (`crates/meerschaum-cli/src/native/{lifecycle,pip,plugin_files}.rs`) and no longer
reach `python.rs::core_action`. Python runs only a plugin's own `setup()` (worker op `setup`); a native (C-ABI)
plugin's `setup` goes through the SDK `call` and starts no Python. Compose's `init_root`, `compose_init` and
`check_and_install_plugins` issue the same sysargs as v4; they re-invoke this binary under the project
environment (`MrsmRunner`), so they reach the native handlers and never `core_action`. Messages were compared
with released 4.1.4 against a temp root and a loopback repository (`install plugins`: fresh, up to date, force,
failing `setup`, no version, missing archive, `--skip-deps`; `install required`/`install packages`: success,
offline failure, `--force`, empty request; `setup plugins`: bool/tuple/None/raise/odd returns; `compose init` in a
fresh root). Deviations:

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W13I-001 | `safely_extract_tar` compares path strings (`abs_target.startswith(abs_directory)`), so a member `../<name>.txt` escapes a directory named `<name>`; v4 also extracts symlink/hardlink members and buffers members in memory | components are compared and every destination parent is canonicalized on disk against the output directory before a write; symlink and hardlink members are refused (`Links are not allowed in plugin archives`); regular members stream to disk with a 256 MiB cap; special members are skipped; the temp directory is cleared before extraction and removed on every exit path; plugin names must be a single path component (`is_safe_plugin_name`) | closes a real sibling-prefix escape and a symlink escape found in review (2026-10-16); every other message is v4's |
| W13I-002 | the archive is `<name>.tar.gz` and `binaryornot>=0.4.4` is unpinned: 0.6.0 (the reference venv) calls every `.gz` binary, so a repository's JSON answer (`[false, "Archive ... could not be found."]`) ends as `Failed to extract plugin` plus a gzip warning; 0.4.4 reads the content | content is inspected (0.4.4 semantics): the server's own message is printed and the install ends there | v4's result depends on an unpinned library; the content rule is the one the code was written for |
| W13I-003 | `Plugin.get_dependencies` reads `required` from the source text and deletes the first `=` of the list (`'pandas>=2'` becomes `'pandas>2'`) and ends the list at the first `]` even in a string or comment | the list comes from the manifest when the plugin imported; else from the source with a string- and comment-aware bracket match, specifiers intact | v4's text reader mangles specifiers; `// PARITY` repair in `plugin_files::static_required` |
| W13I-004 | an API repository without credentials prints `Please login with the command login <keys>.` before each request | not printed (the native client does not emit it) | cosmetic; the default `api:mrsm` has credentials |
| W13I-005 | `reload` imports every plugin again after an install | the manifest refresh (changed plugin trees only; the worker pool is recycled when any changed) | D14; the step prints nothing and returns v4's `(True, "Success")` |
| W13I-006 | bootstraps `uv` into the host interpreter (`pip install uv --break-system-packages`), updates `pip` itself, downloads `get-pip.py`, adds `--prerelease=allow` for pinned pre-releases | none of these; with `pip` as the installer the `wheel`/`setuptools`/`PyYAML` bootstrap still runs | v5 never changes the host interpreter or reaches the network on its own |
| W13I-007 | plugin version of an installed plugin is read by importing it; a plugin's `print()` in `setup()` goes to stdout | read from the source (`__version__ = '...'`); worker prints go to the worker's stderr; `--debug` echoes the installer command line without v4's `dprint` traces | no import in the CLI process |
| W13I-008 | `get_plugins_names()` lists plugins in symlink-farm (file-system) order | sorted by name within each plugins directory, first directory first | stable output |

`MRSM_NO_AUTO_INSTALL`: an explicit install (`install required`, `install packages`) still runs, as in v4. While the variable
is set the native code skips every bootstrap: no `uv venv --seed` (the fallback `python -m venv` gets `--without-pip`), no
`wheel`/`setuptools`/`PyYAML`/`uv` install into the venv. This follows the repaired V4-002 (row 44, user decision 2026-10-16).
`--dry-run` reports `auto_install_enabled`.

Failed-import discovery (decided with released 4.1.4, plugins `broken.py` with a failing import, `ok.py`, `pkg/__init__.py`,
`no_init/mod.py`): `show plugins` lists `pkg, broken, ok` and not `no_init`; `setup plugins` counts `broken` (the import
error is warned only when something imports it). So discovery is a file-system test (`<name>/__init__.py` or `<name>.py`),
never an import; `compose/plugins.rs::plugin_names_in_dirs` now applies the `__init__.py` rule and needs no runtime. A native
plugin directory (`plugin.toml`) counts as installed and shadows a Python plugin of the same name.

Remaining `core_action` fall-through after this task (`crates/meerschaum-cli/src/python.rs::core_action`), computed as
the action table (`actions.rs`) minus the branches of `dispatch.rs::run_action` and `native::run`: `login`, `os`, `stack`,
`sql`, `api start` (`_api_start`), `bootstrap connectors|jobs|pipes|plugins`, `clear cache`, `copy connectors`,
`delete cache|config|connectors|plugins|venvs`, `drop indices`, `edit definition|jobs|plugins|tokens`, `flush indices|pipes`,
`register connectors|tokens`, `restart|show|start|stop daemons`, `show packages|schedules|targets|tokens`, `start connectors|gui|worker`,
`uninstall packages|plugins`, `verify packages|plugins|venvs`. Inside native handlers three more calls remain:
`prune venvs` -> `uninstall packages` (`native/venvs.rs:590`), `upgrade meerschaum|packages|plugins` (`native/venvs.rs:619`) and
the register-plugins fallback (`native/manage.rs:752`). Nonlocal executors are not covered.

## Wave 13 (task E): native web terminal (`/webterm/{session_id}`, `/dash/webterm`)

`meerschaum-server::webterm` (PTY bridge on `portable-pty`, tmux sessions, terminado's wire protocol) replaces the Tornado /
terminado process and the API-host mount (`meerschaum/api/host/_webterm.py` deleted; the host config always sends
`no_webterm: true`; `meerschaum/api/routes/__init__.py` now imports `routes/_webterm.py` only when `_include_webterm`, which the
host never sets). Deviations from 4.1.4, all deliberate:

| Behavior | v4.1.4 | v5 | Why |
|---|---|---|---|
| Who gets a shell | any logged-in user when `api:permissions:actions:non_admin` is true (its default) | administrators, or any user the actions gate (`is_user_allowed_to_execute`) lets run actions | the terminal is a shell on the server; the v4 default handed one to every registered user |
| Second login on one server | `500` (`Failed to apply update to Pipe('mrsm','sessions')`) | works, the sessions are independent | the Python bug is not carried over |
| Shell echo | the headless shell never echoed | the PTY echoes | real PTY |
| New tmux window | the page typed a command into the terminal | `["new_window"]` message (v5 addition; v4 clients never send it) | no typing into the user's shell |
| `start webterm` | starts the Tornado process | prints that the terminal is part of `start api` and fails | there is no separate process |
| Environment of the tmux shell | a JSON file read by a `python -c` bootstrap | a `0600` sourced `webterm.env` in `WEBTERM_INTERNAL_RESOURCES_PATH` | no Python in the path; secrets stay out of `ps` |

Quirks kept (`// PARITY:` in `webterm.rs`): the websocket of an unauthenticated session is accepted and closed with code 1000;
`--no-auth` authenticates every session id; `COLUMNS`/`LINES` are removed from the shell's environment.
The `webterm` tests run with tmux present (`tmux_sessions_survive_reconnects_and_carry_the_environment`, `tests/console`
`[tmux]` params) and absent (`without_tmux_on_the_path_each_tab_gets_a_plain_shell`, the `no-tmux-binary` console param
whose server PATH holds no `tmux`).

## Wave 15 (task M): nonlocal job executors (`api:<label>`, `systemd`) and executor-aware compose

Layering: `meerschaum-jobs` stays on `meerschaum-core`; the jobs crate owns the client traits (`remote::RemoteJobs` /
`RemoteJobsResolver` for `api:<label>`, `runner::ProcessRunner` for `systemctl --user`) and `meerschaum-cli` injects the
implementations (`native/remote_jobs.rs` over `meerschaum_api::APIConnector` and the Session's single Tokio runtime). See
`crates/README.md`, "Job executors: client injection". `jobs_backend.rs` is the server side of `/jobs/*` over the local and systemd
executors (the routes themselves are `meerschaum-server`'s).

| Behavior | v4.1.4 | v5 | Why |
|---|---|---|---|
| A failed remote read (unreachable endpoint, `401`/`403`, `5xx`) | `get_job_metadata` and friends return `{}` / `False` / `None`, so a listing shows no jobs and `compose down` finds nothing to delete | decisions (listings, ownership, replacement, `compose up|down|stop`) use the `try_*` reads and fail with the cause (`Could not read the jobs of this project's executor: ...`); display-only getters (`get_job_status`) still answer `stopped` | an empty answer must never read as success (a failed `down` would silently leave jobs running) |
| `get_executor_keys_from_context()` | `meerschaum:executor`, else systemd when `systemctl --user` works, `is_installed('meerschaum', allow_outside_venv=False)` and the root is the default root, else `local` | same, minus the `is_installed` clause (the `mrsm` binary is always installed); a debug build refuses the default root so it never chooses `systemd` | `// PARITY:` in `job.rs`. v4 has no `MRSM_EXECUTOR` variable: the executor is `meerschaum:executor` (`MRSM_MEERSCHAUM` / `MRSM_CONFIG`) or `-e` |
| Listing systemd jobs named with spaces (compose names jobs like `project sync (instance)`) | lists the dash-normalized unit name (`my-job`), which never "exists" and cannot be read back | the real name is recovered from `systemd/services/<name>/` | `// PARITY (divergence)` in `systemd.rs` |
| `delete_job` / `create_job` after the job directory is gone | `path.exists()` is false for the now-dangling `mrsm-<name>.service` link, so the link is left behind and the next `create_job` raises `FileExistsError` | the dangling link is removed (`symlink_metadata`) and replaced | `// PARITY:` in `systemd.rs` |
| What runs in the unit | `python -m meerschaum <sysargs>` with `entry.py` redirecting stdio/result when `MRSM_SYSTEMD_LOG_PATH` is set | the unit text is byte-identical (golden: `tests/fixtures/systemd_executor.json`, generated by `gen_systemd_units.py` with the interpreter and temporary paths normalized); the `mrsm` binary strips `-m meerschaum` and, in `systemd_job::run_if_requested`, wraps itself: copies the child's output into the rotating log with timestamps, feeds it the FIFO, forwards signals, records the result | a binary has no import hook; the observable files and commands are the same |
| Job names | banned: `,;'"$#=*&!` + backtick + `~` | additionally `/`, `\`, NUL, the empty name, `.` and `..` (`Job::new`, `SystemdExecutor::create_job/delete_job`); `Daemon::cleanup` and the supervisor's delete-after-completion remove a directory only when it is `<jobs dir>/<one component>` (canonical parent check) | `DELETE /jobs/%2E%2E` resolved to `<root>/jobs/..` and `remove_dir_all` deleted the whole root. Tests: `meerschaum-jobs/tests/job_names.rs`, `executors_api.rs::path_like_job_names_never_touch_the_servers_root` (raw HTTP and CLI) |
| `-m meerschaum` prefix | `python -m meerschaum` has no counterpart in a binary | stripped only by `systemd_job::run_if_requested`, i.e. inside a unit (the `MRSM_SYSTEMD_*` variables are set); `mrsm -m meerschaum ...` keeps `-m` as the metric filter | stripping it everywhere turned `mrsm -m meerschaum delete pipes -y` into a delete of every pipe. Test: `executors_systemd_job.rs::a_leading_dash_m_is_kept_outside_a_unit` |
| Interpreter of a systemd job | `ExecStart=<sys.executable> -m meerschaum ...` | the unit also carries `Environment=MRSM_PYTHON=<absolute interpreter>` (the creating process's `MRSM_PYTHON`, resolved against `PATH`, else its `python3`; when none resolves nothing is written and `create_job` prints a warning, `unpinned_python_warning`) | the unit runs the `mrsm` binary, which would otherwise pick whatever `python3` the unit's `PATH` has. The golden unit-text comparisons (`layout.rs`, `systemd_executor.rs`) drop exactly this line; `the_unit_pins_the_workers_interpreter` covers it |
| Stopping a systemd job | `systemctl kill -s SIGINT` signals the control group (the job once) | the wrapper (`systemd_job.rs`) does not forward a signal when `INVOCATION_ID` is set (systemd already delivered it to the job); a wrapper started by hand still forwards | forwarding delivered a second SIGINT, which the job treats as force-quit (exit 130 mid-chunk). Test: `a_signal_the_service_manager_delivered_to_the_cgroup_is_not_forwarded_again` signals both PIDs |
| `POST /jobs/{name}`: `properties.env` | the client's `env` is replaced by `{MRSM_NONINTERACTIVE: '1', **os.environ}` of the server | same, except the Compose project stamp (`MRSM__COMPOSE_PROJECT`) the client sent is kept | v4 loses the stamp, so projects sharing one `api:` executor cannot tell their jobs apart; nothing else of the client's `env` reaches the server's process environment (no `LD_PRELOAD`, no `PATH`). Test: `a_created_jobs_environment_is_the_servers_plus_the_compose_stamp`. Migration guide, "Job executors" |
| Finding a running job without a PID file | `_find_detached_pids` matches `daemon_id='<id>'` on every process of the machine | a Rust supervisor (`--jobs-dir <dir>` on its command line) only counts for the root whose `jobs/` it runs in; Python daemons still match by name | a job named like one running under another root was reported "already running" (and `stop` could have killed the other root's job). Test: `a_same_named_supervisor_of_another_root_is_not_found` |
| `lines_to_show` of 0 and below | `lines[(-1 * n):]`: `n > 0` the last `n` lines; `0` is `lines[-0:]`, every line (`do_action_async` sets it); `n < 0` is `lines[|n|:]`, skipping the first `|n|` | same (`job::slice_start`, `// PARITY:`; the value stays signed through `MonitorSource::lines_to_show`); it was "emit none" for 0 and "every line" for negatives | `mrsm api <label> <action>` printed nothing for a job which finished before the monitor connected. Existed at HEAD (latent: HEAD's server had no jobs backend) |
| `mrsm api <label> <action>` result | `job.result` is read from the job after the monitor returns | same: when the websocket closes without `MRSM_STOP` (a finished job with no output), `do_action` reads `result` from the job's metadata; the server closes the socket after the result and when its follower ends; the client treats a reset without a closing handshake as the end of the stream | the server's log watcher held a channel sender, so a follower which ended without a result left the websocket open forever and the client hung (new with `LocalJobsBackend` in `server.rs`; HEAD wired no jobs backend). Test: `api_action_streams_the_remote_output_and_returns_the_remote_result` (60 s deadline) |

Not covered by an automated test: a real `systemctl --user` (only `real_systemctl_smoke`, `#[ignore]` and gated on
`MRSM_TEST_SYSTEMD_SMOKE=1`, which asks for the state of a unit that does not exist). Every lifecycle test uses
`runner::FakeSystemctl`, which records the commands and answers canned `show` properties.

Not taken by this slice: `show daemons`, `start|stop|restart daemons` are the CLI daemons (`_internal/cli/workers.py`), not job
executor views. N1 made them native (`native/residual.rs`; see the `show|start|stop|restart daemons` row of the task N1 section
below). `edit jobs` is native (`native/jobs.rs::edit_jobs`).

## Wave 15 (task O): the config registry is owned by Rust, `MRSM{}` resolves in Rust

`meerschaum_core::config` holds the lossless value `ConfigValue` (`config/lossless.rs`: dates and datetimes, `bytes`,
integers of any size, tuples, sets, mappings with typed keys), so `read_config`, `get_config`, `write_config` and the
`MRSM{}` substitution never go through the JSON projection (`meerschaum_core::Value`, `yaml::{load,dump}` keep it for
compose). `crates/meerschaum-py/src/config_api.rs` binds the whole surface in `_core`, gated as `GATED_FEATURES` entries
(`get_config`, `get_plugin_config`, `write_plugin_config`, `read_config`, `write_config`, `search_and_substitute_config`,
`revert_symlinks_config`, `get_keyfile_path`, `get_key_backups_dir`, `list_config_backups`, `backup_config_key`,
`restore_config_backup`, `apply_environment_patches`, `apply_environment_uris`, `sync_files`); the Python bodies behind the
guards are what wave 16 deletes (they remain only as the gate's `python` oracle).

**Where the in-process tree lives.** Python callers hold the dicts `get_config` returns and mutate them
(`get_config('system', 'connectors', 'sql')['chunksize'] = 5`, plugins editing their own section) and expect the change on
the next call, which only a Python object can give. So the process-wide tree is ONE `dict` created and owned by the
extension (`_core.config_dict()`; `meerschaum.config.config` resolves to it through a module `__getattr__`, so
`from meerschaum.config import config` gives the live object and `set_config(cf)` / `reload` swap it through
`_core.config_set` / `config_reset`; `meerschaum/config/_store.py`). The registry itself keeps no state between calls: a
call that needs a rule (anything but a read of a loaded, symlink-free subtree) converts the keys it can touch into a
`ConfigValue` tree, runs `ConfigState` on it and writes back only the top-level keys that changed, so every other key keeps
its object identity. Python objects with no config form (functions, connectors) travel as opaque placeholders and come back
as the same object. Nothing is cached; `set_config`, `replace_config`, environment patches and callers editing returned
dicts are seen on the next call. The read-only fast path (row 12) is unchanged in shape and now lives in `config_api.rs`.
This holds in both `MRSM_CORE` modes: the dict is the extension's whenever the extension is built.

**`MRSM{}` in `_core.get_parameters`.** The resolved parameters of a pipe have their symlinks substituted by the same
registry call (`config_api::substitute_parameters`: no `MRSM{` text means no conversion at all); `core/Pipe/_attributes.py`
no longer calls `search_and_substitute_config` after the Rust call (it stays on the non-Rust branch only).

**Also moved to Rust:** `sync_files(['stack'])` / `sync_yaml_configs` (`config/sync.rs`; the Compose, Grafana and
extensions-script files, header kept; the first load of the `stack` key calls it as v4 does), the config backups and
rollback (`config/backup.rs`), `_allow_write_missing` / `write_missing`, `with_filenames`, yaml/yml/json preference, header
comment and key order on write, the malformed-file warning text, `MRSM_CONFIG` / `MRSM_PATCH` / `MRSM_<TYPE>_<LABEL>` and the
`patch_config/` directories. Messages v4 prints (`print()`) or warns (`warn()`) are recorded in order as `Notice`s and
replayed on stdout / stderr at the boundary.

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W15O-1 | `get_config` on a key whose YAML holds a date raises `TypeError` (`json.dumps` in `search_and_substitute_config`) and the file is reported as unparseable | `datetime.date` / `datetime.datetime` / `bytes` / big ints survive `read_config`, `get_config` and `write_config`; the substitution text carries them as placeholders and swaps them back (G7 closed) | the registry is lossless; `MRSM_CORE=python` keeps the v4 failure as the oracle |
| W15O-2 | `write_config` opens the key's file with `open(path, 'w+')` before dumping (truncating it) and removes the file when the dump fails | a failed dump leaves the file as it was (JSON and an unknown filetype too); the text is written to a temp file in the same directory, synced and renamed over the target (mode of the replaced file kept; a symlink is written through), and a failure removes only the temp file, never the config file | a config file holds connector passwords: a failed or interrupted write must never delete, truncate or half-write it (`// PARITY:` in `write_config_with`, `write_file_atomically`). Tests: `tests/test_config_registry.py`, `crates/meerschaum-core/tests/config_write.rs` |
| W15O-2c | `open(path, 'w+')` keeps the file's inode, owner, group, ACLs, xattrs and SELinux label; a new file gets mode `0666 & ~umask` | the temporary file is created with the target's mode (`0600` for a new file) before any byte is written, then `fchown`ed to the target's owner and group (best effort: kept where the process may, ignored otherwise); a new config file is `0600`, not umask-default | the secrets never sit in a wider-than-target file; xattrs, ACLs and SELinux labels are **not** copied (the replacement takes the directory's defaults) and the inode changes (a hard link to the old file keeps the old content); mtime is the write time |
| W15O-2d | `open(path, 'w+')` works on a single bind-mounted file (`/meerschaum/config/plugins.json` in Docker) and on a writable file in a directory the process cannot write | the atomic path falls back to rewriting the existing, writable file in place (truncate, write, `fsync`; inode, mode, owner and attributes kept) when creating the temp file or renaming it fails with `EPERM`, `EACCES`, `EBUSY` or `EXDEV`; the temp file is removed; any other error (`ENOSPC`, ...) is reported and the file is left as it was; a new file in an unwritable directory is a `PermissionError`. The in-place path is not atomic (v4's behavior) | containers and permission layouts v4 supported must keep working (`write_file_with` in `config/mod.rs`; unit tests `write_fallback_tests` inject `EBUSY` / `EXDEV` / `ENOSPC` through the rename function and use a real read-only directory) |
| W15O-10 | the paths object has no native counterpart | the native view of the live config (`config_api::live_config`, used by the sync flow and the worker manifest) carries `PLUGINS_DIR_PATHS` (several `MRSM_PLUGINS_DIR` entries) and, when it is not `<root>/venvs`, `VIRTENV_RESOURCES_PATH` (`MRSM_VENVS_DIR`, `set_venvs_dir`) into `Paths`. Consumers traced: no crate reachable from the extension (worker, engine, connector, plugin, py) reads the venvs directory from `Config`; the CLI's `venvs`/`pip` code reads it from its own `Runtime` paths. The value is carried so a future consumer is not silently wrong | `_core.live_config_paths()` exposes it; tests `test_the_native_config_carries_the_plugins_and_venvs_dirs`, `test_the_native_config_defaults_to_the_root_plugins_and_venvs` |
| W15O-2b | a read-only file raises `PermissionError` from `open()`; the file is intact | same: the target is probe-opened for writing (no truncation) first, and `write_config` raises the I/O error (`PermissionError`, `FileNotFoundError`, ...) from the extension; `ConfigState::take_write_error` carries it | a plain rename would otherwise silently replace a read-only file |
| W15O-3 | `get_keyfile_path` calls `read_config(keys=[key])`, which parses the file (and warns when it is malformed) | looks at the file names only; no parse, no warning | the lookup never needed the content; the warning is raised by the read that follows |
| W15O-4 | `meerschaum.config.config = {...}` rebinds the module global | the module's class has a `config` property: reading gives the live dict, assigning replaces it exactly as `set_config(cf)` does (non-dicts raise), so `get_config` sees the new object | the dict is owned by the extension; no split config (`tests/test_config_registry.py`) |
| W15O-6 | n/a (v4 has no extension) | an extension built before the registry moved to Rust (it lacks `config_dict` / `config_set` / `config_reset`) is reported by `meerschaum._core.load()` as not built, with `scripts/rust/build_py.sh` in the error (`REQUIRED_SYMBOLS`) | no `AttributeError` from inside an import |
| W15O-7 | `import meerschaum` builds the config dict in Python | same: the live dict is created in `meerschaum/config/_store.py` and **the extension adopts it on first use** (`config_api.rs::store()`; `set_store` / `reset_store` hand a replacement to an already loaded extension, `apply_layer` goes through `set_store`). A bare `import meerschaum`, `import meerschaum.config` and `meerschaum.utils.dtypes` never load the 115 MB extension, in either `MRSM_CORE` mode; `apply_environment_patches` / `apply_environment_uris` return before touching it when no `MRSM_CONFIG` / `MRSM_PATCH` / `MRSM_<TYPE>_<LABEL>` variable is set. With such a variable set, or on the first `get_config` in rust mode, loading is legitimate | measured on one host, 10 runs each, median of `python -c 'import meerschaum'` with a temp root and no `MRSM_*` env: v5 153 ms vs released 4.1.4 151 ms. Tests: `crates/meerschaum-py/tests/test_core.py::test_import_is_lazy`, `tests/test_config_registry.py` (lazy import in both modes, adoption identity, env connectors at import) |
| W15O-8 | in `get_config` (not in `_config()`), `sync_files([key])` runs when a key is first loaded even under `replace_config` (`config/__init__.py:233` has no `_allow_write_missing` check), so v4 writes the Compose/Grafana/initdb files into the real root during a temporary config replacement | `sync_files` is skipped whenever writes are disabled (`replace_config`, `_allow_write_missing=False`) | a temporary replacement must not write the real root (`tests/test_config_registry.py::test_replace_config_writes_no_derivative_files`) |
| W15O-9 | `get_config(debug=True)` prints `Indexing keys: (...)` (`dprint(..., color=False)`); `backup_config_key(debug=True)` prints `No config file to back up for '<key>'.`, `'<key>' unchanged since last backup; skipping.`, `Failed to back up '<key>':\n<error>`, `Backed up '<key>' to '<path>'.`; `restore_config_backup(debug=True)` prints the pre-restore backup's line | the same lines through `meerschaum.utils.debug.dprint` (the registry records them as `Notice::Debug`, never `take_warnings`); the `Failed to back up` error text is Rust's `io::Error` text, not Python's `[Errno N]` form. `read_config` / `write_config` / `get_keyfile_path` take `debug` and print nothing, as v4 (`write_config` never reads it) | `tests/test_config_registry.py::test_debug_prints_the_lines_v4_prints`, `config_write.rs::backups_record_the_debug_lines_v4_prints` |
| W15O-5 | `sync_files` ran for the `stack` key on the first `get_config('stack', ...)` | same, in Rust; a malformed `stack.yaml` or derivative file is warned with v4's text and left as is | no change in output (byte-compared with v4 on the Compose, Grafana and initdb files; the only difference is the v5 `system.engine` section inside the Compose `MEERSCHAUM_API_CONFIG`, G5) |

Not moved (and why): `edit_config` (the interactive `edit config` action loop: editor, prompts, rollback prompt) and
`config/_patch.py`, `config/_default.py`, `config/stack/*`, `config/paths.py` remain the SDK/Python data; the `edit config`
action loop is the CLI's (`meerschaum-cli`, wave 15 task N1). `get_config` micro-benchmark re-run in wave 15: PERF.md,
"Wave 15 (task O)" (no regression).

## Wave 15 (task N1): the residual `core_action` fall-throughs, group A (config, connectors, tokens, cache, pipes, misc)

Native now (nothing in the group reaches `python.rs::core_action`; `native/residual.rs` is the router `dispatch::run_action`
calls first, and `tests/residual_actions_a.rs::no_group_a_action_reaches_the_python_worker` runs every action with
`MRSM_PYTHON` pointing at a program that does not exist): `login` (`login.rs`), `os` / `sql` / `stack` (`os_sql_stack.rs`),
`bootstrap connectors|pipes|jobs` (`bootstrap.rs`), `register|copy|delete|start connectors` and `delete config`
(`connectors.rs`), `clear|delete cache` (`cache.rs`), `edit definition` (`definition.rs`), `register|edit|show tokens`
(`tokens.rs`), `show schedules|targets`, `drop|flush indices`, `flush pipes`, `api [start]`, `edit config --rollback`
(`residual.rs`), `start gui` (`gui.rs`). The prompts are `native/ask.rs` (`prompt`, `choose`, `get_password`, with v4's `default` /
`default_editable` / password semantics) over `Host::{read_line, read_line_interruptible, read_secret, read_line_prefilled}`;
`native/terminal.rs` has the terminal primitives (hidden input, an editable prefill, captured subprocess output with a timeout).
The reference for every row below is `.venv-ref/bin/python -s -E -m meerschaum <action>` against a temporary root.

Ctrl-C vs end of input: v4 raises `KeyboardInterrupt` for Ctrl-C (the wizards catch it to skip a question or abort) and
`EOFError` for the end of input (an error: `Failed to execute ... with EOFError`). `ask` returns an `ActionError` of class
`KeyboardInterrupt` only when a terminal reader reported Ctrl-C (`terminal::take_interrupt`), else `EOFError`; the wizards
(`bootstrap`, `login`, `edit definition`) catch the first and propagate the second, as v4 does. On a terminal, `ask` reads with
`reedline` so Ctrl-C reaches it; the plain `prompt`/`yes_no` readers still end the process on Ctrl-C (same as the rest of the CLI).

| Behavior | v4.1.4 | v5 | Why |
|---|---|---|---|
| Prompts without a terminal | `prompt_toolkit` prints `Warning: Input is not a terminal (fd=0).` and redraws the question several times | the question once, then the answer is read from stdin | the redraw is an artifact of `prompt_toolkit`'s non-tty mode; the questions, defaults and `[y/N]` suffixes are the same |
| `register tokens` / `edit tokens` on a SQLite instance | fail with a pydantic `ValidationError` (`scopes` comes back from SQLite as JSON text) | work: the token is registered / edited (the output and the prompts are v4's) | `// PARITY (divergence)` in `tokens.rs`: v4's failure is a bug, not a contract; on PostgreSQL both work |
| `edit tokens` with no terminal | prompts `Label:` / `Expiration:` / `Scope:` with the old value prefilled | the typed line is appended to the prefill, like `prompt_toolkit` does for piped input | `Host::read_line_prefilled` |
| SQL error text of `sql <label> "<bad query>"` | the SQLAlchemy / DBAPI message plus a `UserWarning` source line | the driver's message (`error returned from database: ... near "SELEC": syntax error`); then `None`, exit 0 as in v4 | `// PARITY:` `read()` returns `None` after logging, the action prints it and succeeds (`actions/sql.py`) |
| `sql ... --debug` | `dprint` lines (module names, durations) | none | module paths and timings of the Python implementation |
| Empty frame from `sql` | `|-----|` separator (tabulate sees no columns) | the same (`format/tabulate.rs`, zero-row frames drop the alignment colons) | fixed in this slice; `|:----|` was a v5 bug |
| `start connectors a b c` | the connectors are collected in a `set` (random order); the `Please login with the command ...` warning for API connectors that need one is printed too (W15N2-011) | argument order, duplicates removed; the login warning is printed too | the order cannot be matched |
| `stack config` | the compose project with `MRSM_CONFIG` carrying the v4 config | the same project; `MRSM_CONFIG` also carries the v5 `system.engine` defaults (`python_workers`, `inclusive_cap`, `flat_ledger`) | v5 config |
| `start gui` | tornado web terminal + a `pywebview` window around `/webterm/mrsm` | `start api --no-auth` on a free port in the background, a `pywebview` window around `/dash/`; without the `gui` extra it fails naming `pip install "meerschaum[gui]"` | the terminal lives in the console now; the window is the only part that runs Python (`python -c`, never the worker) |
| `delete cache` size | `humanize.naturalsize(get_directory_size(...))` | the same units (decimal, one decimal); the sum counts directories' own `st_size` like `rglob('*')` | environment dependent (a tmpfs directory is a few bytes, ext4 4096) |
| `login` for a connector defined by a URI (`MRSM_API_<LABEL>`) | `c.__dict__` has the credentials parsed from the URI | the connector's parsed attributes decide whether to prompt | `login.rs` builds the connector first |

Review follow-ups (wave 15 review, N1 items #2, #5, #12-#15, #23):

| Behavior | v4.1.4 | v5 | Why |
|---|---|---|---|
| `show|start|stop|restart daemons` | the CLI daemons of `_internal/cli/workers.py` (a warm interpreter per `experimental:cli_daemon`) | `show daemons` answers `No CLI daemons are running.`; `stop daemons` recreates the (empty) `.internal/cli` directory like v4 with no workers and succeeds; `start daemons` / `restart daemons` fail with `The CLI daemons are not used in Meerschaum v5: actions run natively in mrsm.` | there is no interpreter to keep warm; v4's lock/restart prompts only apply to running workers |
| `start pipeline ... --timeout-seconds N` | each step runs in a child interpreter (`venv_exec`) killed by `poll_process` after N seconds; `Failed to execute pipeline within N second(s).` (N printed as given, `1.0 second`) | the step runs in a child `mrsm __mrsm_pipeline_child__ <line> <patch json>` whose result comes back between `<MRSM_RESULT>` fences exactly like v4; same message, same kill | `// PARITY:` in `dispatch.rs::timed_pipeline_step` |
| `start connectors` on a down database | `_wrap_retry_connect` retries 50 times, printing the driver's error each time | keys are validated without connecting; each attempt builds the connector and tests it, so a late-starting service is picked up; a refused TCP connection fails the attempt at once (`connection to server at "h", port p failed: Connection refused`) instead of waiting for the pool's acquire timeout | tests start a listener late and use a never-open port |
| Password of `sql <label>` | `pgcli`/`mycli` get the URL with the password | the URL without the password and `PGPASSWORD` / `MYSQL_PWD` / `MSSQL_CLI_PASSWORD` + `SQLCMDPASSWORD` in the child environment; `sql <label>` no longer connects before starting the client | the password stays out of `ps` |
| `timed_input` (the wait of `start connectors`, `--min-seconds` loops) | `input()` under `SIGALRM`; at the end of input it sleeps the interval | polls stdin with the deadline, no reader thread (nothing is consumed after the call); at the end of input it waits out the interval | a thread blocked in `read` stole later input and EOF made loops spin |
| `api <label> <command>` output | streamed as the remote job logs | printed line by line as it arrives (channel from the task that runs `do_action`) | was buffered until the job ended |
| API-client warnings queue | n/a | installed only by the `mrsm` binary's `main` (`api_warnings::install`), removed by the server (`start api|mcp`), capped at 256; other hosts (the extension) keep `tracing` | the sink is process-wide and nothing drains it outside a CLI |
| Result of a job run by `mrsm` (`api <label> <action>`, `-d` jobs) | `Daemon._run_exit` ends the job log with `mrsm.pprint(result)` (a success tuple goes through `print_tuple`, no padding); v4's client stops reading when the status turns `stopped`, before that line, so `api` shows the result once | the job worker (`MRSM_JOB_RESULT_PATH` set) writes the result file and prints the tuple (`dispatch.rs::main`), so `show logs` ends with the result; `api` holds back the job's trailing result block (`TupleTailFilter`) and drops it when it equals the final result; a worker with no action (`mrsm -y`) fails with `The job has no action to run ...` instead of opening a shell | v4's client only avoids the duplicate by timing; a job whose worker never writes a result (killed, or `MRSM_JOB_PROGRAM` overrides) gets no result line in its log (the supervisor stays plain) |
| `start gui` server | tornado on the webterm host | `start api --no-auth` bound to `127.0.0.1` on a free port, stopped with SIGTERM (SIGKILL after 5 s); the unauthenticated loopback console is a documented choice | the server has no one-time-token mode |

Measured against v4 with identical output (modulo ids, timestamps, durations and ANSI): `show schedules|targets|tokens`,
`show connectors`, `register|copy|delete connectors` (including the already-exists parameter diff), `delete|clear cache`,
`os`, `sql` (read, exec, `--nopretty`, unknown label), `stack config` and the generated project files, `drop|flush indices`,
`flush pipes` (with and without `--begin`), `edit definition`, `edit config --rollback`, `bootstrap connectors|pipes` (the
wizards' prompts, defaults and results), `login --help` and the unknown-connector path.

Not covered by an automated test: `start gui` with a real `pywebview` window (only the missing-extra path), `sql` / `sql <label>` with
no query (opens `pgcli`, `mycli`, `litecli`, ... or the plain client on a terminal), a real `docker compose` (`stack up|down`;
`stack config` prints the project when `docker` exists and fails with the docker error otherwise), and Ctrl-C at a wizard prompt
(needs a pty). Observed in passing, not in this slice: `show data` prints `Last 1440.0 minutes of data` where v4 prints `1440`
(`native/show.rs`, the backtrack value arrives as a float).

`mrsm edit config <key>` with `MRSM_CONFIG` set (relayed from task N1): the native action now pops the key from the
in-memory config before the `get_config(key, write_missing=True)` lookup, as v4's `_edit_key` does
(`config.pop(key, {})`, `config/_edit.py:44`), and puts it back when the edited file does not parse. Before, a key an
env patch had loaded never reached the lookup that creates the file, so the editor opened a missing file and the action
reported `Could not parse key`. Test: `crates/meerschaum-cli/tests/actions.rs::edit_config_creates_the_key_file_when_mrsm_config_patched_the_key`.

## Wave 15 (task N2): the residual `core_action` fall-throughs, group B (plugins, packages, venvs, upgrade)

Native now (nothing in the group reaches `python.rs::core_action`; `native::subactions` answers the parent actions
`uninstall` / `verify` / `upgrade` without a valid subaction and a registry entry without a handler fails loudly instead of
falling back): `uninstall plugins` / `delete plugins` / `edit plugins` / `verify plugins` / `upgrade plugins` and the upload half
of `register plugins` (`plugin_admin.rs`: archive build with `.gitignore` handling, `register`/`delete` through the
`meerschaum-api` client), `show packages` / `uninstall packages` / `verify packages` / `upgrade packages` / `upgrade meerschaum`
(`packages.rs` over `pip.rs`; the dependency groups are the generated `packages_table.rs`, `scripts/rust/gen_packages_table.py`),
`delete venvs` / `verify venvs` (`venv_admin.rs`), `bootstrap plugins` (`bootstrap_plugins.rs` over the generated
`bootstrap_templates.json`, `scripts/rust/gen_bootstrap_templates.py`) and `start worker` (`subactions.rs`). `prune venvs` calls
the native `uninstall packages`. Python still runs only what imports a plugin: `verify plugins` and the post-mutation manifest
refresh start the worker when a Python plugin is installed; a native-only or empty plugins directory starts none
(`tests/residual_actions_b.rs` runs the forms that need no Python with `MRSM_PYTHON` pointing at a program that does not exist).
`MRSM_NO_AUTO_INSTALL` only stops implicit bootstraps (`uv venv --seed`, the repair in `verify packages`); `upgrade` / `install` /
`uninstall` always run. The reference is released 4.1.4 (`.venv-ref/bin/python -s -E -m meerschaum <action>`, run outside the
repository against a temporary root). Measured with identical output (modulo ids, paths and ANSI): `show packages <group>`
(pretty and `--nopretty`, `api` / `full` / `dash`, unknown group), `uninstall plugins` (no names, unknown name, refusal, `--yes`,
`--force`), `uninstall packages` (no names), `start worker` (no index, bad index), `delete venvs` (none, unknown name),
`verify venvs`, `verify plugins` (installed, missing, an import failure), `upgrade` / `verify` without a subaction, `upgrade
packages bogus`, `upgrade plugins` (none), `edit plugins` (no name), `bootstrap plugins` (the written `plugins/<name>.py` for
several answers, byte for byte).

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W15N2-001 | `start worker <n>` runs the experimental CLI daemon (`system.experimental.cli_daemon`) | the argument checks of v4 (`No worker index is provided.`, `Invalid worker index.`), then a failure saying the CLI worker is not used in v5 | v5 has no CLI daemon (spec: "No CLI daemon"); actions run natively |
| W15N2-002 | `verify packages` imports every package to read its version and repairs an old one | reads versions from `*.dist-info` in the `mrsm` venv, probes the host interpreter once with `find_spec` (nothing imported) and only reports `Verified N packages.`; a package found in the host interpreter is counted, never changed; `N` depends on what is installed, as in v4 | importing the packages executed their code; v5 does not modify the host interpreter outside `upgrade meerschaum` |
| W15N2-003 | `verify plugins` imports each plugin in the CLI process | a fresh worker imports them (`reload`), a native plugin counts as imported when the SDK opened it, a failing plugin's traceback comes from the worker | the CLI has no Python |
| W15N2-004 | `edit plugins` / `bootstrap plugins` / `edit definition` / `edit config` open `$EDITOR`, else install and run `pyvim`, which is also the fallback when the editor cannot be started | one helper (`native/editor.rs`): `$VISUAL`, else `$EDITOR`, else `vi`, split into words and started directly (never through a shell); an editor that cannot be started is an error (`Unable to open editor '<program>' for '<path>': ...`), never a silent success | no package installs behind an editor prompt; the user must see why nothing opened |
| W15N2-005 | `register plugins` asks for a changed description in a `prompt_toolkit` multiline box (`Esc+Enter`) | one line; the default is the plugin's module docstring read from the source text (so a plugin that fails to import still offers it) | the multiline box needs a terminal; no import in the CLI |
| W15N2-006 | `delete venvs` without names lists in `os.listdir` order | sorted | determinism |
| W15N2-007 | `verify venvs` asks `venv_exists`/`activate_venv` of Python | file-level: `<venv>/bin/python<major>.<minor>` must exist and answer `-V` within a second; a missing or dangling interpreter re-creates the venv, a link naming another version is renamed (as v4) | no Python in the CLI |
| W15N2-008 | `bootstrap plugins` with `MRSM_NOASK` loops forever asking for the action name | ends with an error | v4 hangs |
| W15N2-009 | `upgrade packages <group> --venv <name>`: `venv` is ignored (always `mrsm`) | the same (`// PARITY:`) | v4 quirk kept |
| W15N2-010 | `verify packages` warns `...Try installig via Meerschaum with install packages '{install_name}'` with the literal braces | the same text (`// PARITY:`) | v4 quirk kept |
| W15N2-011 | an `api:` connector without credentials prints `Please login with the command ...` (`warn`) before the first request | RESOLVED (N1): `meerschaum_api::client::warnings` (`set_warning_sink`) replaces every client `tracing::warn!` in `login`, `token`, `pipes`, `plugins`, `tokens` and `instance` (each has a v4 `warn()` counterpart in `connectors/api/*.py`, same text, e.g. `Failed to check if {pipe} exists:\n{text}`, `str(detail)` without JSON quotes); the CLI installs a sink in `native::session` (`native/api_warnings.rs`) that queues the message and `Runtime::print`/`eprint` print it through the formatter on stderr (v4's text and style, `Please login` byte-identical to `.venv-ref`, once per connector per process); tests `an_api_connector_without_credentials_warns_to_login_once` (CLI) and `client_warnings_reach_the_host_sink_with_v4_text` (`meerschaum-api/tests/warnings.rs`) | v4's `warn(msg)` without `stack=False` also prints the Python source line (`path:lineno: UserWarning: ...`); v5 prints the message line only. Failures about `mrsm:` pipes (the engine's own state, which the API refuses and v4 never probes) stay on `tracing` (`warn_pipe`), or `sync pipes -i api:x` would print one warning per state pipe. `create_pipe`/`get_pipe_id`/`get_pipe_rowcount` parse-failure warnings of v4 have no Rust site (the response is parsed leniently) |
| W15N2-012 | prompts read from a pipe: `prompt_toolkit` prints `Warning: Input is not a terminal (fd=0).` and redraws the question | the question once, the answer from stdin | `prompt_toolkit` artifact (same as the group A rows) |
| W15N2-013 | `upgrade meerschaum` runs `pip install meerschaum[...]` in the interpreter that runs `mrsm` (`venv=None`) with `--break-system-packages` | the interpreter is `MRSM_PYTHON`, else the `python3` / `python` next to the (symlink-resolved) `mrsm` binary (a `pipx`, `uv tool` or `pip` environment); it must have the `meerschaum` distribution installed (checked with `python -E`, so `PYTHONPATH` does not count) or the action refuses with a message naming `MRSM_PYTHON`; `uv` gets `--python <that interpreter>`; `--break-system-packages` and every other installer flag is added only when passed explicitly (`sub_args`) | v4's `python3`-on-`PATH` guess could upgrade the system Python |
| W15N2-014 | a plugin name is used as given in paths (`uninstall plugins ../.. --force` removes `venvs/../..`; `mrsm` is the shared core venv) | one check (`plugin_files::validate_plugin_name`, applied by `split_repo`, `bootstrap plugins`, `install plugins` before any request and the `plugin:` requirements): empty, `/`, `\`, NUL, `..`, a leading `.` and `mrsm` are refused with `Invalid plugin name: '<name>'` | path traversal |
| W15N2-015 | (n/a) | `tests/generated_tables.rs` fails when `native/packages_table.rs` or `native/bootstrap_templates.json` differ from `scripts/rust/gen_{packages_table,bootstrap_templates}.py`; both generators read the Python sources as text (no `meerschaum` import), so `cargo test --workspace` and `scripts/rust/ci.sh generated` check them without the extension | stale generated data |
| W15N2-016 | `edit plugins` of a plugin that does not exist offers to bootstrap it and ignores the wizard's result | the wizard's failure (invalid name, an editor that cannot be started) is the action's result | a failed editor start must not look like success |
| W15N2-017 | `delete venvs <name>` joins the name into `venvs/<name>` unchecked (`delete venvs ../x -y` removes a directory outside `venvs/`) | a name must be one plain directory name (not empty, no `/`, `\`, NUL or `..`, no leading `.`) or the action fails with `Invalid venv name: '<name>'`; `mrsm` is still never deleted | path traversal (v4 has the same flaw) |
| W15N2-018 | the editor is `os.environ.get('EDITOR', 'pyvim')` passed as one argument (`call([EDITOR, path])`); `$VISUAL` is not read, `EDITOR="code -w"` fails to start and falls back to `pyvim` | `$VISUAL` first (the Unix convention), then `$EDITOR`, split into words with `shlex` rules (still no shell: `;`, `$()` and globbing stay literal arguments), then `vi`; a start failure is reported, never retried with another editor | usability (`code -w`, `emacsclient -t`) without a shell; no package installs behind an editor prompt |

Not covered by an automated test: `verify packages` repairing an old
version (needs a wheel older than a pinned minimum), and Ctrl-C at the `bootstrap plugins` prompts (needs a pty). A fixture fix
rides along: `write_wheel` in `tests/plugin_lifecycle_support/mod.rs` now lists the wheel's files in `RECORD`, otherwise
`uv pip uninstall --target` removes the dist-info but leaves the package.

## Wave 15 (task L): sync paths

`Pipe.sync()` under `MRSM_CORE=rust` runs `_core.sync_pipe` for everything a caller can pass; the "Not routed yet" list of
Wave 13 (task H) is closed except for the rows below. The façade (`_sync.py::_sync_native`) builds a native context
(`_native_context`: the instance's `_core.SqlConnector`, the other Rust SQL connectors the process holds, the attributes of
connectors defined ad hoc) and `src/facade.rs` builds the engine's registry: native connectors (`sql:`, `api:`, `valkey:`) by keys
or attributes, Python connectors (`plugin:`, `@make_connector`, custom instances) through the worker adapter on the process's worker
session (`MRSM_PYTHON`, defaulting to the façade's interpreter; started only when a call reaches Python). `Pipe.verify` uses the same
session (`verify_api.rs`; `debug`, forwarded keywords, plugin and API/Valkey sources, references, templates and `MRSM{}` symlinks
all run natively; `_rust_verify_connectors` is replaced by `_rust_verify_context`).

Native now: no-data syncs (`sql:` metadefinition incl. the in-place `INSERT ... SELECT`, `plugin:` `fetch()` returning a frame, a list,
a generator, `chunk_hook` results, `True` or `None`, a plugin's own `sync()`, `@make_connector` sources, `api:` / `valkey:` sources);
strings, tuples, generators and iterables of chunks (`sync_chunks` semantics, first chunk alone, three attempts per later chunk),
Polars `LazyFrame`; `force` / `retries` / `min_seconds`, `workers`, `blocking=False` with callbacks (a thread around the native call,
same `Spawned asyncronous sync` message), `debug=True` (the `tracing` sink prints the engine's debug events; the sync message
carries v4's `on table ... in N seconds.` suffix), plugin keyword arguments; temporary pipes, `reference`s, `{{ }}` templates and
`MRSM{}` symlinks (the engine resolves references and templates against the registered state of the pipes they name, fetched through
the registry: `engine::sync::sync_inner`); `mssql`, `duckdb`, `oracle` (see W15L-006) and in-memory SQLite instances; `api:`, `valkey:`
and custom `@make_connector` instances; `SyncResult.changed_intervals`; `mrsm.SyncResult`.

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W15L-001 | a tuple passed as data is taken for a chunk generator and raises `TypeError: 'tuple' object is not an iterator` | the tuple is rows, as `pd.DataFrame` takes it (`// PARITY` note in `_sync.py`) | v4 bug, not contract |
| W15L-002 | Dask DataFrames sync | `TypeError` naming `migrating-to-v5` (D20); `get_data(as_dask=True)` is still the Python body until wave 16 | Dask is removed in 5.0 |
| W15L-003 | `SyncResult.changed_intervals` did not exist | the grid cells (width = the chunk interval, epoch-aligned) of the rows handed to the instance, merged into runs; a superset of the rows that changed (the ledger's exact delta needs a second `filter_existing`, which a bare `sync()` does not pay for); an edge outside Python's datetime range (a row at year 1) is `None`; `[]` when nothing was written | engine ledger semantics are for graph runs only |
| W15L-004 | with a SQLite source, v4's chunk label carries the stored microseconds (`2026-01-01 00:00:00.000000`) because the chunk still holds text | the label is built from the parsed datetimes (`2026-01-01 00:00:00`); `tests/test_native_sync.py::_untimed` normalizes it | the Rust driver parses `TIMESTAMP` columns at the read |
| W15L-005 | PostgreSQL `NUMERIC` read back: `Decimal('3')`, `Decimal('2.5')`, `Decimal('-0.10')` (each value keeps the scale it was stored with; `NaN` / `Infinity` are `Decimal` objects) | **RESOLVED.** An unconstrained-or-declared PG `NUMERIC` now travels as decimal text (`ColumnKind::NumericText`: `Utf8` tagged `meerschaum:dtype=numeric`, decoded from the binary format, `driver/postgres.rs::pg_numeric_text`) and the kernel builds an exact `Decimal` per value (arbitrary precision: a 40-digit value is exact; `NaN` / `Infinity` / `-Infinity` are kept; a database NULL stays null). The old `Decimal128` carried one scale per batch (the largest in the batch) and rounded later batches to the first batch's scale. MySQL `DECIMAL(p,s)`, MSSQL and DuckDB columns have one declared scale for every value, so they keep `Decimal128`. **Driver contract changed on purpose:** `tests/driver_postgres.rs` now asserts `Utf8` tagged `numeric` with the texts `1.25` / `-10.5` (the old `-10.50` was the padding) and the NaN / Infinity / 40-digit / NULL cases; `driver_mysql.rs` and `frame_roundtrip.rs` (kernel Frame to Arrow, a different path) are unchanged and pass. `get_pipe_docs` keeps `Decimal('NaN')` as a value (`frame_docs_py`). Tests: `test_numeric_values_read_back_like_v4` (sqlite, postgis, mariadb, mysql; mixed `3`, `2.5`, `-0.10`, 40 digits on PostgreSQL; python mode equals rust mode), `test_postgres_numeric_special_values_read_back_like_v4` | exactness per value, as `psycopg2` |
| W15L-006 | `oracle` instances use `python-oracledb` (thin mode, no client library) | `oracle` is routed to the Rust `oracle` driver (ODPI-C, thick mode: needs `libclntsh`). Without `libclntsh` the sync and verify entry points keep the v4 Python body, announced by a warning once per process (`_native_unavailable`, `_no_rust_driver`): the one explicit exception to "no silent fallback"; every other unavailable connector is a failed `SyncResult` / `verify` tuple. Wave 16 replaces it with an error or a thin driver (orchestrator decision: accepted) | the Oracle test skips without the client |
| W15L-007 | `sql:memory` is one SQLAlchemy in-memory database | **RESOLVED.** `SQLConnector._shared_memory_database()`: the connector's `database=':memory:'` becomes one database file (a private temporary directory under `/dev/shm` when it exists, removed when the connector is collected or the process exits) that SQLAlchemy (`read()` / `exec()` / `.engine`) and the Rust pool both open, so `Pipe` methods and the SQLAlchemy side see the same tables. This holds in `python` mode too (`_create_engine.py` opens the same file for SQLAlchemy whichever mode runs): one database for both paths in both modes is the intent, and v4 itself had only the SQLAlchemy side. The directory is removed by the creating process only (`_remove_memory_directory` checks the pid: a forked child that exits normally must not delete its parent's database; `test_a_forked_child_does_not_delete_the_memory_database`). A named `cache=shared` memory URI cannot work: the extension statically links its own SQLite (no `libsqlite3` in its `NEEDED` list) and `_sqlite3` loads the system one, and two SQLite libraries do not share a cache. Residual: a plugin's Python worker rebuilds the connector from its attributes and gets its own file (it never saw the host's memory database either). Test: `test_in_memory_sqlite_is_one_database_for_pipes_and_sqlalchemy` | D16 and one database |
| W15L-008 | a reference pipe's `Pipe.parameters` resolve lazily in the Python object | the Rust verify resolves `reference`s / templates against the instance before reading `columns` / `dtypes` (`verify_api.rs`), the one-pipe sync resolves them in `sync_inner` (before this, a one-pipe sync of a pipe whose `columns` came from a `reference` treated it as having no datetime column and persisted a naive `dtypes: {dt: datetime64[us]}`); every SQL connector of a call can resolve a cross-instance template against the others (`facade::install_instance_provider`) | the engine's `known` snapshot exists only in graph runs |
| W15L-009 | `MRSM{}` symlinks and every config read in a sync follow the live config (`set_config()`, in-place edits of `meerschaum.config.config`, `MRSM_CONFIG`) | **RESOLVED.** Every native entry point (`sync_pipe`, `verify_pipe`, the engine bindings) snapshots the live config with the interpreter lock held (`facade::publish_live_config`, files overlaid with the live dictionary) before releasing it and publishes it as `meerschaum_core::config::Config::current()`, which the engine's `resolve_parameters`, the template resolver and the engine bindings (`engine_api.rs`, which used to cache `Config::from_env()` forever) read; no runtime thread calls into Python for the config, no config file is parsed per call (`config_api::live_config` converts the live dictionary and `Config::bare` holds it; only the file of a top-level key never loaded is read, read-only, and stored in the dictionary), a `_core.SqlConnector` method whose pipe holds an `MRSM{}` symlink publishes the live config itself (`pipe_bridge::core_pipe`), so `prepare_pipe` never reads a stale snapshot, and the engine's defaults, backtrack and filter limits come from the call-time snapshot, not from what the connectors captured when built. Test: `test_mrsm_symlinks_follow_the_live_config` (two syncs with the configured column changed between them match v4 in both modes) | live config |
| W15L-010 | `Series.astype('Int64')` of an `object` column holding `Decimal('2.2')` raises, and the valkey instance then stores the column as `string` | the kernel's `pandas_astype` truncated it to `2` for every integer target. pandas' masked integers (`Int64`, `UInt8`, ...) now refuse a fractional float or decimal; the Arrow-backed dtypes keep their documented truncation (`crates/meerschaum-kernel/tests/astype_masked_int.rs`). Found by `tests/test_sync.py::test_sync_null_indices[valkey]` once the valkey instance ran natively | v4 oracle (pandas 2.3) |
| W15L-011 | `Pipe.sync()` of an unsupported value, or a pipe whose connectors cannot be built, fell back to the Python body with a debug line | `pyarrow` Tables and RecordBatches sync natively; anything else raises `TypeError` naming the accepted types (a generator's chunk too); an unbuildable instance or a SQL connector without a Rust driver is a failed result (Oracle without `libclntsh` excepted, W15L-006); the interpreter of the workers is passed to the worker session (`Session::set_worker_python`, `MRSM_PYTHON` still wins) and the process environment is never modified | no silent fallback |
| W15L-013 | the first plugin sync (or verify, or engine run) of a root hung forever with a defunct worker | **FIXED.** The façade waited on the runtime while holding the interpreter lock (`build_registry`: `python_manifest` / `native_plugins`, which start the Python worker on a root without a cached manifest; `register_adhoc`; the `engine_api` builder), while the worker's messages reach the `tracing` sink (`logging.rs`), which takes the lock on a runtime thread: a deadlock the worker's exit never resolved. Those calls run in `py.allow_threads`. Test: `test_a_fresh_root_plugin_sync_with_debug_does_not_deadlock` (fresh root, `debug=True`); the earlier reports of `tests/test_maintenance.py::test_get_size` hanging on a fresh root were this | no hang |
| W15L-014 | v4 raises `OSError: Could not activate virtual environment '<plugin>'` when a plugin's venv was never created | unchanged (plugin import uses `Venv(init_if_not_exists=False)`); `tests/test_native_sync.py` creates the venvs of its test plugins first (`plugin_venvs`, `init_venv`), so a fresh `MRSM_ROOT_DIR` works in both modes | test hygiene |
| W15L-015 | an `api:` client warning about the engine's own `mrsm:` pipes (probes with no v4 counterpart) | stays off the Python user's screen: it uses the `tracing` target `meerschaum_api::state_pipes`, which `logging.rs` drops (the CLI also keeps it on `tracing`) | silent as in the CLI |
| W15L-012 | `Pipe.sync` of a `numeric` column holding `Decimal('NaN')`, `None` and `Decimal('±Infinity')` (v4.1.4 against postgis, SQLite, MariaDB and MySQL, temporary root) | the same in `rust` mode (checked row by row: `n IS NULL`, the stored text, `get_pipe_docs`): `NaN` and `None` are stored as SQL NULL (`get_data(as_docs=True)` returns `None`, not `Decimal('NaN')`; the NaN sentinel only exists in frames), `±Infinity` is stored as `Infinity` / `-Infinity` on PostgreSQL `NUMERIC` and SQLite (text) and read back as `Decimal('Infinity')`; MySQL and MariaDB cannot store an infinity, so a sync holding one fails in both implementations and a sync with only `NaN` / `None` stores NULLs. No Rust change was needed. Test: `test_nan_and_infinity_numerics_are_stored_like_v4` | v4 behavior kept |

Carried over from the wave 13 review: MySQL / MariaDB coverage of `enforce: false` is added
(`crates/meerschaum-sql/tests/instance_enforce_false.rs`, the `mysql_family_*` tests on the MySQL 5.7 and MariaDB containers). Their
expectations are Rust-only (the v4 oracle cannot reach those containers): `datetime` columns come back naive, so a naive axis updates
the same key, a UTC-aware axis (and an integer axis, cast to the pipe's UTC `datetime` dtype) raises pandas' merge error with the
delta on the left (`datetime64[us, UTC] and datetime64[us]`), `enforce` on updates UTC rows.

**Still reaches Python (exact list; L2 now owns the `Pipe` method bodies below).** `Pipe.get_data`, `get_backtrack_data`, `get_rowcount`, `get_sync_time`, `get_columns_types`,
`get_columns_indices`, `enforce_dtypes`, `filter_existing`, `fetch`, `deduplicate` keep their Python wrappers (pandas sorting of the
result, `add_missing_columns`, `as_iterator` / `fresh` / `as_dask` handling, the generic chunked deduplication loop for instances
without `deduplicate_pipe`); the work itself is done by the routed `SqlConnector` methods (and the bound `_core.enforce_dtypes` /
`filter_unseen_df`), so a `sql:` instance never reaches SQLAlchemy through them. They are the façade wave 16 keeps, not v4 logic with a
Rust twin; a non-SQL instance (`api:`, `valkey:`, custom) reads through its Python connector (the `valkey` package is no longer
installed by default, D20). `Pipe.fetch()` called directly on a `sql:` source uses `SQLConnector.fetch` (routed to `get_pipe_metadef` in
Rust). Oracle without `libclntsh` (W15L-006). `get_data(as_dask=True)` (W15L-002).

**Cost of the per-call config snapshot.** `config_api::live_config` (no file parse) costs about 0.07 ms per publish (a `_core.SqlConnector.pipe_exists` call on a pipe with an `MRSM{}` symlink, 0.257 ms, against 0.191 ms without); a small native `Pipe.sync()` of one document on SQLite is 40-42 ms median before and after (the previous publish, which parsed the config files and the whole dictionary per call, did not stand out against the rest of the call at that size; it is no longer paid at all).

**Verification.** `MRSM_CORE=rust` and `MRSM_CORE=python`, `MRSM_TEST_FLAVORS=sqlite`: the whole `tests/` suite (ignores as in CLAUDE.md) 947 passed /
10 skipped (rust), 928 passed / 29 skipped (python). `tests/test_native_sync.py` + `tests/test_w9_verify.py` with
`MRSM_TEST_FLAVORS=sqlite,postgis,mssql,valkey,oracle`: 158 passed, 1 skipped (Oracle: no `libclntsh` on this host). New tests:
plugin sources (frame / generator / `chunk_hook` / `None` / `True`), a plugin's own `sync()`, a `@make_connector` source, a SQL
definition source, forwarded plugin keywords, `blocking=False`, `changed_intervals`, `mrsm.SyncResult`, in-memory SQLite, duckdb and
mssql instances (rows and counts equal the SQLite python-mode run), a custom instance (`w15lmem`), a valkey instance (raw RESP), an
`api:` instance against a loopback `mrsm start api`, a crashing worker and a worker killed while idle (no hang), verify variants
(`debug`, `force`, plugin keyword, reference pipe) against python mode. Test plugins: `tests/plugins/w15l_plugsrc.py`,
`w15l_plugsync.py`, `w15l_plugdie.py`, `w15l_custom.py`. Rust: `cargo test -p meerschaum-py -p meerschaum-worker -p meerschaum-engine`
(`MRSM_TEST_PG_URI`), `cargo test -p meerschaum-sql --test instance_enforce_false` (all flavors), clippy `-D warnings` on those crates.

## Wave 15 (task L2): the `Pipe` methods over an instance run in Rust

Under `MRSM_CORE=rust` the Python `Pipe` methods that wrap an instance connector call one `_core.pipe_*` binding each;
the algorithms (v4's bodies in `core/Pipe/_data.py`, `_dtypes.py`, `_fetch.py`, `_deduplicate.py` and the `get_sync_time` /
`filter_existing` region of `_sync.py`) are `crates/meerschaum-py/src/pipe_methods.rs` (async, over the `InstanceConnector`
trait) and the bindings `src/pipe_methods_api.rs` (gate features `pipe_get_data`, `pipe_get_data_chunks`,
`pipe_get_backtrack_data`, `pipe_get_rowcount`, `pipe_get_sync_time`, `pipe_filter_existing`, `pipe_enforce_dtypes`,
`pipe_infer_dtypes`, `pipe_fetch`, `pipe_get_backtrack_interval`, `pipe_deduplicate`). `MRSM_CORE=python` keeps the v4 bodies
below each guard as the oracle until wave 16 deletes them.

**One context for every instance.** The façade builds the call's context once (`_data.py::_rust_context`, on top of L's
`_native_context`): the pipe instance's `_core.SqlConnector` (or none), the other Rust SQL connectors the process holds (a SQL
source), the attributes of connectors defined ad hoc, and, for a SQL instance, the Python pipe's cached `get_columns_types()`
(v4 caches it for `pipes:dtypes:columns_types_cache_seconds`). `pipe_methods_api::open` turns it into a `Reader`: a Rust SQL
instance is reached directly (no config file is read); `api:` / `valkey:` instances are the Rust clients, `@make_connector` and
`plugin:` instances go through the worker adapter (D12), exactly as `Pipe.sync` does.

**What the generic path does.** A SQL instance reads through `SqlConnector::get_pipe_data_and_state`, which already carries
`Pipe.get_data()`'s post-processing (column selection, null-filled missing columns, dtype enforcement, sorting, `limit`); every
other instance goes through the Rust port of v4's body: `exists`, `get_pipe_data` / `get_pipe_docs`, `select_columns` /
`omit_columns` (with v4's two warnings), `add_missing_columns`, `Pipe.enforce_dtypes`, the `_sort_df` ordering and `limit`. The
frames then take the route v4's did: pandas through `read_hints`, then `Pipe.enforce_dtypes(df, dtypes=pipe_dtypes)` through the
kernel binding, whose output rules decide the pandas backends. `as_iterator` is `_core.PipeChunks` (lazy: the plan
`min_dt` / `max_dt` / `get_chunk_bounds` and one read per chunk, as `_get_data_as_iterator`), wrapped in a generator by the façade
(`types.GeneratorType`, what callers test). `deduplicate` calls the instance's own `deduplicate_pipe` (SQL) and, for an instance
without one (the trait's "not supported" message, as `hasattr` was in v4) or `_use_instance_method=False`, the chunk loop of v4
(bound time, `get_chunk_bounds`, per-chunk `get_data` of the index columns, `clear`, `sync`, rowcount check, one retry pass,
v4's messages).

**Still Python (exact list).** Nothing a caller can reach under `rust` needs a v4 body, except two documented cases that wave 16
turns into errors:

- `get_data(as_dask=True)`: a `TypeError` naming `migrating-to-v5` under `rust` (D20). The Dask path exists only in the `python` oracle.
- An instance whose pipe has no usable connector: a SQL connector without a Rust driver is a `RuntimeError` naming the reason; Oracle
  without `libclntsh` and an instance that cannot be built (`instance_connector` is `None`) keep v4's neutral result (`None`, `0`,
  `{}`) until the bodies go (W15L-006, L's `_native_unavailable`).

Closed in the follow-up: the connector's own `get_backtrack_data` (the `InstanceConnector::has_backtrack_data` capability, true for a
Python connector that defines the method: v4's `hasattr`); `fetch(sync_chunks=True)` / `fetch(chunk_hook=...)` (a Python source gets the
worker's chunk hook, which syncs each chunk natively through the engine's callbacks, or the caller's hook per chunk; a `sql:`, `api:` or
`valkey:` source ignores both, as in v4, where `SQLConnector.fetch()` hands back its chunk generator); `get_sync_time(remote=True)` /
`get_rowcount(remote=True)` for every source (a Rust `sql:` connector, the `api:` / `valkey:` clients, plugin and custom sources through
the worker with `Connector::get_sync_time_remote` / `get_rowcount_remote`; a source without the method warns and gives `None` / `0`, a
`get_pipe_rowcount()` without a `remote` keyword warns "do not support remote rowcounts" and gives `0`, as v4); input types (the boundary
of `Pipe.sync`: `is_accepted_data` / `unsupported_data_error`, Arrow tables and NumPy arrays included, anything else a `TypeError`
naming the accepted types); `deduplicate(workers=N)` (chunks run concurrently, `get_num_workers` bounds them).

Thin façade that stays (arguments and caches, no algorithm): `Pipe.exists` (the cache), `get_columns_types` / `get_dtypes` (the cache
and `apply_patch_to_config` of the parameters, `_attributes.py`, owned by O), `parse_date_bounds` (`dateutil` for text bounds; the timezone /
integer-axis rules are `engine::sync::parse_date_bound`, used by the Rust methods), `get_doc` / `get_docs` / `get_value` (a `get_data` call
each), `get_size`, `get_chunk_interval` / `get_chunk_bounds` / `get_chunk_bounds_batches` (routed in wave 8), `get_bound_interval` /
`get_bound_time` (`_verify.py`, L's file: the Rust `deduplicate` has its own port).

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W15L2-001 | `Pipe.enforce_dtypes(<list / dict / tuple / text>)` builds a DataFrame with `pd.DataFrame` + `parse_df_datetimes` and returns pandas' inferred backends (`Int64`, `int64[pyarrow]`, `object`) | the kernel's `pipe_enforce_dtypes` result through `read_hints` (`int64`, `float64`, `string`); values, column order and the datetime dtype are equal | pandas' inference of Python objects is not reproducible without pandas; the sync path (which never returns the frame) is exact. `tests/test_pipe_methods_native.py` compares dtype families |
| W15L2-002 | `filter_existing()` returns the `unseen` / `update` frames of a pandas merge against the existing rows: a string column whose existing backend is `large_string[pyarrow]` (a custom instance returning Arrow-backed frames) merges to `object` | `string` (the declared dtype's pandas type); SQL instances (and every instance with `string` / `object` existing frames) are equal | the merge's dtype rules depend on both frames' pandas backends; the kernel's diff is value-exact |
| W15L2-003 | `Pipe.fetch()` from a `sql:` source returns `read(as_iterator=True)` chunks whose timestamps are what SQLAlchemy returns (SQLite: text) | a generator of chunks of `system:connectors:sql:chunksize` rows with parsed datetimes (the Rust driver, as in W15L-004); a plugin `fetch()` that returns a list, a dictionary or a generator comes back as one DataFrame / a generator of DataFrames; a caller's `chunk_hook` on a *plugin* source runs on the host for every chunk a generator yields (v4 passes the callable into the plugin, which a worker process cannot receive) | the sync path handles every shape natively; a direct `fetch()` call is rare |
| W15L2-004 | `deduplicate(workers=N)` maps the chunks over a thread pool | the chunks run up to `workers` at a time (`Pipe.get_num_workers()`, so one for a connector that is not thread-safe) on the runtime; messages and results come back in chunk order, as `pool.map` | |
| W15L2-005 | a non-SQL instance's `get_columns_types()` is cached on the Python pipe for `columns_types_cache_seconds` | the native methods ask the instance (one extra `get_pipe_columns_types` per call for `api:`, `valkey:` and custom instances; a SQL instance keeps the Python cache and the Rust driver's own) | the Python connector classes of `api:` / `valkey:` need packages the base wheel does not ship (D20) |
| W15L2-006 | `Pipe.exists()` is cached (`pipes:sync:exists_cache_seconds`) | the native methods ask the instance (a SQL instance caches table metadata in the driver) | same |
| W15L2-007 | `get_data(**kw)` forwards unknown keywords to the instance (`chunksize`, plugin arguments); `fresh` skips the local cache | forwarded (see W15L2-009); `fresh` is ignored | the native reads have no local cache |
| W15L2-008 | `infer_dtypes(persist=True)` on a missing table returns `{}` without editing; on an existing one edits the pipe | same (an empty result is taken for a missing table) | an existing table always has columns |

Quirks reproduced on purpose (v4 behavior of the instance, visible through the new routes): a Valkey instance's `end` is
inclusive (`ZRANGEBYSCORE`), its `get_pipe_docs()` ignores `limit` / `order` and its `get_sync_time()` ignores `params`; the
generic `deduplicate` pluralizes the final failure message with the retry count of failed chunks
(`// PARITY` in `pipe_methods.rs`).

**Verification.** `MRSM_CORE=rust` and `MRSM_CORE=python`, `MRSM_TEST_FLAVORS=sqlite`, the whole `tests/` suite (ignores as in
CLAUDE.md): 1127 passed / 20 skipped (rust), 1108 passed / 39 skipped (python). `tests/test_pipe_methods_native.py` (new: every
method, every keyword above, on SQLite, PostgreSQL schema `w15l2`, the custom `w15lmem` instance and Valkey 6399; python mode is
the oracle where it can run, Valkey is compared with the same data on SQLite; the follow-ups add the `w15l2back` connector's own
`get_backtrack_data`, remote reads of `w15l2src` / `w15l2nosrc` plugin sources and a Valkey source, `fetch(sync_chunks=True)` and
`fetch(chunk_hook=...)` of a plugin source, the `TypeError` and Arrow-table boundary, `workers` (timed) and PostgreSQL `NUMERIC` specials):
289 passed / 26 skipped with `MRSM_TEST_FLAVORS=sqlite,postgis,valkey`. Rust: `cargo test -p meerschaum-py -p meerschaum-connector
-p meerschaum-worker` (8 new tests in `pipe_methods.rs`: the generic path equals the SQL pipeline on every `get_data` variant, missing pipes,
sync times and rowcounts, chunk plans, backtrack, `filter_existing`, the generic `deduplicate`), `cargo clippy -p meerschaum-py
-p meerschaum-connector -p meerschaum-worker --all-targets -- -D warnings`. `tests/test_pipe_methods_native.py` creates the plugin venvs it needs (`w15l_custom`, `w15l2_custom`) and runs on a fresh temporary root.

PostgreSQL `NUMERIC`: `pipe.get_data()` keeps `Decimal('Infinity')` / `Decimal('-Infinity')` in frames and documents and `Decimal('NaN')` in
frames. `Pipe.sync` stores `Decimal('NaN')` as `NULL` (`SELECT n IS NULL` is true), so the documents hold `None` there; that is the sync
path (task L), not the read.
temporary root makes `get_connector('w15lmem')` return `None`).

**Second review follow-ups (wave 15, L2).**

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W15L2-009 | `get_data(**kw)`, `get_backtrack_data(**kw)` and `deduplicate(**kwargs)` pass unknown keywords to the instance's methods; `get_data(begin='')`, `begin_add_minutes`, `end_add_minutes`, `replace_nulls` and `skip_existing_cols_check` reach a SQL instance's `get_pipe_data_query` | the same: a SQL instance reads those five natively (`SqlConnector::get_pipe_data_and_state_with`), a custom Python instance receives every keyword through the worker (`InstanceConnector::with_call_kwargs`); `get_backtrack_data` has no fallback body (every keyword is forwarded or named) | review #6, #7 |
| W15L2-010 | `filter_existing()`: the `unseen` and `update` rows come from a merge of `delta.fillna(pd.NA)`: a `Decimal('NaN')` in an object column is `None`; `Series.apply(attempt_cast_to_numeric)` of an integer column with nulls hands pandas floats, so a `3` is `Decimal('3.0')` | reproduced at the boundary (`pipe_methods_api::nan_decimals_to_null`) and in the kernel's numeric cast (`cast_column`, `// PARITY:`) | review #8 |
| W15L2-011 | `enforce_dtypes()` of a `datetime64[ns, UTC]` frame stays `ns` (`coerce_timezone` keeps a Series' unit) | `kernel_api::keep_datetime_unit` restores the input column's unit; a frame that was not a datetime column takes the pipe dtype's unit (`us`) | review #8; the kernel's values carry no unit |
| W15L2-012 | `get_data(as_docs=True)`: a SQL `NULL` is `None` for every dtype; `Decimal('NaN')` / `Decimal('±Infinity')` of a PostgreSQL `NUMERIC` are `Decimal`s | the same: a float `NaN` (the kernel's float null) is `None`, a `Decimal` special stays a value (the datetime's `tzinfo` is `datetime.timezone.utc`, v4's psycopg gives `ZoneInfo('Etc/UTC')`; the two compare equal) | orchestrator gate finding |

A regression found on the way and fixed: `config_api::live_config` (task O) built its `Paths` from the root alone, so a native call's config had no `MRSM_PLUGINS_DIR`: the worker manifest was empty and every custom or plugin connector failed with "No instance connector is registered". It now takes the plugins directories from `meerschaum.config.paths.PLUGINS_DIR_PATHS`.

**Third review follow-ups (wave 15, L2).**

| Id | v4 behavior | v5 behavior | Why |
|---|---|---|---|
| W15L2-013 | `filter_existing()` on PostgreSQL reports a stored row whose nullable `int` is `NULL` as changed (the `NULL` read back compares unequal with the incoming `NaN`) | no change is reported for an unchanged row | v4 quirk not reproduced; `tests/test_pipe_methods_native.py::_special_rows` keeps the stored rows non-null |
| W15L2-014 | `get_data(as_docs=True)` on a PostgreSQL-family instance returns a geometry column as the hex EWKB text (psycopg); on SQLite as shapely objects; `filter_existing()` of a declared `geometry` column holding WKT text returns a GeoDataFrame `geometry` column, of shapely objects an `object` column | the same (`Reader::raw_geometries`; the WKT columns of the input decide the `geometry` hint) | review round 3 |
| W15L2-015 | an instance whose Rust build failed runs the v4 SQLAlchemy bodies | `RuntimeError` naming the cause ("no usable Rust driver") from every `Pipe` read; Oracle without `libclntsh` keeps the v4 body with a warning once per process (as `Pipe.sync`), W15L-006, until wave 16. The reason is structured (`_sync.NativeUnavailable.kind`: `oracle_client`, `no_driver`, `unbuildable`), never matched on its text | review round 3 |
