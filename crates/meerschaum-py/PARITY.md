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
| `core/Pipe/_attributes.py` (`get_parameters`, `parameters`, `columns`, `dtypes`, `indices`, `get_precision`, `update_parameters`, `target`) | `_core.Pipe` keys / raw parameters / `target` (bound); `_core.get_parameters(core_pipe, pipe)` (gate feature `get_parameters`) resolves references and `{{ Pipe() }}` through `_core.Pipe.resolved_parameters` and returns the `_symlinks` map; `MRSM{}` substitution still runs through the Python `search_and_substitute_config` afterwards (the Rust call has no config handle in the façade) | **Routed** (wave 8, top-level call under `MRSM_CORE=rust`). Open: Rust-side `MRSM{}` once the config registry owns the Python dict |
| `core/Pipe/_cache.py`, `_edit.py`, `_register.py`, `_delete.py`, `_drop.py`, `_clear.py`, `_index.py`, `_copy.py`, `_show.py`, `_bootstrap.py` | thin: instance calls (bound) plus Python-side caches and interactive prompts | Caches move into Rust with the engine; prompts stay SDK |
| `core/Pipe/_sync.py`, `_fetch.py`, `_verify.py`, `_deduplicate.py`, `_data.py`, `_dtypes.py`, `_compress.py`, `_maintenance.py` | `meerschaum-engine` / `meerschaum-worker` (sync, verify, fetch, filter_existing, get_chunk_bounds) and the kernel; `Pipe.verify` → `_core.verify_pipe(pipe, connectors, ...)` (`src/verify_api.rs`, gate feature `verify_pipe`); `Pipe.sync` has no `_core` binding yet | `Pipe.get_chunk_interval` and `get_chunk_bounds` are routed (wave 8). **`verify` is routed** (wave 9): `_core.verify_pipe` runs `meerschaum_cli::native::verify::verify_pipe_with` (the CLI's verify, every chunk synced by the Rust engine) on a registered-only, worker-less façade `Session` holding the `_core.SqlConnector`s of the pipe's instance and SQL source (`Session::register_sql`), so ad-hoc connectors built from Python work and the config registry is never read for connectors. Output goes to `sys.stdout` / `sys.stderr`; afterwards the Python pipe drops its table caches (`_invalidate_cache(hard=True)`, `_attributes_sync_time`). **Stays Python** (`_verify.py::_rust_verify_connectors` returns `None`): `debug=True`; any `**kwargs` other than `chunksize` / `check_existing` (they go to `pipe.sync()`, e.g. `force`, `retries`, `_inplace`, plugin arguments); a source that is not `sql:` (plugins, `@make_connector` connectors, API/Valkey sources: they need the worker or Python objects); an instance or source without a Rust connector (`mssql`, `oracle`, `duckdb`, in-memory SQLite, non-SQL instances); parameters with `reference` / `references`, any `MRSM{` symlink, a `{{ }}` template in `columns` / `dtypes` / `verify` / `precision`, or a cross-instance `{{ Pipe(..., instance=...) }}` template (the verification reads those before the engine resolves them). Fixed on the way (CLI too): remote sync times and row-counts are asked of the source connector, not the instance (v4 `self.connector if remote`); `check_rowcounts_only` of a cross-connector pipe reported 0 out-of-sync chunks before. Parity: `tests/test_w9_verify.py` (sqlite and postgis schema `w9q`, in-place and cross-connector, nine verify variants each: tuples, chunk bounds and rows equal in both modes). **Still open**: the `sync` entry |
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
and run the Python query builders or stub-`self` calls; the python-mode-only assertions in
`tests/test_live_parameters.py` (`test_python_mode_parameters_are_a_copy`); and every test that goes
through `Pipe.sync` / `verify` / `fetch` / `get_data` logic that is still Python (they pass under
`rust` because the instance calls are Rust, but the sync algorithm itself is the Python one).

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
| G7 | A date inside config YAML (`when: 2024-01-01`) is a `datetime.date` through `read_config(substitute=False)` (lossless), but `get_config` fails in python mode (`json.dumps` in `search_and_substitute_config`, as v4.1.4 does) and returns the string `'2024-01-01'` in rust mode (the registry is JSON-valued) | v4 quirk kept in python mode; the rust registry stores `meerschaum_core::Value` |

Fixture hygiene found by this work: `scripts/parity/run.sh` exports `MRSM_PARITY_REFERENCE`, which v4 reads as a
`parity:reference` connector the moment `meerschaum` is imported. `gen_yaml.py` therefore removes every `MRSM_*`
variable before the import and keeps the version for its own guard (the first recording had leaked the connector into
the recorded default config).

**Config ownership (deliverable 5): stopped at a clean boundary; nothing is duplicated.** `read_config`,
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
