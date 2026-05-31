# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repo.

## Commands

### Development CLI

`./scripts/mrsm.sh <args>` runs `python -m meerschaum` with `MRSM_ROOT_DIR=./test_root` and `MRSM_PLUGINS_DIR=./tests/plugins` — never touches real config.

### Running Tests

```bash
# Run all tests (requires Docker for databases)
./scripts/test.sh db "" <pytest-args>

# Run a single test file (no Docker needed for sqlite-only tests)
MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins python -m pytest tests/test_pipes.py -v

# Run a specific test
MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins python -m pytest tests/test_pipes.py::test_sync -v

# Limit to specific DB flavors (skips others)
MRSM_TEST_FLAVORS=sqlite python -m pytest tests/ -v
```

Test connectors in `tests/connectors.py`. Default flavor set: `api,timescaledb`; set `MRSM_TEST_FLAVORS=sqlite` for fast local runs without Docker.

**Test-harness gotchas (learned the hard way):**
- **⚠️ Plain `python -m pytest` imports the STALE `~/.local` install, not the repo.** There is a separate `meerschaum` installed at `~/.local/lib/pythonX.Y/site-packages/meerschaum` (e.g. v3.3.1) that shadows the working tree under a bare `python -m pytest`. It silently lacks new modules (`connectors/sql/_partition.py`) and has older behavior, so tests fail/pass for reasons that have nothing to do with your changes. **Always force the repo onto the path: prefix `PYTHONPATH=$(pwd)`** (or just use `scripts/test.sh`, which sets things up correctly). Verify with a throwaway test asserting `meerschaum.__file__` / `meerschaum.__version__`. This single confound can burn hours — check it FIRST when a test result seems impossible.
- **Flavor keys ≠ SQL flavor names.** `tests/connectors.py` defines `timescaledb`, `timescaledb_ha`, `postgis`, `mariadb`, `mysql`, `mssql`, `oracle`, `sqlite`, `api`, `valkey`. There is **no `postgresql` key** — the PostgreSQL-family test connector is `postgis` (and `timescaledb`). `MRSM_TEST_FLAVORS=postgresql` resolves to an empty connector set and `test.sh` exits before pytest runs.
- **`scripts/test.sh` hardcodes `--ff` and `-n=auto`** (line ~134-135), so it requires `pytest-cache` and `pytest-xdist`. Passing `-p no:cacheprovider` to it breaks `--ff` (`unrecognized arguments: --ff`). To run a single test with non-default pytest flags, invoke `PYTHONPATH=$(pwd) python -m pytest` directly with the `MRSM_*` env vars exported (don't go through `test.sh`).
- **`test.sh` runs one flavor at a time** and shares `test_root` + the test-API port 8989 — never run two `test.sh` invocations (or a `test.sh` + a direct API-flavor pytest) concurrently; they collide on the API server and DB tables (the symptom is a flood of `Connection refused` / pool-exhaustion failures that look like real bugs but are pure contention).
### Fixed in this cycle: `get_sync_time(params=...)` on partitioned MariaDB

The v3.4.0 `hypertable=True` default auto-partitions datetime pipes. On **MariaDB** specifically, `SQLConnector.get_sync_time` returned `None` for a populated, partitioned pipe when `params` were passed (broke `test_parametrized_sync_time[mariadb]` and, via a `None` min bound, `test_get_data_iterator[mariadb]`). Root cause: a MariaDB 12.x optimizer bug where the CTE-wrapped `... ORDER BY <dt> DESC LIMIT 1` over a `RANGE COLUMNS`-partitioned table with a `WHERE` clause does a partition index scan that stops early and returns zero rows. (The data and DB are fine — raw `WHERE id='a'` returns the row; only the ordered-LIMIT-1 form on the partitioned table fails. MySQL/PostgreSQL/MSSQL/TimescaleDB do **not** exhibit it — MariaDB-only.) **Fix** (`meerschaum/connectors/sql/_pipes.py`, in `get_sync_time` just before `wrap_query_with_cte`): for `self.flavor == 'mariadb' and not remote and self._should_partition(pipe)`, compute the bound with `MAX(dt)` / `MIN(dt)` (which scans pruned partitions correctly) instead of `ORDER BY ... LIMIT 1`. Flavor-gated, so every other flavor and non-partitioned MariaDB keep the original path.

**Test-running lesson:** `get_sync_time` drives every incremental sync — verify changes to it with the full sqlite `test_sync.py`+`test_verify.py`+`test_pipe_data.py` AND per-flavor partition runs, not just the failing tests. (And read pytest summaries carefully: "N passed, 2 warnings" is not "2 failed".)

### Docs

```bash
./scripts/zensical.sh   # serve the user-facing docs at docs/zensical/
./scripts/pdoc.sh       # build the Python API reference docs
```

### Build

```bash
./scripts/build.sh      # cleans, builds docs, generates requirements, runs python -m build
```

## Architecture Overview

ETL framework centered on **pipes** — named data streams synced into tables. Each pipe: three keys, metadata managed by **instance connector**, data from **source connector**.

### Core Abstractions

**`SuccessTuple`** (`Tuple[bool, str]`) — universal return type for all actions, pipe methods, connector methods. Return `(True, "Success")` or `(False, "reason")`. Never raise from action-level code — catch and return `SuccessTuple`.

**`warn(msg)` / `error(msg, exception)` / `info(msg)` / `dprint(msg, debug=debug)`** — use from `meerschaum.utils.warnings` not `print()`. `warn`: Python warnings system (stackable). `error`: raises `Exception`. `dprint`: debug-only, requires `debug=debug`. `info`: user-facing status.

---

## Pipes in Depth

### Identity

Pipe = three string keys:

| Key | Arg in constructor | Meaning |
|---|---|---|
| `connector_keys` | 1st positional or `connector=` | Data source (e.g. `'sql:main'`, `'plugin:noaa'`) |
| `metric_key` | 2nd positional or `metric=` | Label for the data stream (e.g. `'weather'`) |
| `location_key` | 3rd positional or `location=` | Optional tag/shard (default `None`) |

4th arg (or `instance=` / `mrsm_instance=`) = **instance connector** — where metadata and data are stored.

### Target Table Name

`pipe.target` = table name. Default: `{connector_keys.replace(':', '_')}_{metric_key}` (plus `_{location_key}` if set). Override via `parameters['target']`, `parameters['target_name']`, or `parameters['target_table']`. Long names truncated per-flavor. SQL instances: full name is `schema.target`.

### The `parameters` Dictionary

`pipe.parameters` — central metadata dict. Top-level keys:

| Key | Type | Purpose |
|---|---|---|
| `columns` | `Dict[str, str]` | Maps semantic role → column name. Key roles: `datetime`, `id`, `primary`, `value`. |
| `indices` / `indexes` | `Dict[str, str\|List[str]]` | Additional non-unique indices for performance. |
| `dtypes` | `Dict[str, str]` | Explicit column dtypes. Values are Meerschaum dtype strings (see below). |
| `tags` | `List[str]` | Labels for grouping pipes. |
| `fetch` | `Dict` | Source-specific fetch config. `backtrack_minutes` (int) controls how far before sync time to re-fetch. |
| `verify` | `Dict` | Config for verification syncs. `chunk_minutes` (default 43200 — 30 days) is also the authoritative native-partition width; aliases `chunk_hours`/`chunk_days`/`chunk_weeks`/`chunk_years`/`chunk_seconds` (first set wins, like `bound_*`). Integer axis: `chunk_range` is used verbatim (else convert via `precision`, else minutes verbatim — legacy). Also `bound_days`. Resolved by `Pipe.get_chunk_interval()`. |
| `sql` | `str` | SQL definition for SQL-connector pipes. Supports `{{ Pipe(...) }}` syntax. |
| `target` | `str` | Override the table name. |
| `upsert` | `bool` | If `True`, create a unique index on `columns` and upsert rows. |
| `static` | `bool` | If `True`, never alter the schema (no new columns added). |
| `autoincrement` | `bool` | If `True`, add an auto-incrementing primary key column. |
| `autotime` | `bool` | If `True`, automatically add a `datetime` timestamp column on insert. |
| `hypertable` | `bool` | Defaults to `True`. On TimescaleDB, creates the target as a hypertable. On other flavors supporting native range partitioning (`postgresql`/`postgis`, `mysql`/`mariadb`, `mssql`), datetime-axis pipes are range-partitioned on the datetime column by default; set `False` to opt out. Only newly created tables are affected — pre-existing plain tables aren't retroactively partitioned (`_create_missing_partitions_pg` skips a non-partitioned parent). Width reuses `verify.chunk_minutes`, boundaries epoch-aligned (deterministic). Per-sync partition-creation cap `system.connectors.sql.instance.max_partitions_per_sync` (default 10,000). `Pipe.verify()` calls `get_chunk_bounds(align=True)` so its chunk edges coincide with these partition edges (`get_chunk_bounds` defaults `align=False` — `begin`-anchored — elsewhere). Partitions auto-created in `sync_pipe` before insert (PostgreSQL: `PARTITION OF ... FOR VALUES FROM/TO`; MySQL: inline at create + `ALTER TABLE ADD PARTITION`; MSSQL: partition function+scheme + `SPLIT RANGE`, dropped with the table). |
| `hypercore` | `bool` | TimescaleDB only. If `True` (default), enable the Hypercore columnstore at `CREATE TABLE` (`tsdb.segmentby`/`tsdb.orderby`), auto-creating a columnstore policy. `False` = plain row-store hypertable. |
| `compress` | `bool\|Dict` | If truthy, install a columnstore (compression) policy. Dict overrides `segmentby`, `orderby`, `after`. The columnstore policy *is* the compression policy (`add_columnstore_policy` ≡ legacy `add_compression_policy`). |
| `enforce` | `bool` | If `False`, skip dtype enforcement on incoming data. |
| `null_indices` | `bool` | If `True`, allow `NULL` values in index columns. |
| `precision` | `str\|Dict` | Datetime precision unit: `'ns'`, `'us'`, `'ms'`, `'s'`, `'m'`, `'h'`, `'d'`. Aliases defined in `MRSM_PRECISION_UNITS_ALIASES`. |
| `mixed_numerics` | `bool` | If `False`, prevent int→float columns from being coerced to `numeric`. |
| `reference` | `str\|Dict\|Pipe` | A single reference pipe whose parameters are inherited. |
| `references` | `List[str\|Dict\|Pipe]` | Same as `parents`/`children` — multiple reference pipes whose parameters are merged. |
| `parents` / `children` | `List[str\|Dict\|Pipe]` | Pipe relationship graph (informational and for SQL pushdown). |
| `parent` / `child` | `str\|Dict\|Pipe` | Singular alias for first parent/child. |

Mutate in memory: `pipe.update_parameters({'key': value}, persist=False)` or assign like `pipe.upsert = True`. Persist: call `pipe.edit()` or pass `persist=True`.

**Key convenience attributes and methods:**
- `pipe.metric` / `pipe.location` — aliases for `metric_key` / `location_key`
- `pipe.tzinfo` — returns the `tzinfo` of the datetime column (`UTC`, `None`, etc.)
- `pipe.get_value(col, params=None, ...)` — single scalar from `get_data()`
- `pipe.get_doc(params=None, ...)` — single row as `dict`
- `pipe.filter_existing(df, enforce_dtypes=False, ...)` — accepts `enforce_dtypes=True` to suppress dtype mismatch warnings

### Columns and Dtypes

`pipe.columns` shortcut to `pipe.parameters['columns']`. `'datetime'` column drives incremental sync (begin/end window). `'id'` column forms composite uniqueness key. Pipes without `'datetime'` work but lose incremental behavior.

Supported dtypes (stored in `parameters['dtypes']`):
- `'datetime'` — timezone-aware timestamp (stored as `TIMESTAMPTZ` or equivalent)
- `'int'` — integer (used when the datetime axis is an integer epoch)
- `'numeric'` — arbitrary-precision decimal (`NUMERIC` in SQL)
- `'uuid'` — UUID columns
- `'json'` — JSON/JSONB
- `'bytes'` — binary / bytea
- `'geometry[srid]'` — PostGIS geometry (e.g. `'geometry[EPSG:4326]'`, `'geometry[ESRI:102003]'`)
- Any Pandas dtype string is also valid (e.g. `'Int64'`, `'float64'`, `'bool'`, `'object'`)

Dtype mapping in `meerschaum/utils/dtypes/sql.py` (`get_pd_type_from_db_type`, `get_db_type_from_pd_type`).

### Sync Flow

`pipe.sync(df=None, begin='', end=None, ...)` — main entry point. Flow:

1. **Fetch** — if `df` not provided, calls `pipe.fetch()` → `pipe.connector.fetch(pipe, begin, end, ...)`. SQL: executes metadefinition. Plugin: calls `fetch()`. API: hits remote.
2. **Dtype enforcement** — `pipe.enforce_dtypes(df)` casts to registered dtypes.
3. **Filter existing** — `pipe.filter_existing(df)` fetches overlapping window (`get_backtrack_data`), diffs via `filter_unseen_df()` to find new/changed rows.
4. **Sync to instance** — `instance_connector.sync_pipe(pipe, df)` writes filtered rows. SQL: INSERT/UPDATE (or UPSERT if `upsert=True`).

`begin=''` = use sync time minus backtrack. `None` = no lower bound. Explicit datetime/int overrides.

### SQL Connector Fetch (Metadefinition)

For `connector_keys` starting with `'sql:'`, `get_pipe_metadef()` builds:

```
WITH "definition" AS (
    <pipe.parameters['sql'] or pipe.parameters['fetch']['definition']>
)
SELECT * FROM "definition"
WHERE "<dt_col>" >= <begin - backtrack>
  AND "<dt_col>" < <end>
```

If `parent` pipe set, pushdown `WHERE` hits parent's table (allows SQL dtype conversion). `{{ Pipe('connector', 'metric') }}` resolved to target table at query time.

### `{{ Pipe() }}` Syntax

Reference other pipes with `{{ Pipe('ck', 'mk', 'lk') }}`. Resolved by `replace_pipes_syntax()` in `meerschaum/utils/pipes.py`. Attribute chains: `{{ Pipe('a', 'b').columns['datetime'] }}`. Self-reference: `{{ self.parameters['key'] }}`.

### `MRSM{}` Config Symlinks

Reference config values from pipe parameters with `MRSM{key1:key2:key3}` syntax:

```python
pipe = mrsm.Pipe('demo', 'cfg', parameters={
    'username': 'MRSM{meerschaum:connectors:sql:main:username}',
})
print(pipe.parameters['username'])  # resolved at access time
```

### `params` Filter

`params: Dict[str, Any]` passed to `get_data()`, `get_backtrack_data()`, `get_rowcount()`, `get_sync_time()`, `get_pipes()`, `build_where()`, etc. generates `WHERE` clause via `meerschaum.utils.sql.build_where()`. Values:
- Single value: `{'color': 'red'}` → `WHERE color = 'red'`
- List: `{'color': ['red', 'blue']}` → `WHERE color IN ('red', 'blue')`
- Negation prefix `_`: `{'color': '_red'}` → `WHERE color != 'red'`
- Negation list: `{'color': ['_red', '_blue']}` → `WHERE color NOT IN ('red', 'blue')`
- Mixed list: `{'color': ['red', '_blue']}` → `WHERE color IN ('red') AND color NOT IN ('blue')`
- Null: `{'color': 'None'}` or `{'color': None}` → `WHERE color IS NULL`
- Negated null: `{'color': '_None'}` → `WHERE color IS NOT NULL`
- Dict value: `{'meta': {'k': 'v'}}` → `WHERE CAST(meta AS TEXT) = '{"k": "v"}'`

**Column validation for SQL pipes:** `pipe.enforce=True` (default) filters `params` to existing columns before building `WHERE` — unknown columns dropped. Pass `pipe.enforce=False` to skip.

In-memory filtering: `meerschaum.utils.dataframe.query_df(df, params)` — same negation semantics.

### Verification Syncs

`pipe.verify(begin, end, chunk_interval, ...)` re-syncs historical range in chunks (default 43200 min — 30 days). Uses `pipe.get_rowcount()` to compare remote vs local; re-syncs only mismatched chunks. Configured via `parameters['verify']`.

### Native Partitioning Internals (non-obvious)

The `hypertable=True` default (v3.4.0) makes a *lot* of datetime-axis pipes partitioned on PG/MySQL/MSSQL. Two interactions are easy to break:

- **Upsert conflict target must include the partition column.** A partitioned table (TimescaleDB hypertable *or* native range-partitioned PG/MySQL/MSSQL) folds the `datetime` column into a composite primary key (`_get_create_table_query_from_dtypes` in `meerschaum/utils/sql.py`). So the upsert `ON CONFLICT (...)` / join columns **must** include `dt_col` — `ON CONFLICT (primary_key)` alone has no matching unique constraint and PostgreSQL rejects it. The `join_cols` logic in `SQLConnector.sync_pipe` (`connectors/sql/_pipes.py`, the `partition_upsert` branch) extends the historical TimescaleDB-only `[dt_col, primary_key]` rule to any `self._should_partition(pipe)` table. A non-partitioned upsert pipe with a `primary` key keeps the historical primary-key-only conflict target.
- **Dask DataFrames give lazy scalars.** In `connectors/sql/_partition.py`, `series.min()`/`series.max()` on a *Dask* DataFrame return a lazy `dask...Scalar` (no `to_pydatetime`, not a real number). Partition-range helpers must materialize them (`_materialize_scalar`, which calls `.compute()`) before `int()`/datetime coercion, or `_create_missing_partitions_*` raises `TypeError: int() argument must be ... not 'Scalar'`.
- **`get_chunk_bounds(align=True)`** (used by `Pipe.verify()`) anchors interior chunk edges to the Unix-epoch grid so they coincide with partition boundaries; it defaults to `align=False` (begin-anchored) everywhere else.

### Maintenance operations (v3.4.0)

`vacuum` / `analyze` / `compress` / `decompress` / `get_size` / `repartition` are a uniform family: each is a `Pipe` method (`core/Pipe/_maintenance.py`, `_compress.py`, `_data.py`) delegating to an instance-connector `<op>_pipe()` method. Implemented for `SQLConnector` (per-flavor in `connectors/sql/_compress.py`, `_maintenance.py`, `_partition.py`), stubbed on the base `InstanceConnector` (return a failure `SuccessTuple`), and wired through `APIConnector` (`connectors/api/_pipes.py` + `api/routes/_pipes.py`, all six symmetric — `get_pipe_size`, `compress_pipe`, `decompress_pipe`, `vacuum_pipe`, `analyze_pipe`, `partition_pipe`). User-facing actions: `vacuum/analyze/compress/decompress/partition pipes`, `show sizes`, `show partitions`. Docs: `docs/zensical/reference/pipes/maintenance.md`.

---

## SQL Security Patterns

### `clean()` — keyword blocklist

`clean(substring)` in `meerschaum/utils/sql.py` raises on banned SQL keywords (`;`, `--`, `drop`, `union`, `insert`, `update`, `delete`, `create`, `alter`, `truncate`, `exec`, `/*`). **Keyword blocklist only** — quote-only payloads like `' OR '1'='1` not caught. Use parameterized queries for user input; `clean()` for internal names (tables, columns).

### `dateadd_str()` — `datepart` whitelist

`datepart` validated against `_VALID_DATEPARTS = frozenset({'year', 'month', 'day', 'hour', 'minute', 'second'})`. **Exception:** `datepart=None` valid when `begin` is int — returns early before `datepart` used. Whitelist check must come after `isinstance(begin, int)` early-return.

### Table name escaping in `.format()` queries

Functions building queries via `.format()` (e.g. `get_table_cols_types()`, `get_table_cols_indices()` in `meerschaum/utils/sql.py`) escape table/schema names with `s.replace("'", "''")` before interpolation. Never skip for new format-based queries.

---

## API Server Pipe Cache

API server keeps shared `pipes_dict` (in `meerschaum/api/__init__.py`) caching live `Pipe` objects between requests. Two behaviors:

- `get_pipe(refresh=False)` — returns **shared** `pipes_dict` object (mutations/cache clears persist across requests).
- `get_pipe(refresh=True)` — creates **throwaway** `Pipe` not stored in `pipes_dict`; cache cleared on it has no effect on future requests.

**Stale column-types cache:** After dtype-changing sync, `pipes_dict` object retains `_columns_types` up to 60 s (`columns_types_cache_seconds`). `DELETE /pipes/{ck}/{mk}/{lk}/cache` (added 2025-05) clears it via `pipe._invalidate_cache(hard=True)` on the shared object. `APIConnector.sync_pipe` calls `self.delete_pipe_cache(pipe)` after every sync.

---

## Connector Hierarchy

```
Connector                         (meerschaum/connectors/_Connector.py)
└── InstanceConnector             (meerschaum/connectors/instance/_InstanceConnector.py)
    ├── SQLConnector              (meerschaum/connectors/sql/)
    ├── APIConnector              (meerschaum/connectors/api/)
    └── ValkeyConnector           (meerschaum/connectors/valkey/)
```

`Connector` reads config from `MRSM_ROOT_DIR/config/connectors.yaml` keyed by `type:label`. `REQUIRED_ATTRIBUTES` lists required keys. Cast to string returns `"type:label"`.

`InstanceConnector` adds pipes/users/plugins/tokens interface. `IS_INSTANCE = True` makes connector usable as instance. `IS_THREAD_SAFE = True` enables concurrent reads (used by `get_pool`). Custom connector with `IS_INSTANCE = True` auto-adds its `type` to `meerschaum.connectors.instance_types`.

All major connector classes compose methods via class-level imports:

```python
class SQLConnector(InstanceConnector):
    from ._pipes import fetch_pipes_keys, sync_pipe, get_pipe_data, ...
    from ._sql import read, to_sql, exec_queries, ...
    from ._fetch import fetch, get_pipe_metadef
```

Each logical group of methods lives in separate `_*.py`. New methods go in appropriate file, not class definition.

### Adding a Custom Connector

```python
import meerschaum as mrsm
from meerschaum.connectors import InstanceConnector, make_connector

@make_connector
class FooConnector(InstanceConnector):
    IS_INSTANCE = True
    IS_THREAD_SAFE = False
    REQUIRED_ATTRIBUTES = ['host', 'port']
    # Implement all InstanceConnector abstract methods...
```

See `meerschaum/connectors/instance/_InstanceConnector.py` for full interface and `docs/zensical/reference/connectors/instance-connectors.md` for method-by-method guide.

`fetch_pipes_keys()` returns list of key tuples or dict (pipe ID → key tuple, optional params/tags appended). Dict form returns full pipe attributes in one round-trip.

---

## Actions

Top-level actions (`sync`, `show`, `register`, `copy`, etc.) are modules in `meerschaum/actions/`. Receive `**kwargs` matching argparse namespace from `meerschaum/_internal/arguments/_parser.py`.

Common kwargs:
- `connector_keys: List[str]`, `metric_keys: List[str]`, `location_keys: List[str]`
- `mrsm_instance: str` — instance connector keys
- `begin: datetime`, `end: datetime`
- `params: Dict[str, Any]`
- `tags: List[str]`
- `debug: bool`
- `yes: bool`, `force: bool`, `noask: bool`

Subactions auto-discovered by naming convention (e.g. `sync_pipes` in `actions/sync.py`). Add: define `def <action>_<subaction>(**kwargs) -> SuccessTuple` — no registration needed.

Plugin actions use `@make_action` from `meerschaum.plugins` or `meerschaum.actions`.

---

## Code Composition Patterns

### Mixin-via-Import Pattern

`Pipe` and connectors compose methods via class-level `from ._file import func` imports. When adding to `Pipe`:
- Data retrieval → `meerschaum/core/Pipe/_data.py`
- Sync logic → `meerschaum/core/Pipe/_sync.py`
- Attributes/properties → `meerschaum/core/Pipe/_attributes.py`
- Cache → `meerschaum/core/Pipe/_cache.py`
- Then add the import to `meerschaum/core/Pipe/__init__.py`

### Lazy Imports with `attempt_import`

Never import heavy packages at module level. Use:

```python
from meerschaum.utils.packages import attempt_import

pandas = attempt_import('pandas')                        # installs into 'mrsm' venv if missing
dateutil = attempt_import('dateutil', venv='mrsm')
shapely = attempt_import('shapely', venv='mrsm')
```

For packages in plugin venv:
```python
requests = attempt_import('requests', venv='my_plugin')
```

For type hints on heavy types:
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    pd = attempt_import('pandas')
```

### `Venv` Context Manager

When calling plugin/connector code using plugin's venv:
```python
from meerschaum.utils.venv import Venv
from meerschaum.connectors import get_connector_plugin

with Venv(get_connector_plugin(pipe.connector)):
    result = pipe.connector.fetch(pipe, begin=begin)
```

### `filter_arguments` / `filter_keywords`

Connectors/plugins may not accept all kwargs:
```python
from meerschaum.utils.misc import filter_arguments, filter_keywords

# filter_arguments: returns (args, kwargs) filtered to match func's signature
args, kw = filter_arguments(func, *args, **kwargs)
result = func(*args, **kw)

# filter_keywords: returns only kwargs accepted by func
kw = filter_keywords(func, **kwargs)
result = func(**kw)
```

---

## Pipes Dictionary

`get_pipes()` returns `{connector_keys: {metric_key: {location_key: Pipe}}}` (`location_key` is `None` when absent). Use:
- `as_list=True` to get `[Pipe, ...]`
- `flatten_pipes_dict(pipes_dict)` from `meerschaum.utils.pipes`
- `pipes_dict_from_list(pipes_list)` to reconstruct the hierarchy

### Key Negation

Prefix filter value with `_` to negate. Applies to `connector_keys`, `metric_keys`, `location_keys`, `tags`, `datetime_dtypes`. Via `separate_negation_values()` in `meerschaum.utils.misc`.

---

## Config System

`get_config(*keys, patch=False)` reads YAML config registry at `MRSM_ROOT_DIR/config/`. Keys are path segments: `get_config('meerschaum', 'instance')` → `config.meerschaum.instance`.

`STATIC_CONFIG` in `meerschaum/_internal/static.py` holds compile-time constants. Import:
```python
from meerschaum._internal.static import STATIC_CONFIG
prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
```

Plugin config: `get_plugin_config(*keys)` / `write_plugin_config(value, *keys)` — reads/writes under `plugins.<plugin_name>`.

---

## Cache System

`Pipe._cache_value(key, value, memory_only=False)` and `Pipe._get_cached_value(key)` — two-layer cache:
- **Memory** (always): stored in `pipe.__dict__` under `_<key>`.
- **Disk** (when `memory_only=False`): pickled to `MRSM_ROOT_DIR/cache/<instance_hash>/<pipe_id>/`.

Keys starting with `_` = memory-only. Cache keyed per instance-hash (derived from connector config); changing host/port invalidates.

Cached values: `'_id'` (row ID), `'attributes'` (full metadata), `'_attributes_sync_time'` (last refresh), `'precision'`.

---

## Jobs System

`meerschaum.jobs.Job(name, sysargs, executor_keys='local')` wraps `meerschaum.utils.daemon.Daemon` to run `sysargs` as background process.

- `executor_keys='local'` — managed daemon process under `MRSM_ROOT_DIR/jobs/`
- `executor_keys='systemd'` — creates a `systemd` user service
- `executor_keys='api:label'` — posts the job to a remote API instance

Jobs persist across restarts. Logs stream via `job.monitor_logs(callback)`. `job.start()` / `job.stop()` / `job.pause()` return `SuccessTuple`.

---

## Plugins System

Plugins: Python files/packages in `MRSM_PLUGINS_DIR`. Recognized functions:

| Function | When called |
|---|---|
| `register(pipe, **kw) -> dict` | When `mrsm register pipe` runs; return initial `parameters` |
| `fetch(pipe, begin, end, **kw) -> df\|generator` | During `sync pipes`; return new data |
| `sync(pipe, **kw) -> SuccessTuple` | Override the full sync process |
| `setup(**kw) -> SuccessTuple` | On first install / `mrsm setup plugins` |

Decorator extensions:
- `@make_action` — register a function as an action
- `@api_plugin` — add FastAPI routes (receives `app: FastAPI`)
- `@dash_plugin` — add Dash callbacks (receives `dash_app: Dash`)
- `@web_page('/path')` — add a page to the web dashboard (nest inside `@dash_plugin`)
- `@pre_sync_hook` / `@post_sync_hook` — called before/after every sync

Custom CLI args: `add_plugin_argument('--foo', type=str, help='...')` from `meerschaum.plugins`.

Package dependencies: `required = ['requests>=2.0', 'pandas']`. Install into plugin-named venv.

---

## Environment Variables

| Variable | Purpose |
|---|---|
| `MRSM_ROOT_DIR` | Isolates all config, data, jobs, and cache |
| `MRSM_PLUGINS_DIR` | Override plugins directory |
| `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` | Set connector by URI (e.g. `MRSM_SQL_MAIN=postgresql://...`) |
| `MRSM_TEST_FLAVORS` | Comma-separated DB flavors to test against |
| `MRSM_API_TEST` | URI for the test API connector |
| `PDOC_ALLOW_EXEC` | Must be `1` for pdoc to build the API docs |
