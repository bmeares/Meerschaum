# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Development CLI

`./scripts/mrsm.sh <args>` runs `python -m meerschaum` with `MRSM_ROOT_DIR=./test_root` and `MRSM_PLUGINS_DIR=./tests/plugins` so it never touches the user's real config.

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

Test connectors are defined in `tests/connectors.py`. The default flavor set is `api,timescaledb`; set `MRSM_TEST_FLAVORS=sqlite` for fast local runs without Docker.

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

Meerschaum is an ETL framework centered on **pipes** — named data streams synced into tables. Each pipe is identified by three keys, stored in a metadata table managed by an **instance connector**, and fetches data from a **source connector**.

### Core Abstractions

**`SuccessTuple`** (`Tuple[bool, str]`) is the universal return type for all actions, pipe methods, and connector methods. Always return `(True, "Success")` or `(False, "reason")`. Never raise exceptions from action-level code — catch and return a `SuccessTuple`.

**`warn(msg)` / `error(msg, exception)` / `info(msg)` / `dprint(msg, debug=debug)`** — use these from `meerschaum.utils.warnings` rather than `print()`. `warn` uses Python's warnings system (stackable). `error` raises an exception (default `Exception`). `dprint` is debug-only and requires passing `debug=debug`. `info` is for user-facing status messages.

---

## Pipes in Depth

### Identity

A pipe is uniquely identified by three string keys:

| Key | Arg in constructor | Meaning |
|---|---|---|
| `connector_keys` | 1st positional or `connector=` | Data source (e.g. `'sql:main'`, `'plugin:noaa'`) |
| `metric_key` | 2nd positional or `metric=` | Label for the data stream (e.g. `'weather'`) |
| `location_key` | 3rd positional or `location=` | Optional tag/shard (default `None`) |

The 4th constructor argument (or `instance=` / `mrsm_instance=`) is the **instance connector** — where pipe metadata (parameters, registration) and data are stored.

### Target Table Name

`pipe.target` is the table name in the instance database. By default: `{connector_keys.replace(':', '_')}_{metric_key}` (plus `_{location_key}` if set). Override via `parameters['target']`, `parameters['target_name']`, or `parameters['target_table']`. Long names are truncated per-flavor. On SQL instances, the full qualified name is `schema.target`.

### The `parameters` Dictionary

`pipe.parameters` is the central metadata dict. Important top-level keys:

| Key | Type | Purpose |
|---|---|---|
| `columns` | `Dict[str, str]` | Maps semantic role → column name. Key roles: `datetime`, `id`, `primary`, `value`. |
| `indices` / `indexes` | `Dict[str, str\|List[str]]` | Additional non-unique indices for performance. |
| `dtypes` | `Dict[str, str]` | Explicit column dtypes. Values are Meerschaum dtype strings (see below). |
| `tags` | `List[str]` | Labels for grouping pipes. |
| `fetch` | `Dict` | Source-specific fetch config. `backtrack_minutes` (int) controls how far before sync time to re-fetch. |
| `verify` | `Dict` | Config for verification syncs. `chunk_minutes` (default 1440), `bound_days`. |
| `sql` | `str` | SQL definition for SQL-connector pipes. Supports `{{ Pipe(...) }}` syntax. |
| `target` | `str` | Override the table name. |
| `upsert` | `bool` | If `True`, create a unique index on `columns` and upsert rows. |
| `static` | `bool` | If `True`, never alter the schema (no new columns added). |
| `autoincrement` | `bool` | If `True`, add an auto-incrementing primary key column. |
| `autotime` | `bool` | If `True`, automatically add a `datetime` timestamp column on insert. |
| `enforce` | `bool` | If `False`, skip dtype enforcement on incoming data. |
| `null_indices` | `bool` | If `True`, allow `NULL` values in index columns. |
| `precision` | `str\|Dict` | Datetime precision unit: `'ns'`, `'us'`, `'ms'`, `'s'`, `'m'`, `'h'`, `'d'`. Aliases defined in `MRSM_PRECISION_UNITS_ALIASES`. |
| `mixed_numerics` | `bool` | If `False`, prevent int→float columns from being coerced to `numeric`. |
| `reference` | `str\|Dict\|Pipe` | A single reference pipe whose parameters are inherited. |
| `references` | `List[str\|Dict\|Pipe]` | Same as `parents`/`children` — multiple reference pipes whose parameters are merged. |
| `parents` / `children` | `List[str\|Dict\|Pipe]` | Pipe relationship graph (informational and for SQL pushdown). |
| `parent` / `child` | `str\|Dict\|Pipe` | Singular alias for first parent/child. |

Mutating parameters in memory: `pipe.update_parameters({'key': value}, persist=False)` or assign directly like `pipe.upsert = True`. To persist to the instance, call `pipe.edit()` or pass `persist=True`.

**Key convenience attributes and methods:**
- `pipe.metric` / `pipe.location` — aliases for `metric_key` / `location_key`
- `pipe.tzinfo` — returns the `tzinfo` of the datetime column (`UTC`, `None`, etc.)
- `pipe.get_value(col, params=None, ...)` — single scalar from `get_data()`
- `pipe.get_doc(params=None, ...)` — single row as `dict`
- `pipe.filter_existing(df, enforce_dtypes=False, ...)` — accepts `enforce_dtypes=True` to suppress dtype mismatch warnings

### Columns and Dtypes

`pipe.columns` is a shortcut to `pipe.parameters['columns']`. The `'datetime'` column drives all incremental syncing logic (begin/end window). The `'id'` column (or any additional named columns) forms the composite uniqueness key. Pipes without a `datetime` column still work but lose incremental behavior.

Supported Meerschaum dtypes (stored in `parameters['dtypes']`):
- `'datetime'` — timezone-aware timestamp (stored as `TIMESTAMPTZ` or equivalent)
- `'int'` — integer (used when the datetime axis is an integer epoch)
- `'numeric'` — arbitrary-precision decimal (`NUMERIC` in SQL)
- `'uuid'` — UUID columns
- `'json'` — JSON/JSONB
- `'bytes'` — binary / bytea
- `'geometry[srid]'` — PostGIS geometry (e.g. `'geometry[EPSG:4326]'`, `'geometry[ESRI:102003]'`)
- Any Pandas dtype string is also valid (e.g. `'Int64'`, `'float64'`, `'bool'`, `'object'`)

Dtype mapping between Meerschaum strings and DB types lives in `meerschaum/utils/dtypes/sql.py` (`get_pd_type_from_db_type`, `get_db_type_from_pd_type`).

### Sync Flow

`pipe.sync(df=None, begin='', end=None, ...)` is the main entry point. The flow:

1. **Fetch** — if `df` is not provided, calls `pipe.fetch()` which delegates to `pipe.connector.fetch(pipe, begin, end, ...)`. For SQL connectors this executes the metadefinition query. For plugin connectors this calls the plugin's `fetch()` function. For API connectors this hits the remote API.
2. **Dtype enforcement** — `pipe.enforce_dtypes(df)` casts incoming data to registered dtypes.
3. **Filter existing** — `pipe.filter_existing(df)` fetches the overlapping time window from the instance (`get_backtrack_data`) and computes the diff via `filter_unseen_df()` to find only new/changed rows.
4. **Sync to instance** — `instance_connector.sync_pipe(pipe, df)` writes the filtered rows. For SQL instances this generates INSERT/UPDATE queries (or UPSERT if `upsert=True`).

The `begin` parameter defaults to `''` (empty string), which signals "use the pipe's sync time minus backtrack interval". `None` means "no lower bound". An explicit datetime/int overrides.

### SQL Connector Fetch (Metadefinition)

For `connector_keys` starting with `'sql:'`, the connector's `get_pipe_metadef()` builds the query:

```
WITH "definition" AS (
    <pipe.parameters['sql'] or pipe.parameters['fetch']['definition']>
)
SELECT * FROM "definition"
WHERE "<dt_col>" >= <begin - backtrack>
  AND "<dt_col>" < <end>
```

If a `parent` pipe is set, the pushdown `WHERE` clause is applied to the parent's table instead (allows dtype conversion via SQL). The `{{ Pipe('connector', 'metric') }}` syntax in the SQL definition is resolved to the target table name at query time.

### `{{ Pipe() }}` Syntax

SQL definitions and parameter values can reference other pipes using `{{ Pipe('ck', 'mk', 'lk') }}`. This is resolved by `replace_pipes_syntax()` in `meerschaum/utils/pipes.py`. Attribute chains work: `{{ Pipe('a', 'b').columns['datetime'] }}`. Use `{{ self.parameters['key'] }}` to self-reference.

### `MRSM{}` Config Symlinks

Reference Meerschaum config values from within pipe parameters using `MRSM{key1:key2:key3}` syntax:

```python
pipe = mrsm.Pipe('demo', 'cfg', parameters={
    'username': 'MRSM{meerschaum:connectors:sql:main:username}',
})
print(pipe.parameters['username'])  # resolved at access time
```

### `params` Filter

`params: Dict[str, Any]` passed to `get_data()`, `get_backtrack_data()`, `get_rowcount()`, `get_sync_time()`, `get_pipes()`, `build_where()`, etc. generates a `WHERE` clause. The actual SQL is built by `meerschaum.utils.sql.build_where()`. Values support:
- Single value: `{'color': 'red'}` → `WHERE color = 'red'`
- List: `{'color': ['red', 'blue']}` → `WHERE color IN ('red', 'blue')`
- Negation prefix `_`: `{'color': '_red'}` → `WHERE color != 'red'`
- Negation list: `{'color': ['_red', '_blue']}` → `WHERE color NOT IN ('red', 'blue')`
- Mixed list: `{'color': ['red', '_blue']}` → `WHERE color IN ('red') AND color NOT IN ('blue')`
- Null: `{'color': 'None'}` or `{'color': None}` → `WHERE color IS NULL`
- Negated null: `{'color': '_None'}` → `WHERE color IS NOT NULL`
- Dict value: `{'meta': {'k': 'v'}}` → `WHERE CAST(meta AS TEXT) = '{"k": "v"}'`

**Column validation for SQL pipes:** when `pipe.enforce=True` (the default), `get_pipe_data_query()` filters `params` to only columns that actually exist on the table before building the `WHERE` clause — unknown columns are silently dropped. Pass `pipe.enforce=False` to skip this check.

For in-memory filtering on a DataFrame, use `meerschaum.utils.dataframe.query_df(df, params)` — same negation semantics apply.

### Verification Syncs

`pipe.verify(begin, end, chunk_interval, ...)` re-syncs the entire historical range in chunks (default 1440-minute chunks). Uses `pipe.get_rowcount()` to compare remote vs local counts and only re-syncs mismatched chunks. Configured via `parameters['verify']`.

---

## Connector Hierarchy

```
Connector                         (meerschaum/connectors/_Connector.py)
└── InstanceConnector             (meerschaum/connectors/instance/_InstanceConnector.py)
    ├── SQLConnector              (meerschaum/connectors/sql/)
    ├── APIConnector              (meerschaum/connectors/api/)
    └── ValkeyConnector           (meerschaum/connectors/valkey/)
```

`Connector` reads config from `MRSM_ROOT_DIR/config/connectors.yaml` keyed by `type:label`. The class attribute `REQUIRED_ATTRIBUTES` lists keys that must be set (either in config or passed directly). When cast to string, a connector returns `"type:label"`.

`InstanceConnector` adds the full pipes/users/plugins/tokens interface that all instances must implement. Set `IS_INSTANCE = True` to make a connector usable as an instance. Set `IS_THREAD_SAFE = True` if the connector supports concurrent reads (used by `get_pool` for parallel pipe fetching). Registering a custom connector type with `IS_INSTANCE = True` automatically adds its `type` to `meerschaum.connectors.instance_types`.

**All major connector classes compose their methods via class-level imports:**

```python
class SQLConnector(InstanceConnector):
    from ._pipes import fetch_pipes_keys, sync_pipe, get_pipe_data, ...
    from ._sql import read, to_sql, exec_queries, ...
    from ._fetch import fetch, get_pipe_metadef
```

This means each logical group of methods lives in a separate `_*.py` file. New methods belong in the appropriate file, not the class definition file.

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

See `meerschaum/connectors/instance/_InstanceConnector.py` for the full interface and `docs/zensical/reference/connectors/instance-connectors.md` for the method-by-method guide.

`fetch_pipes_keys()` may return either a list of key tuples or a dict where keys are pipe IDs (int) and values are key tuples (with optional parameters/tags appended). The dict form lets the instance return full pipe attributes in a single round-trip.

---

## Actions

Each top-level action (`sync`, `show`, `register`, `copy`, etc.) is a module in `meerschaum/actions/`. Actions receive `**kwargs` matching the argparse namespace from `meerschaum/_internal/arguments/_parser.py`.

Common kwargs passed to all actions:
- `connector_keys: List[str]`, `metric_keys: List[str]`, `location_keys: List[str]`
- `mrsm_instance: str` — instance connector keys
- `begin: datetime`, `end: datetime`
- `params: Dict[str, Any]`
- `tags: List[str]`
- `debug: bool`
- `yes: bool`, `force: bool`, `noask: bool`

Subactions are functions within the module auto-discovered by naming convention (e.g. `sync_pipes` in `actions/sync.py`). To add a subaction, define `def <action>_<subaction>(**kwargs) -> SuccessTuple` in the module — no registration needed.

Plugin actions use `@make_action` from `meerschaum.plugins` or `meerschaum.actions`.

---

## Code Composition Patterns

### Mixin-via-Import Pattern

Both `Pipe` and connectors use class-level `from ._file import func` imports to compose methods from separate files. This avoids large monolithic classes. When adding functionality to `Pipe`:
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

For packages that must be in a specific plugin's venv:
```python
requests = attempt_import('requests', venv='my_plugin')
```

Use `TYPE_CHECKING` for type hints on heavy types:
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    pd = attempt_import('pandas')
```

### `Venv` Context Manager

When calling a plugin's or connector's code that uses the plugin's venv:
```python
from meerschaum.utils.venv import Venv
from meerschaum.connectors import get_connector_plugin

with Venv(get_connector_plugin(pipe.connector)):
    result = pipe.connector.fetch(pipe, begin=begin)
```

### `filter_arguments` / `filter_keywords`

Connectors and plugins may not accept all kwargs. Use:
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

`get_pipes()` returns `{connector_keys: {metric_key: {location_key: Pipe}}}`. `location_key` is `None` when absent. Use:
- `as_list=True` to get `[Pipe, ...]`
- `flatten_pipes_dict(pipes_dict)` from `meerschaum.utils.pipes`
- `pipes_dict_from_list(pipes_list)` to reconstruct the hierarchy

### Key Negation

Prefix any filter value with `_` to negate it. Applies to `connector_keys`, `metric_keys`, `location_keys`, `tags`, `datetime_dtypes`. Implemented via `separate_negation_values()` from `meerschaum.utils.misc`.

---

## Config System

`get_config(*keys, patch=False)` reads the hierarchical YAML config registry at `MRSM_ROOT_DIR/config/`. Keys are path segments: `get_config('meerschaum', 'instance')` reads `config.meerschaum.instance`.

`STATIC_CONFIG` in `meerschaum/_internal/static.py` holds compile-time constants (e.g. `negation_prefix = '_'`). Import as:
```python
from meerschaum._internal.static import STATIC_CONFIG
prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
```

Plugin config: `get_plugin_config(*keys)` / `write_plugin_config(value, *keys)` — reads/writes under `plugins.<plugin_name>` in the config registry.

---

## Cache System

`Pipe._cache_value(key, value, memory_only=False)` and `Pipe._get_cached_value(key)` provide two-layer caching:
- **Memory** (always): stored in `pipe.__dict__` under `_<key>`.
- **Disk** (when `memory_only=False`): pickled to `MRSM_ROOT_DIR/cache/<instance_hash>/<pipe_id>/`.

Keys starting with `_` are treated as memory-only regardless. The cache is keyed per instance-hash (derived from the connector's stable config), so changing a host/port invalidates cached data.

Important cached values: `'_id'` (pipe's row ID in the pipes table), `'attributes'` (full pipe metadata), `'_attributes_sync_time'` (last refresh timestamp), `'precision'`.

---

## Jobs System

`meerschaum.jobs.Job(name, sysargs, executor_keys='local')` wraps `meerschaum.utils.daemon.Daemon` to run a `sysargs` string as a background process.

- `executor_keys='local'` — runs as a managed daemon process under `MRSM_ROOT_DIR/jobs/`
- `executor_keys='systemd'` — creates a `systemd` user service
- `executor_keys='api:label'` — posts the job to a remote API instance

Jobs persist across restarts. Their logs stream via `job.monitor_logs(callback)`. `job.start()` / `job.stop()` / `job.pause()` return `SuccessTuple`.

---

## Plugins System

Plugins are Python files/packages in `MRSM_PLUGINS_DIR`. Recognized functions (called automatically by the system):

| Function | When called |
|---|---|
| `register(pipe, **kw) -> dict` | When `mrsm register pipe` runs; return initial `parameters` |
| `fetch(pipe, begin, end, **kw) -> df\|generator` | During `sync pipes`; return new data |
| `sync(pipe, **kw) -> SuccessTuple` | Override the full sync process |
| `setup(**kw) -> SuccessTuple` | On first install / `mrsm setup plugins` |

Decorator-based extensions:
- `@make_action` — register a function as an action
- `@api_plugin` — add FastAPI routes (receives `app: FastAPI`)
- `@dash_plugin` — add Dash callbacks (receives `dash_app: Dash`)
- `@web_page('/path')` — add a page to the web dashboard (nest inside `@dash_plugin`)
- `@pre_sync_hook` / `@post_sync_hook` — called before/after every sync

Custom CLI args: `add_plugin_argument('--foo', type=str, help='...')` from `meerschaum.plugins`.

Declare package dependencies: `required = ['requests>=2.0', 'pandas']`. These install into a venv named after the plugin.

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
