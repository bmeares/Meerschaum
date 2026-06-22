# 🐍 Python API

Most of this documentation is written from the perspective of the `mrsm` command-line interface, but everything you can do from the CLI you can also do from Python. This page is a practical cheat sheet for the Python path — import once and go:

```python
import meerschaum as mrsm
```

!!! tip "Full API reference"
    This page covers the everyday essentials. The complete, auto-generated API reference (every class, method, and signature) lives at **[docs.meerschaum.io](https://docs.meerschaum.io)**.

## Constructing a Pipe

A [pipe](/reference/pipes/) is identified by three keys — connector, metric, and (optional) location — plus an **instance** where its metadata and data are stored.

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'plugin:noaa', 'weather',     # connector keys, metric key
    'atl',                         # optional location key
    instance='sql:main',           # where metadata + data live
    columns={
        'datetime': 'timestamp',   # drives incremental syncing
        'id': 'station',           # composite uniqueness key
    },
    dtypes={'station': 'string'},  # explicit column dtypes
)
```

Key constructor arguments:

| Argument | Meaning |
|---|---|
| `connector` (1st positional) | Data source keys, e.g. `'plugin:noaa'`, `'sql:main'`. |
| `metric` (2nd positional) | Label for the data stream, e.g. `'weather'`. |
| `location` (3rd positional) | Optional tag/shard. Defaults to `None`. |
| `instance=` | Instance connector keys (where data is stored). |
| `columns=` | Maps semantic roles (`datetime`, `id`, ...) to column names. |
| `dtypes=` | Explicit column dtypes. |
| `parameters=` | The full metadata dict (`columns`, `dtypes`, `fetch`, `tags`, ...). |

!!! note "`columns`, `dtypes`, etc. are shortcuts"
    Passing `columns=` (or `dtypes=`, `tags=`, `target=`, ...) just sets the corresponding key inside `parameters`. See [Parameters](/reference/pipes/parameters/) for the complete list.

## Syncing

[`pipe.sync()`](/reference/pipes/syncing/) is the main entry point. Pass a DataFrame (or dict of lists) directly, or omit it to fetch from the pipe's connector.

```python
# Sync a DataFrame you built yourself:
import pandas as pd
df = pd.read_csv('weather.csv')
success, msg = pipe.sync(df)

# Fetch from the pipe's connector (e.g. a plugin or SQL definition):
pipe.sync()

# Sync a bounded interval:
pipe.sync(begin='2024-01-01', end='2024-02-01')
```

Every sync returns a [`SuccessTuple`](#successtuple-convention):

```python
success, msg = pipe.sync(df)
if not success:
    print(f"Sync failed: {msg}")
```

## Reading

See [Reading Data](/reference/pipes/reading/) for the full set of options.

```python
# DataFrame of rows:
df = pipe.get_data(begin='2024-01-01', end='2024-02-01')

# Rows as a list of dicts (no pandas needed):
docs = pipe.get_docs(limit=10)

# A single scalar from one column:
latest = pipe.get_value('val', params={'station': 'KGMU'}, order='desc')

# A single row as a dict:
doc = pipe.get_doc(order='desc')

# Row count and latest sync time:
n = pipe.get_rowcount()
ts = pipe.get_sync_time()
```

All read methods accept a `params` filter that builds a `WHERE` clause. Prefix a value with `_` to negate it:

```python
df = pipe.get_data(params={'station': ['KGMU', 'KATL']})  # IN (...)
df = pipe.get_data(params={'station': '_KGMU'})           # != 'KGMU'
```

See [`params` Filtering and Negation](/reference/pipes/reading/#params-filtering-and-negation) for the complete table.

## Finding Pipes

Use [`mrsm.get_pipes()`](https://docs.meerschaum.io/meerschaum.html#get_pipes) to query the pipes registered on an instance. It returns a nested dictionary by default, or a flat list with `as_list=True`.

```python
# Flat list of every pipe on an instance:
pipes = mrsm.get_pipes(instance='sql:main', as_list=True)

# Filter by keys and tags:
pipes = mrsm.get_pipes(
    connector_keys='plugin:noaa',
    metric_keys=['weather'],
    tags=['production'],
    instance='sql:main',
    as_list=True,
)

for pipe in pipes:
    print(pipe, pipe.get_rowcount())
```

Key filters (`connector_keys`, `metric_keys`, `location_keys`, `tags`) accept a string or list, and any value may be negated with a leading `_`.

## Connectors

Fetch (or build) a connector with [`mrsm.get_connector()`](https://docs.meerschaum.io/meerschaum.html#get_connector). Pass `type` and `label` separately or as combined keys:

```python
conn = mrsm.get_connector('sql', 'main')
# equivalent:
conn = mrsm.get_connector('sql:main')

# A SQL connector can read and write directly:
df = conn.read("SELECT * FROM my_table")
```

## CLI ↔ Python Mapping

| Command | Python equivalent |
|---|---|
| `mrsm show pipes` | `mrsm.get_pipes(as_list=True)` |
| `mrsm register pipe` | `pipe.register()` |
| `mrsm sync pipes` | `pipe.sync()` |
| `mrsm verify pipes` | `pipe.verify()` |
| `mrsm deduplicate pipes` | `pipe.deduplicate()` |
| `mrsm drop pipe` | `pipe.drop()` |
| `mrsm delete pipe` | `pipe.delete()` |
| `mrsm edit pipes` | `pipe.edit()` |

## `SuccessTuple` Convention

Most pipe and connector methods (`sync`, `register`, `drop`, `delete`, `verify`, ...) return a **`SuccessTuple`** — a plain `(bool, str)` tuple of `(success, message)` — instead of raising exceptions. Always check the boolean:

```python
success, msg = pipe.register()
if not success:
    print(f"Could not register: {msg}")
```

This convention is what lets actions chain reliably without try/except scaffolding.

## See Also

- [Pipes](/reference/pipes/) — the core data abstraction
- [Parameters](/reference/pipes/parameters/) — the full `parameters` dictionary
- [Reading Data](/reference/pipes/reading/) — `get_data`, `get_docs`, `params` filters
- [Syncing](/reference/pipes/syncing/) — the fetch / filter / upsert flow
- [Connectors](/reference/connectors/) — sources and instances
- [docs.meerschaum.io](https://docs.meerschaum.io) — the complete auto-generated API reference
