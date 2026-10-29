# 📖 Reading Data

Use `Pipe.get_data()` and its convenience wrappers to read rows from the instance.

### `get_data()`

Returns a DataFrame: a [Polars](https://pola.rs) `DataFrame` by default (see
[DataFrame library](#dataframe-library) for pandas). Accepts `begin`, `end`, `params`,
`select_columns`, `omit_columns`, `limit`, and `order` to filter and shape the result. `begin` is
inclusive and `end` is exclusive (`begin <= datetime < end`).

A keyword argument `get_data()` does not know (`start=`, `where=`, ...) is ignored with a warning that suggests the one you probably meant (`begin`, `params`, ...). Keywords the instance reads itself, such as a SQL instance's `begin_add_minutes` or a custom instance connector's own, never warn. `get_backtrack_data()` warns the same way.

```python
import meerschaum as mrsm
import polars as pl

pipe = mrsm.Pipe('demo', 'temperature', instance='sql:local')

# All rows:
df = pipe.get_data()

# Time-bounded slice:
df = pipe.get_data(begin='2024-01-01', end='2024-02-01')

# Only specific columns:
df = pipe.get_data(select_columns=['dt', 'station', 'val'])

# Filtered by params:
df = pipe.get_data(params={'station': ['KGMU', 'KATL']})

# Cap rows returned:
df = pipe.get_data(limit=100)

# Then work with it in Polars:
daily = df.group_by(pl.col('dt').dt.date().alias('day')).agg(pl.col('val').mean())
```

### DataFrame library

Every Python API that returns a DataFrame (`get_data()`, `get_backtrack_data()`, `fetch()`,
`SQLConnector.read()`, `Plan.to_dataframe()`, the frames given to a `chunk_hook` or to a custom
instance connector's `sync_pipe()`) returns the library of one setting, `dataframe` in
`meerschaum.yaml` (`mrsm edit config`):

```yaml
dataframe: polars   # or pandas
```

- **A new root** is written with `dataframe: polars`. Install Polars with
  `pip install 'meerschaum[polars]'`.
- **A root created by Meerschaum 4 or by an earlier 5.0 release candidate** keeps pandas: the first
  time 5.0.0rc15 (or newer) opens it, `dataframe: pandas` is added to its `meerschaum.yaml` (one
  line; the rest of the file is kept) and a one-line notice says so. Change it to `polars` to
  switch.
- **`MRSM_DATAFRAME=pandas`** (or `polars`) overrides the setting for one process and the jobs it
  starts.
- **Per call**, `as_pandas=True` or `as_polars=True` overrides both (`as_polars=False` asks for
  pandas, as in Meerschaum 4; asking for both is an error):

```python
pandas_df = pipe.get_data(as_pandas=True)
polars_df = pipe.get_data(as_polars=True)
```

Inputs never depend on the setting: `pipe.sync()` and every other method that takes data accept
pandas, Polars and Arrow data, dictionaries and lists. `pipe.enforce_dtypes(df)`,
`pipe.filter_existing(df)` and `meerschaum.utils.dataframe.enforce_dtypes(df, ...)` return the
library they were given.

Geometry columns come back as GeoArrow WKB (`Binary`) in Polars, with the CRS in the column's field
metadata (`shapely.from_wkb()` reads the values), and as a `geopandas.GeoDataFrame` with
`as_pandas=True`. When the configured library is not installed, the error names the extra to
install (`pip install 'meerschaum[polars]'` or `'meerschaum[pandas]'`), or Meerschaum installs it
into its own virtual environment when automatic installs are allowed.

`as_chunks=True` yields frames of the same library. Neither `as_polars` nor `as_pandas` can be
combined with `as_docs=True`. (`as_dask=True` is not supported: it raises a `TypeError`; read
large tables in chunks instead.)

**Chunked iteration**, pass `as_chunks=True` (alias `as_iterator=True`) to get a generator of DataFrames, each covering a time-bound slice. Useful for large datasets that don't fit in memory:

```python
for chunk_df in pipe.get_data(as_chunks=True):
    process(chunk_df)
```

**Errors**, a pipe whose table does not exist yet (never synced) returns `None`. When the
instance cannot be read (an unreachable server, a permission error), `get_data()` raises the
database's error; on an `api:` instance, an API server that cannot be reached or fails the read
(`500`) raises too. Pass `silent=True` to get `None` instead:

```python
df = pipe.get_data(silent=True)
```

`get_rowcount()` and `get_sync_time()` behave the same way (0 and `None` for a never-synced
pipe, `silent=True` for 0 / `None` on errors).

### `get_rowcount()`

The number of rows, optionally between `begin` (inclusive) and `end` (exclusive) and filtered by
`params`:

```python
pipe.get_rowcount()
# 1250004
pipe.get_rowcount(begin='2024-01-01', params={'station': 'KGMU'})
# 8760
```

On large tables, `estimate=True` asks the database's catalog statistics instead of counting
(instant; close to the real count, since the statistics trail recent writes): PostgreSQL and
PostGIS (`pg_class.reltuples`), TimescaleDB (`approximate_row_count()`), MySQL / MariaDB,
MSSQL and Oracle. Small tables (under 100,000 rows), SQLite and DuckDB are counted exactly, as
are counts with `begin`, `end`, `params` or `remote`:

```python
pipe.get_rowcount(estimate=True)
# 1249870
```

### `get_docs()`

Returns `List[Dict[str, Any]]`, rows as plain Python dictionaries, without a DataFrame library. Ideal for JSON APIs, small targeted queries, or when no DataFrame is needed.

```python
docs = pipe.get_docs()
# [{'dt': ..., 'station': 'KGMU', 'val': 44.1}, ...]

# With filters:
docs = pipe.get_docs(params={'station': 'KGMU'}, limit=10)
```

Combine with `as_chunks=True` to get an `Iterator[List[Dict]]` chunked by time bounds:

```python
for chunk in pipe.get_docs(as_chunks=True):
    send_to_api(chunk)
```

### `get_doc()`

Returns a single row as `Dict[str, Any]` (or `None`). Equivalent to `get_docs(limit=1)[0]`:

```python
doc = pipe.get_doc(params={'station': 'KGMU'}, order='desc')
print(doc)
# {'dt': datetime(...), 'station': 'KGMU', 'val': 44.1}
```

### `get_value()`

Returns a single scalar value from one column (or `None`). Useful when you need exactly one cell:

```python
latest_val = pipe.get_value('val', params={'station': 'KGMU'}, order='desc')
print(latest_val)
# 44.1
```

### `params` Filtering and Negation

All read methods accept a `params` dictionary that maps column names to filter values. Prefix any value with `_` to negate it.

| Syntax | SQL equivalent |
|---|---|
| `{'col': 'foo'}` | `WHERE col = 'foo'` |
| `{'col': ['foo', 'bar']}` | `WHERE col IN ('foo', 'bar')` |
| `{'col': '_foo'}` | `WHERE col != 'foo'` |
| `{'col': ['_foo', '_bar']}` | `WHERE col NOT IN ('foo', 'bar')` |
| `{'col': ['foo', '_bar']}` | `WHERE col IN ('foo') AND col NOT IN ('bar')` |
| `{'col': None}` or `{'col': 'None'}` | `WHERE col IS NULL` |
| `{'col': '_None'}` | `WHERE col IS NOT NULL` |

```python
# Single value
docs = pipe.get_docs(params={'station': 'KGMU'})

# Include list
df = pipe.get_data(params={'station': ['KGMU', 'KATL']})

# Exclude one value
df = pipe.get_data(params={'station': '_KGMU'})

# Exclude a list
df = pipe.get_data(params={'station': ['_KGMU', '_KATL']})

# Mixed include/exclude
df = pipe.get_data(params={'station': ['KGMU', '_KATL']})

# Null / not-null
df = pipe.get_data(params={'station': None})       # IS NULL
df = pipe.get_data(params={'station': '_None'})    # IS NOT NULL
```
