# 📖 Reading Data

Use `Pipe.get_data()` and its convenience wrappers to read rows from the instance.

### `get_data()`

Returns a `pd.DataFrame`. Accepts `begin`, `end`, `params`, `select_columns`, `omit_columns`, `limit`, and `order` to filter and shape the result.

```python
import meerschaum as mrsm

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
```

**Chunked iteration** — pass `as_chunks=True` (alias `as_iterator=True`) to get a generator of DataFrames, each covering a time-bound slice. Useful for large datasets that don't fit in memory:

```python
for chunk_df in pipe.get_data(as_chunks=True):
    process(chunk_df)
```

### `get_docs()`

Returns `List[Dict[str, Any]]` — rows as plain Python dictionaries without loading pandas. Ideal for JSON APIs, small targeted queries, or when pandas is not needed.

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
