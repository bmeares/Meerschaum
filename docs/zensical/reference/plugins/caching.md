# 🗃️ Caching

Some things are slow to compute: a query over a big table, a chart, a call to someone else's API. `meerschaum.utils.cache` remembers the result for a while, shares it with every process of your deployment when Valkey is around, and forgets it when the pipes it came from sync.

```python
import meerschaum as mrsm
from meerschaum.utils.cache import cache

weather = mrsm.Pipe('plugin:noaa', 'weather', instance='sql:main')

@cache(ttl=300, pipes=[weather])
def daily_means(station: str):
    df = weather.get_data(params={'station': station})
    return df.groupby(df['timestamp'].dt.date)['temperature'].mean().reset_index()
```

The first call computes; later calls with the same arguments return the cached result until five minutes have passed or `weather` syncs, whichever comes first.

## The decorator

`#!python @cache(ttl=300, pipes=None)` (or a bare `#!python @cache`) caches a function's results by its arguments.

| Parameter | Meaning |
|---|---|
| `ttl` | Seconds a result is kept (default `300`; `None` keeps it until a listed pipe syncs or it is evicted). |
| `pipes` | Pipes whose syncs make a result stale: a list, or a function of the call's arguments that returns them (`pipes=lambda pipe, **kw: [pipe]`). |

The decorated function also has `cache_clear()` (forget its results), `cache_key(*args, **kwargs)` and `uncached` (the function itself). Arguments that are pipes, dates, sets or DataFrames make stable keys; other objects use their `repr()`, so use it on functions rather than methods.

## Values by key

```python
from meerschaum.utils.cache import cache

cache.set('station-names', ['north', 'south'], ttl=3600)
cache.get('station-names')            # ['north', 'south']
cache.get('missing', default=[])      # []
cache.set('report', report, pipes=[weather])   # forgotten when `weather` syncs
cache.delete('station-names')
cache.clear()                         # everything cached
```

## What can be cached

JSON values (`dict`, `list`, `str`, numbers, `bool`, `None`), `datetime`, `date`, `time`, `Decimal`, `UUID` and `bytes`, and pandas and Polars DataFrames (as Arrow IPC when `pyarrow` is installed). Nothing is pickled, so nothing in Valkey can run code when it is read. Tuples come back as lists. You get a copy, from memory and Valkey alike: changing it does not change the cache.

A function whose result cannot be stored still returns it; the cache warns once and does not keep it.

## Where values live

- **Valkey**, when the connector `api:cache:connector` (default `valkey:main`) answers: the web console, its API routes, jobs and your scripts share the values. Keys start with `mrsm:cache:`.
- **Process memory** otherwise (thread-safe; at most `cache.max_entries` values and `cache.max_bytes` bytes, least recently used out first).

The cache asks Valkey once, and again a minute after it did not answer. The web console says so once at startup when your plugin uses the cache and the configured connector does not answer. Choose another connector, or none, in `mrsm edit config api`:

```yaml
cache:
  connector: valkey:cache   # or null for process memory only
```

## When a listed pipe syncs

Every sync is recorded on the pipe's instance (`pipe.get_sync_status()`): jobs, `mrsm sync pipes`, Pipelines, the API and `pipe.sync()` alike. A cached value remembers the latest recorded sync of each of its `pipes`, and a newer one makes it stale. Two syncs of a pipe a few seconds apart can be recorded as one, so the second may only be noticed at the next recorded sync or when `ttl` runs out.
