# 🗝️ Valkey Connectors

The [`ValkeyConnector`](https://docs.meerschaum.io/meerschaum/connectors/valkey.html#ValkeyConnector) syncs pipes to a [Valkey](https://valkey.io/) instance. Valkey is an in-memory key-value store forked from Redis, so the same connector also works against a **Redis** server.

Unlike the [`SQLConnector`](/reference/connectors/sql-connectors/), which is backed by a relational database, the `ValkeyConnector` is an [instance connector](/reference/connectors/instance-connectors/) that stores pipes' data and metadata directly in Valkey keys, sets, and sorted sets.

- **Implementation:** built-in ([docs](https://docs.meerschaum.io/meerschaum/connectors/valkey.html#ValkeyConnector))
- **Type:** `valkey`

Similar to `sql:main`, the built-in connector `valkey:main` connects to the Valkey instance in the [Meerschaum stack](/reference/stack/).

## When to Use Valkey

Reach for a `valkey:` instance when you want a lightweight, fast, in-memory store and don't need full SQL semantics:

- Caching or short-lived data streams where Valkey is already part of your stack.
- Edge or embedded deployments where running a SQL database is overkill.
- Reading from existing Valkey keys (sets, sorted sets, or plain values) as a *source* connector (see [Fetching from Valkey](#fetching-from-valkey)).

Prefer a [`SQLConnector`](/reference/connectors/sql-connectors/) when you need rich `params` filtering pushed down to the database, joins, in-place syncs, native partitioning/hypertables, or large persistent datasets. Valkey filtering happens in-memory after reading the candidate rows, so it is best suited to moderate volumes.

## Configuration

The connector is built from connection attributes or a single `uri` string. The only required attribute is `host`.

| Attribute | Required | Default | Description |
|-----------|----------|---------|-------------|
| `host` | ✅ | — | Valkey/Redis hostname. |
| `port` | | `6379` | Server port. |
| `username` | | `default` | Username (Valkey ACL). |
| `password` | | — | Password. |
| `db` | | `0` | Logical database number. |
| `socket_timeout` | | `300` | Socket timeout in seconds. |

!!! example "Connector config"

    ```yaml
    username: default
    password: mrsm
    host: localhost
    port: 6379
    db: 0
    socket_timeout: 300
    ```

    ```yaml
    uri: valkey://default:mrsm@localhost:6379/0?timeout=300s
    ```

When a `uri` is set it takes precedence over the individual attributes (the client is built with `valkey.Valkey.from_url()`).

### Environment Connectors

Like other connectors, you may define a Valkey connector entirely from an [environment variable](/reference/connectors/#-environment-connectors) using the `MRSM_<TYPE>_<LABEL>` convention:

```bash
export MRSM_VALKEY_REMOTE='valkey://default:mrsm@valkey.example.com:6379/0?timeout=300s'
```

This registers `valkey:remote` for the lifetime of the process.

## Data Layout

The connector maps Meerschaum's document model onto Valkey data structures. The most relevant keys for a pipe with target table `T`:

| Key | Structure | Purpose |
|-----|-----------|---------|
| `mrsm_pipe:<ck>:<mk>:<lk>` | string | The pipe's integer `_id`. |
| `mrsm_pipe:<ck>:<mk>:<lk>:parameters` | string | JSON-serialized pipe `parameters`. |
| `mrsm_pipes` | sorted set | Registry of all pipes (scored by `pipe_id`). |
| `T` (the quoted target) | sorted set **or** set | Index documents for the pipe's rows. |
| `T:datetime_column` | string | Name of the datetime column for `T`, if any. |
| `<index-key>` | string | The full serialized row document, keyed by its index values. |

How rows are stored depends on whether the pipe has a `datetime` column:

- **With a `datetime` column**, rows are added to a **sorted set** (`ZADD`) scored by the datetime value's Unix timestamp. This enables efficient `begin`/`end` range reads via `ZRANGEBYSCORE`, and `get_sync_time()` is an O(1) `ZREVRANGE`/`ZRANGE` lookup.
- **Without a `datetime` column**, rows are stored in a plain **set** (`SADD`) with no ordering, and reads return all members.

Each row's full document is stored under a separate key derived from its index column values; the entries in the target set hold only the index reference (`ix`) plus the datetime value. Because `:` is the Valkey key separator, colons inside index values are escaped (the replacement token is configured at `STATIC_CONFIG['valkey']['colon']`).

Column dtypes are tracked in `parameters['valkey']['dtypes']` (rather than from the store itself) and reapplied on read so values round-trip correctly.

## Capabilities & Limitations

The `ValkeyConnector` implements the full [instance connector interface](/reference/connectors/instance-connectors/), including:

- Pipe registration, editing, existence checks, dropping, and deletion.
- `sync_pipe` with insert / update and `upsert` support, plus `static` dtype handling.
- `get_pipe_data`, `get_sync_time`, `get_pipe_rowcount`, `clear_pipe`, and `fetch_pipes_keys` (with tag filtering).
- Users and plugins tables (so a `valkey:` instance can back the [Web API](https://api.mrsm.io/docs)).

Compared to [`SQLConnector`](/reference/connectors/sql-connectors/):

- **Filtering** via `params`, `begin`, and `end` is applied **in-memory** after reading candidate documents (datetime ranges are narrowed at the store level via the sorted-set score, but column `params` are filtered client-side).
- There are **no SQL features** — no joins, no in-place syncs, no native partitioning/hypertables.
- **Thread safety:** the connector inherits `IS_THREAD_SAFE = False`, so it is not used for concurrent-read connection pools the way a thread-safe SQL connector is.

## Example

Register a pipe on a `valkey:` instance and sync some data:

```python
import meerschaum as mrsm
import pandas as pd

pipe = mrsm.Pipe(
    'demo', 'temperature', 'home',
    instance='valkey:main',
    columns={'datetime': 'timestamp', 'id': 'sensor'},
)
pipe.register()

pipe.sync(pd.DataFrame([
    {'timestamp': '2025-01-01 00:00:00', 'sensor': 'a', 'value': 20.5},
    {'timestamp': '2025-01-01 01:00:00', 'sensor': 'a', 'value': 21.0},
]))

print(pipe.get_data())
print(pipe.get_sync_time())
```

Because `timestamp` is the `datetime` column, the rows are stored in a sorted set, so subsequent incremental syncs and `begin`/`end` reads are efficient.

### Fetching from Valkey

A `valkey:` connector can also act as a **source**. Point a pipe at an existing Valkey key via `parameters['valkey']['key']`; the `fetch` method reads sets (`SMEMBERS`), sorted sets (`ZRANGEBYSCORE`, honoring `begin`/`end`), or plain string values:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'valkey:main', 'events',
    instance='sql:main',
    parameters={'valkey': {'key': 'incoming_events'}},
)
pipe.sync()
```

For the full API, see the [`ValkeyConnector` reference on docs.meerschaum.io](https://docs.meerschaum.io/meerschaum/connectors/valkey.html#ValkeyConnector).
