# 🧩 Parameters

This page catalogs useful keys in the `parameters` dictionary.

!!! tip "Custom Keys"

    Remember that you have complete control of the `parameters` dictionary to store metadata in your pipes. It's a common pattern to set values under a custom key for your plugin.

    ```python
    pipe = mrsm.Pipe('a', 'b', parameters={'a': 1})
    pipe.parameters.update({'a': 2})
    pipe.edit() # alias: pipe.update()


    print(mrsm.Pipe('a', 'b').parameters['a'])
    # 2
    ```

---------------

## `autoincrement`

If a `primary` index is defined (see [columns](#columns) below) and `autoincrement` is set, create the primary key as an auto-incrementing integer column.

## `columns`

The values of the `columns` dictionary are columns to be treated as a composite primary key. For example, the following would be used for a table with the index columns `ts` and `stationId`:


!!! note inline end ""
    Index columns **must be** immutable and unique as a collection.

```yaml
columns:
  datetime: "ts"
  id: "stationId"
```

### The `datetime` Index

The column specified as the `datetime` index will be used as the datetime axis for bounding, e.g. as the time axis for a hypertable.

You may specify an integer column as the `datetime` axis if you explictly set its `dtype` as `int` (see [dtypes](#dtypes) below).

### The `primary` Index

The column specified as the `primary` index will be created as the primary key. If `autoincrement` is set, then the primary key will be created as an incremental integer column.

You may designate the same column as both the `datetime` and `primary` indices.

## `dtypes`

Meerschaum data types allow you to specify how columns should be parsed, deserialized, and stored. In addition to special types like `numeric`, `uuid`, `json`, and `bytes`, you may specify other Pandas data types (e.g. `datetime64[ns]`).

Below are the supported Meerschaum data types. See the [SQL dtypes source](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/dtypes/sql.py) to see which types map to specific database types.

| Data Types                            | Examples                                          | Python, Pandas Types                                                            | SQL Types Notes                                                                     |
|---------------------------------------|---------------------------------------------------|---------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| `int`                                 | `1`                                               | `int`, `Int64`, `int64[pyarrow]`, etc.                                          | `BIGINT`                                                                            |
| `float`                               | `1.1`                                             | `float`, `float64`, `float64[pyarrow]`, etc.                                    | `DOUBLE PRECISION`, `FLOAT`                                                         |
| `string`                              | `'foo'`                                           | `str`, `string[python]`                                                         | `TEXT`, `NVARCHAR(MAX)` for MSSQL, `NVARCHAR2(2000)` for Oracle.                    |
| `datetime`                            | `Timestamp('2024-12-26 00:00:00+0000', tz='UTC')` | `datetime`, `datetime64[ns]`, `datetime64[ns, UTC]` (timezone-aware by default) | `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIMEOFFSET` for MSSQL. Offsets are coerced to UTC. |
| `numeric`, `numeric[precision,scale]` | `Decimal('1.000')`                                | `Decimal`                                                                       | `NUMERIC`, `DECIMAL`. Uses `precision` and `scale` if provided.                     |
| `uuid`                                | `UUID('df2572b5-e42e-410d-a624-a14519f73e00')`    | `UUID`                                                                          | `UUID` where supported. `UNIQUEIDENTIFIER` for MSSQL                                |
| `bool`                                | `True`                                            | `boolean[pyarrow]`                                                              | `BOOL`, `BIT`, `INT` for Oracle, MSSQL, MySQL / MariaDB, `FLOAT` for SQLite.        |
| `json`                                | `{"foo": "bar"}`                                  | `dict`, `list`                                                                  | `JSONB` for PostgreSQL-like flavors, otherwise `TEXT`.                              |
| `bytes`                               | `b'foo bar'`                                      | `bytes`                                                                         | `BYTEA`, `BLOB`, `VARBINARY`                                                        |

## `enforce`

The `enforce` parameter controls whether a pipe coerces incoming data to match the set data types (default `True`). If your workload is performance-sensitive, consider experimenting with `enforce=False` to skip the extra work required to ensure incoming data matches the configured dtypes.

## `fetch`

The `fetch` key contains parameters concerning the [fetch stage](/reference/pipes/syncing/) of the syncing process.

### `fetch:backtrack_minutes`

How many minutes of overlap to request when fetching new rows ― see [Backtracking](/reference/pipes/syncing/#backtracking). Defaults to 1440.

### `fetch:definition`

!!! example inline end ""
    ```yaml
    fetch:
      definition: |-
        SELECT *
        FROM foo
    ```
    
The base SQL query to be run when fetching new rows. Aliased as `sql` for convenience. This only applies to pipes with [`SQLConnectors`](/reference/connectors/sql-connectors/) as connectors.

## `indices`

The `indices` dictionary (alias `indexes`) allows you to create additional and multi-column indices. Whereas `columns` is for specifying uniqueness, `indices` is for performance tuning.

The keys specified in `columns` are included in `indices` by default, as well as a multi-column index `unique` for the columns in `columns`.

In the example below, the unique constraint is only created for the columns `ts` and `station`, and an additional multi-column index is created on the columns `city`, `state`, and `country`.

```yaml
connector: sql:main
metric: temperature
columns:
  datetime: ts
  id: station
indices:
  geo: ['city', 'state', 'country']
parameters:
  upsert: true
  sql: |-
    SELECT
      ts,
      station,
      city,
      state,
      country,
      temperature_c,
      ((1.8 * temperature_c) + 32) as temperature_f
    FROM weather
```

## `null_indices`

Toggle whether a pipe will allow null indices (default `True`). Set this to `False` for a performance improvement in situations where null index values are not expected.

## `tags`

A list of a pipe's [tags](/reference/pipes/tags/), e.g.:

```yaml
tags:
- production
- foo
```

## `schema`

When syncing a pipe via a `SQLConnector`, you may override the connector's configured schema for the given pipe. This is useful when syncing against multiple schemas on the same database with a single `SQLConnector`.

```yaml
schema: production
```

## `sql`, `query`

Aliases for `fetch:definition`.

## `static`

Setting `static` will prevent new columns from being added and existing columns from changing types. This is useful for critical production situations where schemata are externally managed.

## `upsert`

Setting `upsert` to `true` enables high-performance syncs by combining inserts and updates into a single transaction.

Upserts rely on unique constraints on indices and as such should be used in situations where a table's schema is fixed. See the [upsert SQL source](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/sql.py) for further details.

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'demo', 'upsert',
    instance = 'sql:local',
    columns = ['datetime', 'id'],
    parameters = {'upsert': True},
)

mrsm.pprint(
    pipe.sync(
        [
            {'datetime': '2023-01-01', 'id': 1, 'val': 1.1},
            {'datetime': '2023-01-02', 'id': 2, 'val': 2.2},
        ]
    )
)
#  🎉 Upserted 2 rows.
```

## `valkey`

The `valkey` key is used internally to keep internal metadata separate from user configuration when syncing against a `ValkeyConnector`.

## `verify`

The `verify` key contains parameters concerning [verification syncs](/reference/pipes/syncing/#verification-syncs).

### `verify:bound_days`

The key `verify:bound_days` specifies the interval when determining the bound time, which is limit at which long-running verfication syncs should stop.

In addition to days, alias keys are allowed to specify other units of time. In order of priority, the supported keys are the following:

- `bound_minutes`
- `bound_hours`
- `bound_days`
- `bound_weeks`
- `bound_years`
- `bound_seconds`

### `verify:chunk_minutes`

The key `verify:chunk_minutes` specifies the size of chunk intervals when verifying a pipe. See [`Pipe.get_chunk_bounds()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_chunk_bounds).