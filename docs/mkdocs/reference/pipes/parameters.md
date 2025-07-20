# 🧩 Parameters

This page catalogs useful keys in the `parameters` dictionary.

!!! note "Custom Keys"

    Remember that you have complete control of the `parameters` dictionary to store metadata in your pipes. It's a common pattern to set values under a custom key for your plugin.

    ```python
    pipe = mrsm.Pipe('a', 'b', parameters={'a': 1})
    pipe.parameters.update({'a': 2})
    pipe.edit() # alias: pipe.update()


    print(mrsm.Pipe('a', 'b').parameters['a'])
    # 2
    ```


??? tip "Dynamically symlink to other pipes' attributes"
    Reference attributes of other pipes using the `{{ Pipe(...) }}` syntax. These references are resolved at run-time when `Pipe.parameters` is accessed. To inherit another pipe's entire `parameters`, see [`reference`](/reference/pipes/parameters/#reference) below.

    ```python
    import json
    import meerschaum as mrsm

    some_pipe = mrsm.Pipe(
        'demo', 'symlink',
        instance='sql:memory',
        parameters={
            'target': 'some_table',
            'columns': {
                'datetime': 'id',
                'id': 'id',
            },
            'upsert': True,
            'custom': {
                'stations': ['KATL', 'KGMU', 'KCEU'],
            },
        },
    )
    some_pipe.register()

    pipe = mrsm.Pipe(
        'demo', 'symlink', 'child',
        instance='sql:memory',
        parameters={
            'target': "{{ Pipe('demo', 'symlink', instance='sql:memory').target }}",
            'columns': "{{ Pipe('demo', 'symlink', instance='sql:memory').columns }}",
            'upsert': "{{ Pipe('demo', 'symlink', instance='sql:memory').upsert }}",
            'custom': "{{ Pipe('demo', 'symlink', instance='sql:memory').parameters['custom'] }}",
            'parent_pipe_id': "{{ Pipe('demo', 'symlink', instance='sql:memory').id }}",
        },
    )

    print(json.dumps(pipe.parameters, indent=4))
    # {
    #     "target": "some_table",
    #     "columns": {
    #         "datetime": "id",
    #         "id": "id"
    #     },
    #     "upsert": true,
    #     "custom": {
    #         "stations": [
    #             "KATL",
    #             "KGMU",
    #             "KCEU"
    #         ]
    #     },
    #     "parent_pipe_id": 1
    # }  
    ```


---------------

## `autoincrement`

If a `primary` index is defined (see [columns](#columns) below) and `autoincrement` is set, create the primary key as an auto-incrementing integer column.

!!! warning
    This may only work for pipes stored in `sql` instances.

---------------

## `autotime`

!!! note inline end "Precision units"
    See the description of `precision` [below](#precision) for timestamp values captured by `autotime`.

Similar to `autoincrement`, `autotime` will generate the current timestamp for each document synced. When the `datetime` column has an integer dtype, the generated value will be the number of `Pipe.precision` units since the Unix epoch.

This works for pipes stored in all instances, making it a good alternative to `autoincrement`. Setting `autotime` on pipes without a `datetime` axis will add the column `ts` (without treating it as an index).

??? example
    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'autotime',
        instance='sql:memory',
        autotime=True,
        columns={'datetime': 'timestamp_utc'},
    )
    pipe.sync("reading:100.1")

    df = pipe.get_data()
    print(df)
    #    reading                    timestamp_utc
    # 0    100.1 2025-07-18 16:47:12.145056+00:00
    ```

---------------

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

---------------

## `dtypes`

Meerschaum data types allow you to specify how columns should be parsed, deserialized, and stored. In addition to special types like `numeric`, `uuid`, `json`, and `bytes`, you may specify other Pandas data types (e.g. `datetime64[ns]`).

Below are the supported Meerschaum data types. See the [SQL dtypes source](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/dtypes/sql.py) to see which types map to specific database types.

| Data Types                                     | Examples                                          | Python, Pandas Types                                                            | SQL Types Notes                                                                     |
|------------------------------------------------|---------------------------------------------------|---------------------------------------------------------------------------------|-------------------------------------------------------------------------------------|
| `int`                                          | `1`                                               | `int`, `Int64`, `int64[pyarrow]`, etc.                                          | `BIGINT`                                                                            |
| `float`                                        | `1.1`                                             | `float`, `float64`, `float64[pyarrow]`, etc.                                    | `DOUBLE PRECISION`, `FLOAT`                                                         |
| `string`                                       | `'foo'`                                           | `str`, `string[python]`                                                         | `TEXT`, `NVARCHAR(MAX)` for MSSQL, `NVARCHAR2(2000)` for Oracle.                    |
| `datetime`                                     | `Timestamp('2024-12-26 00:00:00+0000', tz='UTC')` | `datetime`, `datetime64[ns]`, `datetime64[ns, UTC]` (timezone-aware by default) | `TIMESTAMP`, `TIMESTAMPTZ`, `DATETIMEOFFSET` for MSSQL. Offsets are coerced to UTC. |
| `numeric`, `numeric[precision,scale]`          | `Decimal('1.000')`                                | `Decimal`                                                                       | `NUMERIC`, `DECIMAL`. Uses `precision` and `scale` if provided.                     |
| `uuid`                                         | `UUID('df2572b5-e42e-410d-a624-a14519f73e00')`    | `UUID`                                                                          | `UUID` where supported. `UNIQUEIDENTIFIER` for MSSQL                                |
| `bool`                                         | `True`                                            | `boolean[pyarrow]`                                                              | `BOOL`, `BIT`, `INT` for Oracle, MSSQL, MySQL / MariaDB, `FLOAT` for SQLite.        |
| `json`                                         | `{"foo": "bar"}`                                  | `dict`, `list`                                                                  | `JSONB` for PostgreSQL-like flavors, otherwise `TEXT`.                              |
| `bytes`                                        | `b'foo bar'`                                      | `bytes`                                                                         | `BYTEA`, `BLOB`, `VARBINARY`                                                        |
| `geometry`, `geometry[type,srid]`, `geography` | `Point`, `MultiLineString`, etc.                  | `shapely.Point`, etc.                                                           | `GEOMETRY`, `GEOMETRY[POINT, 4326]`, etc.                                           |

---------------

## `enforce`

The `enforce` parameter controls whether a pipe coerces incoming data to match the set data types (default `True`). If your workload is performance-sensitive, consider experimenting with `enforce=False` to skip the extra work required to ensure incoming data matches the configured dtypes.

!!! warning
    Some instance connectors behave unexpectedly when `enforce=False` (e.g. SQLite).
    
---------------

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

---------------

## `indices`

The `indices` dictionary (alias `indexes`) allows you to create additional and multi-column indices. Whereas `columns` is for specifying uniqueness, `indices` is for performance tuning.

The keys specified in `columns` are included in `indices` by default, as well as a multi-column index `unique` for the columns in `columns`.

??? example

    In the example below, the unique constraint is only created for the columns `ts` and `station`, and an additional multi-column index is created on the columns `city`, `state`, and `country`.

    ```yaml
    connector: sql:main
    metric: temperature
    columns:
      datetime: ts
      id: station
    indices:
      geo: ['city', 'state', 'country']
    upsert: true
    parameters:
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
---------------

## `mixed_numerics`

Toggle whether a pipe will coerce mixed integer and float values into the `numeric` dtype (default `True`). Set this to `False` to disable this behavior, preventing potential schema changes on the target table (akin to `static=True`).

---------------

## `null_indices`

Toggle whether a pipe will allow null indices (default `True`). Set this to `False` for a performance improvement in situations where null index values are not expected.

---------------

## `precision`

The unit set by `precision` determines the value of the timestamp captured by `autotime` in units since the Unix Epoch in UTC (see above). By default, the `datetime` axis dtype determines `Pipe.precision` (e.g. `datetime64[ns, UTC]` is `nanosecond` precision).

| Units         | Aliases    | Datetimes                       | Integers              |
|---------------|------------|---------------------------------|-----------------------|
| `nanosecond`  | `ns`       | `2025-07-18 16:02:18.701925991` | `1752854538701925991` |
| `microsecond` | `us`       | `2025-07-18 16:02:18.701925`    | `1752854538701925`    |
| `millisecond` | `ms`       | `2025-07-18 16:02:18.701`       | `1752854538701`       |
| `second`      | `sec`, `s` | `2025-07-18 16:02:18`           | `1752854538`          |
| `minute`      | `min`, `m` | `2025-07-18 16:02`              | `29214242`            |
| `hour`        | `hr`, `h`  | `2025-07-18 16:00`              | `486904`              |
| `day`         | `d`        | `2025-07-18`                    | `20287`               |

---------------

## `reference`

A pipe may inherit the base parameters from another reference pipe. Set `reference` to the keys of the base pipe, and additional keys will override the base parameters. To symlink subsets of other pipes' parameters, see the example at top of the page on using the `{{ Pipe(...) }}` syntax.

??? example

    ```python
    import meerschaum as mrsm

    base_pipe = mrsm.Pipe(
        'demo', 'reference', 'parent',
        instance='sql:memory',
        columns={
            'datetime': 'ts',
            'id': 'id',
        },
        parameters={
            'custom': {
                'foo': 'bar',
                'color': 'red',
            },
        },
    )
    base_pipe.register()

    pipe = mrsm.Pipe(
        'demo', 'reference', 'child',
        instance='sql:memory',
        parameters={
            'reference': {
                'connector': 'demo',
                'metric': 'reference',
                'location': 'parent',
                'instance': 'sql:memory',
            },
            'custom': {
                'color': 'blue',
            },
        },
    )

    print(pipe.parameters)
    # {'custom': {'foo': 'bar', 'color': 'blue'}, 'columns': {'datetime': 'ts', 'id': 'id'}} 
    ```

---------------

## `schema`

When syncing a pipe via a `SQLConnector` (ignored on SQLite), you may override the connector's configured schema for the given pipe. This is useful when syncing against multiple schemas on the same database with a single `SQLConnector`.

```yaml
schema: production
```

---------------

## `sql`, `query`

Aliases for `fetch:definition`.

---------------

## `static`

Setting `static` will prevent new columns from being added and existing columns from changing types. This is useful for critical production situations where schemata are externally managed.

---------------

## `tags`

A list of a pipe's [tags](/reference/pipes/tags/), e.g.:

```yaml
tags:
- production
- foo
```

---------------

## `upsert`

Setting `upsert` to `true` enables high-performance syncs by combining inserts and updates into a single transaction.

Upserts rely on unique constraints on indices and as such should be used in situations where a table's schema is fixed. See the [upsert SQL source](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/sql.py) for further details.

!!! warning
    Ensure your instance connector supports upserts before enabling `upsert` (e.g. `SQLConnector`).

??? example

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

---------------

## `valkey`

The `valkey` key is used internally to keep internal metadata separate from user configuration when syncing against a `ValkeyConnector`.

---------------

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