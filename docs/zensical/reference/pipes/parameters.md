# 🧩 Parameters

This page catalogs useful keys in the `parameters` dictionary.

!!! note "Custom Keys"

    Remember that you have complete control of the `parameters` dictionary to store metadata in your pipes. It's a common pattern to set values under a custom key for your plugin.

    ```python
    pipe = mrsm.Pipe('a', 'b', parameters={'a': 1})
    pipe.update_parameters({'a': 2}, persist=True)

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
            'value': 123,
            'self_value': "{{ self.parameters['value'] }}",
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
    #     "value": 123,
    #     "self_value": 123,
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

This works for pipes stored in all instances, making it a good alternative to `autoincrement`. Setting `autotime` on pipes without a `datetime` axis will add the column `ts` (without treating it as an index); the default name may be configured at the keys `pipes.autotime.column_name_if_datetime_missing`.

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

## `compress`

Set `compress` to enable compression for a pipe's target table to reduce disk usage.

For **TimescaleDB** hypertables (the default stack), compression is handled by the [Hypercore columnstore](https://www.tigerdata.com/docs/build/columnar-storage/setup-hypercore). The columnstore (compression) policy is the same mechanism under both names — `add_columnstore_policy` is the modern equivalent of the legacy `add_compression_policy`.

By default ([`hypercore`](#hypercore)), the columnstore is already enabled at table creation and a policy is auto-created. Setting `compress` additionally (re)installs a columnstore policy automatically on sync, so newly synced chunks are converted in the background. Running the `compress pipes` action (or `Pipe.compress()`) additionally converts any existing uncompressed chunks immediately.

!!! tip "One-shot compression"
    Run `compress pipes --no-policy` to convert existing chunks now **without** installing an ongoing columnstore policy (any pre-existing policy is left untouched). Useful for a one-time reclaim on a pipe you don't want to keep auto-compressing.

!!! tip "Decompressing"
    Run the `decompress pipes` action (or `Pipe.decompress()`) to reverse compression: it removes the columnstore policy, converts compressed chunks back to row-store, and disables the columnstore so future synced chunks stay uncompressed (MySQL / MariaDB and MSSQL revert their native table compression). Pass `decompress pipes --no-policy` to decompress existing chunks now while leaving the policy in place — handy for a bulk backfill, after which chunks are recompressed on the policy's schedule.

You may set `compress` to `true` to use sensible defaults (segment by the `id` column, order by the `datetime` column descending), or to a dictionary with the following keys for fine-grained control:

- `after`
- `segmentby`
- `orderby`

### `compress.after`

How old a chunk must be before the policy converts it (e.g. `'7 days'`). Defaults to `7 days`.

### `compress.segmentby`

The column(s) to segment by. Accepts a single column name (string) or a list of column names. Defaults to the `id` index — unless `id` is the unique [`primary`](#the-primary-index) key, in which case it is dropped from `segmentby` (a high-cardinality `segmentby` defeats columnstore compression) and moved into `orderby`.

### `compress.orderby`

The column(s) to order by within compressed batches. Accepts a single column name (string) or a list of column names, each with an optional `ASC` / `DESC` suffix (e.g. `'ts DESC'`). Defaults to the `datetime` index, descending.

!!! warning
    TimescaleDB compression requires the target table to be a [hypertable](#hypertable). Other flavors fall back to their native table compression where supported (e.g. `ROW_FORMAT=COMPRESSED` for MySQL / MariaDB, `DATA_COMPRESSION = PAGE` for MSSQL); flavors without table-level compression return an informative failure.

??? example

    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'compress',
        instance='sql:main',
        columns={'datetime': 'ts', 'id': 'station'},
        compress={
            'after': '30 days',
            'segmentby': ['station'],
            'orderby': ['ts DESC'],
        },
    )
    ```

    Inspect the on-disk size of a pipe's target table with `Pipe.get_size()` or the `show targets` action:

    ```python
    print(pipe.get_size())
    # 1859584
    ```

---------------

## `dtypes`

You will often want to explictly set the dtypes for certain columns, which you can do with the `dtype` parameters.

If not explicitly set, dtypes are inferred from the first sync, and syncing conflicting dtypes into an inferred dtype column will alter the column's type (unless [`static`](#static) is `True`. See [`mixed_numerics`](#mixed_numerics) for handling syncing floats into integer columns).

??? tip "Generic, specific, or both"
    You may either choose the base Meerschaum dtype (e.g. `int`) or a specific Pandas-supported dtype (e.g. `int32[pyarrow]`):

    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'dtypes',
        temporary=True,
        instance='sql:local',
        autotime=True,
        columns={'datetime': 'ts', 'id': 'id'},
        dtypes={
            'id': 'int32[pyarrow]',
            'ts': 'datetime64[ms, UTC]',
            'val': 'float',
        },
    )
    pipe.sync("id:1,val:2.2")

    df = pipe.get_data()
    print(df.dtypes)
    # id          int32[pyarrow]
    # val                float64
    # ts     datetime64[ms, UTC]
    # dtype: object 

    pipe.sync('id:2,foo:3')
    print(pipe.dtypes)
    # {'id': 'int32[pyarrow]', 'val': 'float', 'ts': 'datetime64[ms, UTC]', 'foo': 'int64[pyarrow]'}

    pipe.sync("id:3,foo:4.4")
    print(pipe.dtypes)
    # {'id': 'int32[pyarrow]', 'val': 'float', 'ts': 'datetime64[ms, UTC]', 'foo': 'numeric'}

    pipe.sync("id:4,bar:5.5")
    print(pipe.dtypes)
    # {'id': 'int32[pyarrow]', 'val': 'float', 'ts': 'datetime64[ms, UTC]', 'foo': 'numeric', 'bar': 'float64[pyarrow]'}

    pipe.sync("id:5,bar:text")
    print(pipe.dtypes)
    # {'id': 'int32[pyarrow]', 'val': 'float', 'ts': 'datetime64[ms, UTC]', 'foo': 'numeric', 'bar': 'string[pyarrow]'}
    ```

Below are the supported Meerschaum data types. See the [SQL dtypes source](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/dtypes/sql.py) to see which types map to specific database types.

| Data Types | Examples | Python, Pandas Types | SQL Types Notes |
|---|---|---|---|
| `int` | `1` | `int`, `Int64`, `int64[pyarrow]`, `Int32`, `int32[pyarrow]`, etc. | `BIGINT`, `INT` |
| `float` | `1.1` | `float`, `float64`, `float64[pyarrow]`, etc. | `DOUBLE PRECISION`, `FLOAT` |
| `string` | `'foo'` | `str`, `string[python]`, etc. | `TEXT`, `NVARCHAR(MAX)` for MSSQL, `NVARCHAR2(2000)` for Oracle. |
| `datetime` (tz-aware) | `Timestamp('2024-12-26 00:00:00+0000', tz='UTC')` | `datetime`, `datetime64[us, UTC]`, etc. | `TIMESTAMPTZ`, `DATETIMEOFFSET`. Offsets are coerced to UTC. |
| `datetime64[precision]` (tz-naive) | `Timestamp('2025-07-23 00:00:00')` | `datetime`, `datetime64[us]`, etc. | `TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE` |
| `date` | `date(2025, 1, 1)` | `date`, `date32[day][pyarrow]`, `date64[ms][pyarrow]` | `DATE` |
| `numeric`, `numeric[precision,scale]` | `Decimal('1.000')` | `Decimal` | `NUMERIC`, `DECIMAL`. Use `precision` and `scale` if provided. |
| `uuid` | `UUID('df2572b5-e42e-410d-a624-a14519f73e00')` | `UUID` | `UUID` where supported. `UNIQUEIDENTIFIER` for MSSQL, otherwise `TEXT`. |
| `bool` | `True` | `boolean[pyarrow]` | `BOOL`, `BIT`, `INT` for Oracle, MSSQL, MySQL / MariaDB, `FLOAT` for SQLite. |
| `json` | `{"foo": "bar"}` | `dict`, `list` | `JSONB` for PostgreSQL-like flavors, otherwise `TEXT`. |
| `bytes` | `b'foo bar'` | `bytes`, `binary[pyarrow]` | `BYTEA`, `BLOB`, `VARBINARY`, otherwise base64-encoded. |
| `geometry`, `geometry[type,srid]`, `geography` | `Point`, `MultiLineString`, etc. | `shapely.Point`, etc. | `GEOMETRY`, `GEOMETRY[POINT, 4326]`, `GEOGRAPHY`, etc. for PostGIS. Otherwise base64-encoded WKB. |

---------------

## `enforce`

The `enforce` parameter controls whether a pipe coerces incoming data to match the set data types (default `True`). If your workload is performance-sensitive, consider experimenting with `enforce=False` to skip the extra work required to ensure incoming data matches the configured dtypes.

!!! warning
    Some instance connectors behave unexpectedly when `enforce=False` (e.g. SQLite).
    
---------------

## `fetch`

The `fetch` key contains parameters concerning the [fetch stage](/reference/pipes/syncing/) of the syncing process.

### `fetch.backtrack_minutes`

How many minutes of overlap to request when fetching new rows ― see [Backtracking](/reference/pipes/syncing/#backtracking). Defaults to 1440.

### `fetch.definition`

!!! example inline end ""
    ```yaml
    fetch:
      definition: |-
        SELECT *
        FROM foo
    ```
    
The base SQL query to be run when fetching new rows. Aliased as `sql` or `query` for convenience. This only applies to pipes with [`SQLConnectors`](/reference/connectors/sql-connectors/) as connectors.

---------------

## `hypertable`

On **TimescaleDB**, the target table is created as a hypertable by default. Set `hypertable` to `False` to create a plain table instead.

On **PostgreSQL / PostGIS**, **MySQL / MariaDB**, and **MSSQL**, `hypertable` controls [native range partitioning](/reference/connectors/sql-connectors/#native-range-partitioning) and also **defaults to `True`** — so a pipe with a [`datetime`](#the-datetime-index) column partitions its target table by range on that column. Set `hypertable` to `False` to opt out (a plain table). Requires a `datetime` column; ignored on flavors without native range partitioning (e.g. SQLite, DuckDB, Oracle). Only newly created tables are affected — a pre-existing plain table is not retroactively partitioned (use `partition pipes` to rebuild it).

The partition width is the pipe's chunk interval ([`verify.chunk_minutes`](#verifychunk_minutes) and its aliases, default 43200 — 30 days), and partition boundaries are epoch-aligned — the same datetime always maps to the same partition regardless of insert order, and verification chunks (`get_chunk_bounds(align=True)`) coincide with partition edges.

`verify.chunk_minutes` is the **authoritative** partition width: editing it does not retroactively reshape an existing table, and changing it for a populated table can produce misaligned, overlapping partitions. To change an existing table's width, run the `partition pipes` action (or [`Pipe.repartition()`](https://docs.meerschaum.io/meerschaum.html#Pipe.repartition)), which rebuilds the table at the new width:

```bash
# Rebuild the table to 7-day partitions.
mrsm partition pipes -i sql:main -m weather --chunk-minutes 10080
```

??? example

    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'partition',
        instance='sql:main',  # a PostgreSQL/MySQL/MSSQL connector
        columns={'datetime': 'ts', 'id': 'station'},
        parameters={
            'hypertable': True,
            'verify': {'chunk_minutes': 43200},  # 30-day partitions (default)
        },
    )
    ```

---------------

## `hypercore`

TimescaleDB-only. By default (`True`), the [Hypercore columnstore](https://www.tigerdata.com/docs/build/columnar-storage/setup-hypercore) is enabled when the hypertable is created — the `CREATE TABLE` declares `tsdb.segmentby` / `tsdb.orderby` (resolved the same way as [`compress`](#compress)), which causes TimescaleDB to auto-create a columnstore policy that converts old chunks in the background.

Set `hypercore` to `False` for a plain row-store hypertable (no columnstore, no auto-policy). Has no effect unless the pipe is a [`hypertable`](#hypertable).

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

## `parents`

Associate a pipe with its upstream pipes via the `parents` (or `parent`) key. For example, syncing a SQL pipe with a defined `parent` allows for changing the data type of the `datetime` column between integer and timestamp values. The complement to `parents` is `children` (`child`).

Set `parents` to a list of keys (`connector`, `metric`, `location`, etc.) or a string for the `Pipe` constructor.

??? example
    The `parent` key corresponds to the constructor for the parent pipe.

    ```yaml
    pipes:
    - connector: foo
      metric: bar
      parameters:
        children: 
        - connector: foo
          metric: bar
          location: child

    - connector: foo
      metric: bar
      location: child
      parameters:
        parent: "Pipe('foo', 'bar')"

    ```


## `precision`

The unit set by `precision` determines the value of the timestamp captured by `autotime` in units since the Unix Epoch in UTC (see above). By default, the `datetime` axis dtype determines `Pipe.precision` (e.g. the default `datetime64[us, UTC]` is `microsecond` precision).

| Units         | Aliases    | Datetimes                       | Integers              |
|---------------|------------|---------------------------------|-----------------------|
| `nanosecond`  | `ns`       | `2025-07-18 16:02:18.701925991` | `1752854538701925991` |
| `microsecond` | `us`       | `2025-07-18 16:02:18.701925`    | `1752854538701925`    |
| `millisecond` | `ms`       | `2025-07-18 16:02:18.701`       | `1752854538701`       |
| `second`      | `sec`, `s` | `2025-07-18 16:02:18`           | `1752854538`          |
| `minute`      | `min`, `m` | `2025-07-18 16:02`              | `29214242`            |
| `hour`        | `hr`, `h`  | `2025-07-18 16:00`              | `486904`              |
| `day`         | `d`, `D`   | `2025-07-18`                    | `20287`               |


For additional functionality, you may set `precision` as a dictionary with the following keys:

- `unit`
- `interval`
- `round_to`

### `precision.unit`

The precision unit to use when capturing the current timestamp. Setting `precision` as a string is shorthand for setting `precision.unit`.

### `precision.interval`

!!! warning inline end
    Nanosecond precision only supports an interval of 1.

When rounding the current timestamp, `precision.interval` determines the size of the delta (default 1). For example, when `precision.unit='minute'` and `precision.interval=15`, then the current timestamp will be rounded down (or nearest, see below) to even 15-minute intervals.

??? example

    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'precision', 'interval',
        instance='sql:memory',
        autotime=True,
        columns={'datetime': 'ts'},
        precision={
            'unit': 'minute',
            'interval': 15,
        },
    )
    pipe.sync("a:1")

    df = pipe.get_data()
    print(df)

    #    a                        ts
    # 0  1 2025-07-23 13:15:00+00:00
    ```

### `precision.round_to`

This determines the direction to which the current timestamp is coerced when rounding (See [`meerschaum.utils.dtypes.round_time()`](https://docs.meerschaum.io/meerschaum/utils/dtypes.html#round_time)). Accepted values are the following:

- `down` (default)
- `up`
- `closest`

---------------

## `reference`

A pipe may inherit the base parameters from another reference pipe. Set `reference` to the keys of the base pipe, and additional keys will override the base parameters. To symlink subsets of other pipes' parameters, see the example at top of the page on using the `{{ Pipe(...) }}` syntax. The `reference` key may be a dictionary or literal string to a `Pipe` constructor. Note that if the instance keys are not set, the instance keys of the pipe will be used as the default (to handle changing default instances across environments).

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

### `verify.bound_days`

The key `verify.bound_days` specifies the interval when determining the bound time, which is limit at which long-running verfication syncs should stop.

In addition to days, alias keys are allowed to specify other units of time. In order of priority, the supported keys are the following:

- `bound_minutes`
- `bound_hours`
- `bound_days`
- `bound_weeks`
- `bound_years`
- `bound_seconds`

??? example

    ```python
    import meerschaum as mrsm
    
    pipe = mrsm.Pipe(
        'foo', 'bar',
        parameters={
            'verify': {
                'bound_days': 366,
            },
        },
    )
    ```

### `verify.chunk_minutes`

The key `verify.chunk_minutes` specifies the size of chunk intervals when verifying a pipe (see [`Pipe.get_chunk_bounds()`](https://docs.meerschaum.io/meerschaum.html#Pipe.get_chunk_bounds)). It is also the **partition width** for [natively range-partitioned](/reference/connectors/sql-connectors/#native-range-partitioning) pipes. Defaults to 43200 (30 days).

You may instead specify the size with one of these aliases (like the `bound_*` keys). If several are set, the first on this priority list wins:

- `chunk_minutes`
- `chunk_hours`
- `chunk_days`
- `chunk_weeks`
- `chunk_years`
- `chunk_seconds`

??? example

    ```python
    import meerschaum as mrsm

    # These two pipes use the same chunk size.
    mrsm.Pipe('foo', 'minutes', parameters={'verify': {'chunk_minutes': (1440 * 7)}})
    mrsm.Pipe('foo', 'days', parameters={'verify': {'chunk_days': 7}})
    ```

### `verify.chunk_range`

For a pipe with an **integer** [`datetime`](#the-datetime-index) axis, `verify.chunk_range` sets the chunk size directly in the axis's own units (used verbatim). This is the integer-axis counterpart to `chunk_minutes`.

When `chunk_range` is not set, the time-based size above is converted to the axis's units using the pipe's [`precision`](#precision); if no `precision` is set, the chunk's value in minutes is used verbatim (legacy behavior).

??? example

    ```python
    import meerschaum as mrsm

    # An integer-axis pipe chunked 1000 units at a time.
    pipe = mrsm.Pipe(
        'foo', 'int_axis',
        columns={'datetime': 'ts'},
        dtypes={'ts': 'int'},
        parameters={'verify': {'chunk_range': 1000}},
    )
    ```