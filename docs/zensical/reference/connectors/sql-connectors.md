# 🗂️ SQL Connectors

Meerschaum's first-class connector is the [`SQLConnector`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector). Several built-in `sql` connectors are defined by default:

Connector | Description | URI
----------|-------------|----
**`sql:main`** (default) |  The pre-configured [TimescaleDB](https://github.com/timescale/timescaledb) instance included in the [Meerschaum Stack](/reference/stack/). It corresponds to a database running on `localhost` and is therefore shared amongst environments. | `postgresql+psycopg://mrsm:mrsm@localhost:5432/meerschaum`
**`sql:local`** | A SQLite file within the [Meerschaum root directory](/reference/environment/#mrsm_root_dir). Because `sql:local` is contained in the root directory, it is isolated between environments. | `sqlite:///$MRSM_ROOT_DIR/sqlite/mrsm_local.db`
**`sql:memory`** | An in-memory SQLite database. This is not persistent and is isolated per-process. | `sqlite:///:memory:`

Add new connectors with [`bootstrap connectors`](/reference/connectors/#-creating-a-connector) or by setting [environment variables](/reference/connectors/#-environment-connectors).

## Supported Flavors

The following database flavors are confirmed to be feature-complete through the Meerschaum [test suite](https://github.com/bmeares/Meerschaum/blob/main/tests/connectors.py) and are listed in descending order of compatability and performance.

- TimescaleDB
- PostgreSQL
- Citus
- SQLite
- MariaDB
- MySQL 5.7+
- DuckDB
- Microsoft SQL Server
- Oracle SQL

## In-place Syncs

When a pipe has the same fetch and instance connectors, syncing will occur entirely within the database context through SQL. As such, this is a high-performance method to incrementally materialize views.

??? example "Inplace syncing example"

    ```python
    import meerschaum as mrsm

    weather_pipe = mrsm.Pipe(
        'plugin:noaa', 'weather', 'atl',
        columns = {
            'datetime': 'timestamp',
            'id': 'station',
        },
        parameters = {
            'noaa': {
                'stations': ['KATL'],
            },
        },
    )
    inplace_pipe = mrsm.Pipe(
        'sql:main', 'weather_avg', 'atl',
        columns = {
            'datetime': 'day',
            'station': 'station',
        },
        parameters = {
            'sql': f"""
                SELECT
                    TIME_BUCKET('1 day', timestamp) AS "day",
                    station,
                    AVG("temperature (degC)") AS avg_temp
                FROM "{weather_pipe.target}"
                GROUP BY "day", station
            """,
        },
    )

    ### Because the input and output connectors are both `sql:main`,
    ### syncing occurs entirely in SQL and nothing is loaded into RAM.
    success, msg = inplace_pipe.sync()

    df = inplace_pipe.get_data()
    df
    #           day station   avg_temp
    # 0  2023-11-22    KATL       12.2
    # 1  2023-11-24    KATL  15.916667
    # 2  2023-11-25    KATL  10.579167
    # 3  2023-11-26    KATL       9.01
    # 4  2023-11-27    KATL   7.951852
    # ..        ...     ...        ...
    # 58 2024-05-21    KATL  25.208696
    # 59 2024-05-22    KATL  25.185714
    # 60 2024-05-23    KATL  26.004545
    # 61 2024-05-24    KATL  26.479167
    # 62 2024-05-25    KATL       25.0
    # 
    # [63 rows x 3 columns]
    ```

## Native Range Partitioning

[TimescaleDB](https://github.com/timescale/timescaledb) auto-creates chunks on insert. Other flavors do not, so Meerschaum declares a natively range-partitioned table and pre-creates the partitions a sync needs. This is controlled by the [`hypertable`](/reference/pipes/parameters/#hypertable) parameter, which **defaults to `True`** (the same flag and default TimescaleDB uses) — so a pipe with a [`datetime`](/reference/pipes/parameters/#the-datetime-index) column is partitioned by default on:

- **PostgreSQL** / **PostGIS**
- **MySQL** / **MariaDB**
- **Microsoft SQL Server**

Set `hypertable` to `False` to opt out and create a plain table. A partitioned pipe must define a `datetime` column (the partition axis); `hypertable` has no effect on flavors without native range partitioning (SQLite, DuckDB, Oracle). Pre-existing plain tables are never retroactively partitioned — partition creation is skipped for a table that isn't already declaratively partitioned, so enabling `hypertable` only affects tables created afterward (use `partition pipes` to rebuild an existing one).

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

### Boundaries

The partition width is the pipe's chunk interval ([`verify.chunk_minutes`](/reference/pipes/parameters/#verifychunk_minutes) and its `chunk_hours`/`chunk_days`/… aliases, default 43200 — 30 days). Each `[lo, hi)` boundary is **epoch-aligned**: the grid is anchored to the Unix epoch (`1970-01-01`), so a given datetime always lands in the same partition no matter which rows arrive first. This makes partitioning deterministic and lets [verification syncs](/reference/pipes/syncing/#verification-syncs) align their chunk edges (`Pipe.get_chunk_bounds(align=True)`) to the partition boundaries.

The `datetime` axis may be an integer epoch ([`dtype` `int`](/reference/pipes/parameters/#dtypes)); the width then comes from [`verify.chunk_range`](/reference/pipes/parameters/#verifychunk_range) (or the time-based width converted via the pipe's `precision`), and boundaries are plain integers aligned to the interval. The partition column is folded into the table's primary key, as each flavor requires the partition key to be part of the PK / clustered index.

### Per-flavor mechanics

The partitioned parent is created with flavor-specific DDL when the table is first created; child partitions are then created on demand in `sync_pipe`, **before** each batch of rows is inserted, by walking the interval grid from the dataframe's minimum to maximum datetime (capped at `system.connectors.sql.instance.max_partitions_per_sync`, default 10,000, per sync — raise that or `chunk_minutes` if you hit the warning).

| Flavor | Parent table DDL | Adding partitions |
|---|---|---|
| **PostgreSQL** / **PostGIS** | `PARTITION BY RANGE (<dt>)` — an empty parent holding no rows. | `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES FROM (lo) TO (hi)` per missing child. |
| **MySQL** / **MariaDB** | `PARTITION BY RANGE COLUMNS (<dt>) (…)` — initial partitions declared inline (MySQL cannot create a zero-partition table), computed from the first sync's dataframe. | `ALTER TABLE … ADD PARTITION (… VALUES LESS THAN (hi))`, appending upward from the highest existing boundary. |
| **MSSQL** | A `CREATE PARTITION FUNCTION … AS RANGE RIGHT FOR VALUES (…)` plus `CREATE PARTITION SCHEME` are created first; the table's clustered PK is placed on the scheme. | `ALTER PARTITION SCHEME … NEXT USED` + `ALTER PARTITION FUNCTION … SPLIT RANGE (boundary)` per new boundary. The scheme and function are dropped with the table. |

For PostgreSQL the highest partition is determined from the requested grid directly; for MySQL/MariaDB and MSSQL the connector reads the highest existing boundary (`information_schema.PARTITIONS` and `sys.partition_range_values`, respectively) and only appends partitions at or beyond it, so re-syncing an existing range adds nothing. Datetime boundary literals carry their offset on PostgreSQL (`TIMESTAMPTZ`) and are normalized to naive UTC on MySQL/MariaDB (which store datetimes timezone-naive).

### Changing the partition width

`verify.chunk_minutes` is the **authoritative** partition width. It is read at sync time, so editing it does not retroactively reshape an existing table — and worse, a changed width laid over an existing grid (e.g. a 7-day grid over 30-day partitions) produces overlapping boundaries that PostgreSQL rejects outright. Treat the width as fixed for the life of a table.

To actually change the width of an existing table, use the `partition pipes` action (or [`Pipe.repartition()`](https://docs.meerschaum.io/meerschaum.html#Pipe.repartition)), which updates `verify.chunk_minutes` and rebuilds the table at the new width:

```bash
# Rebuild to 7-day partitions (defaults to the pipe's verify.chunk_minutes if omitted).
mrsm partition pipes -i sql:main -m weather --chunk-minutes 10080
```

| Flavor | Repartition strategy |
|---|---|
| **TimescaleDB** | `set_chunk_time_interval()` — applies to **future** chunks only; existing chunks keep their size (no rewrite). |
| **PostgreSQL** / **PostGIS**, **MySQL** / **MariaDB**, **MSSQL** | The table is rebuilt at the new width: its data is read, the table is dropped, and the data is re-synced (recreating the table and its partitions). The pinned width is updated. |

!!! warning "Rebuild cost"
    The non-TimescaleDB rebuild reads the whole table into memory and briefly drops it before re-syncing, so run it during a maintenance window for large tables. Choosing a sensible width up front avoids the need to repartition: a too-small interval over a wide range creates many partitions (and may hit the 10,000-per-sync cap), while a too-large interval reduces the benefit of partition pruning.

## Utility Functions

If you work closely with relational databases, you may find the `SQLConnector` very useful. See below for several handy functions that Meerschaum provides:

### [`SQLConnector`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector) Methods

#### [`read()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.read)

Pass the name of a table or a SQL query into `SQLConnector.read()` to fetch a Pandas DataFrame.

```python
import meerschaum as mrsm
conn = mrsm.get_connector("sql:main")
df = conn.read('SELECT 1 AS foo')
df
#    foo
# 0    1

```

`read()` also supports [server-side cursors](https://www.psycopg.org/docs/usage.html#server-side-cursors), allowing you to efficiently stream chunks from the result set:

```python

### Set `as_iterator=True` to return a dataframe generator.
### 
table = 'sql_main_weather_avg_atl'
chunks = conn.read(table, chunksize=25, as_iterator=True)
for chunk in chunks:
    print(f"Loaded {len(chunk)} rows.")

# Loaded 25 rows.
# Loaded 25 rows.
# Loaded 13 rows.
```

#### [`to_sql()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.to_sql)

Wrapper around [`pandas.DataFrame.to_sql()`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html). Persist your dataframes directly to tables.

```python
import meerschaum as mrsm
import pandas as pd

conn = mrsm.get_connector('sql:main')
df = pd.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
results = conn.to_sql(df, 'foo', as_dict=True)

mrsm.pprint(results)
# {
#     'target': 'foo',
#     'method': "functools.partial(<function psql_insert_copy at 0x7f821594d800>, schema='public')",
#     'chunksize': 100000,
#     'num_rows': 2,
#     'start': 203452.165351391,
#     'end': 203452.255752858,
#     'duration': 0.09040146699408069,
#     'success': True,
#     'msg': 'It took 0.09 seconds to sync 2 rows to foo.'
# }
```

#### [`exec()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.exec)

Execute SQL queries directly and return the SQLAlchemy result. This is useful for queries without result sets, like `DROP`, `ALTER`, `CREATE`, `UPDATE`, `INSERT`, etc., as well as executing stored procedures.

```python
import meerschaum as mrsm
conn = mrsm.get_connector('sql:main')
_ = conn.exec('DROP TABLE IF EXISTS foo')
_ = conn.exec("CREATE TABLE foo (bar INT)")

result = conn.exec("INSERT INTO foo (bar) VALUES (1), (2)")
print(f"Inserted {result.rowcount} rows.")
# Inserted 2 rows.
```

#### [`exec_queries()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.exec_queries)

A safer option to execute multiple queries is passing a list to `SQLConnector.exec_queries()`. The flag `break_on_error` will roll back the transaction if any of the provided queries fail.

```python
import meerschaum as mrsm
conn = mrsm.get_connector('sql:main')
conn.exec('DROP TABLE IF EXISTS foo')

### Transaction should fail and roll back,
### meaning `foo` will not be created.
queries = [
    'CREATE TABLE foo (bar INT)',
    'CREATE TABLE foo (a INT, b INT)',
]
results = conn.exec_queries(queries, break_on_error=True, silent=True)
success = len(results) == len(queries)
assert not success

from meerschaum.utils.sql import table_exists
assert not table_exists('foo', conn)
```

### [`meerschaum.utils.sql`](https://docs.meerschaum.io/meerschaum/utils/sql.html) Functions

!!! warning ""
    🚧 This section is still under construction ― code snippets will be added soon!

#### [`build_where()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#build_where)

Build a `WHERE` clause based on the `params` filter.

```python
import meerschaum as mrsm
from meerschaum.utils.sql import build_where

conn = mrsm.get_connector('sql:main')
print(build_where({'foo': [1, 2, 3]}, conn))
# 
# WHERE
#     "foo" IN ('1', '2', '3')

print(build_where({'foo': ['_3', '_4']}, conn))
# 
# WHERE
#     ("foo" NOT IN ('3', '4'))
```

#### [`wrap_query_with_cte()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#wrap_query_with_cte)

Wrap a subquery in a CTE and append an encapsulating query.

```python
from meerschaum.utils.sql import wrap_query_with_cte
sub_query = "WITH foo AS (SELECT 1 AS val) SELECT (val * 2) AS newval FROM foo"
parent_query = "SELECT newval * 3 FROM src"
query = wrap_query_with_cte(sub_query, parent_query, 'mssql')
print(query)
# WITH foo AS (SELECT 1 AS val),
# [src] AS (
#     SELECT (val * 2) AS newval FROM foo
# )
# SELECT newval * 3 FROM src
```

#### [`table_exists()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#table_exists)

Check if a table exists.

```python
import pandas as pd
import meerschaum as mrsm

df = pd.DataFrame([{'a': 1}])
conn = mrsm.get_connector('sql:main')
conn.to_sql(df, 'foo')

from meerschaum.utils.sql import table_exists
print(table_exists('foo', conn))
# True

conn.exec("DROP TABLE foo")
print(table_exists('foo', conn))
# False
```

#### [`get_sqlalchemy_table()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#get_sqlalchemy_table)

Return a SQLAlchemy table object.

```python
import pandas as pd
import meerschaum as mrsm

df = pd.DataFrame([{'a': 1}])
conn = mrsm.get_connector('sql:main')
conn.to_sql(df, 'foo')

from meerschaum.utils.sql import get_sqlalchemy_table
table = get_sqlalchemy_table('foo', conn)
print(table.columns)
# {'a': Column('a', BIGINT(), table=<foo>)}
```

#### [`get_table_cols_types()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#get_table_cols_types)

Return a dictionary mapping a table's columns to data types, even during a not-yet-committed session.

```python
import pandas as pd
import meerschaum as mrsm

df = pd.DataFrame([{'a': 1}])
conn = mrsm.get_connector('sql:main')
conn.to_sql(df, 'foo')

from meerschaum.utils.sql import get_table_cols_types
cols_types = get_table_cols_types('foo', conn)
print(cols_types)
# {'a': 'BIGINT'}
```