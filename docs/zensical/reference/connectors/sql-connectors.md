# 🗂️ SQL Connectors

Meerschaum's first-class connector is the [`SQLConnector`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector). Several built-in `sql` connectors are defined by default:

Connector | Description | URI
----------|-------------|----
**`sql:main`** (default) |  The pre-configured [TimescaleDB](https://github.com/timescale/timescaledb) instance included in the [Meerschaum Stack](/reference/stack/). It corresponds to a database running on `localhost` and is therefore shared amongst environments. | `postgresql+psycopg://mrsm:mrsm@localhost:5432/meerschaum`
**`sql:local`** | A SQLite file within the [Meerschaum root directory](/reference/environment/#mrsm_root_dir). Because `sql:local` is contained in the root directory, it is isolated between environments. | `sqlite:///$MRSM_ROOT_DIR/sqlite/mrsm_local.db`
**`sql:memory`** | An in-memory SQLite database. This is not persistent and is isolated per-process. | `sqlite:///:memory:`

Add new connectors with [`bootstrap connectors`](/reference/connectors/#creating-a-connector) or by setting [environment variables](/reference/connectors/#environment-connectors).

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

PostGIS and GeoPackage are supported for `geometry` columns. DuckDB and Oracle have additional driver requirements:

### DuckDB

In Python, install `meerschaum[sql-duckdb]` and leave `MRSM_DUCKDB_LIB` unset. Rust Pipe operations and SQLAlchemy then share the public Python DuckDB driver, so both interfaces see the same writes. The extra requires `duckdb >= 1.5.6` and `duckdb-engine >= 0.17.0`.

The standalone binary and Python installations without the optional DuckDB driver use DuckDB's own shared library (`libduckdb`, version 1.4 or newer). When no cached or system library is available, the first connection downloads the official release for your platform into `<root>/drivers/` and verifies its checksum.

- `mrsm install driver duckdb` downloads it ahead of time; `mrsm uninstall driver duckdb` removes the cached copy.
- `mrsm show drivers` reports whether DuckDB (and the Oracle client) can be found, and where.
- On an air-gapped host, or with [`MRSM_NO_AUTO_INSTALL`](/reference/environment/#mrsm_no_auto_install) set (which blocks the download, and DuckDB's extension auto-install), point [`MRSM_DUCKDB_LIB`](/reference/environment/#mrsm_duckdb_lib) at a `libduckdb` file you copied there.

Setting `MRSM_DUCKDB_LIB` explicitly selects the native C API in Python too. SQLAlchemy access is then refused to prevent separate database instances from losing writes. Unset it, install the extra, and restart the interpreter to use both interfaces. A process that first selected the native driver also needs a restart after installing the extra. See the [migration guide](/reference/migrating-to-v5/) for connection settings and process restrictions.

The native C API path refuses unsupported per-connection options, including read-only settings. Use the Python extra when configuring these options.

### Oracle SQL

The `oracle` flavor needs [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html) (the `libclntsh` library) installed on the machine; Meerschaum cannot download it for you. Make it visible to the loader:

| Platform | Make the client library findable |
|---|---|
| Linux | Add its directory to `ldconfig` (e.g. a file in `/etc/ld.so.conf.d/` followed by `ldconfig`) or set `LD_LIBRARY_PATH`. |
| macOS | Set `DYLD_LIBRARY_PATH` to the Instant Client directory. |
| Windows | Add the Instant Client directory (containing `oci.dll`) to `PATH`. |

`mrsm show drivers` tells you whether the library was found.

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

[TimescaleDB](https://github.com/timescale/timescaledb) auto-creates chunks on insert. Other flavors do not, so Meerschaum declares a natively range-partitioned table and pre-creates the partitions a sync needs. This is controlled by the [`hypertable`](/reference/pipes/parameters/#hypertable) parameter, which **defaults to `True`** (the same flag and default TimescaleDB uses), so a pipe with a [`datetime`](/reference/pipes/parameters/#the-datetime-index) column is partitioned by default on:

- **PostgreSQL** / **PostGIS**
- **MySQL** / **MariaDB**
- **Microsoft SQL Server**

Set `hypertable` to `False` to opt out and create a plain table. A partitioned pipe must define a `datetime` column (the partition axis); `hypertable` has no effect on flavors without native range partitioning (SQLite, DuckDB, Oracle). Pre-existing plain tables are never retroactively partitioned, partition creation is skipped for a table that isn't already declaratively partitioned, so enabling `hypertable` only affects tables created afterward (use `partition pipes` to rebuild an existing one).

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

The partition width is the pipe's chunk interval ([`verify.chunk_minutes`](/reference/pipes/parameters/#verifychunk_minutes) and its `chunk_hours`/`chunk_days`/… aliases, default 43200, 30 days). Each `[lo, hi)` boundary is **epoch-aligned**: the grid is anchored to the Unix epoch (`1970-01-01`), so a given datetime always lands in the same partition no matter which rows arrive first. This makes partitioning deterministic and lets [verification syncs](/reference/pipes/syncing/#verification-syncs) align their chunk edges (`Pipe.get_chunk_bounds(align=True)`) to the partition boundaries.

The `datetime` axis may be an integer epoch ([`dtype` `int`](/reference/pipes/parameters/#dtypes)); the width then comes from [`verify.chunk_range`](/reference/pipes/parameters/#verifychunk_range) (or the time-based width converted via the pipe's `precision`), and boundaries are plain integers aligned to the interval. The partition column is folded into the table's primary key, as each flavor requires the partition key to be part of the PK / clustered index.

### Per-flavor mechanics

The partitioned parent is created with flavor-specific DDL when the table is first created; child partitions are then created on demand in `sync_pipe`, **before** each batch of rows is inserted, by walking the interval grid from the dataframe's minimum to maximum datetime (capped at `system.connectors.sql.instance.max_partitions_per_sync`, default 10,000, per sync, raise that or `chunk_minutes` if you hit the warning).

| Flavor | Parent table DDL | Adding partitions |
|---|---|---|
| **PostgreSQL** / **PostGIS** | `PARTITION BY RANGE (<dt>)`, an empty parent holding no rows. | `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES FROM (lo) TO (hi)` per missing child. |
| **MySQL** / **MariaDB** | `PARTITION BY RANGE COLUMNS (<dt>) (…)`, initial partitions declared inline (MySQL cannot create a zero-partition table), computed from the first sync's dataframe. | `ALTER TABLE … ADD PARTITION (… VALUES LESS THAN (hi))`, appending upward from the highest existing boundary. |
| **MSSQL** | A `CREATE PARTITION FUNCTION … AS RANGE RIGHT FOR VALUES (…)` plus `CREATE PARTITION SCHEME` are created first; the table's clustered PK is placed on the scheme. | `ALTER PARTITION SCHEME … NEXT USED` + `ALTER PARTITION FUNCTION … SPLIT RANGE (boundary)` per new boundary. The scheme and function are dropped with the table. |

The datetime column a table is partitioned by keeps its type: [`verify schema`](/reference/actions/#verify-schema) leaves a type change of it out of the plan (PostgreSQL, PostGIS and MSSQL cannot change it in place; MySQL and MariaDB only between `DATE` and `DATETIME` or between integer types) and applies the other changes.

For PostgreSQL the highest partition is determined from the requested grid directly; for MySQL/MariaDB and MSSQL the connector reads the highest existing boundary (`information_schema.PARTITIONS` and `sys.partition_range_values`, respectively) and only appends partitions at or beyond it, so re-syncing an existing range adds nothing. Datetime boundary literals carry their offset on PostgreSQL (`TIMESTAMPTZ`) and are normalized to naive UTC on MySQL/MariaDB (which store datetimes timezone-naive).

### Changing the partition width

`verify.chunk_minutes` is the **authoritative** partition width. It is read at sync time, so editing it does not retroactively reshape an existing table, and worse, a changed width laid over an existing grid (e.g. a 7-day grid over 30-day partitions) produces overlapping boundaries that PostgreSQL rejects outright. Treat the width as fixed for the life of a table.

To actually change the width of an existing table, use the `partition pipes` action (or [`Pipe.repartition()`](https://docs.meerschaum.io/meerschaum.html#Pipe.repartition)), which updates `verify.chunk_minutes` and rebuilds the table at the new width:

```bash
# Rebuild to 7-day partitions (defaults to the pipe's verify.chunk_minutes if omitted).
mrsm partition pipes -i sql:main -m weather --chunk-minutes 10080
```

| Flavor | Repartition strategy |
|---|---|
| **TimescaleDB** | `set_chunk_time_interval()`, applies to **future** chunks only; existing chunks keep their size (no rewrite). |
| **PostgreSQL** / **PostGIS**, **MySQL** / **MariaDB**, **MSSQL** | The table is rebuilt at the new width: its data is read, the table is dropped, and the data is re-synced (recreating the table and its partitions). The pinned width is updated. |

!!! warning "Rebuild cost"
    The non-TimescaleDB rebuild reads the whole table into memory and briefly drops it before re-syncing, so run it during a maintenance window for large tables. Choosing a sensible width up front avoids the need to repartition: a too-small interval over a wide range creates many partitions (and may hit the 10,000-per-sync cap), while a too-large interval reduces the benefit of partition pruning.

## Utility Functions

If you work closely with relational databases, you may find the `SQLConnector` very useful. See below for several handy functions that Meerschaum provides:

### [`SQLConnector`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector) Methods

`read()`, `exec()`, `value()`, `exec_queries()` and `to_sql()` run your SQL on the connector's own
connection pool, the one its pipes use, for every flavor. They need no extra package (no
SQLAlchemy); `read()` returns a DataFrame of the [configured library](/reference/pipes/) (Polars,
or pandas with `as_pandas=True` or `dataframe: pandas`).

Pass parameters instead of formatting values into the text:

| Style | Example |
|---|---|
| `:name` with a dictionary (SQLAlchemy's `text()` style) | `conn.read("SELECT * FROM t WHERE id = :id", params={'id': 1})` |
| `%(name)s` with a dictionary | `conn.exec("DELETE FROM t WHERE id = %(id)s", {'id': 1})` |
| `?` or `%s` with a list or tuple, in order | `conn.value("SELECT COUNT(*) FROM t WHERE ts > ?", ['2024-01-01'])` |
| A list of dictionaries runs the statement once per dictionary | `conn.exec("INSERT INTO t VALUES (:a)", [{'a': 1}, {'a': 2}])` |

SQLAlchemy constructs work too (`text()`, `select()`, `insert()`, ...): they are compiled for the
connector's flavor and run the same way.

`read()`, `exec()` and `value()` raise the database's error when a query fails (a missing
table, a syntax error, an unreachable server). The errors are the standard Python database
exceptions (`OperationalError`, `ProgrammingError`, `IntegrityError`, `DataError`, ...,
importable from `meerschaum.connectors.sql`); when SQLAlchemy is imported they are also
instances of SQLAlchemy's exceptions of the same name, so existing `except` clauses keep
working. Pass `silent=True` to get `None` instead, without a warning:

```python
conn.read('SELECT * FROM missing_table')
# OperationalError: no such table: missing_table
# [SQL: SELECT * FROM missing_table]

conn.read('SELECT * FROM missing_table', silent=True)
# None
```

On a [protected connector](index.md#protected-connectors), `exec()` and `exec_queries()` raise
`PermissionError` and `to_sql()` returns a failure unless writes are allowed. `read()` and
`value()` keep reading: a read statement (`SELECT`, `WITH`, `SHOW`, `EXPLAIN`, a table name)
runs in a transaction that is rolled back (and read-only on PostgreSQL, MySQL and MariaDB), and
a statement that writes (`DELETE ... RETURNING`, `CREATE TABLE ... AS SELECT`, several
statements) raises `PermissionError`:

```python
prod = mrsm.get_connector('sql:prod')  # protected: true
prod.read('SELECT * FROM weather LIMIT 10')    # a DataFrame
prod.value('DELETE FROM weather RETURNING id')
# PermissionError: Refusing to execute SQL on protected instance 'sql:prod'. Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1).
```

#### [`read()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.read)

Pass the name of a table or a SQL query into `SQLConnector.read()` to fetch a DataFrame.

```python
import meerschaum as mrsm
conn = mrsm.get_connector("sql:main")
df = conn.read('SELECT 1 AS foo')
df
# shape: (1, 1)
# ┌─────┐
# │ foo │
# │ --- │
# │ i32 │
# ╞═════╡
# │ 1   │
# └─────┘
```

With `as_pandas=True` (or `dataframe: pandas`), the frame is the one `pandas.read_sql_query()`
builds: the same columns, dtypes and values Meerschaum 4 returned (`coerce_float`, `dtype` and
`index_col` apply as there).

`read()` streams large results in chunks: `as_iterator=True` returns a generator of DataFrames
of `chunksize` rows (only one chunk is in memory at a time), `as_chunks=True` a list of them,
and `chunk_hook` calls a function on each chunk:

```python
table = 'sql_main_weather_avg_atl'
chunks = conn.read(table, chunksize=25, as_iterator=True)
for chunk in chunks:
    print(f"Loaded {len(chunk)} rows.")

# Loaded 25 rows.
# Loaded 25 rows.
# Loaded 13 rows.
```

#### [`to_sql()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.to_sql)

Persist a DataFrame (pandas or Polars) to a table. `if_exists` decides what happens when the
table exists: `'replace'` (the default) drops and creates it, `'append'` inserts into it, and
`'fail'` refuses. A new table gets the column types `pandas.DataFrame.to_sql()` would create
(override them with `dtype={'col': 'TEXT'}`), and the rows are inserted in one transaction
(PostgreSQL `COPY` and MSSQL `OPENJSON` when `system:connectors:sql:bulk_insert` is on).

```python
import meerschaum as mrsm
import polars as pl

conn = mrsm.get_connector('sql:main')
df = pl.DataFrame([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
results = conn.to_sql(df, 'foo', as_dict=True)

mrsm.pprint(results)
# {
#     'target': 'foo',
#     'method': 'multi',
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

Execute SQL directly. This is useful for statements without result sets, like `DROP`, `ALTER`,
`CREATE`, `UPDATE`, `INSERT`, etc., as well as executing stored procedures.

```python
import meerschaum as mrsm
conn = mrsm.get_connector('sql:main')
_ = conn.exec('DROP TABLE IF EXISTS foo')
_ = conn.exec("CREATE TABLE foo (bar INT)")

result = conn.exec("INSERT INTO foo (bar) VALUES (1), (2)")
print(f"Inserted {result.rowcount} rows.")
# Inserted 2 rows.

for row in conn.exec("SELECT bar FROM foo ORDER BY bar"):
    print(row.bar, row[0], row._mapping['bar'])
# 1 1 1
# 2 2 2
```

The result works like SQLAlchemy's: `fetchall()`, `fetchone()`, `fetchmany()`, `first()`,
`one()`, `scalar()`, `scalars()`, `mappings()`, `keys()`, `rowcount` and iteration. Rows are
tuples whose values are what the database's Python driver returns (`int`, `float`, `Decimal`,
`str`, `bytes`, `datetime`, `date`, `dict` for PostgreSQL JSON, `UUID`, ...), also readable by
column name.

#### [`exec_queries()`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector.exec_queries)

A safer option to execute multiple queries is passing a list to `SQLConnector.exec_queries()`:
they run in one transaction. The flag `break_on_error` will roll back the transaction if any of
the provided queries fail.

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
success = len(results) == len(queries) and all(results)
assert not success

from meerschaum.utils.sql import table_exists
assert not table_exists('foo', conn)
```

#### `engine` (SQLAlchemy)

`conn.engine` is a SQLAlchemy engine for code that needs one (an ORM, `pandas.read_sql()` on
your own terms). It needs the `sql` extra (`pip install 'meerschaum[sql]'`) and opens the
database through Python's own driver, separately from the connector's pool.

!!! warning "SQLite: one SQLite library per file"
    On SQLite, `conn.engine` uses Python's `sqlite3` module, a second copy of SQLite in the
    process. Two copies keep separate lock bookkeeping for one file, and closing the file in one
    drops the other's locks ([How To Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html),
    2.2.1). Do not write a SQLite file through `conn.engine` while the same process reads or
    writes it through pipes, `read()` or `exec()`; use `read()` and `exec()` instead.

`exec(..., with_connection=True)` (which returns a SQLAlchemy connection), a custom
`to_sql(method=...)` and Dask DataFrames use the engine too. A connector with
`native_sql: false` runs all of its raw SQL through SQLAlchemy, as Meerschaum 4 did.

### [`meerschaum.utils.sql`](https://docs.meerschaum.io/meerschaum/utils/sql.html) Functions

!!! warning ""
    🚧 This section is still under construction, code snippets will be added soon!

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
