# 🗂️ SQL Connectors

Meerschaum's first-class connector is the [`SQLConnector`](https://docs.meerschaum.io/meerschaum/connectors.html#SQLConnector). Several built-in `sql` connectors are defined by default:

Connector | Description | URI
----------|-------------|----
**`sql:main`** (default) |  The pre-configured [TimescaleDB](https://github.com/timescale/timescaledb) instance included in the [Meerschaum Stack](/reference/stack/). It corresponds to a database running on `localhost` and is therefore shared amongst environments. | `postgresql+psycopg://mrsm:mrsm@localhost:5432/meerschaum`
**`sql:local`** | A SQLite file within the [Meerschaum root directory](/reference/environment/#mrsm_root_dir). Because `sql:local` is contained in the root directory, it is isolated between environments. | `sqlite:///$MRSM_ROOT_DIR/sqlite/mrsm_local.db`
**`sql:memory`** | An in-memory SQLite database. This is not persistent and is isolated per-process. | `sqlite:///:memory:`

Add new connectors with [`bootstrap connectors`](http://localhost:8000/reference/connectors/#-creating-a-connector) or by setting [environment variables](http://localhost:8000/reference/connectors/#-environment-connectors).

## Supported Flavors

The following database flavors are confirmed to be feature-complete through the Meerschaum [test suite](https://github.com/bmeares/Meerschaum/blob/main/tests/connectors.py) and are listed in descending order of compatability and performance.

- TimescaleDB
- PostgreSQL
- Citus
- SQLite
- MariaDB
- MySQL 5.7+
- DuckDB
- Microsoft SQL Server (2016+ recommended)
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

#### [`table_exists()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#table_exists)

#### [`get_sqlalchemy_table()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#get_sqlalchemy_table)

#### [`get_table_cols_types()`](https://docs.meerschaum.io/meerschaum/utils/sql.html#get_table_cols_types)

