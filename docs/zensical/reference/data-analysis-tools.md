
# 🔧 Data Analysis Tools

## 📈 Visualization Stack

Meerschaum comes with a [pre-configured data visualization stack](/reference/stack/) so that you can spend your time on analysis rather than configuring servers.

<asciinema-player src="/assets/casts/stack.cast" autoplay="true" loop="true" size="small" preload="true" rows="10"></asciinema-player>

<img src="/assets/screenshots/grafana-dashboard.png" alt="Example Grafana Dashboard" width="85%" style="display: block; margin-left: auto; margin-right: auto;">

## 🐻‍❄️ Polars Integration

Meerschaum returns [Polars](https://pola.rs) DataFrames, so you can tap into your datastreams with
Polars' fast, expressive API. Install it with `pip install 'meerschaum[polars]'`.

```python
>>> import meerschaum as mrsm
>>> import polars as pl
>>>
>>> weather_pipe = mrsm.Pipe('plugin:noaa', 'weather')
>>>
>>> ### Optionally filter by datetime or other parameters.
>>> df = weather_pipe.get_data(
...   begin = '2020-01-01',
...   end = '2021-01-01',
...   params = {'station': ['KATL', 'KCEU']},
... )
>>> df.group_by('station').agg(pl.col('temperature').mean())
```

You can create new pipes on any [instance](/reference/connectors/#instances-and-repositories) from a DataFrame and index column names.
Polars DataFrames and LazyFrames (collected at the sync boundary) sync directly:

```python
>>> import meerschaum as mrsm, polars as pl
>>> df = pl.read_csv('data.csv')
>>> csv_pipe = mrsm.Pipe(
...   'csv', 'weather',
...   instance='sql:local',
...   columns={'datetime': 'timestamp', 'id': 'station'},
... )
>>> csv_pipe.sync(df)
```

!!! question "What are instances?"
    A Meerschaum instance is a connector to a database or API that itself contains Meerschaum pipes. [Connectors](/reference/connectors/), such as database connections, are represented in the format `type:label`.

    The instance `sql:local` defaults to a built-in SQLite database.

`Pipe.filter_existing()` accepts Polars frames and returns Polars frames.
Rows are compared natively, the existing rows are read as Polars,
and the anti-join is performed by Polars without a pandas round-trip.
Columns which Polars cannot compare (`uuid`, `object`, and geometry)
fall back to pandas internally and are converted back on return.

```python
unseen_df, update_df, delta_df = csv_pipe.filter_existing(pl.DataFrame(docs))
```

You can also forego pipes and read a table or query directly with `conn.read()`:

```python
>>> import meerschaum as mrsm
>>> mrsm.get_connector('sql:local').read('weather')
shape: (1, 2)
┌─────┬─────────────────────┐
│ id  ┆ datetime            │
│ --- ┆ ---                 │
│ i64 ┆ datetime[μs]        │
╞═════╪═════════════════════╡
│ 1   ┆ 2021-06-06 12:00:00 │
└─────┴─────────────────────┘
```

## 🐼 Pandas Integration

Prefer [pandas](https://pandas.pydata.org/)? Set `dataframe: pandas` in `meerschaum.yaml`
(`mrsm edit config`), or pass `as_pandas=True` to one call, and install
`pip install 'meerschaum[pandas]'`. A root created by Meerschaum 4 or an earlier 5.0 release
candidate keeps pandas on its own (see [DataFrame library](/reference/pipes/reading/#dataframe-library)).

```python
>>> import meerschaum as mrsm, pandas as pd
>>>
>>> df = weather_pipe.get_data(as_pandas=True)
>>>
>>> ### pandas DataFrames sync as they always have.
>>> csv_pipe.sync(pd.read_csv('data.csv'))
>>>
>>> ### A wrapper around `df.to_sql()`.
>>> conn = mrsm.get_connector()
>>> conn.to_sql(pd.read_csv('data.csv'), chunksize=1000)
```


## 🧙‍♂️ SQLAlchemy Integration

If you're already using [SQLAlchemy](https://www.sqlalchemy.org/), Meerschaum can fit neatly into your workflow.

```python
>>> import meerschaum as mrsm
>>> from meerschaum.utils.sql import get_sqlalchemy_table
>>> conn = mrsm.get_connector()
>>>
>>> ### Directly access the `sqlalchemy` engine.
>>> print(conn.engine)
Engine(postgresql://mrsm:***@localhost:5432/meerschaum)
>>>
>>> table = get_sqlalchemy_table('table', connector=conn)
>>> query = sqlalchemy.insert(table).values({'foo': 1, 'bar': 2})
>>>
>>> conn.exec(query)
<sqlalchemy.engine.cursor.LegacyCursorResult object at 0x7fbe992e4190>
```

You can select a single value with `conn.value()`:

```python
>>> import meerschaum as mrsm
>>> conn = mrsm.get_connector()
>>> query = ("SELECT datetime "
...          "FROM table "
...          "WHERE id = 1 "
...          "ORDER BY datetime DESC "
...          "LIMIT 1")
>>> conn.value(query)
datetime.datetime(2021, 6, 6, 12, 0)
```


## ⌨️ SQL CLI

The `sql` command lets you quickly interact with your databases.

<asciinema-player src="/assets/casts/sql-cli.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

For PostgreSQL, MySQL, SQLite, DuckDB, and Oracle flavors, Meerschaum first looks for
[Rainfrog](https://github.com/achristmascarl/rainfrog), a Rust terminal interface with a query
editor, highlighting, history, and schema navigation. Install it separately with
`cargo install rainfrog --locked`. Oracle requires its client libraries, and Rainfrog's DuckDB
driver is unavailable in some builds. Meerschaum does not install a SQL client implicitly.

If Rainfrog is absent, Meerschaum tries the existing Python flavor client (`pgcli`, `mycli`,
`litecli`, or `gadwall`) and then the plain database client. MSSQL retains `mssqlcli` / `sqlcmd`.
`mrsm sql LABEL --nopretty` always selects the plain client (`psql`, `mysql`, `sqlite3`,
`duckdb`, or `sqlcmd`). A nonzero client exit status fails the action.

Set a connector's `cli` attribute to choose an installed executable explicitly, or configure
`system:connectors:sql:cli:FLAVOR` for a flavor-wide preference. For example:

```yaml
system:
  connectors:
    sql:
      cli:
        postgresql: pgcli
        sqlite: rainfrog
```

Connection credentials are passed in the child environment rather than command-line arguments.

The `sql` command also lets you read from tables or execute queries on any of your databases directly from the command line.

<asciinema-player src="/assets/casts/sql-shell.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>
