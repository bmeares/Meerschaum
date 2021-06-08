<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# Feature Showcase

The goal behind Meerschaum is to be *The Data Engineer's Toolbox*: to offer features that help you:

- Create and manage your datastreams
- Quickly build a scalable, customizable backend
- Connect unrelated systems for analysis
- Integrate new systems with your existing infrastructure

No matter your situation, Meerschaum can drastically reduce your development time. Continue reading below to see how Meerschaum can help you take your backend to another level.

## Pipes

With Meerschaum, your datastreams are called [pipes](/reference/pipes/). Pipes are organized into a hierarchy so that you can manage select datastreams at once.

![Meerschaum pipes hierarchy](weather_pipes.png)

### Bootstrapping
The [bootstrap command](/get-started/bootstrapping-a-pipe/) walks you through the steps for creating a new pipe and makes adding datastreams straightforward.

<asciinema-player src="/assets/casts/bootstrap.cast" autoplay="true" loop="true" preload="true"></asciinema-player>

### Syncing Service

Data are added

![Meerschaum fetching algorithm](fetch.png)

### Instances

## Data Analysis Tools

### Turn-key Visualization Stack

Meerschaum comes with a [pre-configured data visualization stack](/get-started/starting-the-stack/). You can deploy Grafana and a database in seconds, and additional services may be easily added with `mrsm edit config stack`.

<asciinema-player src="/assets/casts/stack.cast" autoplay="true" loop="true" size="small" preload="true" rows="10"></asciinema-player>

![Example Grafana Dashboard](grafana-dashboard.png)

### Pandas Integration

If you use [pandas](https://pandas.pydata.org/), Meerschaum lets you tap into your datastreams as DataFrames.

```python
>>> import meerschaum as mrsm
>>> 
>>> weather_pipe = mrsm.Pipe('plugin:noaa', 'weather')
>>> 
>>> ### Optionally filter by datetime or other parameters.
>>> df = weather_pipe.get_data(
...   begin = '2020-01-01',
...   end = '2021-01-01',
...   params = {'station': ['KATL', 'KCEU']},
... )
>>> 
```

You can create new pipes on any instance (database) from a DataFrame and index column names.

```python
>>> import meerschaum as mrsm, pandas as pd
>>> df = pd.read_csv('data.csv')
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

You can also forego pipes and access tables directly.

```python
>>> import meerschaum as mrsm, pandas as pd
>>> 
>>> conn = mrsm.get_connector()
>>> df = pd.read_csv('data.csv')
>>> 
>>> ### Wrapper around `df.to_sql()`.
>>> conn.to_sql(df, chunksize=1000)
```

Read a table or query into a Pandas DataFrame with `conn.read()`:

```python
>>> import meerschaum as mrsm
>>> mrsm.get_connector().read('table')
   id            datetime
0   1 2021-06-06 12:00:00
```


### SQLAlchemy Integration

If you're already using [SQLAlchemy](https://www.sqlalchemy.org/), Meerschaum can fit neatly into your workflow.

```python
>>> import meerschaum as mrsm
>>> from meerschaum.connectors.sql._tools import get_sqlalchemy_table
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


### SQL CLI

The `sql` command lets you quickly interact with your databases.

<asciinema-player src="/assets/casts/sql-cli.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

For certain flavors, Meerschaum is integrated with tools from the [dbcli](https://www.dbcli.com/) project that let you drop into an interactive SQL database environment.

The `sql` command also lets you read from tables or execute queries on any of your databases directly from the command line.

<asciinema-player src="/assets/casts/sql-shell.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

## Background Jobs

Some actions need to run continuously, such as running the API or syncing pipes in a loop. Rather than relying on `systemd` or `cron`, you can use the built-in jobs system.

All Meerschaum actions may be executed as background jobs by adding `-d` or `--daemon` flags or by prefacing the command with `start job`. A specific label may be assigned to a job with `--name`.

You can monitor the status of jobs with `show logs`, which will follow the logs of running jobs.

<asciinema-player src="/assets/casts/jobs.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

## Plugins

## Interactive Environments

### Meerschaum Shell

### Web Dashboard

## Ongoing Research
