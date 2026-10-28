# Your first Pipe

A **Pipe** connects a data source to a table you can keep up to date. Let's collect temperature readings in a local SQLite database.

Start in the workspace from [Setup](index.md). Every command below uses `sql:local`.

## Add a little data

Create a source table with one reading:

```bash
mrsm sql local "CREATE TABLE readings (id INTEGER PRIMARY KEY, ts TEXT, temperature REAL)"
mrsm sql local "INSERT INTO readings VALUES (1, '2026-01-01 12:00:00', 21.5)"
```

## Give the Pipe a name and a query

```bash
mrsm register pipes -c sql:local -m temperatures -i sql:local \
  --params '{"columns":{"datetime":"ts","primary":"id"},"dtypes":{"ts":"datetime"},"fetch":{"definition":"SELECT * FROM readings"}}'
```

The three pieces to remember:

| Part | In this example |
|---|---|
| Source (`-c`) | `sql:local`, where the query runs |
| Metric (`-m`) | `temperatures`, the name of this stream |
| Instance (`-i`) | `sql:local`, where the Pipe and its table live |

The datetime column tells the Pipe where to resume. The primary column identifies each reading.

## Sync and look

```bash
mrsm sync pipes -i sql:local -m temperatures
mrsm show data -i sql:local -m temperatures
```

You'll see the reading **21.5** in the Pipe's table. The source table stays separate.

Run the sync again:

```bash
mrsm sync pipes -i sql:local -m temperatures
```

There is still **one row**. Re-reading the same data does not add a duplicate.

??? example "Watch the actual run"
    <asciinema-player src="/assets/casts/v5-first-pipe.cast" size="small" preload="true" rows="18"></asciinema-player>

??? example "Use the same Pipe from Python"
    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe('sql:local', 'temperatures', instance='sql:local')
    success, message = pipe.sync()
    print(message)
    print(pipe.get_data(as_docs=True))
    ```

    Plain dictionaries work with the base package. For a Polars or pandas DataFrame, [install the matching extra](index.md#install) and call `pipe.get_data()`.

[Add new rows →](incremental-sync.md){ .md-button .md-button--primary }

For other sources, a [Python plugin](../reference/plugins/writing-plugins.md) can fetch data from a file, an API, or your own code.
