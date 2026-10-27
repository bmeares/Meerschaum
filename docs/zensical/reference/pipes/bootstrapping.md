<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🥾 Bootstrapping

The `bootstrap pipes` action includes a setup wizard to guide you through the process of creating new pipes. In this tutorial, we'll be looking at the prompts you'll encounter to help you better understand what each question is asking.

??? example "🎦 Watch an Example"
    The below example demonstrates how to create a pipe from an existing SQL database table.

    In this case, the remote table is called `weather_table` and has a datetime column `timestamp` and ID column `station`. The pipe has the definition `SELECT * FROM weather_table`.
    <asciinema-player src="/assets/casts/bootstrap.cast"></asciinema-player>

To get started, run the following command:

```bash
mrsm bootstrap pipe --shell
```

!!! summary ""
    The `--shell` flag launches an interactive Meerschaum shell after executing the provided action. You can open the Meershaum shell with the `mrsm` command or execute actions on the command line.



## 🔑 Keys

You will be asked for a pipe's three keys: a connector, metric, and location. For more information about how pipes are constructed, consult the [pipes reference page](/reference/pipes/#pipes).

### 🔌 Connector

The first question you will see is *Where are the data coming from?*, after the list of recognized [connectors](/reference/connectors/) (the ones in your configuration, `MRSM_<TYPE>_<LABEL>` environment variables and `plugin:` connectors). A connector defines how Meerschaum interacts with other servers (e.g. username, password, host, etc.). While you type, a dropdown narrows the list down.

If you know what connector you need, go ahead and type its keys (type and label separated by a colon, like `sql:myserver`), otherwise type 'New' to define a new connector. You can read more about how connectors work on the [Connectors reference page](/reference/connectors/).

??? tip "New Connector"

    If you chose `New` to define a new connector, you'll be presented with a screen asking you to choose the connector's type. The type determines the protocol over which data will be transferred, so it's important to choose wisely! You can consult this [Connectors Type table](/reference/connectors/) for the pros, cons, and use cases for each type of connector. The most common connectors are `sql` and `api` connectors, which correspond to your own SQL databases or Meerschaum instances.

    After choosing the connector's type, you assign it a label. These two keys identify your connector and are represented with a colon, e.g. `sql:main` for the default connector or `sql:local` for the SQLite instance.

### 📊 Metric

The metric is a label that describes the contents of a pipe. Pick something descriptive, such as `weather` or `power`.

### 📍 Location *(optional)*

The location is an additional label you may assign to differentiate two pipes that share a connector and metric. If you omit the location, the location key will be `None` (`NULL`).

## 📄 SQL Query (`sql` connectors)

For a pipe with a `sql` connector, the query comes next: the rows the pipe syncs. In a terminal your editor opens a `.sql` file for it (the comment lines at the top are dropped); when the input is not a terminal (a script piping answers in), the query is read as one line. Read another pipe's table with `{{ Pipe('connector', 'metric') }}`.

The wizard runs the query right away and prints its first five rows. If the query fails (a typo, a missing table), it shows the database's error and asks whether to edit the query again.

!!! important "SQL definition caveats"
    The SQL query definition will not be executed directly but rather encased by a larger query via a `WITH` clause. Therefore don't include `ORDER BY` or other keywords which are disallowed in SQL views.

## 🕓 Datetime and ID columns

Next come the datetime column and, optionally, an ID column. These columns index the pipe's table and are necessary for efficiently syncing new data. For a `sql` pipe the choices are the columns of the preview, numbered: the datetime column is guessed from the column types (then from names such as `timestamp` or `created_at`) and an `id` column is the ID's default, so pressing Enter accepts them. For other connectors, type the column names. If you make a mistake, you can change the columns later with `edit pipes`.

!!! info ""
    If the ID is omitted, it is assumed that the pipe contains a single stream of data (i.e. datetimes are unique). The ID is used for partitioning and multiplexing sub-streams into a single pipe.

## 📄 Definition (other connectors)

For a pipe with an `api` connector, you will be prompted for the connector, metric, and location of the remote pipe.

For other types of connectors (like `mqtt` and `plugin`), your editor will open the pipe's attributes YAML file. Metadata needed for extracting data will fall under the `fetch:` key (for example, `mqtt` needs a `topic`. Plugin pipes should not require anything specific).

## 📥 The First Sync

Finally, the wizard asks whether to sync the pipe now; the default answer is yes. The query (or the connector's fetch) runs, and the table is created and indexed. A `sql` pipe registered without a query is not synced: set its query with `mrsm edit definition -c <connector> -m <metric>` first. Check out the [syncing reference page](/reference/pipes/syncing) for more information on how the syncing process works.


## ⤵️ Dropping Tables

If you want to drop a pipe's table without losing metadata, you can later execute the command `drop pipes`. The command `delete pipes` will drop pipes and remote registration information.
