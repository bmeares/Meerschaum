# Bootstrap a Pipe

The `bootstrap pipes` action includes a setup wizard to guide you through the process of creating new pipes. In this tutorial, we'll be looking at the prompts you'll encounter to help you better understand what each question is asking.

To get started, run the following command:

```bash
mrsm bootstrap pipe --shell
```

!!! tip ""
    The `--shell` flag launches an interactive Meerschaum shell after executing the provided action. You can open the Meershaum shell with the `mrsm` command or execute actions on the command line.
    
## Keys

You will be asked for a pipe's three keys: a connector, metric, and location. For more information about how pipes are constructed, consult the [pipes reference page](/reference/pipes/#pipes).

### Connector

The first question you will see is *Where are the data coming from?* along with a list of recognized [connectors](/reference/connectors/). A connector defines how Meerschaum interacts with other servers (e.g. username, password, host, etc.).

If you know what connector you need, go ahead and type its keys (type and label separated by a colon, like `sql:myserver`), otherwise type 'New' to define a new connector. You can read more about how connectors work on the [Connectors reference page](/reference/connectors/).

#### New Connector

If you chose `New` to define a new connector, you'll be presented with a screen asking you to choose the connector's type. The type determines the protocol over which data will be transferred, so it's important to choose wisely! You can consult this [Connectors Type table](/reference/connectors/#type) for the pros, cons, and use cases for each type of connector. The most common connectors are `sql` and `api` connectors, which correspond to your own SQL databases or Meerschaum instances.

After choosing the connector's type, you assign it a label. These two keys identify your connector and are represented with a colon, e.g. `sql:main` for the default connector or `sql:local` for the SQLite instance.

### Metric

The metric is a label that describes the contents of a pipe. Pick something descriptive, such as `weather` or `power`.

### Location (optional)

The location is an additional label you may assign to differentiate two pipes that share a connector and metric. If you omit the location, the location key will be `None` (`NULL`).

## Datetime and ID columns
You will be prompted for the names of the datetime column and optionally an ID column. These columns will be used to index the pipe's table and are necessary for efficiently syncing new data. Consult your data source for the column names, and if you make a mistake, you can change the column names later with `edit pipes`.

!!! info ""
    If the ID is omitted, it is assumed that the pipe contains a single stream of data (i.e. datetimes are unique). The ID is used for partitioning and multiplexing sub-streams into a single pipe.

## Definition

The last piece of metadata is the pipe's definition, the information needed to extract the data.

For a pipe with a `sql` connector, your editor will open a `.sql` file for a SQL query.

!!! important "SQL definition caveats"
    The SQL query definition will not be executed directly but rather encased by a larger query via a `WITH` clause. Therefore don't include `ORDER BY` or other keywords which are disallowed in SQL views.

For a pipe with an `api` connector, you will be prompted for the connector, metric, and location of the remote pipe.

For other types of connectors (like `mqtt` and `plugin`), your editor will open the pipe's attributes YAML file. Metadata needed for extracting data will fall under the `fetch:` key (for example, `mqtt` needs a `topic`. Plugin pipes should not require anything specific).

## Syncing

If you provided a definition and the correct index column names, you will be asked if you would like to sync new data into the pipe. When bootstrapping a new pipe, the definition will be executed, and the table will be created and indexed.

### Troubleshooting

In case the sync fails, you can correct the problem by editing the pipe's attributes with the command `edit pipes`. You may also bootstrap an existing pipe to wipe everything and start the process again from the top.

!!! tip "Try before you sync"
    When writing the definition for a `sql` pipe, it's a good idea to first test the SQL query before going through the hassle of bootstrapping a pipe. You can open an interactive SQL session with the `sql <label>` command or in the Python REPL with the `python` command and the following code:
    ```python
    >>> import meerschaum as mrsm
    >>> conn = mrsm.get_connector('sql', '<label>')
    >>> query = """
    ... SELECT * FROM my_table
    ... WHERE foo = 'bar'
    ... """
    >>> df = conn.read(query)
    >>> df
    ```

## Deleting Data

If you want to drop a pipe's table without losing metadata, you can later execute the command `drop pipes`. The command `delete pipes` will drop pipes and remote registration information.