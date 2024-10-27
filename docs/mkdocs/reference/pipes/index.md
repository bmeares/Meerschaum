# 🚰 Pipes
<link rel="stylesheet" type="text/css" href="/assets/css/grid.css" />

A Meerschaum pipe represents a data stream into a table and the necessary parameters to sync new data.

Pipes are identified by three primary components in a hierarchy:

1. 🔌 **Connector** (data source)
2. 📊 **Metric** (label)
3. 📍 **Location** (optional tag)

<p align="center">
<img src="/assets/screenshots/weather_pipes.png" alt="Pipes hierarchy"/>
</p>

In the above screenshot, three pipes are represented according to their keys (you can see this view with `show pipes`). The three pipes share a connector (`plugin:noaa`) and metric (`weather`) but have different locations.

The three keys of any pipe are labels that represent the pipe's connector, metric, and location. Below are brief descriptions of what these keys mean, and you can read about these keys when you [bootstrap a pipe](bootstrapping/).


## Selecting Your Pipes

As you add new data streams, the number of pipes you maintain can quickly grow. You'll need a way to apply the your commands to multiple pipes at a time (e.g. `show pipes`, `drop pipes`, `delete pipes`, etc.).

To filter by these keys, use the arguments `-c`, `-m`, and `-l` (connector, metric, and location).

For example, the screenshot mentioned above may be created by any of these commands, or any combination of the three:

```bash
show pipes -c plugin:noaa
show pipes -m weather
show pipes -l clemson atlanta charleston
```

### Tags

In addition to filtering by the above keys, you can also select a custom group of pipes with [tags](tags/):

```bash
show pipes --tags foo bar
```

### Key Negation

You can select pipes which *don't* have a certain key by prefacing the key with an underscore (`'_'`).

## Parameters

Every pipe has a parameters dictionary where you can store and retrieve metadata.

The screenshots below show the `clemson` pipe's parameters. On the left, you can edit the parameters with `edit pipe -l clemson`, and on the right, you can directly access this dictionary with `pipe.parameters`. These values are set by the `noaa` plugin during registration to determine which data to fetch, and you can even add your own metadata.

<div class="grid-container center">
  <div class="grid-child">
    <img alt="Editing parameters for the Clemson weather pipe" src="/assets/screenshots/edit_parameters.png"/>
  </div>
  <div class="grid-child">
    <img alt="Parameters for the Clemson weather pipe in Python" src="/assets/screenshots/pipe_parameters.png"/>
  </div>
</div>

Some special keys in the parameters dictionary are [`columns`](#columns), [`indices`](#indices), [`fetch`](/reference/pipes/syncing), [`verify`](/reference/pipes/syncing/#verification-syncs), and [`tags`](/reference/pipes/tags/).

### Columns

The `columns` dictionary is how you specify which columns make up a composite primary key when determining updates. The keys are the reference index names, and the values are the column names as seen in the dataset.

Additionally, when `upsert` is `True`, a unique index is created on the designated columns.

!!! tip "The `datetime` column"

    To best take advantage of incremental updates, specify the `datetime` axis.

??? example "`columns` example"

    ```python
    import meerschaum as mrsm

    pipe = mrsm.Pipe(
        'demo', 'temperature',
        instance='sql:local',
        columns={
            'datetime': 'dt',
            'id': 'station',
        },
    )

    pipe.sync([
        {'dt': '2024-01-01 00:00:00', 'station': 'KGMU', 'val': 44.1},
        {'dt': '2024-01-01 00:00:00', 'station': 'KATL', 'val': 48.3},
    ])
    print(pipe.get_data())
    #           dt station   val
    # 0 2024-01-01    KATL  48.3
    # 1 2024-01-01    KGMU  44.1

    pipe.sync([
        {'dt': '2024-01-01 00:00:00', 'station': 'KGMU', 'color': 'blue'},
        {'dt': '2024-01-01 00:00:00', 'station': 'KATL', 'color': 'green'},
    ])
    print(pipe.get_data())
    #           dt station   val  color
    # 0 2024-01-01    KATL  48.3  green
    # 1 2024-01-01    KGMU  44.1   blue
    ```

!!! note ""

    The `datetime` index may be either a timestamp or an integer column. To use an integer `datetime` index, specify the column is `int` under `dtypes`.

    ```yaml
    connector: foo
    metric: bar
    columns:
      datetime: RowNumber
    dtypes:
      RowNumber: int
    ```

### Indices

You may choose to specify additional indices to be created with the `indices` dictionary (alias `indexes`). Whereas the `columns` dictionary is for specifying uniqueness, the `indices` dictionary allows you to specify multi-column indices for performance improvements. This is for extending the `columns` dictionary, so no need to restate the primary index columns.

In the example below, the unique constraint is only created for the columns `ts` and `station`, and an additional multi-column index is created on the columns `city`, `state`, and `country`.

```yaml
connector: sql:main
metric: temperature
columns:
  datetime: ts
  id: station
indices:
  geo: ['city', 'state', 'country']
parameters:
  upsert: true
  sql: |-
    SELECT
      ts,
      station,
      city,
      state,
      country,
      temp_c,
      ((1.8 * temperature_c) + 32) as temp_f
    FROM weather
```