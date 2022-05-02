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

Special keys in the parameters dictionary are [`columns`](#columns), [`fetch`](/reference/pipes/syncing), and [`tags`](/reference/pipes/tags/).

### Columns

Meerschaum pipes are time-series focused. In order to properly [sync your data](syncing/), you need to specify the name of the datetime column.

To set the column names for your pipes, run the command `edit pipes`:

```bash
edit pipes -c sql:foo -m bar
```

Under the `columns` key, define the name of your `datetime` column. You may also define `id` and `value` columns:

```yaml
###################################################
# Edit the parameters for the Pipe 'sql_foo_bar'. #
###################################################

columns:
  datetime: date
  id: station_id
```
