# Pipes

A Meerschaum pipe represents a data stream into a table and the necessary parameters to sync new data.

Pipes are identified by three primary components in a hierarchy:

1. Connector
2. Metric
3. Location

![Pipes hierarchy](/assets/screenshots/weather_pipes.png)

In the above screenshot, three pipes are represented according to their keys (you can see this view with `show pipes`). The three pipes share a connector (`plugin:noaa`) and metric (`weather`) but have different locations.

## Pipes Keys

The three keys of any pipe are labels that represent the pipe's connector, metric, and location. Below are brief descriptions of what these keys mean, and you can read about these keys when creating a pipe when you [bootstrap a pipe](/get-started/bootstrapping-a-pipe/).

### Connector Keys

Connector keys represent a pipe's connector to its data source. These keys are represented as the connector's type and label, separated by a colon. [Here is a more in-depth explanation of what Meerschaum connectors are and how they work.](/reference/connectors)

In case your pipe is static and does not need a specific connector, you are free to use any label you like (no colon required). For example, the connector keys `csv` are often used to group together data sets that were read from `.csv` files.



## Interacting with Pipes

As you add new data streams, the number of pipes you maintain can quickly grow. To filter by these keys, use the arguments `-c`, `-m`, and `-l` (connector, metric, and location).

For example, the screenshot mentioned above may be created by any of these commands, or any combination of the three:

```bash
show pipes -c plugin:noaa
show pipes -m weather
show pipes -l clemson atlanta chareleston
```

​	