# 🚰 Pipes

A Meerschaum pipe represents a data stream into a table and the necessary parameters to sync new data.

Pipes are identified by three primary components in a hierarchy:

1. 🔌 **Connector** (data source)
2. 📊 **Metric** (label)
3. 📍 **Location** (optional tag)

![Pipes hierarchy](/assets/screenshots/weather_pipes.png)

In the above screenshot, three pipes are represented according to their keys (you can see this view with `show pipes`). The three pipes share a connector (`plugin:noaa`) and metric (`weather`) but have different locations.

The three keys of any pipe are labels that represent the pipe's connector, metric, and location. Below are brief descriptions of what these keys mean, and you can read about these keys when you [bootstrap a pipe](bootstrapping/).


## Interacting with Pipes

As you add new data streams, the number of pipes you maintain can quickly grow. To filter by these keys, use the arguments `-c`, `-m`, and `-l` (connector, metric, and location).

For example, the screenshot mentioned above may be created by any of these commands, or any combination of the three:

```bash
show pipes -c plugin:noaa
show pipes -m weather
show pipes -l clemson atlanta charleston
```

​
