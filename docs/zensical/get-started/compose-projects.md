# Save a project

A Compose project keeps your Pipes, connectors, and plugins in one folder. Put it in version control and run the same workflow again.

This small project collects the same two readings as the beginner tutorial, using a Python function as its source.

## Two files

Create a new folder with this layout:

```text
climate-demo/
├── mrsm-compose.yaml
└── plugins/
    └── readings.py
```

Download [mrsm-compose.yaml](../assets/examples/climate-demo/mrsm-compose.yaml) and [readings.py](../assets/examples/climate-demo/plugins/readings.py), or copy them below.

??? example "mrsm-compose.yaml"
    ```yaml
    project_name: climate-demo
    root_dir: ./root
    plugins_dir: ./plugins

    config:
      meerschaum:
        instance: sql:demo
        connectors:
          sql:
            demo:
              flavor: sqlite
              database: climate.db

    sync:
      pipes:
        - connector: plugin:readings
          metric: temperatures
          columns:
            datetime: ts
            primary: id
          dtypes:
            ts: datetime
        - connector: sql:demo
          metric: fahrenheit
          columns:
            datetime: ts
            primary: id
          dtypes:
            ts: datetime
          parameters:
            sql: |-
              SELECT id, ts,
                     ROUND(temperature * 9.0 / 5 + 32, 1) AS fahrenheit
              FROM {{ Pipe('plugin:readings', 'temperatures', instance='sql:demo') }}
    ```

??? example "plugins/readings.py"
    ```python
    """A tiny local source for the Meerschaum Compose tutorial."""


    def fetch(pipe, **kwargs):
        return [
            {'id': 1, 'ts': '2026-01-01 12:00:00', 'temperature': 21.5},
            {'id': 2, 'ts': '2026-01-01 13:00:00', 'temperature': 22.0},
        ]
    ```

The plugin returns ordinary dictionaries. It needs no third-party packages. The SQL Pipe references its parent, just as in [Your first Pipeline](pipelines.md).

## Run it

From the `climate-demo` folder:

```bash
mrsm compose init
mrsm compose run
mrsm compose show data
```

Compose runs these Pipes in their declared order. You should see **21.5°C → 70.7°F** and **22.0°C → 71.6°F**. Run it again: the same rows stay in place.

Compose keeps this project's configuration in `root/` and its SQLite data in `climate.db`. Keep the YAML and plugin in version control; treat the generated root and database as local state.

??? tip "Ready for a real source?"
    Replace the list in `fetch()` with your file or API reader. For larger sources, accept `begin` and `end` and fetch only that window. See [Writing plugins](../reference/plugins/writing-plugins.md).

[Explore the Compose reference →](../reference/compose.md)
