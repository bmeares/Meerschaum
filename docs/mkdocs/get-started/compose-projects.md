# 📦️ Compose Projects

Now that you've got the idea behind pipes, let's write our own plugins and put together a project using [Meerschaum Compose](/reference/compose).

!!! tip "Meerschaum Compose Template Repository"
    If you'd like to jump straight into a Dockerized environment, create a new repository from the [Meerschaum Compose Project Template](https://github.com/bmeares/mrsm-compose-template).

Create a new project directory `awesome-sauce`. This is where our code and data will live (for now).

```bash
mkdir awesome-sauce
cd awesome-sauce
```

Paste the following into `mrsm-compose.yaml`. This defines our pipes and runtime environment.

??? example "`mrsm-compose.yaml`"
    ```yaml
    project_name: "awesome-sauce"

    sync:
      pipes:
        - connector: "plugin:fred"
          metric: "price"
          location: "eggs"
          target: "price_eggs"
          columns:
            datetime: "DATE"
          dtypes:
            "PRICE": "float64"
          parameters:
            fred:
              series_id: "APU0000708111"

        - connector: "plugin:fred"
          metric: "price"
          location: "chicken"
          target: "price_chicken"
          columns:
            datetime: "DATE"
          dtypes:
            "PRICE": "float64"
          parameters:
            fred:
              series_id: "APU0000706111"

        - connector: "sql:tiny"
          metric: "price"
          location: "eggs_chicken_a"
          target: "Food Prices A"
          columns:
            datetime: "DATE"
          parameters:
            query: |-
              SELECT
                e."DATE",
                e."PRICE" AS "PRICE_EGGS",
                c."PRICE" AS "PRICE_CHICKEN"
              FROM "price_eggs" AS e
              INNER JOIN "price_chicken" AS c
                ON e."DATE" = c."DATE"

        - connector: "sql:tiny"
          metric: "price"
          location: "eggs_chicken_b"
          target: "Food Prices B"
          columns:
            datetime: "DATE"
            food: "FOOD"
          parameters:
            query: |-
              SELECT
                "DATE",
                "PRICE",
                'eggs' AS "FOOD"
              FROM "price_eggs"
              UNION ALL
              SELECT
                "DATE",
                "PRICE",
                'chicken' AS "FOOD"
              FROM "price_chicken"


    config:
      meerschaum:
        instance: "sql:tiny"
        connectors:
          sql:
            tiny:
              flavor: "sqlite"
              database: "tiny.db"
    ```

There are two [Connectors](/reference/connectors) used in this project:

- **`plugin:fred`**  
  A [plugin](/reference/plugins/) we will write shortly. This is the data source for our initial pipes.
- **`sql:tiny`**  
  A SQLite file `tiny.db`. It's used as the [instance connector](/reference/connectors/instance-connectors/) as well as a data source.

The SQLite database makes sense, but what is `plugin:fred`? [FRED](https://fred.stlouisfed.org/series/APU0000708111) is the data source we want to use for this project, so let's create our first plugin to fetch this data.

Create a new directory `plugins`:

```bash
mkdir plugins
cd plugins
```

And paste the following into `fred.py`:

??? example "`fred.py`"
    ```python
    import meerschaum as mrsm

    CSV_BASE_URL: str = 'https://fred.stlouisfed.org/graph/fredgraph.csv'

    required = ['pandas']

    def fetch(pipe: mrsm.Pipe, **kwargs) -> 'pd.DataFrame':
        import pandas as pd
        series_id = pipe.parameters.get('fred', {}).get('series_id', None)
        url = f"{CSV_BASE_URL}?id={series_id}"
        df = pd.read_csv(url)
        if series_id in df.columns:
            df['PRICE'] = pd.to_numeric(df[series_id], errors='coerce')
            del df[series_id]
        return df
    ```

The plugin provides one function `fetch()` that takes a pipe, pulls an ID from `pipe.parameters`, and returns the appropriate DataFrame.

Now that we've got our YAML file and plugin, install the [`compose` plugin](/reference/compose):

```bash
mrsm install plugin compose
```

Let's initialize our environment to install our dependencies (`pandas`) into the project's virtual environment for `plugin:fred`. From the parent project directory, run `compose init`:

```bash
cd ../
mrsm compose init
```

![mrsm compose init](/assets/screenshots/compose-init.png)

Run the project file to sync the pipes one-at-a-time:

```bash
mrsm compose run
```

![mrsm compose run](/assets/screenshots/compose-run.png)

🎉 Success! You've just run an ETL pipeline to process the following steps:

- **Ingest eggs prices in the US.**
- **Ingest chicken prices in the US.**
- **Join the tables on `DATE`.**  
  Grow the table horizontally by splitting `PRICE` into `PRICE_EGGS` and `PRICE_CHICKEN`.
- **Union the tables.**  
  Grow the table vertically by adding the index `FOOD` (`'chicken'` or `'eggs'`).

All other Meerschaum actions are executed within the context of this project. For example, let's verify the most recent data with `show data`:

```bash
mrsm compose show data
```

![mrsm compose show data](/assets/screenshots/compose-show-data.png)

When developing, it's useful to hop into a REPL and test out the [Python API](https://docs.meerschaum.io).

```bash
mrsm compose python
```

![mrsm compose python](/assets/screenshots/compose-python.png)

Thank you for making it through the Getting Started guide! This example was based on the May 2023 [Tech Slam 'N Eggs demo project](https://github.com/bmeares/techslamneggs).

There's plenty more great information, such as the [plugins guide](/reference/plugins/writing-plugins/). Have fun building your pipes with Meerschaum!
