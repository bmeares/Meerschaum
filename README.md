<img src="https://meerschaum.io/assets/banner_1920x320.png" alt="Meerschaum banner" style="width: 100%"/>

| PyPI | GitHub | Info | Stats |
|---|---|---|---|
| ![PyPI]( https://img.shields.io/pypi/v/meerschaum?color=%2300cc66&label=Version ) | ![GitHub Repo stars](https://img.shields.io/github/stars/bmeares/Meerschaum?style=social) | ![License](https://img.shields.io/github/license/bmeares/Meerschaum?label=License) | ![Number of plugins]( https://img.shields.io/badge/dynamic/json?color=3098c1&label=Public%20Plugins&query=num_plugins&url=https%3A%2F%2Fapi.mrsm.io%2Finfo ) |
| ![PyPI - Python Version]( https://img.shields.io/pypi/pyversions/meerschaum?label=Python&logo=python&logoColor=%23ffffff ) | ![GitHub Sponsors](https://img.shields.io/github/sponsors/bmeares?color=eadf15&label=Sponsors) | [![meerschaum Tutorials](https://badges.openbase.com/python/tutorials/meerschaum.svg?token=2Yi8Oav9UZYWocO1ncwnIOnpUN5dTnUMWai7lAKTB+k=)](https://openbase.com/python/meerschaum?utm_source=embedded&amp;utm_medium=badge&amp;utm_campaign=rate-badge) | ![Number of registered users]( https://img.shields.io/badge/dynamic/json?color=3098c1&label=Registered%20Users&query=num_users&url=https%3A%2F%2Fapi.mrsm.io%2Finfo ) |

<p align="center">
<img src="https://meerschaum.io/files/images/demo.gif" alt="Meerschaum demo" height="450px">
</p>

## What is Meerschaum?
Meerschaum is a tool for quickly synchronizing time-series data streams called **pipes**. With Meerschaum, you can have a data visualization stack running in minutes.

<p align="center">
<img src="https://meerschaum.io/assets/screenshots/weather_pipes.png"/>
</p>

## Why Meerschaum?

Two words: *incremental updates*. Fetch the data you need, and Meerschaum will handle the rest.

If you've worked with time-series data, you know the headaches that come with ETL.
Data engineering often gets in analysts' way, and when work needs to get done, every minute spent on pipelining is time taken away from real analysis.

Rather than copy / pasting your ETL scripts, simply build pipes with Meerschaum! [Meerschaum gives you the tools to design your data streams how you like](https://towardsdatascience.com/easy-time-series-etl-for-data-scientists-with-meerschaum-5aade339b398) ― and don't worry — you can always incorporate Meerschaum into your existing systems!

### Want to Learn More?

You can find a wealth of information at [meerschaum.io](https://meerschaum.io)!

Additionally, below are several articles published about Meerschaum:

- Interview featured in [*Console 100 - The Open Source Newsletter*](https://console.substack.com/p/console-100)
- [*A Data Scientist's Guide to Fetching COVID-19 Data in 2022*](https://towardsdatascience.com/a-data-scientists-guide-to-fetching-covid-19-data-in-2022-d952b4697) (Towards Data Science)
- [*Time-Series ETL with Meerschaum*](https://towardsdatascience.com/easy-time-series-etl-for-data-scientists-with-meerschaum-5aade339b398) (Towards Data Science)
- [*How I automatically extract my M1 Finance transactions*](https://bmeares.medium.com/how-i-automatically-extract-my-m1-finance-transactions-b43cef857bc7)

## Installation

For a more thorough setup guide, visit the [Getting Started](https://meerschaum.io/get-started/) page at [meerschaum.io](https://meerschaum.io).

### TL;DR

```bash
pip install -U --user meerschaum
mrsm stack up -d db grafana
mrsm bootstrap pipes
```

## Usage

Please visit [meerschaum.io](https://meerschaum.io) for setup, usage, and troubleshooting information. You can find technical documentation at [docs.meerschaum.io](https://docs.meerschaum.io), and here is a complete list of the [Meerschaum actions](https://meerschaum.io/reference/actions/).

### CLI
```bash
### Install the NOAA weather plugin.
mrsm install plugin noaa

### Register a new pipe to the built-in SQLite DB.
### You can instead run `bootstrap pipe` for a wizard.
### Enter 'KATL' for Atlanta when prompted.
mrsm register pipe -c plugin:noaa -m weather -l atl -i sql:local

### Pull data and create the table "plugin_noaa_weather_atl".
mrsm sync pipes -l atl -i sql:local
```

### Python API

```python
import meerschaum as mrsm
pipe = mrsm.Pipe(
    'foo', 'bar',              ### Connector and metric labels.
    target   = 'MyTableName!', ### Table name. Defaults to 'foo_bar'.
    instance = 'sql:local',    ### Built-in SQLite DB. Defaults to 'sql:main'.
    columns  = {
        'datetime': 'dt',      ### Column for the datetime index.
        'id'      : 'id',      ### Column for the ID index (optional).
    },
)
### Pass a DataFrame to create the table and indices.
pipe.sync([{'dt': '2022-07-01', 'id': 1, 'val': 10}])

### Duplicate rows are ignored.
pipe.sync([{'dt': '2022-07-01', 'id': 1, 'val': 10}])
assert len(pipe.get_data()) == 1

### Rows with existing keys (datetime and/or id) are updated.
pipe.sync([{'dt': '2022-07-01', 'id': 1, 'val': 100}])
assert len(pipe.get_data()) == 1

### Translates to this query for SQLite:
###
### SELECT *
### FROM "MyTableName!"
### WHERE "dt" >= datetime('2022-01-01', '0 minute')
###   AND "dt" <  datetime('2023-01-01', '0 minute')
###   AND "id" IN ('1')
df = pipe.get_data(
    begin  = '2022-01-01',
    end    = '2023-01-01',
    params = {'id': [1]},
)

### Shape of the DataFrame:
###           dt  id  val
### 0 2022-07-01   1  100

### Drop the table and remove the pipe's metadata.
pipe.delete()
```

### Simple Plugin

```python
# ~/.config/plugins/example.py

__version__ = '1.0.0'
required = ['requests']

def register(pipe, **kw):
    return {
        'columns': {
            'datetime': 'dt',
            'id'      : 'id',
        },
    }

def fetch(pipe, **kw):
    import requests, datetime, random
    response = requests.get('http://date.jsontest.com/')

    ### The fetched JSON has the following shape:
    ### {
    ###     "date": "07-01-2022",
    ###     "milliseconds_since_epoch": 1656718801566,
    ###     "time": "11:40:01 PM"
    ### }
    data = response.json()
    timestamp = datetime.datetime.fromtimestamp(
        int(str(data['milliseconds_since_epoch'])[:-3])
    )

    ### You may also return a Pandas DataFrame.
    return [{
        "dt"   : timestamp,
        "id"   : random.randint(1, 4),
        "value": random.uniform(1, 100),
    }]
```

## Features

- 📊 **Built for Data Scientists and Analysts**  
  - Integrate with Pandas, Grafana and other popular [data analysis tools](https://meerschaum.io/reference/data-analysis-tools/).
  - Persist your dataframes and always get the latest data.
- ⚡️ **Production-Ready, Batteries Included**  
  - [Synchronization engine](https://meerschaum.io/reference/pipes/syncing/) concurrently updates many time-series data streams.
  - One-click deploy a [TimescaleDB and Grafana stack](https://meerschaum.io/reference/stack/) for prototyping.
  - Serve data to your entire organization through the power of `uvicorn`, `gunicorn`, and `FastAPI`.
- 🔌 **Easily Expandable**  
  -  Ingest any data source with a simple [plugin](https://meerschaum.io/reference/plugins/writing-plugins/). Just return a DataFrame, and Meerschaum handles the rest.
  - [Add any function as a command](https://meerschaum.io/reference/plugins/types-of-plugins/#action-plugins) to the Meerschaum system.
  - Include Meerschaum in your projects with its [easy-to-use Python API](https://docs.meerschaum.io).
- ✨ **Tailored for Your Experience**  
  - Rich CLI makes managing your data streams surprisingly enjoyable!
  - Web dashboard for those who prefer a more graphical experience.
  - Manage your database connections with [Meerschaum connectors](https://meerschaum.io/reference/connectors/).
  - Utility commands with sensible syntax let you control many pipes with grace.
- 💼 **Portable from the Start**  
  - The environment variable `$MRSM_ROOT_DIR` lets you emulate multiple installations and group together your [instances](https://meerschaum.io/reference/connectors/#instances-and-repositories).
  - No dependencies required; anything needed will be installed into a virtual environment.
  - [Specify required packages for your plugins](https://meerschaum.io/reference/plugins/writing-plugins/), and users will get those packages in a virtual environment.

## Support Meerschaum's Development

For consulting services and to support Meerschaum's development, please considering sponsoring me on [GitHub sponsors](https://github.com/sponsors/bmeares).

Additionally, you can always [buy me a coffee☕](https://www.buymeacoffee.com/bmeares)!

### License

Copyright 2021 Bennett Meares

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
