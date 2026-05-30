<img src="https://meerschaum.io/assets/banner_1920x320.png" alt="Meerschaum banner" style="width: 100%"/>

| PyPI | GitHub | Info | Stats |
|---|---|---|---|
| ![PyPI]( https://img.shields.io/pypi/v/meerschaum?color=%2300cc66&label=Version ) | ![GitHub Repo stars](https://img.shields.io/github/stars/bmeares/Meerschaum?style=social) | ![License](https://img.shields.io/github/license/bmeares/Meerschaum?label=License) | ![Number of plugins]( https://img.shields.io/badge/dynamic/json?color=3098c1&label=Public%20Plugins&query=num_plugins&url=https%3A%2F%2Fapi.mrsm.io%2Finfo ) |
| ![PyPI - Python Version]( https://img.shields.io/pypi/pyversions/meerschaum?label=Python&logo=python&logoColor=%23ffffff ) | ![GitHub Sponsors](https://img.shields.io/github/sponsors/bmeares?color=eadf15&label=Sponsors) | [![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/meerschaum)](https://artifacthub.io/packages/search?repo=meerschaum) | ![Number of registered users]( https://img.shields.io/badge/dynamic/json?color=3098c1&label=Registered%20Users&query=num_users&url=https%3A%2F%2Fapi.mrsm.io%2Finfo ) |

<p align="center">
<img src="https://meerschaum.io/files/images/demo.gif" alt="Meerschaum demo" height="450px">
</p>

## What is Meerschaum?

**Meerschaum is an ETL framework for time-series data.** You define **pipes** — named data streams — and Meerschaum keeps them in sync: it fetches only the new or changed rows, deduplicates and upserts them, manages the schema, and handles scheduling, serving, and storage.

Write a few lines of fetch logic; Meerschaum handles the rest of the pipeline. No more copy/pasting ETL scripts, hand-rolling incremental windows, or babysitting cron jobs. Drop it into an existing stack or stand up a full database-and-dashboard stack in minutes.

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('plugin:noaa', 'weather', 'atl', instance='sql:local')
pipe.sync()  ### Pulls only what's new since the last sync.
```

<p align="center">
<img src="https://meerschaum.io/assets/screenshots/weather_pipes.png"/>
</p>

## Features

- ⚡️ **Incremental by default** — [the sync engine](https://meerschaum.io/reference/pipes/syncing/) fetches only new or changed rows and concurrently updates many streams at once. Duplicate rows are ignored; rows with existing keys are updated.
- 📊 **Built for data scientists and analysts** — integrate with [Pandas, Grafana, and friends](https://meerschaum.io/reference/data-analysis-tools/); persist DataFrames and always get the latest data. Skip pandas overhead and read rows as plain dicts with `Pipe.get_docs()`.
- 🗄️ **Production-ready, batteries included** — one-click deploy a [TimescaleDB + Grafana stack](https://meerschaum.io/reference/stack/), serve data org-wide via `FastAPI` (`uvicorn`/`gunicorn`), and secure API instances with scoped auth tokens. Supports PostGIS geometry (incl. ESRI CRS) for geospatial pipelines.
- 💼 **Jobs and scheduling** — run any command as a background [job](https://meerschaum.io/reference/background-jobs/) with `-d`. Built-in scheduler handles cron and interval schedules — no `crontab` or `systemd` setup. Execute locally, via `systemd`, or remotely on an API instance with `--executor-keys`.
- 🔌 **Easily expandable** — ingest any source with a simple [plugin](https://meerschaum.io/reference/plugins/writing-plugins/): just return a DataFrame. [Add any function as a command](https://meerschaum.io/reference/plugins/types-of-plugins/#action-plugins), define parent/child pipe relationships for composable SQL pipelines, or embed Meerschaum via its [Python API](https://docs.meerschaum.io).
- ✨ **Tailored for your experience** — a rich CLI that's surprisingly enjoyable, a web dashboard for the graphically inclined, and [connectors](https://meerschaum.io/reference/connectors/) for SQL, API, Valkey, and custom backends.
- 🧳 **Portable from the start** — `$MRSM_ROOT_DIR` emulates multiple installations and groups [instances](https://meerschaum.io/reference/connectors/#instances-and-repositories). No dependencies required (anything needed installs into a virtual environment), and it's `uv`-compatible: `uv tool install meerschaum`.

### Want to learn more?

Find a wealth of information at [meerschaum.io](https://meerschaum.io), or read up on Meerschaum in the wild:

- Interview featured in [*Console 100 - The Open Source Newsletter*](https://console.substack.com/p/console-100)
- [*A Data Scientist's Guide to Fetching COVID-19 Data in 2022*](https://towardsdatascience.com/a-data-scientists-guide-to-fetching-covid-19-data-in-2022-d952b4697) (Towards Data Science)
- [*Time-Series ETL with Meerschaum*](https://towardsdatascience.com/easy-time-series-etl-for-data-scientists-with-meerschaum-5aade339b398) (Towards Data Science)
- [*How I automatically extract my M1 Finance transactions*](https://bmeares.medium.com/how-i-automatically-extract-my-m1-finance-transactions-b43cef857bc7)

## Installation

For a more thorough setup guide, visit the [Getting Started](https://meerschaum.io/get-started/) page at [meerschaum.io](https://meerschaum.io).

### TL;DR

```bash
pip install meerschaum # or `uv tool install meerschaum[api]`
mrsm stack up -d
mrsm bootstrap pipes
```

## Usage
Visit [meerschaum.io](https://meerschaum.io) for setup, usage, and troubleshooting information. You can find technical documentation at [docs.meerschaum.io](https://docs.meerschaum.io).

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
    'foo', 'bar',
    instance = 'sql:local',                  ### Built-in SQLite DB.
    columns  = {'datetime': 'dt', 'id': 'id'},
)

### Sync a DataFrame (or list of dicts) — creates the table on first run.
pipe.sync([{'dt': '2024-07-01', 'id': 1, 'val': 10}])

### Duplicates are ignored; rows with existing keys are updated.
pipe.sync([{'dt': '2024-07-01', 'id': 1, 'val': 100}])
assert pipe.get_rowcount() == 1

### Read back as a DataFrame, filtered by time range and params.
df = pipe.get_data(begin='2024-01-01', end='2025-01-01', params={'id': [1]})

### Or skip pandas and read plain dicts.
docs = pipe.get_docs(params={'id': [1]})
### [{'dt': datetime(2024, 7, 1), 'id': 1, 'val': 100}]
```

For composable in-database SQL pipelines (`reference` inheritance and `{{ Pipe(...) }}` table resolution), see the [SQL pipes guide](https://meerschaum.io/reference/pipes/syncing/).

### Plugins

Ingest any source by returning rows from a `fetch` function — Meerschaum handles the rest:

```python
# ~/.config/meerschaum/plugins/example.py
__version__ = '1.0.0'
required = ['requests']

def register(pipe, **kw):
    return {'columns': {'datetime': 'dt', 'id': 'id'}}

def fetch(pipe, begin=None, end=None, **kw):
    import requests
    rows = requests.get('https://api.example.com/data').json()
    return rows  ### list of dicts or a Pandas DataFrame
```

## Support Meerschaum's Development

For consulting services and to support Meerschaum's development, please considering sponsoring me on [GitHub sponsors](https://github.com/sponsors/bmeares).

Additionally, you can always [buy me a coffee☕](https://www.buymeacoffee.com/bmeares)!

### License

Copyright 2020-2026 Bennett Meares

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
