<img src="https://meerschaum.io/assets/banner_1920x320.png" alt="Meerschaum banner">

| PyPI | GitHub | License | Stats |
|---|---|---|---|
| ![PyPI]( https://img.shields.io/pypi/v/meerschaum?color=%2300cc66&label=Version ) | ![GitHub Repo stars](https://img.shields.io/github/stars/bmeares/Meerschaum?style=social) | ![PyPI - License](https://img.shields.io/pypi/l/meerschaum?label=Core%20License) | ![Number of plugins]( https://img.shields.io/badge/dynamic/json?color=3098c1&label=Public%20Plugins&query=num_plugins&url=https%3A%2F%2Fapi.mrsm.io%2Finfo ) |
| ![PyPI - Python Version]( https://img.shields.io/pypi/pyversions/meerschaum?label=Python&logo=python&logoColor=%23ffffff ) | ![Lines of code]( https://img.shields.io/tokei/lines/github/bmeares/Meerschaum?label=Total%20Lines ) | ![Plugins - License]( https://img.shields.io/badge/Plugins%20License-You%20decide!-success ) | ![Number of registered users]( https://img.shields.io/badge/dynamic/json?color=3098c1&label=Registered%20Users&query=num_users&url=https%3A%2F%2Fapi.mrsm.io%2Finfo ) |

<p align="center">
<img src="https://meerschaum.io/files/images/demo.gif" alt="Meerschaum demo" height="450px">
</p>

## What is Meerschaum?
Meerschaum is a tool for quickly synchronizing time-series data streams called **pipes**. With Meerschaum, you can have a data visualization stack running in minutes.

The power of the Meerschaum system comes from projects like [pandas](https://pandas.pydata.org/), [sqlalchemy](https://www.sqlalchemy.org/), [fastapi](https://fastapi.tiangolo.com/), and [more](https://meerschaum.io/news/acknowledgements/).

## Why Meerschaum?

If you've worked with time-series data, you know the headaches that come with ETL.
Data engineering often gets in analysts' way, and when work needs to get done, every minute spent on pipelining is time taken away from real analysis.

Rather than copy / pasting your ETL scripts, simply build pipes with Meerschaum! [Meerschaum gives you the tools to design your data streams how you like](https://towardsdatascience.com/easy-time-series-etl-for-data-scientists-with-meerschaum-5aade339b398) ― and don't worry — you can always incorporate Meerschaum into your existing systems!

## Features

- 📊 **Built for Data Scientists and Analysts**  
  - Integrate with Pandas, Grafana and other popular [data analysis tools](https://meerschaum.io/reference/data-analysis-tools/).
  - Persist your dataframes and always get the latest data.
- ⚡️ **Production-Ready, Batteries Included**  
  - [Synchronization engine](https://meerschaum.io/reference/pipes/syncing/) concurrently updates many time-series data streams.
  - One-click deploy a [TimescaleDB and Grafana stack](https://meerschaum.io/reference/stack/) for prototyping.
  - Serve data to your entire organization through the power of `uvicorn`, `gunicorn`, `FastAPI`
- 🔌 **Easily Expandable**  
  -  Ingest any data source with the [plugin system](https://meerschaum.io/reference/plugins/writing-plugins/). Just return a DataFrame, and Meerschaum handles the rest.
  - [Add any function as a command](https://meerschaum.io/reference/plugins/types-of-plugins/#action-plugins) to the Meerschaum system.
  - Include Meerschaum in your projects with its [easy-to-use Python API](https://docs.meerschaum.io).
- ✨ **Tailored for Your Experience**  
  - Rich CLI makes managing your data streams surprisingly enjoyable!
  - Web dashboard for those who prefer a more graphical experience.
  - Manage your database connections with [Meerschaum connectors](https://meerschaum.io/reference/connectors/)
  - Utility commands with sensible syntax let you control many pipes with grace.
- 💼 **Portable from the Start**  
  - The environment variable `$MRSM_ROOT_DIR` lets you emulate multiple installations and group together your [instances](https://meerschaum.io/reference/connectors/#instances-and-repositories).
  - No dependencies required; anything needed will be installed into a virtual environment.
  - Required packages for plugins

## Installation

For a more thorough setup guide, visit the [Getting Started](https://meerschaum.io/get-started/) page at [meerschaum.io](https://meerschaum.io).

### TL;DR

```bash
pip install -U --user meerschaum
mrsm stack up -d db grafana
mrsm bootstrap pipes
```

## Usage Documentation

Please visit [meerschaum.io](https://meerschaum.io) for setup, usage, and troubleshooting information. You can find technical documentation at [docs.meerschaum.io](https://docs.meerschaum.io).

## Plugins

Here is the [list of community plugins](https://meerschaum.io/reference/plugins/list-of-plugins/).

For details on installing, using, and writing plugins, check out the [plugins documentation](https://meerschaum.io/reference/plugins/types-of-plugins) at [meerschaum.io](https://meerschaum.io).

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
