<img src="https://meerschaum.io/images/banner_1920x320.png" alt="Meerschaum banner">

![PyPI](https://img.shields.io/pypi/v/meerschaum?color=%2300cc66&label=Version)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/meerschaum?label=Python&logo=python&logoColor=%23ffffff)
![PyPI - Status](https://img.shields.io/pypi/status/meerschaum?label=Release%20Status)
![Docker Image Size (tag)](https://img.shields.io/docker/image-size/bmeares/meerschaum/latest?label=Image%20Size&logo=docker&logoColor=%23ffffff)
![Lines of code](https://img.shields.io/tokei/lines/github/bmeares/Meerschaum?label=Total%20Lines)
![PyPI - License](https://img.shields.io/pypi/l/meerschaum?label=License)
![Maintenance](https://img.shields.io/maintenance/yes/2020?label=Maintained)


# Build Pipes with Meerschaum
Meerschaum is a platform for quickly creating and managing time-series data streams called Pipes. With Meerschaum, you can have a data visualization stack running in minutes.

Please visit https://meerschaum.io for setup, usage, and troubleshooting information. You can read a brief overview of the project below.

# Table of Contents
- [Disclaimer](#disclaimer)
- [Setup](#setup)
  - [Requirements](#requirements)
  - [Installation](#installation)
  - [Quickstart](#quickstart)
  - [Create Visualizations with Grafana](#create-visualizations-with-grafana)
- [FAQ](#faq)
  - [What are Pipes, exactly?](#what-are-pipes-exactly)
  - [I broke my configuration. How do I start over?](#i-broke-my-configuration-how-do-i-start-over)
  - [I can't open the Meerschaum shell!](#i-cant-open-the-meerschaum-shell)
  - [Connectors don't work for `<database flavor>`!](#connectors-dont-work-for-database-flavor)
- [Thank you](#thank-you)

# Disclaimer
Meerschaum is undergoing active development and is still an alpha release. Expect to encounter bugs, and please open issues when you find them! Meerschaum is distributed with no warranty, so use with care!

Meerschaum was built using powerful open source software like TimescaleDB, Grafana, pandas, and more. Check the [Thank You](#Thank-You) section below for more information about dependencies.

# Setup
## Requirements
Make sure you have [Docker](https://www.docker.com/get-started) and Docker Compose installed.

If you are running Linux, search your repositories for `docker.io` or run this script:
```
curl https://get.docker.com | sh
```

If you're on Windows or MacOS, install [Docker Desktop](https://www.docker.com/get-started).

You can install Docker Compose with `pip`:
```
pip install docker-compose
```

## Installation
To install the `full` version of Meerschaum, install with `pip`:
```
pip install meerschaum[full]
```

## Quickstart
### Launch the Meerschaum Shell
To jump into the `mrsm` shell, run the command:
```
mrsm
```
All standard actions can either be executed from within the `mrsm` shell or on the command line directly. For example, the command
```
$ mrsm show version
```
is equivalent to
```
$ mrsm
> show version
```

### Start the Stack
To start the Meerschaum stack in the background (using `docker-compose -d`), run the command
```
mrsm stack [-d]
```
<img alt="Demonstrating how to start the Meerschaum stack" src="https://i.imgur.com/wyI3CO1.gif" style="max-height: 450px">

### Add a Connector
If you want to create a Pipe for remote data, you first need to define a Connector. Run this command to open the configuration YAML file with your text editor:
```
mrsm edit config
```
For this example, let's add a connector called `exampledb`. Add a new entry under the `sql` section:
```
meerschaum:
  connectors:
    sql:
      exampledb:
        username: myuser
        password: mypass
        flavor: postgresql
        host: myserverhostname
        database: mydb
```
Note that for `sqlite` connectors, only the `database` parameter is needed (path to the `.sqlite` file).

<img alt="Adding a Meerschaum Connector" src="https://imgur.com/7iKvBsV.gif" style="max-height: 450px">


### Register a Pipe
Pipes are defined by three keys:
1. Connection (keys for a connector in the format `type:label`; e.g. `sql:exampledb` for the Connector added above)
2. Metric (label for the contents of the Pipe's data, such as `power` or `temperature`)
3. Location (optional; `None` if omitted)

To register a Pipe, run this command:
```
mrsm register pipes -C sql:exampledb -M mydata
```
This will create a Pipe with the connector we added above (`sql:exampledb`) and the metric `mydata`. The location for this Pipe is `None`.

Next, we need to edit the parameters for this pipe. Run this command to open your text editor:
```
mrsm edit pipes -C sql:exampledb -M mydata
```
For this example, edit the YAML file to look something like this:
```
columns:
  datetime: mydtcolumn
  id: myidcolumn
fetch:
  definition: SELECT * FROM mytable
```
This information describes the remote dataset we will add into the Pipe. The columns defined will be indexed, and the definition is executed on the remote server to get the data.

### Add Data to a Pipe
If you defined the parameters described above, just run the command below to fetch new data and sync the Pipe:
```
mrsm sync pipes
```
If you want to add an existing dataframe to a Pipe, run `pipe.sync(df)` to append the dataframe to the Pipe's table. You can launch into a Python shell with `mrsm python`:
```
>>> import meerschaum as mrsm
>>> pipe = mrsm.Pipe('sql:exampledb', 'mydata')
>>> 
>>> if not pipe.columns: ### if pipe has not been registered, you can define columns here
>>>   pipe.columns = {'datetime' : 'mydtcolumn', 'id' : 'myidcolumn'}
>>> 
>>> import pandas as pd
>>> df = pd.read_csv('mydata.csv')
>>> 
>>> pipe.sync(df)
```
In this case, the Pipe has created and indexed the table `sql_exampledb_mydata` on the Meerschaum TimescaleDB database.

## Create Visualizations with Grafana
Grafana is included in the Meerschaum stack, pre-configured with the Meerschaum TimescaleDB database. Open a web browser and navigate to `http://localhost:3000` and log in to Grafana with username `admin`, password `admin`.

<img alt="Grafana pre-configured with Meerschaum" src="https://imgur.com/cYTfiFT.png" style="max-height: 450px">

# FAQ
## What are Pipes, exactly?
Pipes are built from [TimescaleDB Hypertables](http://https://docs.timescale.com/latest/getting-started/creating-hypertables "TimescaleDB Hypertables"), which are PostgreSQL tables with special datetime indexing and functions. In future releases of Meerschaum, Pipes may be created from [continous aggregate views](https://blog.timescale.com/blog/timescaledb-1-7-fast-continuous-aggregates-with-real-time-views-postgresql-12-support-and-more-community-features/ "continous aggregate views").

## I broke my configuration. How do I start over?
The command to reset your configuration files is:
```
bootstrap config
```
If that doesn't work, just remove the folder `~/.config/meerschaum` (`%AppData%/Meerschaum` on Windows).

To completely remove the stack and its data, run this command:
```
stack down [-v]
```
which is equivalent to `docker-compose down -v`.

## I can't open the Meerschaum shell!
You can invoke `mrsm` directly with `python -m meerschaum`. Check that your `PATH` includes packages installed by `pip`, such as `~/.local/bin`.

## How do I turn off the emoji / colors / I'm running Windows?
Open the configuration file with `mrsm edit config` and search for the key `formatting` under the `system` section. From there, you can turn off emoji (`unicode: false`) or colors (`ansi: false`).

## Connectors don't work for `<database flavor>`!
Although Connectors *should*  work with any database flavor supported by `sqlalchemy` Engines, it is difficult to test against many database flavors. When bugs are encountered, please open an issue and describe your configuration!

As of now, there is (limited) support for the following database flavors:
- PostgreSQL / TimescaleDB
- MySQL / MariaDB
- MSSQL
- Oracle SQL (limited support, testing needed)
- SQLite

# Thank you
I want to give my sincere thanks to the developers of the following projects:
- [Docker](https://www.docker.com/)
- [Pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Uvicorn](https://www.uvicorn.org/)
- [pprintpp](https://pypi.org/project/pprintpp/)
- [CascaDict](https://pypi.org/project/cascadict/)
- [pyvim](https://github.com/prompt-toolkit/pyvim)
- [Colorama](https://github.com/tartley/colorama)
- [more_termcolor](https://github.com/giladbarnea/more_termcolor)
- [SQL CLI Tools](https://github.com/dbcli)
