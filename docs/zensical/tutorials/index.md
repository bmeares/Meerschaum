# 📚 Tutorials

Want to learn Meerschaum hands-on? Follow the written, end-to-end walkthrough below, or scroll down to the [video tutorials](#video-tutorials) if you're more of a visual learner.

## ✍️ Written Tutorial

This tutorial takes you from a fresh install to a self-updating, scheduled pipeline. Each step shows both the command-line (`mrsm`) and a short Python equivalent where it's natural to use one.

!!! tip "Prefer the shell or scripts?"
    Every command below works inside the interactive Meerschaum shell (just run `mrsm` and type the command without the `mrsm` prefix) or as a one-off (`mrsm <command>`). The Python snippets run in any Python session with `import meerschaum as mrsm`.

### 1. Install and pick an instance

Install Meerschaum from [PyPI](https://pypi.org/project/meerschaum/):

```bash
python -m pip install --upgrade meerschaum
```

Your [instance connector](/reference/connectors/) is where pipe metadata and data are stored. The default is `sql:main` (the [pre-configured TimescaleDB stack](/reference/stack/)). If you don't have Docker, use the built-in SQLite instance `sql:local` — no setup required.

Verify your connection:

```bash
mrsm show pipes -i sql:local
```

A successful connection returns `🎉 No pipes to show.`

??? tip "Change your default instance"
    The default instance is `sql:main`. To connect to `sql:local` for the rest of this session, run `instance sql:local` inside the shell, or add `-i sql:local` to any command. See [Setup](/get-started/) for making the change permanent.

### 2. Bootstrap a pipe

Pipes fetch and sync data from [connectors](/reference/connectors/). We'll use the `plugin:noaa` connector from the `noaa` [data plugin](/reference/plugins/types-of-plugins/#data-plugins) to pull weather observations.

**CLI** — install the plugin, then bootstrap interactively:

```bash
mrsm install plugin noaa
mrsm bootstrap pipe -i sql:local
```

When prompted, choose `plugin:noaa` for the connector, `weather` for the metric, skip the location, enter the station `KATL` (Atlanta), keep the default target table, skip tags, answer `n` to skip editing the definition, and answer `y` to sync. See the [bootstrapping guide](/get-started/bootstrap/) for the full walkthrough.

**Python** — build the same pipe directly:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'plugin:noaa', 'weather',
    instance='sql:local',
    parameters={'noaa': {'stations': ['KATL']}},
)
pipe.register()
```

### 3. Sync the pipe and show the data

Syncing fetches new rows from the source and writes them to your instance:

```bash
mrsm sync pipes -i sql:local
```

Preview what landed in the table:

```bash
mrsm show data -i sql:local
```

**Python** — sync and read the data back as a Pandas DataFrame:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('plugin:noaa', 'weather', instance='sql:local')
pipe.sync()

df = pipe.get_data()
print(df)
```

!!! info "Incremental syncs"
    Meerschaum only fetches and writes new or changed rows. Run `sync pipes` again and it picks up where it left off — no duplicate data.

### 4. Set a parameter and re-sync

[Pipe parameters](/reference/pipes/#parameters) control behavior like tags, columns, and dtypes. Let's add a tag so we can group this pipe with others.

**CLI** — edit the pipe's parameters:

```bash
mrsm edit pipes -i sql:local
```

**Python** — set a parameter in memory and persist it with `edit()`, then re-sync:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('plugin:noaa', 'weather', instance='sql:local')
pipe.tags = ['weather', 'tutorial']
pipe.edit()

pipe.sync()
```

Now you can filter by tag — for example, sync only tagged pipes:

```bash
mrsm sync pipes --tags tutorial -i sql:local
```

### 5. Schedule it as a background job

Add `-d` (`--daemon`) to run any command as a [background job](/reference/background-jobs/), and `-s` (`--schedule`) to run it on a recurring schedule. This keeps your weather pipe fresh automatically:

```bash
mrsm sync pipes -m weather -s 'every 30 minutes' --name weather-sync -d -i sql:local
```

**Python** — create and start the same job with the [`Job`](https://docs.meerschaum.io/meerschaum.html#Job) class:

```python
import meerschaum as mrsm

job = mrsm.Job(
    'weather-sync',
    "sync pipes -m weather -s 'every 30 minutes' -i sql:local",
)
success, msg = job.start()
```

!!! tip "Preview a schedule"
    Not sure when a schedule will fire? Check it first with `mrsm show schedule 'every 30 minutes'`. See [Schedules](/reference/background-jobs/#schedules) for the full syntax (`hourly`, `daily starting 00:00`, cron format, and more).

### 6. Monitor the job

List your running jobs and stream their logs:

```bash
mrsm show jobs
mrsm show logs weather-sync
```

When you're done, stop or delete the job:

```bash
mrsm stop jobs weather-sync
mrsm delete jobs weather-sync
```

**Python** — monitor and manage the job programmatically:

```python
import meerschaum as mrsm

job = mrsm.Job('weather-sync')
job.monitor_logs()   # stream output

job.stop()
job.delete()
```

!!! success "You built a self-updating pipeline!"
    You've installed Meerschaum, bootstrapped a pipe, synced and inspected its data, tuned its parameters, and scheduled it as a monitored background job.

    **Next steps:**

    - [Compose projects](/get-started/compose-projects/) — define your whole stack of pipes in a single YAML file.
    - [Pipes reference](/reference/pipes/) — dig into parameters, dtypes, and the sync flow.
    - [Writing plugins](/reference/plugins/writing-plugins/) — build your own data sources.
    - [Background jobs](/reference/background-jobs/) — executors, schedules, and remote jobs.

## 🎥 Video Tutorials

Are you more of a visual learner? Check out these video tutorials below where I show how Meerschaum can make your life as a data analyst easier! From the basics to technical lessons, I walk you through ways to harness the power of Meerschaum in your own projects.

### Meerschaum in 100 Seconds

<div style="text-align: center">
  <iframe width="672" height="378" src="https://www.youtube.com/embed/VFFWe7B33Io" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>

>Short on time? Dive right in with this Fireship-style quickstart tutorial!


## Learning Meerschaum

### Episode 4: A Closer Look at Data Plugins

<div style="text-align: center">
  <iframe width="672" height="378" src="https://www.youtube.com/embed/t9tFD4afSD4" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>

>Last week, we took our first steps with plugins and built a minimalist pipeline, but today, we're going all in on learning what's really possible with the power of data plugins!

### Episode 3: Intro to Fetch Plugins

<div style="text-align: center">
  <iframe width="672" height="378" src="https://www.youtube.com/embed/rHfGOZFDQsU" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>

>In today's episode, we're stepping up to the challenge and building our data stream with a SINGLE LINE of code — thanks to the power of Pandas and Meerschaum plugins!

### Episode 2: Intro to Instances

<div style="text-align: center">
  <iframe width="672" height="378" src="https://www.youtube.com/embed/iOhPn4RjImQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>

>In this thrilling installment of *Learning Meerschaum,* we take a look at managing your team's data in multiple instances. Whether you're connecting directly to databases or serving your data over the Internet, Meerschaum gives you the power to scale up your projects!

### Episode 1: The Basics

<div style="text-align: center">
  <iframe width="672" height="378" src="https://www.youtube.com/embed/cS9ZAG4INPk" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>
</div>

>In this episode of *Learning Meerschaum*, I show how you can build your SQL ETL pipelines using Meerschaum, an open source ETL / ELT framework, and how you can quickly visualize your data with Grafana, a web-based BI tool.
