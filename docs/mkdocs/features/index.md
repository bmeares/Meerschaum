<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# Feature Showcase

The goal behind Meerschaum is to be *The Data Engineer's Toolbox*: to offer features that help you:

- Create and manage your datastreams
- Quickly build a scalable, customizable backend
- Connect unrelated systems for analysis
- Integrate new systems with your existing infrastructure

No matter your situation, Meerschaum can drastically reduce your development time. Continue reading below to see how Meerschaum can help you take your backend to another level.

## Pipes

With Meerschaum, your datastreams are called [pipes](/reference/pipes/). Pipes are organized into a hierarchy so that you can manage select datastreams at once.

![Meerschaum pipes hierarchy](weather_pipes.png)

### Bootstrapping
The [bootstrap command](/get-started/bootstrapping-a-pipe/) walks you through the steps for creating a new pipe and makes adding datastreams straightforward.

<asciinema-player src="/assets/casts/bootstrap.cast" autoplay="true" loop="true" preload="true"></asciinema-player>

### Syncing Service

Data are added

## Data Analysis Tools

### Turn-key Visualization Stack

Meerschaum comes with a [pre-configured data visualization stack](/get-started/starting-the-stack/). You can delpoy Grafana and a database in seconds, and additional services may be easily added with `mrsm edit config stack`.

<asciinema-player src="/assets/casts/stack.cast" autoplay="true" loop="true" size="small" preload="true" rows="10"></asciinema-player>

![Example Grafana Dashboard](grafana-dashboard.png)

### Integrated with Pandas

### SQL CLI

The `sql` command lets you quickly interact with your databases.


<asciinema-player src="/assets/casts/sql-cli.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>

For certain flavors, Meerschaum is integrated with tools from the [dbcli](https://www.dbcli.com/) project that let you drop into an interactive SQL database environment.

## Background Jobs

## Plugins

## Interactive Environments

### Meerschaum Shell

### Web Dashboard



## Scalable Design

## Ongoing Research
