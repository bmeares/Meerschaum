<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🥞 Meerschaum Stack

The Meerschaum stack is an integrated collection of services designed to help you start visualizing your data as quickly as possible.

The `stack` command wraps [Docker Compose](https://docs.docker.com/compose/) to create a full-stack Meerschaum installation ― services such as a database instance, API server, and pre-configured Grafana instance.

## 🐳 Services

The default stack starts 4 services:

| Service | Description                                                                            | Image                                   |
|---------|----------------------------------------------------------------------------------------|-----------------------------------------|
| db      | TimescaleDB instance corresponding to `sql:main`.                                      | `timescale/timescaledb:latest-pg16-oss` |
| api     | Production Meerschaum Web API server corresponding to `api:main`.                      | `bmeares/meerschaum:api`                |
| valkey  | Valkey instance corresponding to `valkey:main`. Used for caching sessions in by `api`. | `bitnami/valkey:latest`                 |
| grafana | Grafana instance connected to `sql:main`.                                              | `grafana/grafana:latest`                |

You can edit the stack's Docker Compose file with `mrsm edit config stack`.

!!! tip "Entering the Meerschaum Docker image"
    Hop into a shell in the `api` service:

    ```bash
    mrsm stack exec -it api mrsm
    ```

### Docker Images

The Docker image [`bmeares/meerschaum:latest`](https://hub.docker.com/r/bmeares/meerschaum) (alias tag `api`) contains the PostgreSQL driver and other dependencies to run the API server.

To connect to Microsoft SQL Server or Oracle SQL, use the Docker image `bmeares/meerschaum:full` which has drivers pre-installed.

Images are tagged with the following scheme for each release:

- `latest` / `api`  
  Contains the PostgreSQL driver and dependencies to run an API instance.
- `full`  
  In addition to `api` dependencies, contains drivers for Microsoft SQL Server and Oracle SQL as well as graphical depdencies.
- `minimal`  
  Contains the PostgreSQL driver but no Python packages.
- `{version}-api`  
  The `api` image pinned to a specific version.
- `{version}-full`  
  The `full` image pinned to a specific version.
- `{version}-minimal`  
  The `minimal` image pinned to a specific version.

## 🗒️ Requirements

You need [Docker](https://www.docker.com/get-started) installed to run the stack. To install Docker, follow [this guide](https://docs.docker.com/engine/install/) or do the following:

=== "Windows / MacOS"
    Install [Docker Desktop](https://www.docker.com/get-started).

=== "Linux"
    Search your repositories for `docker.io` or run this script:

    ```bash
    curl https://get.docker.com | sh
    ```

    Don't forget to add yourself to the `docker` user group and log out and back in.

    ```bash
    sudo usermod -aG docker myusername
    ```


## 🟢 Starting the Stack

Run the following command to bring up a database and Grafana:

```bash
mrsm stack up -d db grafana
```

<asciinema-player src="/assets/casts/stack.cast" autoplay="true" loop="true" size="small" preload="true" rows="10"></asciinema-player>

!!! note ""
    The `stack` command is a wrapper around a pre-configured [`docker-compose`](https://docs.docker.com/compose/) project. Don't worry if you don't have `docker-compose` installed; in case it's missing, Meerschaum will automatically install it within a virtual environment for its own use.

    Refer to the [`docker-compose` overview page](https://docs.docker.com/compose/reference/overview/) to see the available `stack` commands.

<img src="/assets/screenshots/grafana-dashboard.png" alt="Example Grafana Dashboard" width="85%" style="display: block; margin-left: auto; margin-right: auto;">

!!!info ""
    Grafana is included in the Meerschaum stack, pre-configured with the Meerschaum TimescaleDB database.

    Open a web browser and navigate to [http://localhost:3000](http://localhost:3000) where you can log into Grafana with username `admin`, password `admin`.

## 🛑 Stopping the Stack

If you want to stop all the services in the stack, run the stack command with `down`:

```bash
mrsm stack down
```

To remove all services in the stack and delete all data, use the `-v` flag:

```bash
mrsm stack down -v
```

<asciinema-player src="/assets/casts/stack-down.cast" autoplay="true" loop="true" size="small" preload="true" rows="12"></asciinema-player>

!!! warning "Data Loss Warning"
    The `-v` flag in `stack down -v` will delete ALL volumes in the stack. That includes pipes' data!

    To delete a specific service's volume, run the command `docker volume rm`, e.g. to delete just Grafana's data:
    ```bash
    docker volume rm mrsm_grafana_storage
    ```

## 📝 Editing the Stack

Certain parameters like the main database username and password are linked from the [connectors](/reference/connectors/) configuration, which may be accessed with `mrsm edit config`.

You can find the complete [Docker Compose](https://docs.docker.com/compose/) YAML file with:

```bash
mrsm edit config stack
```

<asciinema-player src="/assets/casts/edit-stack.cast" autoplay="true" loop="true" size="small" preload="true"></asciinema-player>
