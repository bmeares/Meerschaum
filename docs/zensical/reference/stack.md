<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🥞 Meerschaum Stack

The Meerschaum stack is an integrated collection of services designed to help you start visualizing your data as quickly as possible.

The `stack` command wraps [Docker Compose](https://docs.docker.com/compose/) to create a full-stack Meerschaum installation ― services such as a database instance, API server, and pre-configured Grafana instance.

## 🐳 Services

The default stack starts 4 services:

| Service | Description                                                                            | Image                                   |
|---------|----------------------------------------------------------------------------------------|-----------------------------------------|
| db      | TimescaleDB instance corresponding to `sql:main`.                                      | `timescale/timescaledb-ha:pg18`         |
| api     | Production Meerschaum Web API server corresponding to `api:main`.                      | `bmeares/meerschaum:<version>-full`     |
| valkey  | Valkey instance corresponding to `valkey:main`. Used for caching sessions in by `api`. | `valkey/valkey-bundle:latest`           |
| grafana | Grafana instance connected to `sql:main`.                                              | `grafana/grafana:latest`                |

You can edit the stack's Docker Compose file with `mrsm edit config stack`.

The `api` service runs the `full` image of the same release as the `mrsm` that writes the compose file (for example `bmeares/meerschaum:5.0.0-full`) instead of a floating tag, so the API server and the client always speak the same version, and API plugins and the Dash console work without further installs. The image is built for `linux/amd64` and `linux/arm64`.

!!! info "Upgrading an existing stack to 5.x"
    Earlier versions of the stack ran `bmeares/meerschaum:api`. `mrsm stack` regenerates the compose file on every run: a root without `config/stack.yaml` takes the new image the next time you run `mrsm stack up -d`, which recreates only the `api` container (the volumes and the database are kept). After `pip install --upgrade meerschaum` the tag follows the new version in the same way, so run `mrsm stack up -d` after every upgrade (or `mrsm stack pull` first to fetch the image ahead of time).

    A `config/stack.yaml` that has a `docker-compose.yaml` section keeps its own `api` image, because the defaults only fill in what is missing. To move it, edit `services:api:image` with `mrsm edit config stack` to `bmeares/meerschaum:<version>-full` (or delete the `api` service's `image` line so the default applies), then run `mrsm stack up -d`. `mrsm stack config` prints the compose file that will run.

!!! tip "Entering the Meerschaum Docker image"
    Hop into a shell in the `api` service:

    ```bash
    mrsm stack exec -it api mrsm
    ```

### 🗄️ The Database Setup

The first time the `db` service starts on an empty volume, the image's entrypoint sets up the database. A new stack mounts Meerschaum's setup script as a single file into the image's own setup directory:

```yaml
volumes:
  - meerschaum_db_data:/home/postgres/pgdata
  - <root>/db/initdb/create-extensions.sql:/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro
```

so the image's scripts run first and Meerschaum's runs last:

1. The image creates the `timescaledb` extension in the `postgres`, `template1` and `POSTGRES_DB` databases (new databases created from `template1` have it too).
2. `TIMESCALEDB_TELEMETRY: 'off'` (the stack default) turns off TimescaleDB's telemetry.
3. [`timescaledb-tune`](https://github.com/timescale/timescaledb-tune) tunes `postgresql.conf` for the container's memory and CPU limits. Set `NO_TS_TUNE: 'true'` in the service's `environment` to skip it (the stack's `command` still sets `max_connections` and `shared_buffers`, which win over the tuned values).
4. The image creates the `timescaledb_toolkit` extension.
5. `create-extensions.sql` creates `timescaledb`, `postgis`, `timescaledb_toolkit` and `pg_stat_statements` in `POSTGRES_DB`, each only when the image provides it (a `NOTICE` in the container's log names each one it skips, so another PostgreSQL image such as `postgres:18` or `postgis/postgis` still starts), and adds `pg_stat_statements` to `shared_preload_libraries` (keeping the entries already there) so its view can be queried once the server is up.

The `db` healthcheck asks the server over TCP (`pg_isready -h 127.0.0.1 -p 5432 ...`, with a 300 second `start_period`): the entrypoint's setup server only listens on its Unix socket, so the service turns healthy (and `api` and `grafana` start) only after the setup is done. A passing check turns it healthy at once; the long start period only keeps a slow first setup (e.g. on a Raspberry Pi) from being reported unhealthy, which would abort `mrsm stack up`.

!!! note "The setup runs once"
    Docker runs the setup only when the data volume is empty. `mrsm stack` writes `create-extensions.sql` under your root directory once, when it is missing, and never overwrites it, so you may edit it before the first `mrsm stack up` (e.g. to create more extensions). Changing it later does nothing for an existing database: see [Upgrading the stack](/reference/stack-upgrades/) (`mrsm upgrade extensions`) for updating the extensions and settings of a database that already exists.

!!! info "Stacks created before 5.0"
    Earlier versions mounted the whole `<root>/db/initdb` directory over `/docker-entrypoint-initdb.d`, which hid the image's own scripts (no telemetry opt-out, no tuning, no extensions in `template1`), and checked health over the Unix socket. `mrsm stack` regenerates the compose file on every run: a root without `config/stack.yaml` (v4's `mrsm stack` never writes one) takes the new defaults, and the next `mrsm stack up -d` recreates the `db` container once (the data volume is kept; extra scripts in `<root>/db/initdb/` are no longer mounted for a future fresh volume). A `stack.yaml` with a `docker-compose.yaml` section keeps that section as it is. See the [migration guide](/reference/migrating-to-v5/#deployment).

### Docker Images

Choose a variant of [`bmeares/meerschaum`](https://hub.docker.com/r/bmeares/meerschaum):

| Tag | Included | Best suited to |
|---|---|---|
| `full` / `latest` | Native CLI, Python SDK and all extras (including Dash/API), Cargo, rustc, C/C++ build tools, pip, Git and interactive tools | First-time users and plugin development |
| `rust` | Native CLI, core API/MCP, database connectors and runtime libraries; no Python or compilers | Small native deployments and prebuilt Rust plugins |
| `rust-full` | Everything in `rust`, plus Cargo, rustc, Git and native build tools; no Python | Building Rust source plugins without a Python installation |
| `slim` / `minimal` | Python and the base wheel; no optional extras or Rust toolchain | Small Python installations where dependencies are supplied separately |
| `api` | Deprecated alias of `full` | Existing deployments |

Every variant runs the same Rust engine and uses the same configuration and plugin formats.
The Dash console and Python plugins require a Python variant. A `.rs` source plugin needs
Cargo for its first build and after edits; use `rust-full` or `full`, or ship a prebuilt plugin
with `rust`. The toolchain variants also cache the base dependencies for single-file Rust plugins,
so a minimal Rust plugin can build offline. Additional plugin dependencies use normal Cargo
downloads. Native-only deployments never need a Python host. Mount `/home/meerschaum/.cargo`
when you want dependency downloads to persist across `rust-full` or `full` containers.

Pin a version with `{version}-{variant}`, for example `5.0.1-rust` or
`5.0.1-full`. Releases publish `linux/amd64` and `linux/arm64` and update the floating tags;
pre-releases publish only version-qualified tags, for `linux/amd64`.
`{version}-api` continues to alias `{version}-full`.

Build locally with `docker buildx build --target rust --load -t mrsm:rust .` (replace
`rust` with `rust-full`, `slim`, or `full`). A plain `docker build .` produces `full`.
`full` intentionally has a larger size budget because it includes both language toolchains.
Native drivers may still require vendor libraries, such as Oracle Instant Client.

Containers are upgraded by pulling the desired image and recreating the service, preserving
the mounted Meerschaum root. `mrsm upgrade mrsm` inside a container reports this ownership
rather than changing packages in the running image.

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

## 💾 Backing Up and Upgrading the Database

`mrsm backup postgres` takes an online backup of the stack's database (nothing is stopped) into
`<root>/backups/postgres/<timestamp>/`, and `mrsm restore postgres` puts a backup into a new volume
and switches the stack to it, keeping the current one. `mrsm upgrade postgres` moves the database
to a newer PostgreSQL major the same way (a backup first, then a verified restore). See
[Backing up and restoring](/reference/stack-upgrades/#backing-up-and-restoring) and
[Upgrading PostgreSQL](/reference/stack-upgrades/#upgrading-postgresql).

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
