
# 🥞 Meerschaum Stack

The Stack uses [Docker Compose](https://docs.docker.com/compose/) to run a database, the API and console, Valkey, and Grafana together. Start it with `mrsm stack up -d`. See [upgrades and backups](upgrades.md) when maintaining an existing Stack.

## 🐳 Services

The default stack starts 4 services:

| Service | Description                                                                            | Image                                   |
|---------|----------------------------------------------------------------------------------------|-----------------------------------------|
| db      | TimescaleDB instance corresponding to `sql:main`.                                      | `timescale/timescaledb-ha:pg18`         |
| api     | Production Meerschaum Web API server corresponding to `api:main`.                      | `bmeares/meerschaum:<version>-full`     |
| valkey  | Valkey instance corresponding to `valkey:main`. Used for caching sessions in by `api`. | `valkey/valkey-bundle:latest`           |
| grafana | Grafana instance connected to `sql:main`.                                              | `grafana/grafana:latest`                |

You can edit the stack's Docker Compose file with `mrsm edit config stack`.

The `api` service runs the `full` image of the same release as the `mrsm` that writes the compose file (`bmeares/meerschaum:<version>-full`), so the API server and the client use the same version, and API plugins and the Dash console work without further installs. The image is built for `linux/amd64` and `linux/arm64`.

??? info "Upgrading an existing stack to 5.x"
    Earlier versions of the stack ran `bmeares/meerschaum:api`. `mrsm stack` regenerates the compose file on every run: a root without `config/stack.yaml` takes the new image the next time you run `mrsm stack up -d`, which recreates only the `api` container (the volumes and the database are kept). After `pip install --upgrade meerschaum` the tag follows the new version in the same way, so run `mrsm stack up -d` after every upgrade (or `mrsm stack pull` first to fetch the image ahead of time).

    A `config/stack.yaml` that has a `docker-compose.yaml` section keeps its own `api` image, because the defaults only fill in what is missing. To move it, edit `services:api:image` with `mrsm edit config stack` to `bmeares/meerschaum:<version>-full` (or delete the `api` service's `image` line so the default applies), then run `mrsm stack up -d`. `mrsm stack config` prints the compose file that will run.

!!! tip "Entering the Meerschaum Docker image"
    Hop into a shell in the `api` service:

    ```bash
    mrsm stack exec -it api mrsm
    ```

### 🗄️ The Database Setup

<details markdown="1">
<summary>First-run setup, extensions, and health checks</summary>

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
    Docker runs the setup only when the data volume is empty. `mrsm stack` writes `create-extensions.sql` under your root directory once, when it is missing, and never overwrites it, so you may edit it before the first `mrsm stack up` (e.g. to create more extensions). Changing it later does nothing for an existing database: see [Upgrading the stack](/reference/stack/upgrades/) (`mrsm upgrade extensions`) for updating the extensions and settings of a database that already exists.

??? info "Stacks created before 5.0"
    Earlier versions mounted the whole `<root>/db/initdb` directory over `/docker-entrypoint-initdb.d`, which hid the image's own scripts (no telemetry opt-out, no tuning, no extensions in `template1`), and checked health over the Unix socket. `mrsm stack` regenerates the compose file on every run: a root without `config/stack.yaml` (v4's `mrsm stack` never writes one) takes the new defaults, and the next `mrsm stack up -d` recreates the `db` container once (the data volume is kept; extra scripts in `<root>/db/initdb/` are no longer mounted for a future fresh volume). A `stack.yaml` with a `docker-compose.yaml` section keeps that section as it is. See the [migration guide](/reference/migrating-to-v5/#deployment).

</details>

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

Pin a version with `{version}-{variant}`, for example `5.0.2-rust` or
`5.0.2-full`. Releases publish `linux/amd64` and `linux/arm64` and update the floating tags;
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

### Built-in dashboards

The Stack provisions two dashboards in Grafana's **Meerschaum Dashboards** folder. Both read the instance's internal `mrsm_engine_*` tables and filter by Pipe and Pipeline.

- **Syncs**: every completed sync.
    - **Overview**: totals, outcomes, duration and P95, write rate (**rows affected/s**), failure rate, retries and chunks, the slowest Pipes and recent failures.
    - **Pipes**: duration, write rate, failures and rows affected per Pipe, a Pipe summary and the sync history.
    - **Pipelines**: duration, rows affected, retries and failures per Pipeline, a Pipeline summary and recorded run durations.
- **Engine**: what the [Syncing Engine](../pipelines/state.md) records.
    - **Runs**: runs by outcome, run duration, node outcomes and planned actions, failures and retries, node start delay against the plan, and the recent runs.
    - **Freshness**: each Pipe's latest sync and time since its last success, data watermarks, and each child's lag behind its parents.
    - **Dirty-range backlog**: the parents' changed ranges each child has not synced yet, and how long they have waited.
    - **Nodes** and **Streams**: failed and retried nodes, per-Pipe timing and critical-path share, and stream flushes.

Rows affected count inserted, updated, upserted and deleted rows. Replacements count deleted plus new rows; unchanged syncs affect zero rows.

!!! note "Upgrading from 5.0.0rc34 or earlier"
    The **Meerschaum Performance**, **Pipe Performance** and **Pipeline Performance** dashboards are replaced by **Syncs**. The next `mrsm stack` command removes their files when they are exactly as Meerschaum wrote them, and Grafana drops them. A customized copy, a symlink or another dashboard under the same file name is kept, with a warning; delete it when you no longer need it.

Grafana queries the configured database directly with its data-source credentials. Its visibility follows those database permissions. The web console and HTTP API apply their Pipe and team access rules. See [Sync metrics](../api-instance/sync-metrics.md) for capture and history limits.

### Grafana's read-only data source

The stack provisions the **Meerschaum Main** data source in Grafana. Since 5.1 it connects as `mrsm__main_ro`, a read-only role, instead of `sql:main`'s user (the database's owner, a superuser in the stack's own database). Grafana's anonymous access is on in the default stack, so anyone who can open a dashboard could otherwise run any SQL with the owner's rights.

- **What it reads.** Every table, view and hypertable in the schemas `sql:main`'s user owns (and `public`), including tables created there later, plus the schemas you list in `stack:grafana:readonly_schemas`. Nothing of a team's (a [team's schema or database](/reference/api-instance/teams/#a-database-for-each-team)), not even when its schema is listed.
- **What it cannot do.** Write, create tables, schemas or databases, install extensions (such as `dblink`), or act as any other role. Its sessions are read-only by default.
- **Its password** is generated once and kept in `api/.grafana_readonly_password` under Meerschaum's root (readable by you only).
- **How it is set up.** `mrsm stack up` adds a one-shot `grafana_readonly` service, built from the `db` service's image, that creates or corrects the role with `psql` once `db` is healthy, on every `stack up`. Grafana starts only after it succeeded: if it fails, `stack up` fails and `docker logs` of that container says why. Grafana is recreated whenever the provisioned data sources change, and updates **Meerschaum Main** in place (same name and uid), so existing dashboards keep working.
- **Who writes the compose file.** Only `mrsm stack` writes `stack/docker-compose.yaml` for a stack with a `grafana` service, with the setup service above (and, with `api:grafana`, Grafana's sign-in settings), or whose `api` service gets `MRSM_CONFIG` / `MRSM_PATCH` (see [Credentials in files](#credentials-in-files)). Python no longer writes it when it first reads the `stack` config, nor does `meerschaum.config.stack.write_stack()`, so the file is never in a form that starts Grafana without them or holds the connectors' credentials. Run `mrsm stack config` (or any `mrsm stack` command) to write it before using `docker compose` by hand.

!!! warning "Upgrading from 5.0"
    The next `mrsm stack up -d` switches **Meerschaum Main** to the read-only role, also when your `config/stack.yaml` holds a copy of the old default (`sql:main`'s user, as `MRSM{...}` references or as literal values). A dashboard that writes through **Meerschaum Main**, or reads a schema owned by another role, stops working. To let it read another schema, list it:

    ```yaml
    ### `mrsm edit config stack`
    grafana:
      readonly_schemas:
        - reports
    ```

    then run `mrsm stack up -d`. A data source whose user you changed to someone else is left as you wrote it (with a warning while anonymous access is on). There is no setting to give the owner's credentials back: to do that, edit **Meerschaum Main** in `stack:grafana:datasource` yourself and take responsibility for what anonymous visitors can then run.

A stack without a `db` service cannot set the role up: `mrsm stack` warns, and **Meerschaum Main** cannot connect until you create `mrsm__main_ro` on your database. It never falls back to the owner's credentials.

### Credentials in files

Since 5.1, `mrsm stack` gives the containers their credentials in files instead of in the compose file. Up to 5.0 the `api` service's `environment` held `MRSM_CONFIG` (your whole `meerschaum`, `system` and `api` configuration, every connector's password included) and `MRSM_PATCH` (with Grafana's administrator password), so they were in `stack/docker-compose.yaml` and in `docker inspect`. Now, on every `mrsm stack` command:

| Service | Credential | Host file (under `<root>/stack/secrets/`) | In the container |
|---|---|---|---|
| `api` | `MRSM_CONFIG`, `MRSM_PATCH` | `api/mrsm_config.json`, `api/mrsm_patch.json` | `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` = `/run/secrets/meerschaum/mrsm_config.json` / `mrsm_patch.json` |
| `grafana` | the administrator password | `grafana/admin_password` | `GF_SECURITY_ADMIN_PASSWORD__FILE` |
| `grafana_readonly` | `sql:main`'s password and the read-only role's | `grafana_readonly/owner_password`, `grafana_readonly/readonly_password` | read by its command |

Each service mounts only its own subdirectory, read-only, at `/run/secrets/meerschaum`, and carries a label (`io.meerschaum.secrets-revision`, a salted hash of its files) so that `mrsm stack up -d` recreates a container whose credentials changed, as it did when they were in its environment. The compose file and `docker inspect` hold paths.

- **Who can read them.** `<root>/stack/secrets/` is `0700` (set again on every write): other users of the host cannot reach the files. Inside it, each service's directory is `0755` and its files `0644`, because the container's user need not be you: Meerschaum's image runs as uid 1000, Grafana's as 472, TimescaleDB's as 1000. Docker mounts the subdirectory itself, so a container never needs to pass through the `0700` directory. Anyone in the container (and `root` and members of the `docker` group on the host, who can run any container) can read them, as they could read the environment before.
- **Your configuration is kept.** Every `MRSM_CONFIG` / `MRSM_PATCH` entry of the `api` service is moved, whether `config/stack.yaml` holds the 5.0 default (as `MRSM{...}` references or as literal values) or your own: the file holds exactly what the variable would have held, so keys you added still reach the container. An entry that uses Docker Compose interpolation (`<DOLLAR>NAME`) stays in the environment with a warning, and so does everything when the service sets `MRSM_CONFIG_FILE` or `MRSM_PATCH_FILE` itself. Your `config/stack.yaml` is not rewritten.
- **Only an image that understands the files gets them.** The `api` service gets the files when its image is `bmeares/meerschaum:<version>` (any variant) with `<version>` 5.1.0 or newer, which the default (`bmeares/meerschaum:<version>-full`, this release) always is. Any other image (an older version, `latest`, a tag without a version, your own build) keeps the environment form, and `mrsm stack` warns once per command with the reason, because an older API server would start without its configuration. If your image is 5.1.0 or newer, set it explicitly:

    ```yaml
    ### `mrsm edit config stack`
    api_config_files: true    # false: keep the environment form, without the warning
    ```

    When the files cannot be written (a read-only root, for example), the environment form is kept with a warning: the API container never starts without its configuration. Back on the environment form, the old files are removed.
- **Valkey requires its password.** Valkey's images ignore `VALKEY_PASSWORD`, so up to 5.0 the stack's Valkey accepted every client without a password on its published port. Now `mrsm stack` writes `requirepass` with `valkey:main`'s password to `<root>/stack/secrets/valkey/valkey.conf`, starts the server with that file (`command: [valkey-server, /run/secrets/meerschaum/valkey.conf]`) and drops `VALKEY_PASSWORD` from its environment. The next `mrsm stack up -d` recreates the container on its existing volume; the API container and `valkey:main` on this host already use that password (the default is `mrsm`: set your own). When `valkey:main` has no password at all, one is generated once into `<root>/stack/secrets/.valkey_password` (readable by you only) and given to the API container, and `mrsm stack` warns until you point `valkey:main` at it with `password: MRSM{file:<root>/stack/secrets/.valkey_password}`. A `valkey` service with its own `command`, another image, or a `VALKEY_PASSWORD` taken from another variable is left as it is, with a warning.
- **Left in the environment.** `POSTGRES_PASSWORD` stays in the `db` service's environment (as `$POSTGRES_PASSWORD`, interpolated by `docker compose` from `sql:main`'s password, so it is in `docker inspect` but not in the file): `mrsm upgrade extensions`, `mrsm upgrade postgres` and `mrsm restore postgres` read it from the running container. The provisioned data source (`stack/grafana/resources/provisioning/datasources/datasource.yaml`) holds the read-only role's password, because Grafana reads that directory through its own mount; that role can only read.

## 💾 Backing Up and Upgrading the Database

`mrsm backup postgres` takes an online backup of the stack's database (nothing is stopped) into
`<root>/backups/postgres/<timestamp>/`, and `mrsm restore postgres` puts a backup into a new volume
and switches the stack to it, keeping the current one. `mrsm upgrade postgres` moves the database
to a newer PostgreSQL major the same way (a backup first, then a verified restore). See
[Backing up and restoring](/reference/stack/upgrades/#backing-up-and-restoring) and
[Upgrading PostgreSQL](/reference/stack/upgrades/#upgrading-postgresql).

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
