# 🥞 Meerschaum Stack

The Stack uses [Docker Compose](https://docs.docker.com/compose/) to run TimescaleDB, the Meerschaum API and web console, Valkey, and Grafana together. Start it with `mrsm stack up -d`.

See [Upgrading the Stack](upgrades.md) for backups, restores, and database upgrades, or [team execution environments](../team-executors.md) to run isolated per-team executor services alongside the main API.

## 🐳 Services

The default stack starts 4 services:

| Service | Description | Image |
|---|---|---|
| `db` | TimescaleDB instance backing `sql:main`. | `timescale/timescaledb-ha:pg18` |
| `api` | Production Meerschaum Web API and console (`api:main`). | `bmeares/meerschaum:<version>-full` |
| `valkey` | Valkey cache backing `valkey:main` and API sessions. | `valkey/valkey-bundle:latest` |
| `grafana` | Grafana pre-configured with `sql:main`. | `grafana/grafana:latest` |

Edit the stack's Docker Compose configuration with `mrsm edit config stack`, or print the resolved Compose file with `mrsm stack config`.

The `api` service defaults to `bmeares/meerschaum:<version>-full` (`linux/amd64` and `linux/arm64`), matching your installed `mrsm` release. After upgrading `meerschaum`, run `mrsm stack up -d` to recreate the `api` container on the new image while keeping your volumes. If an older `config/stack.yaml` pins `bmeares/meerschaum:api`, remove or update `services:api:image` to follow the release tag.

!!! tip "Entering the Meerschaum Docker image"

    Open a Meerschaum shell inside the running `api` container:

    ```bash
    mrsm stack exec -it api mrsm
    ```

### 🗄️ The Database Setup

When `db` starts on an empty volume, Meerschaum mounts `<root>/db/initdb/create-extensions.sql` to `/docker-entrypoint-initdb.d/100_meerschaum.sql:z,ro` so the image's own setup scripts run first and Meerschaum's runs last:

1. The image installs `timescaledb` in `postgres`, `template1`, and `POSTGRES_DB`, disables telemetry (`TIMESCALEDB_TELEMETRY: 'off'`), runs `timescaledb-tune` (disable with `NO_TS_TUNE: 'true'`), and installs `timescaledb_toolkit`.
2. `create-extensions.sql` enables `timescaledb`, `postgis`, `timescaledb_toolkit`, and `pg_stat_statements` (skipping any extension unavailable in a custom image) and appends `pg_stat_statements` to `shared_preload_libraries`.

The `db` healthcheck polls TCP (`pg_isready -h 127.0.0.1 -p 5432`, 300-second `start_period`) so `api` and `grafana` wait until first-time initialization completes.

!!! note "Setup runs once per volume"

    Docker only runs `/docker-entrypoint-initdb.d` when the data volume is empty. You can customize `<root>/db/initdb/create-extensions.sql` before your first `mrsm stack up`. For an existing database, run [`mrsm upgrade extensions`](upgrades.md).

### Docker Images

Choose a variant of [`bmeares/meerschaum`](https://hub.docker.com/r/bmeares/meerschaum):

| Tag | Included | Best suited to |
|---|---|---|
| `full` / `latest` | Native CLI, Python SDK, all extras (Dash/API), Cargo, `rustc`, C/C++ build tools, `pip`, and Git | First-time users and plugin development |
| `rust` | Native CLI, core API/MCP, database connectors, runtime libraries (no Python or compilers) | Small native deployments and prebuilt Rust plugins |
| `rust-full` | Everything in `rust`, plus Cargo, `rustc`, Git, and native build tools (no Python) | Building `.rs` source plugins without Python |
| `slim` / `minimal` | Python and the base wheel (no optional extras or Rust toolchain) | Minimal Python deployments with custom dependencies |
| `api` | Deprecated alias of `full` | Existing deployments |

Every variant runs the same Rust engine and config format. Dash and Python plugins require a Python variant (`full` or `slim`); `.rs` source plugins require Cargo (`full` or `rust-full`; mount `/home/meerschaum/.cargo` to persist downloaded crates).

Pin releases with `{version}-{variant}` (for example `5.0.2-rust` or `5.0.2-full`). Stable releases publish `linux/amd64` and `linux/arm64` and update floating tags; pre-releases publish versioned `linux/amd64` tags only. Build locally with:

```bash
docker buildx build --target rust --load -t mrsm:rust .
```

Upgrade containers by pulling the new image and recreating the service—`mrsm upgrade mrsm` inside a container reports image ownership rather than mutating the running image.

## 🗒️ Requirements

You need [Docker](https://www.docker.com/get-started) installed to run the stack:

=== "Windows / MacOS"
    Install [Docker Desktop](https://www.docker.com/get-started).

=== "Linux"
    Install `docker.io` from your package manager or run:

    ```bash
    curl https://get.docker.com | sh
    sudo usermod -aG docker $USER
    ```

    Log out and back in so the `docker` group takes effect.

## 🟢 Starting the Stack

Bring up the database and Grafana (or omit service names to start the full stack):

```bash
mrsm stack up -d db grafana
```

<asciinema-player src="/assets/casts/stack.cast" loop="true" preload="true" rows="16" idle-time-limit="2" poster="npt:0:04"></asciinema-player>

!!! note ""

    `mrsm stack` wraps a pre-configured [Docker Compose](https://docs.docker.com/compose/) project (if `docker-compose` is missing, Meerschaum installs it automatically in an internal virtual environment). Open Grafana at [http://localhost:3000](http://localhost:3000) with username `admin` and password `admin`.

<img src="/assets/screenshots/grafana-dashboard.png" alt="Example Grafana Dashboard" width="85%" style="display: block; margin-left: auto; margin-right: auto;">

### Built-in dashboards

The Stack provisions two dashboards in Grafana's **Meerschaum Dashboards** folder, reading the instance's `mrsm_engine_*` tables with Pipe and Pipeline filters:

- **Syncs:** Overview (totals, outcomes, P95 duration, rows affected/s, failure rate, retries, slowest pipes), plus per-Pipe and per-Pipeline breakdowns.
- **Engine:** [Syncing Engine](../pipelines/state.md) runs and outcomes, Pipe freshness and parent lag, dirty-range backlog, failed/retried nodes, and stream flushes.

Rows affected sum inserted, updated, upserted, and deleted rows (replacements count deleted plus new rows; unchanged syncs affect zero). On upgrades from `5.0.0rc34` or earlier, unmodified legacy performance dashboards are replaced automatically. Grafana queries the database directly using its data-source permissions; see [Sync metrics](../api-instance/sync-metrics.md) for capture limits.

### Grafana's read-only data source

Since 5.1, the provisioned **Meerschaum Main** data source connects as `mrsm__main_ro`—a read-only PostgreSQL role—instead of `sql:main`'s superuser, protecting stacks where Grafana anonymous access is enabled.

- **Scope:** Reads `public`, schemas owned by `sql:main`, and any extra schemas listed in `stack:grafana:readonly_schemas`. Never sees isolated [team schemas or databases](/reference/api-instance/teams/#a-database-for-each-team), and cannot write, create objects, or install extensions.
- **Setup:** On every `mrsm stack up`, a one-shot `grafana_readonly` service creates or reconciles the role (password stored in `<root>/api/.grafana_readonly_password`) once `db` is healthy. A stack without a `db` service warns and never falls back to owner credentials.
- **Compose generation:** Run `mrsm stack config` (or any `mrsm stack` command) to write `stack/docker-compose.yaml` before calling `docker compose` directly.

To allow Grafana to query an extra schema, add it in `mrsm edit config stack` and run `mrsm stack up -d`:

```yaml
grafana:
  readonly_schemas:
    - reports
```

### Credentials in files

Since 5.1, `mrsm stack` delivers credentials as mounted read-only files under `<root>/stack/secrets/` (`0700` on host) at `/run/secrets/meerschaum` instead of embedding secrets in `docker-compose.yaml` or `docker inspect`. Each container receives a `io.meerschaum.secrets-revision` label so `mrsm stack up -d` recreates containers when credentials change.

| Service | Host file (under `<root>/stack/secrets/`) | Container usage |
|---|---|---|
| `api` | `api/mrsm_config.json`, `api/mrsm_patch.json` | `MRSM_CONFIG_FILE`, `MRSM_PATCH_FILE` |
| `grafana` | `grafana/admin_password` | `GF_SECURITY_ADMIN_PASSWORD__FILE` |
| `grafana_readonly` | `grafana_readonly/owner_password`, `readonly_password` | Read by role-setup script |
| `valkey` | `valkey/valkey.conf` (`requirepass`) | `valkey-server /run/secrets/meerschaum/valkey.conf` |

- **Image compatibility:** File-based API config activates automatically for `bmeares/meerschaum` 5.1.0+. Older or custom images keep environment variables with a warning; set `api_config_files: true` (or `false`) in `mrsm edit config stack` to override.
- **Valkey password:** Valkey images ignore `VALKEY_PASSWORD`, so `mrsm stack` enforces `requirepass` using `valkey:main`'s password. If `valkey:main` has no password, one is generated in `<root>/stack/secrets/.valkey_password` (link it with `password: MRSM{file:<root>/stack/secrets/.valkey_password}`).
- **Environment variables kept:** `POSTGRES_PASSWORD` remains interpolated as `$POSTGRES_PASSWORD` on `db` so maintenance commands can read it from the running container. Grafana's provisioned `datasource.yaml` stores only the read-only role's password.

## 💾 Backups and Upgrades

Use `mrsm backup postgres` for online snapshots, `mrsm restore postgres` to verify and restore into a new volume, `mrsm upgrade postgres` for PostgreSQL major upgrades, and `mrsm upgrade extensions` to update database extensions. See [Upgrading the Stack](upgrades.md).

## 🛑 Stopping the Stack

Stop all running services:

```bash
mrsm stack down
```

Stop services **and delete all volumes**:

```bash
mrsm stack down -v
```

<asciinema-player src="/assets/casts/stack-down.cast" loop="true" preload="true" rows="16" idle-time-limit="2" poster="npt:0:04"></asciinema-player>

!!! warning "Data Loss Warning"

    `mrsm stack down -v` deletes **every volume** in the stack, including all pipe data. To remove a single service's volume instead, use Docker directly:

    ```bash
    docker volume rm mrsm_grafana_storage
    ```

## 📝 Editing the Stack

Database and cache credentials are linked automatically from your [connectors](/reference/connectors/) (`mrsm edit config`). Edit the Compose configuration directly with:

```bash
mrsm edit config stack
```

<asciinema-player src="/assets/casts/edit-stack.cast" loop="true" preload="true" rows="16" idle-time-limit="2" poster="npt:0:04"></asciinema-player>
