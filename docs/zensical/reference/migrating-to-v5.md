# ⬆️ Migrating to v5

!!! warning "Pre-release guide"

    This guide follows the [v5.0.0 design spec](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) and has been checked against the pre-release builds (the console, plugin-owned Dash apps, the native plugin loader, venv accounting and the Syncing Engine are implemented). Anything marked **⚠️ TBC** may still change before 5.0.0 ships. Report anything that breaks against a pre-release as a GitHub issue tagged `v5-compat`.

Meerschaum v5 is a rewrite of the core, the CLI, jobs, compose, the HTTP API, the MCP server and the web console in **Rust**. The `meerschaum` Python package stays, as the plugin SDK and as a thin façade over the Rust core, so **most pipes, plugins, compose projects, jobs and scripts keep working without changes**. This page lists what you need to check, grouped by who you are.

v4.1.x stays installable and receives security fixes until 5.1.0 ships. There is no v6 planned to "finish" the port, so nothing here is a stopgap.

## 🧭 Who needs to read what

| You... | Read |
|---|---|
| Run `mrsm` on a server, in Docker, or with compose, and only use built-in connectors | [Installing](#installing-and-running), [Configuration](#configuration), [Jobs and the CLI](#jobs-and-the-cli), [Deployment](#deployment) |
| Write fetch / sync / action plugins, custom connectors, or hooks | The above, plus [Python library users](#python-library-users-and-scripts) and [Plugin authors](#plugin-authors) |
| Write `@api_plugin` routes or MCP tools | The above, plus [API plugins](#api-plugins) |
| Write `@dash_plugin` / `@web_page` pages | The above, plus [Web pages and Dash plugins](#web-pages-and-dash-plugins) |
| Want to write plugins in Rust | [Rust plugins](#rust-plugins) |

## ✅ What does not change

- **Pipes.** Keys, `parameters`, every parameter key (`columns`, `dtypes`, `indices`, `tags`, `fetch`, `verify`, `sql`, `target`, `upsert`, `static`, `autoincrement`, `autotime`, `hypertable`, `compress`, `precision`, `null_indices`, `mixed_numerics`, `reference`, `parents`, `children`, `source`), role-keyed `dtypes`, pandas-style dtype strings, every `geometry[...]` notation, integer datetime axes, and the `schema.target` table naming.
- **Templating.** `{{ Pipe() }}` substitution and attribute chains into parameters (`{{ Pipe('sql:main', 'x').columns }}`), `{{ self.* }}`, `MRSM{...}` symlinks, compose `'{sql:label}'` references, `{MRSM_ROOT_DIR}` and `${ENV}` interpolation.
- **Config.** The YAML files and their layout under `MRSM_ROOT_DIR/config/`, `MRSM_SQL_<LABEL>` and `MRSM_API_<LABEL>` environment connectors, `connectors.yaml` URIs, `plugins.json`.
- **Instances.** Existing tables, the internal `_mrsm_internal` schema, user tables and password hashes, token tables, plugin registrations. v5 reads a v4.1 instance as is. No migration script runs on your data.
- **Compose.** Every key and subaction (`up`, `down`, `ps`, `logs`, `run`, `explain`, `init`, `sync pipes`, `start jobs`, ...), `--file`, `--env-file`, `--dry`, `isolation: subprocess`, `plugins: ['name@api:repo']`, `sync.schedule`.
- **Jobs.** The on-disk layout under `MRSM_ROOT_DIR/jobs/`, so jobs created on 4.1 are picked up by 5.0. `-d`, `--name`, `-s` cron schedules, `--loop --min-seconds`, `-w`, `+` and `:` chaining, `show logs`, `attach`, `pause`, `stop`, `delete jobs`, the systemd and `api:` executors.
- **The HTTP API.** Every route, path, method, scope and body shape. JWTs issued by 4.1 validate on 5.0 because the secret file is shared. `--production`, `--secure`, `--host`, `--port`, `api.permissions.*`.
- **Docker.** The `bmeares/meerschaum` and `bmeares/meerschaum:api` images keep their entrypoints and paths (`/meerschaum/plugins`, `/meerschaum/config`).
- **The Python API** for pipes, connectors, users, tokens, config and the plugin SDK. See [Python library users](#python-library-users-and-scripts) for the few things to check.

## 📦 Installing and running

- **Python 3.11 or newer** (only needed when you use Python plugins, the Python API or an extra). 3.10 reaches end of life in October 2026 and is not supported by the v5 wheels.
- **`pip install meerschaum` still works** and now also installs `meerschaum-cli`, a binary wheel containing the native `mrsm` executable. `python -m meerschaum ...` still works and dispatches to the same code.
- **The base wheel has no third-party dependencies.** It depends only on `meerschaum-cli` (the same-version binary wheel). Build tooling is maturin; `setup.py` is gone, so build from source with `pip install .` (maturin backend) or `maturin build`. v4.1 installed about 110 packages into the shared `mrsm` venv on first run (pandas, dask, SQLAlchemy, three PostgreSQL drivers, Dash and its component libraries, rich, FastAPI, and so on). v5's `meerschaum` wheel contains the Rust core and the Python SDK and pulls in nothing else but the CLI binary. Everything heavier is an extra, imported only when used:

    | Extra | Installs | You need it when |
    |---|---|---|
    | `meerschaum[pandas]` | pandas, pyarrow | Your Python code calls `get_data()` without `as_docs=True` / `as_polars=True`, or your `fetch()` returns a DataFrame. Without it, `get_data()` raises an `ImportError` naming the extra (or installs it into the `mrsm` venv on first use when `install=True`, as today). |
    | `meerschaum[polars]` | polars | You use `as_polars=True` or return Polars frames |
    | `meerschaum[geo]` | shapely, pyproj, GeoPandas, geoarrow-types (GeoPolars when released) | You read `geometry[...]` pipes into Python |
    | `meerschaum[sql]` | SQLAlchemy, psycopg, PyMySQL | Your Python code calls `conn.read()`, `conn.exec()`, `conn.value()`, `conn.to_sql()`, `conn.engine` or `get_tables()`. Pipe operations do **not** need it. |
    | `meerschaum[sql-mssql]`, `[sql-oracle]`, `[sql-duckdb]`, `[sql-cockroachdb]` | pyodbc; oracledb; duckdb + duckdb-engine; sqlalchemy-cockroachdb | Raw SQL from Python against those flavors |
    | `meerschaum[api]` | FastAPI, uvicorn, a2wsgi, python-multipart, PyJWT | Any installed plugin uses `@api_plugin`, `@web_page` or registers an MCP tool |
    | `meerschaum[dash]` | Dash, a2wsgi | Any installed plugin uses `@dash_plugin` |
    | `meerschaum[shell]` | ptpython | You use `mrsm python` |
    | `meerschaum[gui]` | pywebview | You use `mrsm start gui` |
    | `meerschaum[cli-tools]` | pgcli, mycli, litecli, gadwall | You use `mrsm sql` to open a database shell |
    | `meerschaum[full]` | `pandas`, `polars`, `geo`, `sql`, `api`, `dash`, `shell` | You want today's behavior |

    The Docker image installs `meerschaum[full]`; a new `bmeares/meerschaum:slim` tag installs only the base wheel.

    **Gone from every extra** (they served code that is now Rust): `dask`, `partd`, `joblib`, `psycopg2-binary`, `asyncpg`, `databases`, `aiosqlite`, `aiomysql`, `GeoAlchemy2`, `rich`, `prompt-toolkit`, `pygments`, `tabulate`, `humanfriendly`, `python-daemon`, `dill`, `psutil`, `watchfiles`, `gunicorn`, `httpx`, `websockets`, `valkey`, `python-jose`, `fastapi-login`, `python-dotenv`, `envyaml`, `terminado`, `tornado`, `dash-bootstrap-components`, `dash-ace`, `dash-extensions`, `dash-daq`, `Flask-Compress`, `modin`, `cmd2`, `ruamel.yaml`. If your plugin imports one of these, declare it in `required` (see [Dependencies and venvs](#dependencies-and-venvs)).

- **No Python at all** is an option: `cargo install meerschaum-cli` or a release binary runs without an interpreter as long as no Python plugin, custom Python connector, API plugin or Dash plugin is used. Point `MRSM_PYTHON` at an interpreter when you add one.
- **Removed:** `pyoxidizer` builds and the standalone "portable" archive. The binary wheel and release binaries replace them.

## ⚙️ Configuration

Everything under `MRSM_ROOT_DIR/config/` is read unchanged. Three things to know:

- `api.uvicorn.*` keys (such as `forwarded_allow_ips`) are honored with a deprecation notice and mapped to `api.server.*`. Rename them at your leisure.
- **New:** `api.permissions.rate_limits`, keyed by route group, so you no longer need HTTP middleware to throttle `/login` (see [API plugins](#api-plugins)):

    ```yaml
    api:
      permissions:
        rate_limits:
          login: {max: 10, window_seconds: 60}
    ```

- **New:** `system.engine.*` keys for the [Syncing Engine](#the-syncing-engine) (`workers`, `python_workers`, `limits.connectors`, `limits.instances`, `auto_rebuild`, `implicit_edges`, `inclusive_cap`, `flat_ledger`, `events`, `state_flush_ms`, `run_history_days`, `debounce_seconds`, `chunk_pipelining`, `default_duration_seconds`). All have defaults; you do not need to set any of them. The full table is in [Engine State](/reference/pipelines/state/#settings).

Run `mrsm show config` on v5 against a v4.1 root directory to see the resolved config.

## 🏃 Jobs and the CLI

Core actions behave the same and take the same flags. `mrsm --help` is diffed against the 4.1 parser in CI, argument for argument.

**Removed or renamed:**

| v4.1 | v5 |
|---|---|
| `--engine laps` | Gone. `--loop` is the only long-running mode and reproduces lap behavior (below). |
| `mrsm install plugin compose` | A no-op with a notice. Compose has been built in since 4.0. |
| `MRSM_CORE=python` or any "use the Python core" switch | Does not exist in 5.0. There is one core. (Pre-release builds still ship the Python core for the parity phase, behind the development-only `MRSM_CORE` gate: unset or `python` runs the Python code, `rust` prefers the Rust core. Do not depend on it. The gate and the Python core are deleted before 5.0.0.) |

**`--loop --min-seconds N` behaves exactly as laps did** for a flat selection: every selected pipe syncs, then the process sleeps `N` seconds after the lap ends, exactly as v4 does. Jobs like these need no change:

```bash
mrsm sync pipes -c plugin:acme -m readings --loop --min-seconds 300 -w 1 -d --name acme-readings
mrsm sync pipes -t nightly -l _staging -s 'daily starting 02:00' -d
```

**`--listen`** keeps working for streaming connectors written in the v4 style (a `sync()` that subscribes and returns while callbacks call `pipe.sync()`). The engine keeps the worker that ran `sync()` alive and supervises it with restarts. You do not need the old `--loop --min-seconds 144000000` workaround anymore; if a job still uses it, it continues to work.

### Compose

`mrsm compose explain`, `init` and `up --dry` read the project's real instance connectors, with the same precedence as v4: the project's `config:` block, with the `MRSM_SQL_<LABEL>` / `MRSM_API_<LABEL>` variables of the project environment layered over it, and the connectors the file itself defines winning for the attributes they set. Registered parameters are compared as stored, and `{{ Pipe() }}` references render to the quoted, schema-qualified table.

- **`explain` does not create databases.** v4 creates the SQLite or GeoPackage file of a project-defined connector (with its `mrsm_*` tables and cache files) just by listing which pipes are registered. v5 treats a file instance whose file does not exist as having no registered pipes and never connects to it. The statuses are the same (every pipe is "Not registered"); the file appears when a pipe is registered.
- **`up --dry` still registers.** As in v4, `--dry` only skips starting jobs: it registers missing pipes, updates changed parameters and removes the project tag from pipes the file no longer defines.
- **An unreachable instance fails the subaction.** As in v4, listing the registered pipes raises (`Exception`) when the instance cannot be reached. The message carries the driver's own error text, which differs from SQLAlchemy's.
- **Plugin-defined instance connectors** (a custom connector type from a project plugin) go through the Python worker like on any other native action. The parity tests cover `sql:` and `api:` instances.

### The Syncing Engine

v5 understands the pipe graph (`parents`, `children` and `{{ Pipe() }}` references) and adds:

```bash
mrsm sync pipes -c sql:main -m daily_rollup --parents     # ancestors first, in dependency order
mrsm sync pipes -c sql:main -m raw_events --children       # then everything downstream
mrsm sync pipes -t reports --parents --resume <run_id>    # re-run only what failed or never ran
```

!!! warning "Not wired yet"

    `--parents`, `--children`, `--resume`, `--loop` and `--listen` run on the engine today, and the web console's `/dash/pipelines` page reads its run history. `mrsm plan pipes` (dry run), `mrsm show graph`, `mrsm show runs` / `show run <id>`, `pipe.sync(parents=, children=)` and `mrsm.Pipeline` are available too. Run history is stored on your instance (the `mrsm_engine_runs` and `mrsm_engine_nodes` tables).

The full guide, with real output, is [Pipelines](/reference/pipelines/). A bare `sync pipes` without `--parents` or `--children` keeps v4 semantics: no capping of a child at its parents' watermark, no rebuilds, no ledger-driven backfills. Two behaviors are new **inside engine runs only** (`--parents`, `--children`, `mrsm.Pipeline`, and `--loop` with a selection that has edges):

- A child is never synced past the minimum of its parents' sync times.
- Changing a pipe's definition (its `sql`, `fetch` config or `dtypes`) marks it for **rebuild** (clear, then full sync). Interactively you are asked; in a job the pipe is marked `rebuild_pending`, descendants keep running on the stale data, and a warning is logged on every run until someone confirms with `--yes` or sets `system.engine.auto_rebuild: true`. Rebuilds never block a job.

## 🐍 Python library users and scripts

`import meerschaum as mrsm` and the public API are unchanged. `mrsm.Pipe`, `mrsm.get_connector`, `mrsm.get_config`, `pipe.sync`, `pipe.get_data`, `pipe.get_sync_time`, `pipe.get_rowcount`, `pipe.verify`, `pipe.filter_existing`, `pipe.enforce_dtypes`, `pipe.parameters` (still a live dict: assign, then `pipe.edit()`), `pipe.columns`, `pipe.dtypes`, `pipe.upsert = True`, pickling, equality and hashing by keys.

Things to check:

- **Return types.** `get_data()` returns pandas by default and a `GeoDataFrame` with `.crs` set when any column is a `geometry[...]` dtype. `as_polars=True` returns Polars (GeoPolars when installed). `as_iterator`, `as_chunks` and `as_docs` keep their shapes. `pipe.sync()` returns a `SyncResult`, which is a `tuple` subclass equal to `(success, message)` with extra fields (`inserted`, `updated`, `upserted`, `duration`, ...). `success, msg = pipe.sync()` still unpacks.
- **Raw SQL needs `meerschaum[sql]`.** `conn.read(sql, params=...)`, `conn.exec(...)`, `conn.value(select)`, `conn.to_sql(df, ...)`, `conn.engine` and `get_tables(conn)` stay SQLAlchemy-backed and keep accepting SQLAlchemy constructs. GeoPandas's `read_postgis(query, conn.engine)` keeps working. Without the extra these raise an `ImportError` naming the extra.
- **Constructing a `Pipe` never touches the instance.** Module-level `mrsm.Pipe(...)` definitions import without a database, as today. Attributes are fetched on first access.
- **Partial-row syncs** on non-upsert pipes (`pipe.sync([{'ts': ..., 'id': ..., 'status': 'done'}])`) update the matching row without nulling other columns, as today.
- **Removed internal modules.** Importing a module from the former Python core raises an `ImportError` whose message names the replacement. The commonly imported ones:

    | Import | v5 |
    |---|---|
    | `meerschaum.connectors.sql._pipes`, `_fetch`, `_sql`, `_create_engine` and other `connectors/sql/_*.py` modules | Removed. Use the public `SQLConnector` methods. |
    | `meerschaum.connectors.sql._users.valid_username` | Kept at the same path as an SDK re-export. |
    | `meerschaum.connectors.sql.tables.get_tables` | Kept at the same path (SQLAlchemy `Table` objects, needs `meerschaum[sql]`). |
    | `meerschaum.core.Pipe._*` internals | Removed. |
    | `meerschaum.actions.<module>` function imports | `meerschaum.actions.actions['sync']` and friends keep their names and dispatch to Rust. |
    | `meerschaum.api.routes.*`, `meerschaum.api.dash.callbacks.*`, `meerschaum.api.dash.pages.*` | Removed. The console is Rust. |
    | `meerschaum.utils.daemon.Daemon` | Removed. Use `meerschaum.jobs.Job`, which keeps `start`, `stop`, `pause`, `get_logs`, `monitor_logs`. |
    | `meerschaum.utils.sql` query builders (`sql_item_name`, `build_where`, `dateadd_str`, `table_exists`, ...) | ⚠️ TBC: the helpers plugins commonly use are kept as thin wrappers over Rust. The final list is in the SDK reference. |

- **Kept SDK names** (verified against real plugins, so implementers must keep them): `SuccessTuple`, `warn`, `error`, `info`, `dprint`, `pprint`, `print_tuple`, `print_options`, `attempt_import` (with `venv=` and `lazy=`), `import_pandas`, `pip_install`, `Venv`, `Plugin(name).module`, `from_plugin_import`, `import_plugins`, `make_connector`, `make_action`, `add_plugin_argument`, `pre_sync_hook`, `post_sync_hook`, `prompt`, `yes_no`, `wget`, `round_time`, `select_pipes`, `string_to_dict`, `items_str`, `filter_keywords`, `filter_arguments`, `parse_df_datetimes`, `query_df`, `none_if_null`, `get_current_timestamp`, `serialize_bytes`, `deserialize_bytes_string`, `serialize_geometry`, `replace_pipes_syntax`, `STOP_EVENT`, `ROOT_DIR_PATH` and `meerschaum.config.paths`, `get_plugin_config` / `write_plugin_config` (including the no-argument form), `User` and `Token` with `hash_password`, `verify_password`, `_password_hash`, `_attributes`, `_user_id`, `Token(user, label, scopes, expiration).register()` and `.get_api_key()`.

## 🔌 Plugin authors

**Data plugins (`fetch`, `sync`, `register`, `setup`), action plugins (`@make_action`, `add_plugin_argument`), hooks, library plugins, `required`, per-plugin venvs and `plugins_dir` lists are unchanged.** The plugin is still a Python module in `MRSM_PLUGINS_DIR`; `fetch` may still return a DataFrame, a GeoDataFrame, a list of dicts, a dict of lists, a JSON string, or a generator of any of those.

What is different underneath, and when it matters:

- **Plugins run in a worker process** when the host is the `mrsm` binary (CLI, jobs, server). The worker is a normal Python interpreter with the SDK loaded; `attempt_import(..., venv='myplugin')` and `Venv` activate venvs exactly as today. Module-level state in your plugin lives for the life of the worker, not of the command, which matches today's job behavior but not today's one-shot CLI behavior. Don't rely on module globals being fresh per action.
- **`@post_sync_hook` and `@pre_sync_hook`** receive the same kwargs (`success_tuple`, `sync_timestamp`, `sync_complete_timestamp`, `sync_duration`, ...) and run in the worker after each native sync.
- **Plugin manifests.** On install, setup and on any source change, Meerschaum records which decorators your plugin uses so that `--help`, tab completion and the server can start without importing Python. If your plugin registers actions or pages **conditionally** (for example, only in production), the manifest sees whatever your module registers at import time with the config present at install time. ⚠️ TBC: `mrsm reload plugins` rebuilds the manifest; the manifest is also rebuilt automatically when a plugin file changes.
- **`fetch(begin, end)` windows in graph runs.** A bare `pipe.sync()` and a flat `sync pipes` pass the same `begin` / `end` as v4. Under `--parents`, `--children` or `mrsm.Pipeline`, a child can be asked to backfill chunk-aligned windows that start earlier than v4's `sync time − backtrack` (see [Windows your plugin receives](/reference/pipelines/#windows-your-plugin-receives)). A plugin that clears its window and recomputes it must be able to rebuild that whole window.
- **Custom connectors** with `@make_connector` keep working, including `IS_INSTANCE = True` connectors implementing the instance interface. A new optional pair, `acquire_pipe_lease` / `release_pipe_lease`, lets the engine coordinate across processes; without it, leases fall back to machine-local file locks with a one-time warning.
- **Streaming connectors.** The v4 pattern keeps working under `--listen` (see [Jobs](#jobs-and-the-cli)). The recommended v5 form is explicit and gets engine-managed micro-batching, acknowledgement after commit and supervision:

    ```python
    # v4 pattern (still works)                     # v5 (recommended)
    def sync(pipe, **kw):                          IS_STREAMING = True
        client.on_message = lambda m: pipe.sync(   
            [parse(m)]                              def subscribe(pipe, sink, **kw):
        )                                               def on_message(m):
        client.connect(...)                                 sink.push([parse(m)], token=m.mid)
        client.loop_start()                             client.on_message = on_message
        return True, "Subscribed."                      client.connect(...)
                                                        client.loop_forever()
    ```

    `sink.push()` buffers; the engine flushes on idle, latency, row or byte triggers (`parameters['stream']`) and calls `sink.ack(tokens)` only after the commit. For MQTT, use QoS 1 and a persistent session.

### Dependencies and venvs

v4.1's shared `mrsm` venv quietly accumulated whatever any plugin asked for: `attempt_import('x')` defaults to `venv='mrsm'` and installs `x` there if missing. Real plugin sets were found calling `attempt_import` without `venv=` for `pandas`, `dash`, `requests`, `plotly`, `dash_bootstrap_components`, `geopandas`, `PIL`, `pypdf`, `pyogrio`, `markdown` and `icalendar`, none of which the plugins declared in `required`. Several of those were only present because Meerschaum's own `[dash]` or `[sql]` extras happened to install them.

In v5 the `mrsm` venv starts out holding only the extras you installed. To keep a plugin working:

1. **Declare what you import.** Every package your plugin uses goes in `required`, including the ones v4.1 used to install for you:

    ```python
    required = ['pandas', 'plotly', 'dash-bootstrap-components', 'requests']
    ```

    Plugins that don't declare a package still work: `attempt_import` installs it on first use, as before. But `mrsm show plugins` warns about every undeclared package, and `mrsm prune venvs` will not know who needs it.

2. **`attempt_import(...)` keeps its signature and its `venv='mrsm'` default**, because a DataFrame or Dash component created in one plugin's venv and handed to another has to come from the same library copy. Pass `venv='<plugin>'` for packages only your plugin uses, exactly as today.

3. **Prune.** Every install `attempt_import` performs is attributed: the requester (a plugin, or `meerschaum` for an extra or core install) is recorded in `venvs/<name>/.mrsm_requests.json`. `mrsm show venvs` lists every venv, its size and each package with its requester:

    ```text
    mrsm
        Path: .../venvs/mrsm
        Size: 5.7 MB
        Packages: 1
          - pip 26.2.1  [unattributed]
    ```

    `mrsm prune venvs --dry` shows what nothing still needs; `mrsm prune venvs mrsm -y` removes it. A package is kept if Meerschaum requested it, if an installed plugin requested it or lists it in `required`, or if a kept package depends on it. **Packages with no recorded requester are never removed.** `mrsm upgrade` prints what a prune would remove, and v5 never deletes packages by itself.

4. **Pin what matters.** With `dash`, `pandas` and friends no longer pinned by Meerschaum, add lower bounds in `required` for anything your plugin is sensitive to.

- **`MRSM_NO_AUTO_INSTALL=1` now disables every implicit install**, including the dependency prechecks that v4 ran anyway (v4 still downloaded `semver` for the version check even with the variable set, and the API imported `uv` at startup). `attempt_import`, `is_installed`, the version checks, the `uv`/`pip`/`wheel` bootstraps and the API host install nothing; when no version parser is available the check compares versions natively and, failing that, assumes the requirement is met. Installs you ask for (`pip_install(...)` from your own code, `mrsm install packages`, `install required`) still run. `uv` is only used if it is already on `PATH`.

- **MCP tools** registered with `meerschaum.mcp.tool(...)` / `resource(...)` keep working. The Rust MCP server lists them alongside the built-in tools and forwards calls to your plugin with the caller's scopes. `get_tools`, `get_resources` and `meerschaum.mcp._registry.get_tools` stay for duplicate-registration guards.

## 🌐 API plugins

`@api_plugin` functions still receive a FastAPI `app` and can add routes, middleware, `StaticFiles` mounts and background threads. `meerschaum.api.endpoints`, `get_api_connector`, `get_cache_connector`, `production`, `debug`, `ScopedAuth`, `CurrentScopes`, `_oauth2.SECRET`, `_tokens.get_token_from_authorization`, `get_pipe` and the `models` stay. `api_plugin` returns your function unchanged, so tests that call `init_app(FastAPI())` against a bare app keep passing.

**What changed:** Meerschaum's own routes are served by the Rust server. Your routes are served by a Python **API host** that the Rust server starts when it sees a plugin needs it and reverse-proxies to. Every path the Rust server does not own, websockets included, reaches your app unchanged, so your public URLs do not move. Install `meerschaum[api]` wherever such plugins run (the Docker images already do).

Three patterns need attention because they assumed one process:

### Middleware that wrapped core routes

Your `@app.middleware('http')` now sees only requests that reach your app: your own routes and anything the Rust server does not own. It no longer sees `/login`, `/pipes/*`, `/mcp` or the console. Audit each middleware:

| Middleware did | v5 |
|---|---|
| Rate-limit `POST /login` | Set `api.permissions.rate_limits.login` in config and delete the middleware. |
| Log usage of your own routes | Keeps working unchanged. |
| Bust a cache after mutations on your own routes | Keeps working unchanged. |
| Add headers or auth checks to core routes | Not possible from a plugin. Put a reverse proxy (NGINX) in front, or open an issue describing the need. |

### Stripping core routes by module name

```python
# v4: hide every core route except /mcp
@app.on_event('startup')
def _strip():
    app.router.routes[:] = [
        r for r in app.router.routes
        if not getattr(r.endpoint, '__module__', '').startswith('meerschaum.')
    ]
```

finds nothing to strip in the host, because the core routes are not in it. Use the existing allowlist instead; it is enforced by the Rust server by route group:

```yaml
# v5 (and 4.1.1+)
api:
  permissions:
    routes:
      allowlist: [mcp, login, plugin:acme]
```

⚠️ TBC: route-group names and whether a `denylist` is added alongside (open question Q13 in the spec).

### Background threads and startup hooks

`@app.on_event('startup')` and threads started inside your `@api_plugin` run in the host process, which starts once per server start and is restarted on crash. Make startup idempotent.

### Sessions from API routes

`meerschaum.api.dash.sessions` (`set_session`, `get_user_from_session`, `get_username_from_session`, `delete_session`, `is_session_active`, `session_is_admin`, `set_session_cookie`, `EXPIRES_SECONDS`) reads and writes the same session store as the Rust console, keyed by the same `mrsm-session-id` cookie. Plugins that implement their own registration or login flows and mint console sessions keep working.

## 🎛️ Web pages and Dash plugins

The console is now rendered by Rust at the same `/dash` paths, with the same page set plus a new [`pipelines` page](/reference/pipelines/visualization/#in-the-web-console). It starts with `mrsm start api` like before, and the `api.permissions.routes.allowlist` route groups hide console pages as in v4. **Meerschaum no longer runs a Dash app of its own.** There are two ways for a plugin to add pages.

### Option A: keep your Dash pages (legacy mode)

`@dash_plugin` still receives a `dash_app`. The difference is **whose app it is**: Meerschaum creates a Dash app for your plugin, mounted inside the API host at `/dash/<plugin>/`, pre-populated with the core components your callbacks reference (`session-store`, `mrsm-location`, `mrsm-theme-*`), with a navbar that links back to the console. `@web_page(...)` functions returning Dash layouts register pages in that app. `meerschaum.api.dash.components.build_cards_grid`, `keys`, `pipes` and `graphs` helpers stay available.

Checklist:

1. **URLs gain a plugin segment.** `/dash/counts` becomes `/dash/acme/counts`. The console redirects the old paths, so bookmarks and the navbar keep working. Inside your app, build links with `dash_app.get_relative_path('/counts')` instead of hardcoding `/dash/counts`.
2. **`mrsm-location` pathnames change accordingly.** A callback that routed on `pathname.startswith('/dash/counts')` should use `dash_app.strip_relative_path(pathname) == 'counts'`.
3. **`index_string`, `external_stylesheets` and `assets/`** are now yours and affect only your pages. Code that spliced CSS into the index template keeps working, but it no longer touches console pages.
4. **You cannot patch console pages.** A callback with an `Output` targeting a component on a Meerschaum page (the pipes dashboard, the jobs page, ...) raises at registration time because that ID does not exist in your app. Move that UI into a page of your own.
5. **`session-store` is filled on page load** from the shared session cookie by the shim. Callbacks that only `State('session-store', 'data')` need no change. Callbacks that *wrote* `session-store` to log a user in should call `meerschaum.api.dash.sessions.set_session` so the console sees the login too.
6. **Install `meerschaum[dash]`** (and `[api]`) wherever the server runs.
7. Dash version compatibility is now your plugin's concern. Pin `dash` in your plugin's `required`, and add `dash-bootstrap-components`, `dash-extensions`, `dash-ace` or `dash-daq` there if you use them: Meerschaum no longer installs them.

### Option B: the page contract (recommended for new pages)

A v5 plugin page is a function that returns an **HTML fragment**. The console wraps it in its own chrome (navbar, theme, session) and serves it at `/dash/<path>`, the same for Python and Rust plugins. Charts are **Plotly figure JSON**: build the figure with `plotly.py` exactly as before, then hand the JSON to the console, which renders it with its vendored Plotly.js. Interactivity is [HTMX](https://htmx.org): an element requests a fragment from one of your routes and swaps it in.

Side by side, a page with a dropdown that re-draws a chart:

=== "v4 (Dash)"

    ```python
    from meerschaum.plugins import dash_plugin, web_page
    from dash import dcc, html, Input, Output
    import plotly.graph_objects as go

    @dash_plugin
    def init_dash(dash_app):

        @web_page('counts', login_required=True, page_group='Reports')
        def counts_page():
            return html.Div([
                dcc.Dropdown(id='acme-site', options=site_options(), value='all'),
                dcc.Graph(id='acme-counts-graph'),
            ])

        @dash_app.callback(
            Output('acme-counts-graph', 'figure'),
            Input('acme-site', 'value'),
        )
        def update_graph(site):
            df = counts_pipe.get_data(params={'site': site} if site != 'all' else None)
            return go.Figure(go.Scatter(x=df['ts'], y=df['count']))
    ```

=== "v5 (page contract)"

    ```python
    from html import escape
    from meerschaum.plugins import web_page, api_plugin, figure
    import plotly.graph_objects as go

    def counts_figure(site):
        df = counts_pipe.get_data(params={'site': site} if site != 'all' else None)
        fig = go.Figure(go.Scatter(x=df['ts'], y=df['count']))
        return figure(fig)          # -> '<div data-mrsm-figure="{...plotly json...}"></div>'

    @web_page('counts', login_required=True, page_group='Reports')
    def counts_page():
        options = ''.join(
            f'<option value="{escape(v)}">{escape(label)}</option>'
            for v, label in site_options()
        )
        return f'''
          <select name="site" hx-get="/acme/counts/figure" hx-target="#acme-fig" hx-trigger="change">
            {options}
          </select>
          <div id="acme-fig">{counts_figure('all')}</div>
        '''

    @api_plugin
    def init_api(app):
        @app.get('/acme/counts/figure', response_class=HTMLResponse)
        def counts_figure_route(site: str = 'all'):
            return counts_figure(site)
    ```

Notes on the contract:

- The figure code did not change. Only the transport did: `dcc.Graph(figure=fig)` became a `div` with a `data-mrsm-figure` attribute carrying the figure JSON. `meerschaum.plugins.figure(fig)` builds that `div` from a `plotly.py` figure (or a figure `dict` or JSON string), escaping the JSON for the attribute; `figure_json(fig)` returns just the compact JSON, byte for byte what a Rust plugin built with the `plotly` crate would emit for the same chart. The console renders every such `div` on load and after each HTMX swap.
- **Maps:** `<div data-mrsm-map='...'>` renders GeoJSON or a layer spec with the vendored MapLibre GL JS. This is the replacement for dash-leaflet.
- **Tables:** return HTML tables; the console adds sorting. Editable grids are your concern.
- **Login and session.** `login_required=True` is enforced by the console before your function runs. ⚠️ TBC: the function may accept `request`, `username` and `session` keyword arguments, filtered by signature like other plugin entry points.
- **Theme.** The `mrsm-theme-*` CSS classes keep their names. Style against them and your fragment matches the console in both themes.
- **No CDN.** Plotly.js, MapLibre GL JS and HTMX are vendored by the console. Don't add `<script src="https://cdn...">` tags; they will be blocked by the console's content security policy and would not work on offline deployments anyway.

A function that returns a string (or a `(html, status)` pair) is a page-contract page; one that returns a Dash layout is a legacy Dash page. The manifest records which, by calling the function once at install time. A page written this way moves to Rust without touching its chart, because the Rust `plotly` crate emits the same figure JSON (see [Rust plugins](#rust-plugins)).

## 🦀 Rust plugins

**v5 is still a Python package** (`pip install meerschaum`). Most users keep writing Python plugins, and they work unchanged. Rust plugins are an option for the native performance path (the core, kernel and engine) and for the Python-free `slim` image.

New in v5. A Rust plugin is a `cdylib` built with the `meerschaum-plugin` crate, installed into `MRSM_PLUGINS_DIR` as `<name>/plugin.toml` plus the shared library, or as a Cargo crate (`plugin.toml`, `Cargo.toml`, `src/`) that Meerschaum builds itself when `cargo` is on `PATH`. It can implement `fetch`, `sync`, `register`, `setup`, `subscribe`, actions, a connector, HTTP `routes` and console `pages`. Data crosses the boundary as Arrow record batches.

```rust
use meerschaum_plugin::{Frame, Plugin, Request, Result};

#[derive(Default)]
struct Acme;

impl Plugin for Acme {
    const NAME: &'static str = "acme";
    const VERSION: &'static str = "1.0.0";
    fn manifest(&self) -> meerschaum_plugin::serde_json::Value { /* actions, pages, hooks */ todo!() }
    fn fetch(&self, request: &Request) -> Result<Option<Frame>> {
        // return an Arrow-backed Frame
        todo!()
    }
}

meerschaum_plugin::export_plugin!(Acme);
```

A plugin directory may contain both a Rust library and a Python module; the Rust plugin replaces a Python plugin of the same name entirely, with no per-function fallback. Python plugins can depend on Rust plugins and vice versa through `required`. The [Rust plugin guide](/reference/plugins/rust-plugins/) covers the layout, source plugins, the manifest, the C ABI, building and loading an example, how to test a plugin, and what is not available yet. A native plugin's console pages return the same HTML fragments and Plotly figure JSON as a Python page-contract page.

## 🚢 Deployment

- **Docker.** `bmeares/meerschaum` and `bmeares/meerschaum:api` keep their entrypoints, `/meerschaum/plugins`, `/meerschaum/config` and `/meerschaum/config/plugins.json`. `docker cp` of plugins into the container keeps working. The default image ships `meerschaum[full]`, so API and Dash plugins work without further installs. `bmeares/meerschaum:slim` ships only the base wheel and is the right base for Rust plugins or plugins that return dicts.
- **Bare metal.** Replace `pip install --user meerschaum==4.1.4` with `pip install --user 'meerschaum[full]>=5,<6'` (or the extras you need). The `mrsm` on your `PATH` is now a native binary from the `meerschaum-cli` wheel.
- **NGINX / reverse proxies.** Paths do not change. The server honors `api.server.forwarded_allow_ips` (old `api.uvicorn.*` key still read). The API host listens on a Unix socket under `MRSM_ROOT_DIR`; nothing else needs to reach it.
- **`start api -p 8224 --host 127.0.0.1 -i sql:main --secure`** and friends are unchanged.
- **Webterm** is served by the Rust API server itself (no Tornado/terminado process, no separate `start webterm`): `/webterm/<session id>` and its websocket keep their paths and the terminado message format, the console embeds the terminal at `/dash/webterm` (xterm.js 6.0.0 with the fit and unicode11 add-ons, vendored, plus the mobile extra-key row), and the shell is `mrsm` on the server's instance. It keeps its tmux session model (`api.webterm.tmux.enabled`, `api.webterm.tmux.session_suffix`): each user's terminal lives in the tmux session `<user><session_suffix>`, so a reload or a second tab reattaches to it, and closing a tab never kills the session. Without `tmux`, each tab gets its own shell. `--no-webterm` and `--no-auth` still turn it off. The terminal websocket refuses cross-origin requests: a reverse proxy in front of the API must pass the original `Host` (or set `X-Forwarded-Host`) or the terminal answers 403. ⚠️ Only administrators get a terminal unless `system.api.permissions.actions.non_admin` is set (v4 let every logged-in user in by default). A second login no longer fails, and the shell echoes.
- **Logs.** The Rust server's logs and the API host's output are interleaved in the API job's logs. ⚠️ TBC: a `--host-logs` filter.

## 🧪 How to upgrade safely

1. **Try v5 in a copy.** `MRSM_ROOT_DIR=/tmp/v5-root mrsm ...` against the same instance. v5 writes no new files into an instance until an engine run happens, and engine state lives in internal `mrsm_engine_*` tables under `_mrsm_internal`.
2. **Lint your compose files:** `mrsm compose explain` now warns about unresolved `{{ Pipe() }}` references, `dtype:` typos (for `dtypes:`) and string-typed `tags`. It never changes behavior.
3. **Dry-run the engine:** `mrsm plan pipes -t <tag>` prints what an engine run would do without moving data.
4. **Import every plugin:** `mrsm show plugins` on v5 imports each plugin and reports `ImportError`s from removed internals, with the replacement named in the message.
5. **Diff `mrsm --help`** against 4.1 if you script around the CLI. Any difference is a bug; please report it.
6. **Start the API and open the console.** If you have `@api_plugin` or `@dash_plugin` plugins, confirm the API host started (`mrsm show jobs` ⚠️ TBC) and walk through each plugin page. Old Dash URLs should redirect.
7. **Keep 4.1 around** in a separate venv for a release cycle. Both versions read the same instance and the same root directory layout, so switching back is a `PATH` change.

## 🗑️ Removed in 5.0, at a glance

| Removed | Replacement |
|---|---|
| The Python core (`connectors/sql/_*.py`, `core/Pipe/_*.py`, `actions/*`, `api/routes/*`, `api/dash/callbacks/*`, `utils/daemon`) | The Rust core; the façade keeps the public API |
| `--engine laps` | `--loop` |
| `pyoxidizer` builds, the portable archive | `meerschaum-cli` wheel, release binaries, `cargo install` |
| Meerschaum's own Dash console | The Rust console; plugin Dash apps under `/dash/<plugin>/` |
| Plugin middleware over core routes | `api.permissions.rate_limits`, `api.permissions.routes.allowlist`, a reverse proxy |
| Patching console pages from a plugin | Your own page (Dash or the page contract) |
| Python 3.10 | 3.11+ |
| `api.uvicorn.*` | `api.server.*` (old keys read with a notice) |
| Meerschaum-installed `dask`, `rich`, `dash-bootstrap-components` and the other packages listed under [Installing](#installing-and-running) | Declare them in your plugin's `required` |
