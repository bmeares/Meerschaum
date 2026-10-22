# ⬆️ Migrating to v5

!!! warning "Pre-release guide"

    This guide follows the [v5.0.0 design spec](https://github.com/bmeares/Meerschaum/blob/v5/docs/design/v5.0.0.md) and has been checked against the pre-release builds (the Dash console, the native plugin loader, venv accounting and the Syncing Engine are implemented). Anything marked **⚠️ TBC** may still change before 5.0.0 ships. Report anything that breaks against a pre-release as a GitHub issue tagged `v5-compat`.

Meerschaum v5 is a rewrite of the core, the CLI, jobs, compose, the HTTP API and the MCP server in **Rust**. The web console stays the Plotly Dash app of v4, served by the Python package. The `meerschaum` Python package stays, as the plugin SDK and as a thin façade over the Rust core, so **most pipes, plugins, compose projects, jobs and scripts keep working without changes**. This page lists what you need to check, grouped by who you are.

v4.1.x stays installable and receives security fixes until 5.1.0 ships. There is no v6 planned to "finish" the port, so nothing here is a stopgap.

## 🧭 Who needs to read what

| You... | Read |
|---|---|
| Run `mrsm` on a server, in Docker, or with compose, and only use built-in connectors | [Installing](#installing-and-running), [Configuration](#configuration), [Jobs and the CLI](#jobs-and-the-cli), [Deployment](#deployment), [Behavior changes](#behavior-changes-at-a-glance) |
| Write fetch / sync / action plugins, custom connectors, or hooks | The above, plus [Python library users](#python-library-users-and-scripts) and [Plugin authors](#plugin-authors) |
| Write `@api_plugin` routes or MCP tools | The above, plus [API plugins](#api-plugins) |
| Write `@dash_plugin` / `@web_page` pages | The above, plus [Web pages and Dash plugins](#web-pages-and-dash-plugins) |
| Want to write plugins in Rust | [Rust plugins](#rust-plugins) |
| Want to see what is new | [New in 5.0](#new-in-50) |

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

## ⚠️ Behavior changes at a glance

Everything below is deliberate, and each item links to the section with the details. Check the ones that apply to you before upgrading.

| Area | v4.1 | v5 |
|---|---|---|
| `MRSM_NO_AUTO_INSTALL=1` | Stopped `attempt_import` from installing, but v4 still downloaded `semver` and the API imported `uv` at startup | Disables **every** implicit install, including the dependency prechecks. Installs you ask for (`install packages`, `pip_install(...)`) still run. [Details](#dependencies-and-venvs) |
| YAML tags | `!!python/object`, `/apply`, `/name`, ... in a config file or a pipe's parameters were constructed (they can import modules and call functions) | Refused with a `could not determine a constructor for the tag` error, exactly like `yaml.safe_load`. Only `!!python/tuple` (what Meerschaum itself writes) is read. [Configuration](#configuration) |
| Web console | A Plotly Dash app in the FastAPI server | The same app (5.0.0rc2), served by the Python API host and proxied by the Rust server; the standalone binary shows a pointer page at `/dash`. [Web pages](#web-pages-and-dash-plugins) |
| Web terminal | A Tornado process; any logged-in user got a shell unless `api.permissions.actions.non_admin` was false or the server ran with `--secure` | Served by the API server with the same gate (`api.permissions.actions.non_admin`, default true; `--secure` and `false` make it administrators-only). The websocket refuses a cross-origin `Origin`, a second login works and the shell echoes. [Deployment](#deployment) |
| Plugin archives | Symlink and hard-link members were extracted | Refused (`Links are not allowed in plugin archives`); members are written under the target directory only, with a size cap. Plugin and venv names are one plain path component. [Deployment](#deployment) |
| `pipe.sync()` | A plain `(bool, str)` tuple | A `SyncResult` that unpacks like one and carries `inserted`, `updated`, `duration`, ... [Python users](#python-library-users-and-scripts) |
| Dask | `get_data(as_dask=True)` and syncing a Dask DataFrame | Raise a `TypeError` naming this guide. Use `as_iterator=True` or `as_polars=True`; pass `ddf.compute()` or a generator of chunks to `sync()`. [Python users](#python-library-users-and-scripts) |
| Config, `MRSM{}`, `{{ Pipe() }}` | Python | Rust. The files, keys and substitution are unchanged, and `get_config` still returns live dictionaries. Config YAML holding dates, bytes or very large integers now loads and writes back unchanged. [Python users](#python-library-users-and-scripts) |
| Writing a config file | Truncated the file, then wrote it | Writes a temporary file and renames it over the target, so a failed write never loses your connector passwords. A new config file is created `0600`. [Configuration](#configuration) |
| Job names, remote job environment, systemd units | See v4 | Stricter names, the server's environment, a pinned `MRSM_PYTHON`. [Jobs and the CLI](#jobs-and-the-cli) |
| CLI daemons | `start|stop|restart|show daemons` managed a warm interpreter | There is nothing to keep warm: `show daemons` reports none and `start daemons` / `start worker` explain that actions run natively. [Deployment](#deployment) |
| Editors | `$EDITOR`, else installed `pyvim` | `$VISUAL`, else `$EDITOR`, else `vi`. Nothing is installed to open an editor. [Deployment](#deployment) |
| Scopes | `*` entered in `edit tokens` / `edit users` was expanded to the built-in scope list; stopping a job over the API required the misspelled scope `josb:stop`; any user could register a token with any scopes, `*` included | `*` is stored literally and grants every scope, custom and future ones included; `POST /jobs/{name}/stop` requires `jobs:stop` (a token that holds the legacy `josb:stop` is still accepted); **a token carries at most its owner's scopes** (asking for more is refused with a `403` naming them), and only a token's owner or an administrator may edit, invalidate or delete it (only an administrator may make an invalidated token valid again). [Tokens](/reference/api-instance/tokens/#scopes) |
| `sql <label>` | Passed the connection URL with the password to `pgcli` / `mycli` | Prefers installed Rainfrog for supported flavors; keeps Python/plain fallbacks and configurable preferences. Credentials travel through the child environment, outside argv. `--nopretty` forces the plain client. |
| `sql <label> "<query>"` | The query went to the driver in one call: SQLite (`sqlite3`) and MySQL / MariaDB (`pymysql`) refused a string holding several statements (`a; b`) with a driver error and ran none of them | The text is sent as written, so `CREATE TABLE t (a INT); INSERT INTO t (a) VALUES (5)` runs on SQLite, MySQL and MariaDB like it already did on PostgreSQL and MSSQL (Oracle still takes one statement per call). Only SQL you type is affected |
| `sql <label> "<query>"` that contains `select` but changes data | v4 sends any text containing `select` through `read()`, which fails after the statement ran and, on most flavors, rolled it back silently: `INSERT ... SELECT` kept its rows only on MSSQL, and `CREATE VIEW ... AS SELECT` was kept on MSSQL, MySQL, MariaDB, Oracle and SQLite but not on PostgreSQL or DuckDB | The statement runs and is **committed** on every flavor, and the action still prints `None` after "This result object does not return rows" (DuckDB prints its `Count` table). |
| DuckDB | The Python `duckdb` package, or (v5 pre-releases) a C++ library built into `mrsm` | Python hosts share the optional Python driver between Pipe operations and SQLAlchemy. Standalone and base-only installs load `libduckdb` at run time, downloading it when needed. Explicit `MRSM_DUCKDB_LIB` selects the native C API and refuses mixed SQLAlchemy access. [Installing and running](#installing-and-running) |
| Time zone objects | `ZoneInfo('Etc/UTC')` from psycopg | `datetime.timezone.utc` (equal values). [Python users](#python-library-users-and-scripts) |
| An unreachable database, API or Valkey server | The first query waited for the driver's own timeouts (30 s of retries for a refused PostgreSQL / MySQL connection, minutes for a host that never answers) | The first connection of a command fails at once with `sql:main: cannot connect to localhost:5432 (connection refused). Is the database running?` (also unknown host, rejected credentials), and a host that does not answer fails after `connect_timeout` (5 s). [Configuration](#configuration) |
| `MRSM_<TYPE>_<LABEL>` variables of an unknown type | Every such variable became a connector (`MRSM_DUCKDB_LIB` became `duckdb:lib`) | Ignored: only the built-in connector types and the types your plugins register are read. [Configuration](#configuration) |
| Changing a pipe's `columns` | Not tracked | A changed `datetime` / `id` / `primary` column is a definition change: an engine run marks the pipe for `rebuild`. [The Syncing Engine](#the-syncing-engine) |

## 📦 Installing and running

- **Python 3.11 or newer** (only needed when you use Python plugins, the Python API or an extra). 3.10 reaches end of life in October 2026 and is not supported by the v5 wheels.
- **`pip install meerschaum` still works** and is the only PyPI package. Its `mrsm` (and `meerschaum`) console script runs the native CLI from the wheel's extension, and `python -m meerschaum ...` runs the same code. `cargo install meerschaum` builds the same CLI as a standalone `mrsm` binary: both install Meerschaum, like `polars`. The console script pays for loading the extension: `mrsm show version` takes about 35 ms (the Python interpreter alone is 15 ms) against about 10 ms for the standalone binary (measured on a Linux x86_64 development machine with the release build). Jobs, `systemd` units and compose steps run `python -m meerschaum`, so they carry the same start-up cost per process.
- **The base wheel has no third-party dependencies.** It depends on nothing. Build tooling is maturin; `setup.py` is gone, so build from source with `pip install .` (maturin backend) or `maturin build`. v4.1 installed about 110 packages into the shared `mrsm` venv on first run (pandas, dask, SQLAlchemy, three PostgreSQL drivers, Dash and its component libraries, rich, FastAPI, and so on). v5's `meerschaum` wheel contains the Rust core and the Python SDK and pulls in nothing else. Everything heavier is an extra, imported only when used:

    | Extra | Installs | You need it when |
    |---|---|---|
    | `meerschaum[pandas]` | pandas, pyarrow | Your Python code calls `get_data()` without `as_docs=True` / `as_polars=True`, or your `fetch()` returns a DataFrame. Without it, `get_data()` raises an `ImportError` naming the extra (or installs it into the `mrsm` venv on first use when `install=True`, as today). |
    | `meerschaum[polars]` | polars | You use `as_polars=True` or return Polars frames |
    | `meerschaum[geo]` | shapely, pyproj, GeoPandas, geoarrow-types (GeoPolars when released) | You read `geometry[...]` pipes into Python |
    | `meerschaum[sql]` | SQLAlchemy, psycopg, PyMySQL | Your Python code calls `conn.read()`, `conn.exec()`, `conn.value()`, `conn.to_sql()`, `conn.engine` or `get_tables()`. Pipe operations do **not** need it. |
    | `meerschaum[sql-mssql]`, `[sql-oracle]`, `[sql-duckdb]`, `[sql-cockroachdb]` | pyodbc; oracledb; duckdb + duckdb-engine; sqlalchemy-cockroachdb | Raw SQL from Python against those flavors |
    | `meerschaum[api]` | FastAPI, uvicorn, a2wsgi, python-multipart, PyJWT, websockets | Any installed plugin uses `@api_plugin`, `@web_page` or registers an MCP tool |
    | `meerschaum[dash]` | Dash, a2wsgi, dash-bootstrap-components, dash-extensions | You want the web console (`/dash`), or any installed plugin uses `@dash_plugin` or a Dash `@web_page`. Includes the `api` extra, dash-ace and pandas, so `pip install meerschaum[dash]` is a working console. The host builds the console's app with `dash_extensions.enrich.DashProxy` (`Trigger`, duplicate outputs, as in v4) when dash-extensions is importable |
    | `meerschaum[shell]` | ptpython | You use `mrsm python` |
    | `meerschaum[gui]` | pywebview | You use `mrsm start gui` |
    | `meerschaum[cli-tools]` | pgcli, mycli, litecli, gadwall | You use `mrsm sql` to open a database shell |
    | `meerschaum[full]` | `pandas`, `polars`, `geo`, `sql`, `api`, `dash`, `shell` | You want today's behavior |

    The Docker image installs `meerschaum[full]`; a new `bmeares/meerschaum:slim` tag installs only the base wheel.

    **Gone from every extra** (they served code that is now Rust): `dask`, `partd`, `joblib`, `psycopg2-binary`, `asyncpg`, `databases`, `aiosqlite`, `aiomysql`, `GeoAlchemy2`, `rich`, `prompt-toolkit`, `pygments`, `tabulate`, `humanfriendly`, `python-daemon`, `dill`, `psutil`, `watchfiles`, `gunicorn`, `httpx`, `valkey`, `python-jose`, `fastapi-login`, `aiofiles`, `python-dotenv`, `envyaml`, `terminado`, `tornado`, `dash-daq`, `Flask-Compress`, `modin`, `cmd2`, `ruamel.yaml`. If your plugin imports one of these, declare it in `required` (see [Dependencies and venvs](#dependencies-and-venvs)).

- **DuckDB uses an optional Python driver or a native library.** In Python, install `meerschaum[sql-duckdb]` and leave `MRSM_DUCKDB_LIB` unset so Pipe operations and SQLAlchemy share one driver. The standalone binary and base-only Python installs use DuckDB's own shared library, `libduckdb` (v1.5.6, MIT, published by the DuckDB project; Meerschaum does not redistribute it). When no cached or system library is available, the first native connection downloads the release asset for your platform (about 41 MB) into `MRSM_ROOT_DIR/drivers/duckdb/<version>/`, after checking it against a SHA-256 pinned in Meerschaum. Later native runs reuse it. `mrsm install driver duckdb` does the download on purpose (`--force` repeats it), `mrsm uninstall driver duckdb` removes it, and `mrsm show drivers` reports each driver's version, source (`env`, `cache`, `system` or `missing`) and path. **Behind a TLS-intercepting proxy** the download trusts the operating system's certificate store as well as its bundled roots, and honors `HTTPS_PROXY`; if it still cannot reach GitHub, run `mrsm install driver duckdb` on a machine that can and copy the library. **Air-gapped hosts:** copy a DuckDB library (v1.4 or newer) there and set `MRSM_DUCKDB_LIB` to its path, or put it on the system library path; `MRSM_NO_AUTO_INSTALL=1` forbids the implicit download and a missing library then fails with a message naming both fixes. In Docker, `--build-arg duckdb_driver=1` bakes it into the image's root. On Linux the library needs the C++ runtime (`libstdc++`), which both Docker images ship. Native Pipe sessions run in UTC (the official build bundles ICU, whose default is the host's zone), aware timestamp parameters keep their instant, and nested, interval and enum values read as the text DuckDB prints.
- **No Python at all** is an option: `cargo install meerschaum --locked` (Rust 1.95 or newer) or a release binary runs without an interpreter as long as no Python plugin, custom Python connector, API plugin or Dash plugin is used. Point `MRSM_PYTHON` at an interpreter when you add one.
- **Windows limits.** The Python API host is reached over loopback TCP with a per-start token on Windows (a Unix socket elsewhere), so `@api_plugin` routes, the Dash console and plugin pages work. Background jobs (`-d`) are still refused, so the console's Exec box and Manage menu (which run actions as jobs) report the refusal. Run those in Docker or WSL.
- **Removed:** `pyoxidizer` builds and the standalone "portable" archive. `pip install meerschaum` and the standalone binary (`cargo install meerschaum`, release archives) replace them.

## ⚙️ Configuration

Everything under `MRSM_ROOT_DIR/config/` is read unchanged. Things to know:

- `api.uvicorn.*` keys (such as `forwarded_allow_ips`) are honored with a deprecation notice and mapped to `api.server.*`. Rename them at your leisure.
- **YAML is parsed by Meerschaum's own codec.** It reads and writes what v4 (PyYAML) did, byte for byte on the files Meerschaum writes, and keeps dates, bytes and very large integers intact. The one deliberate difference: the full-`Loader` tags `!!python/object`, `!!python/apply`, `!!python/name`, `!!python/module` and friends are **refused** like `safe_load` does, so a config file or a pipe's `parameters` can never run code. `!!python/tuple` is still read. A lone surrogate escape (`"\ud800"`) loads as U+FFFD.
- **Config files are written atomically.** `edit config`, `write_config` and the actions that save configuration write a temporary file in the same directory and rename it over the target, keeping its mode, owner and group (a new file is `0600`). A failed write never deletes or truncates the existing file. On a single bind-mounted file (`/meerschaum/config/plugins.json` in Docker) or a writable file in a read-only directory, the write falls back to rewriting the file in place, as v4 did.
- **Default pipe configuration may appear on startup.** Most v5 commands load the pipe defaults and can create `config/pipes.json` when it is missing. This file contains configuration defaults; registered pipes remain in their instance database. `mrsm doctor` uses read-only configuration and does not create it.
- **New:** `api.permissions.rate_limits`, keyed by route group, so you no longer need HTTP middleware to throttle `/login` (see [API plugins](#api-plugins)):

    ```yaml
    api:
      permissions:
        rate_limits:
          login: {max: 10, window_seconds: 60}
    ```

- **Unknown `MRSM_<TYPE>_<LABEL>` variables are ignored.** v4 turned every such variable into a connector (`MRSM_DUCKDB_LIB` became a `duckdb:lib` connector). v5 reads a variable only when `<TYPE>` is a connector type that exists: `sql`, `api`, `valkey`, `plugin`, `systemd`, or a type one of your plugins registers with `@make_connector`. Everything else is skipped (a debug message names it).
- **Connections fail fast.** The first connection a command makes to a SQL, API or Valkey server reports a refused connection, an unresolvable host or rejected credentials immediately, and a host that does not answer fails after `connect_timeout` seconds (a connector attribute, or `MRSM_CONNECT_TIMEOUT` for every connector; 5 by default). A job that loses a connection it had established keeps v4's retry behavior. Serverless or auto-resuming databases (Azure SQL serverless, Neon, Aurora Serverless) can take 15 to 60 seconds to wake: raise `connect_timeout` for those connectors, and note that a job started before its database is up now exits at once instead of retrying for 30 s, so give it `restart: always` (compose) or a systemd unit with `Restart=`. See [`MRSM_CONNECT_TIMEOUT`](/reference/environment/#mrsm_connect_timeout).
- **New:** `system.engine.*` keys for the [Syncing Engine](#the-syncing-engine) (`workers`, `python_workers`, `limits.connectors`, `limits.instances`, `auto_rebuild`, `implicit_edges`, `inclusive_cap`, `flat_ledger`, `events`, `state_flush_ms`, `run_history_days`, `debounce_seconds`, `chunk_pipelining`, `default_duration_seconds`). All have defaults; you do not need to set any of them. The full table is in [Engine State](/reference/pipelines/state/#settings).

Run `mrsm show config` on v5 against a v4.1 root directory to see the resolved config.

## 🏃 Jobs and the CLI

Core actions behave the same and take the same flags. `mrsm --help` is diffed against the 4.1 parser in CI, argument for argument.

**Removed or renamed:**

| v4.1 | v5 |
|---|---|
| `--engine laps` | Gone. `--loop` is the only long-running mode and reproduces lap behavior (below). |
| `mrsm install plugin compose` | A no-op with a notice. Compose has been built in since 4.0. |
| A switch to "use the Python core" | Does not exist in 5.0. There is one core, and it is Rust. |

**Job executors (`systemd`, `api:<label>`) in 5.0.** Three behaviors differ from 4.1, all deliberate:

- **Job names are one path component.** `.`, `..`, the empty name and names containing `/`, `\` or NUL are rejected (4.1 only banned `,;'"$#=*&!` + backtick + `~`). A name is a directory under `jobs/`, so `DELETE /jobs/..` would otherwise address the whole root.
- **The environment of a job created over the API is the server's.** As in 4.1, `POST /jobs/{name}` replaces whatever `properties.env` the client sent with the server's environment. The one exception is the Compose project stamp, which is kept so that projects sharing one remote executor still own their jobs (4.1 loses it). A client cannot set other environment variables of a remote job.
- **Systemd units pin the Python interpreter.** The unit runs the `mrsm` binary and carries `Environment=MRSM_PYTHON=<absolute path>`: the `MRSM_PYTHON` (else the first `python3` on `PATH`) of the process which created the job. Plugins run under that interpreter however the unit is started later. `systemctl --user stop` of a job asks it to stop once; it stops at its next checkpoint, as with a single Ctrl+C.

**`GET /jobs` redacts credentials, and the console's jobs are private.** 4.1 returned every job's `daemon.properties.env` (the server's environment when the job was created: each `MRSM_SQL_*` / `MRSM_API_*` connector URI with its password, and any other secret the process held) to every caller with `jobs:read`. 5.0 masks the password of connector URIs and the value of credential-named variables (`*_PASSWORD`, `*_TOKEN`, `*_SECRET*`, ...), also inside JSON values such as `MRSM_CONFIG`, in `GET /jobs` and `GET /jobs/{name}`; the other variables are unchanged. Automatic workers created by Pipe management and Pipeline runs are visible to their creator and administrators only. rc6 records their kind and owner explicitly, including workers with custom names; older `execute-*` records retain the same visibility restriction. The Exec button uses the existing terminal and does not create an automatic worker.

**Jobs start staggered (new in 5.0.0rc2).** In 4.1 every job started the moment it was asked to, so `compose up`, `restart jobs` or a reboot bringing all your systemd units up at once started every worker, imported every plugin and ran every first lap on top of each other. In v5 each job passes a per-root startup gate first: a random delay of up to `jobs.startup.jitter_seconds` (default 30), a wait while the machine is busy (`jobs.startup.max_load` x CPUs, `jobs.startup.min_free_memory_percent`), and a slot (`jobs.startup.max_concurrent`, default `max(2, CPUs / 2)`) held until the job's first lap is done, the job goes idle (`jobs.startup.settle_seconds`, default 15) or `jobs.startup.slot_timeout_seconds` (default 300) pass. An explicit `mrsm start job x` skips the delay and the busy check (it still waits for a free slot), and `--no-stagger` skips the gate altogether. Bulk starts, `restart jobs` and unit restarts get all of it, so a job may begin a few seconds after you start it. Generated units also carry `RestartSec=5`. `-s` schedules take an optional fixed per-job offset: `-s 'every 5 minutes jitter 30 seconds'`, `--jitter 30s` or `jobs.schedule.jitter_seconds` (default 0).

**Ctrl-C, secret prompts and the shell (new in 5.0.0rc2).**

- **Ctrl-C cancels the running action and nothing else.** In the shell it prints `Interrupted.` and returns to the prompt, which keeps working (at an idle prompt Ctrl-C only clears the line; Ctrl-D or `exit` leave). `mrsm <action>` exits with status 130 and prints `Interrupted.`, with no panic or traceback. A program the shell started (`sh`, `python`) receives the terminal's Ctrl-C itself and the shell waits for it; a second Ctrl-C within two seconds kills a program that ignored the first (an interactive REPL is never killed). Actions that stop on a flag (`--loop`, `--listen`, followed logs) still finish the current step first. A graph run (`--parents`, `--children`) cancelled this way hands the leases of its running pipes back before it exits, so a rerun does not wait for them to expire.
- **Secret prompts echo `*`.** Every password prompt, and each secret connector attribute asked by `bootstrap connectors` (`password`, `api_key`, `secret`, `token`, `client_secret`, ... by name), shows one `*` per character; Backspace erases one. Nothing is echoed when stdin is not a terminal. v4 echoed nothing at all.
- **`mrsm python` needs the package.** It asks the interpreter it would start whether it can `import meerschaum` and, if not, fails with `The meerschaum Python package is not installed for <interpreter>; install it with pip install meerschaum==<version> to use mrsm python`. Nothing is installed and no plain REPL opens instead.
- **`bootstrap pipes` suggests keys.** The connector, metric and location questions show a dropdown which filters as you type (connectors from the configuration, `MRSM_<TYPE>_<LABEL>` variables and plugins; metrics and locations already registered on the instance). Unknown connector keys are accepted with a warning; v4 refused them.
- **`--version` and `--help` never start a Python worker.** Help for a plugin's action reads the plugin metadata the last command cached.

**`show` actions and `--nopretty` (new in 5.0.0rc2).**

- **`show data` truncates a long table (pretty output only).** v4 printed every row, column and cell. v5 prints a table the way a pandas frame is printed: more than `formatting:dataframe:max_rows` (default 60) rows show `min_rows` (10) of them, the first five and the last five around a `…` row; columns that do not fit the terminal are replaced by one `…` column in the middle; a cell longer than `max_colwidth` (50) characters ends in `…`; and the footer states the true size, `[200 rows x 4 columns (showing 10 rows)]`. Only the rows that are shown are read from the instance (the first and the last rows by the datetime axis plus a row count; a pipe without a datetime axis reads its first rows and the count), so `show data all` on a large table is cheap. `--all` prints everything as v4 did, and `--nopretty` never truncates. The keys live under `formatting:dataframe` in `config.yaml` (`max_columns` fixes the column count instead of fitting the terminal width); they have no default entry, so `show config` does not list them until you set one. The width is the terminal's (or `$COLUMNS`); output that goes to a pipe or a file keeps every column.
- **The per-pipe `show` views run concurrently.** `show columns` and `show data` join `show rowcounts`, `show sizes` and `show partitions`: the pipes are read in parallel (`--workers`, default 4, within the connector's pool) and printed in the order of the selection. Instances that cannot run reads in parallel (DuckDB, in-memory SQLite, plugin connectors) keep reading one pipe at a time. Actions that change data (`clear`, `drop`, `delete`, `verify --yes`) stay sequential.
- **`show sizes` counts a shared table once.** Pipes whose `target` is the same table (same database, schema and table) are marked `shared xN`, and the total counts that table once; v4 added its size once per pipe.
- **`--nopretty` exits `1` when the command fails.** v4 always exited `0` with `--nopretty` (it only set the exit code when it printed the result), so scripts had to parse the output. A failed command still prints nothing extra in this mode.
- **Colors match v4.** `show`, `sync`, tables and messages use the same styles, from the same `formatting` config. Output stays colored when it is piped, as in v4; `NO_COLOR` and `formatting:ansi: false` turn color off.

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
- **`explain` is side-effect free (5.0.0rc2).** v4 initialized the root directory, wrote `.compose-cache.pkl` and installed requirements before explaining. In v5 `explain` creates nothing, writes no cache, installs nothing and fetches no plugin; it reports what `up` would install. It prints v4's colors (instance panels, `pipe_repr` styles, JSON).
- **Missing references are findings, and `up` / `run` / `sync` name them (5.0.0rc2).** `explain` lists every connector, instance, plugin, `MRSM{...}` symlink and `{{ Pipe() }}` reference that does not resolve on this host and keeps going. `up`, `run` and `sync pipes` refuse to start on a connector that is not defined, with `connector 'mqtt:x' is not defined on this host (referenced by config.meerschaum.connectors.mqtt.x)` instead of v4's `TypeError` text. Every compose error exits non-zero.
- **`--file` and `-f <file>.yaml` work in any position (5.0.0rc2);** a bare `-f` still follows the logs. New flags: `--project`, `--standalone`, `--no-stagger`. `compose init -c/-m/-l/-t/-i` adds the matching registered pipes to the new file.
- **New: `include:` and `parent:` (5.0.0rc2).** One root compose file can include others (files, directories, globs); every command spans the tree, the root's values win, and jobs started from a root carry the label `MRSM__COMPOSE_ROOT`. See [Compose](compose.md#multiple-projects-include-and-parent). Additive: files without these keys behave as in v4.
- **An unreachable instance fails the subaction (except in `explain`).** As in v4, listing the registered pipes raises (`Exception`) when the instance cannot be reached. The message carries the driver's own error text, which differs from SQLAlchemy's.
- **Plugin-defined instance connectors** (a custom connector type from a project plugin) go through the Python worker like on any other native action. The parity tests cover `sql:` and `api:` instances.

### The Syncing Engine

v5 understands the pipe graph (`parents`, `children` and `{{ Pipe() }}` references) and adds:

```bash
mrsm sync pipes -c sql:main -m daily_rollup --parents     # ancestors first, in dependency order
mrsm sync pipes -c sql:main -m raw_events --children       # then everything downstream
mrsm sync pipes -t reports --parents --resume <run_id>    # re-run only what failed or never ran
```

The engine also drives `mrsm plan pipes` (a dry run), `mrsm show graph`, `mrsm show runs` / `show run <id>`, `pipe.sync(parents=, children=)`, `mrsm.Pipeline`, `--loop` / `--listen` with a selection that has edges, and the Dash console's `/dash/pipelines` page. Run history is stored on your instance (the `mrsm_engine_*` tables).

The full guide, with real output, is [Pipelines](/reference/pipelines/). A bare `sync pipes` without `--parents` or `--children` keeps v4 semantics: no capping of a child at its parents' watermark, no rebuilds, no ledger-driven backfills. Two behaviors are new **inside engine runs only** (`--parents`, `--children`, `mrsm.Pipeline`, and `--loop` with a selection that has edges):

- A child is never synced past the minimum of its parents' sync times.
- Changing a pipe's definition (its `sql`, `fetch` config, `dtypes` or `columns`) marks it for **rebuild** (clear, then full sync). Interactively you are asked; in a job the pipe is marked `rebuild_pending`, descendants keep running on the stale data, and a warning is logged on every run until someone confirms with `--yes` or sets `system.engine.auto_rebuild: true`. Rebuilds never block a job.

## 🐍 Python library users and scripts

`import meerschaum as mrsm` and the public API are unchanged. `mrsm.Pipe`, `mrsm.get_connector`, `mrsm.get_config`, `pipe.sync`, `pipe.get_data`, `pipe.get_sync_time`, `pipe.get_rowcount`, `pipe.verify`, `pipe.filter_existing`, `pipe.enforce_dtypes`, `pipe.parameters` (still a live dict: assign, then `pipe.edit()`), `pipe.columns`, `pipe.dtypes`, `pipe.upsert = True`, pickling, equality and hashing by keys.

Things to check:

- **Return types.** `get_data()` returns pandas by default and a `GeoDataFrame` with `.crs` set when any column is a `geometry[...]` dtype. `as_polars=True` returns Polars (GeoPolars when installed). `as_iterator`, `as_chunks` and `as_docs` keep their shapes. `pipe.sync()` returns a `SyncResult`, which is a `tuple` subclass equal to `(success, message)` with extra fields (`inserted`, `updated`, `upserted`, `duration`, ...). `success, msg = pipe.sync()` still unpacks.
- **`get_data(as_dask=True)` raises.** Dask is removed in 5.0: the call raises a `TypeError` naming this guide. Use `as_iterator=True` (chunks, lazily) or `as_polars=True`; `pipe.sync()` of a Dask DataFrame raises the same way (convert with `ddf.compute()`, or pass a generator of chunks).
- **Result shapes of the native `Pipe` methods.** `pipe.enforce_dtypes(<list, dict, tuple or text>)` returns the columns' values, order and datetime dtype as before, but its pandas backends for untyped columns are the native engine's (`int64`, `float64`, `string`) where pandas' own inference gave `Int64` or `object`. `pipe.filter_existing()` keeps the pandas dtypes of the merge, except that a string column whose stored rows come back Arrow-backed from a custom instance is `string` rather than `object`. `pipe.fetch()` from a `sql:` source returns a generator of chunks with parsed datetimes (SQLite used to return text); a plugin `fetch()` that returns a list or dictionary comes back as one DataFrame, and a caller's `chunk_hook` on a plugin source runs in your process for every chunk the plugin yields. `pipe.deduplicate(workers=N)` runs the chunks concurrently only on thread-safe connectors, as before.
- **Time zone objects.** On PostgreSQL, the datetimes in `get_data()` frames and `as_docs` carry `datetime.timezone.utc` where v4 (psycopg) had `ZoneInfo('Etc/UTC')`. The values compare equal, but `str(tz)`, `repr(tz)`, `tz.key` and `isinstance(tz, ZoneInfo)` differ.
- **`get_data()` / `enforce_dtypes()` dtypes if your v4 never had polars.** v4.1.4 chose its pandas dtypes by whether `polars` could be imported. Every v4 extra that brings pandas (`sql`, `api`, `polars`, `full`) also brings polars, and then declared columns came back Arrow-backed: `double[pyarrow]`, `large_string[pyarrow]`, and `timestamp[ns, tz=UTC][pyarrow]` for a declared `datetime64[ns, UTC]`. A bare `pip install meerschaum==4.1.4` (no extras) never installs polars and returned NumPy `float64`, pandas `string` and `datetime64[us, UTC]` instead. v5 always returns the first set, with or without polars; the values are the same. If you upgrade from a bare v4 install and your code needs the NumPy dtypes, cast after reading, for example `df = df.astype({'dt': 'datetime64[us, UTC]', 'x': 'float64', 's': 'string'})` (works on pandas 2 and 3). `df.convert_dtypes(dtype_backend='numpy_nullable')` is not a substitute: pandas 3 raises on Arrow-backed timestamps with a time zone.
- **New in 5.0: `mrsm.SyncResult`, and every `pipe.sync()` input runs natively.** `mrsm.SyncResult` is the public name of the tuple subclass `pipe.sync()` returns (`success`, `message`, `inserted`, `updated`, `upserted`, `deleted`, `changed_intervals`, `duration`, `begin`, `end`). `pipe.sync()` with no data (a `sql:`, `api:` or `valkey:` source, a plugin or a `@make_connector` connector), text, tuples, generators of chunks, `force`, `workers`, `blocking=False`, `debug=True`, plugin keyword arguments, temporary pipes and pipes with `reference`s or `{{ }}` templates all run in the Rust core. Dask DataFrames are not accepted any more (`TypeError`): call `.compute()` or pass a generator of chunks. A generator of chunks returned by a plugin's `fetch()` is synced one chunk at a time, whatever `workers` says. A tuple is read as rows, as pandas does (v4 raised `TypeError`). `changed_intervals` holds the grid cells (one chunk interval wide) of the rows handed to the instance: a superset of the rows that actually changed. `sql:` instances on `mssql`, `oracle`, `duckdb` and in-memory SQLite run through the Rust drivers too; the `oracle` driver needs the Oracle client library (`libclntsh`) installed, and an in-memory SQLite database (`sql:memory`) is one database file that the `Pipe` methods and `connector.read()` / `exec()` (SQLAlchemy) share. `pyarrow` Tables and RecordBatches sync natively; any other unsupported value raises `TypeError` naming the accepted types. Unconstrained PostgreSQL `NUMERIC` values keep the exact scale they were stored with.
- **`APIConnector` returns an `ApiResponse`.** `make_request()` and the verb helpers (`get`, `post`, `put`, `delete`, ...) return an `ApiResponse`, not a `requests.Response`, because `requests` is no longer a dependency. It offers the common surface only: `status_code`, `ok`, `text`, `content`, `headers` (just `Content-Type`), `url`, `json()`, `raise_for_status()` (raising an `OSError`, as `requests.HTTPError` is) and truthiness. Other `Response` members (`iter_content`, `cookies`, `elapsed`, ...) do not exist, and `make_request` honors `params`, `json`, `data`, one `files` entry and `timeout` only. A refused or reset connection raises the builtin `ConnectionError` (an `OSError`, as `requests.exceptions.ConnectionError` was) with the transport's message. `connector.session` creates a `requests.Session` on access if `requests` is installed, but the connector does not use it. `get_actions()` still returns a response whose `.json()` is the list of actions.
- **`ValkeyConnector.client`** is a real `valkey.Valkey` when that package is importable (Meerschaum never installs it), and otherwise a built-in client covering the commands Meerschaum itself uses (`get`, `set` with `ex`, `px` and `nx`, `delete`, `unlink`, `exists`, `incr`, `keys`, the sorted-set, set, list and hash basics, `flushdb`, `ping`, `execute_command`); any other command raises `NotImplementedError`. Install the `valkey` package in your plugin's venv if you need the rest. Values it returns are `bytes`, as with `valkey.Valkey`.
- **Raw SQL needs `meerschaum[sql]`.** `conn.read(sql, params=...)`, `conn.exec(...)`, `conn.value(select)`, `conn.to_sql(df, ...)`, `conn.engine` and `get_tables(conn)` stay SQLAlchemy-backed and keep accepting SQLAlchemy constructs. GeoPandas's `read_postgis(query, conn.engine)` keeps working. Without the extra these raise an `ImportError` naming the extra (`conn.engine` raises an `AttributeError` with the same message, so `hasattr(conn, 'engine')` is `False`). `conn.URI`, `conn.DATABASE_URL`, `conn.IS_THREAD_SAFE`, `mrsm.get_pipes()` and every pipe operation work without it.
- **DuckDB connections.** Python DuckDB connectors share the public `duckdb` driver between Rust Pipe operations and SQLAlchemy, so both interfaces observe the same writes. Install the `sql-duckdb` extra (`duckdb >= 1.5.6`, `duckdb-engine >= 0.17.0`) and leave `MRSM_DUCKDB_LIB` unset. An explicit override selects the native C API and refuses mixed SQLAlchemy access; unset it and restart to use both. Standalone / base-only installs continue to load the native shared library. Per-file connection settings must agree. Custom `creator`, `module`, and `do_connect` routing hooks are refused before connection access; standard `connect`, `checkout`, and transaction events remain available. Prefer spawn for child processes. Fork children must exec a fresh interpreter before using DuckDB; the public driver cannot safely close inherited connections or finalize inherited worker threads during normal child interpreter shutdown.
- **`get_config` is served by the Rust config registry.** `get_config`, `set_config`, `read_config`, `write_config`, `get_plugin_config`, `write_plugin_config`, `MRSM{}` symlinks, `MRSM_CONFIG` / `MRSM_PATCH` and `MRSM_<TYPE>_<LABEL>` connectors behave as in v4, and the dictionaries `get_config` returns are still live (mutate one and the next call sees it). `meerschaum.config.config` is that live dictionary; assigning a new dict to it (or calling `set_config(cf)`) replaces it. Config YAML values that JSON cannot hold (a `2024-01-01` date, bytes, very large integers) now load, substitute and write back unchanged instead of failing.
- **Constructing a `Pipe` never touches the instance.** Module-level `mrsm.Pipe(...)` definitions import without a database, as today. Attributes are fetched on first access.
- **Partial-row syncs** on non-upsert pipes (`pipe.sync([{'ts': ..., 'id': ..., 'status': 'done'}])`) update the matching row without nulling other columns, as today.
- **`SQLConnector` methods that are gone.** `get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype` and `create_pipe_table_from_df` built the DDL of the Python sync; the Rust core builds the same statements inside `pipe.sync()` and they have no public replacement (read the table with `conn.read()` or `pipe.get_columns_types()`). The pipe, user and plugin methods of `SQLConnector` must be called on a real connector: v4 let tests call the underlying functions with stand-in objects as `self`.
- **`oracle` needs the client library.** Without the Oracle client library (`libclntsh`, Oracle Instant Client) on the loader path, an `oracle` instance fails with a `RuntimeError` that names it; v4 used `python-oracledb`'s thin mode, which needs none.
- **Removed behaviors.** `system:experimental:join_fetch` (undocumented, off by default) is ignored with a warning. The config is read and written by the native YAML codec only: PyYAML and `ruamel.yaml` are never imported or installed.
- **Packages are never replaced behind your back.** Meerschaum no longer installs `semver` (versions are compared by the Rust core), and an installed package that is older than the minimum of its extra (a `pandas` below 3) is reported with a warning (`mrsm upgrade packages pandas`) instead of being upgraded into `venvs/mrsm`. A sync followed by `get_data()` on an installed environment attempts no installation. `meerschaum.utils.misc.interval_str` no longer needs `humanfriendly`.
- **Instances that cannot be built.** As in v4, a pipe whose instance connector does not exist (`instance='sql:nope'`) raises `Unable to parse connector keys 'sql:nope'` from every call that needs it (`get_data`, `get_rowcount`, `sync`, ...); no call answers `None` or `0`.
- **Removed internal modules.** Importing a module from the former Python core raises an `ImportError` whose message names the replacement. The commonly imported ones:

    | Import | v5 |
    |---|---|
    | `meerschaum.connectors.sql._pipes`, `_users`, `_plugins`, `_compress`, `_maintenance`, `_partition`, `_fetch` | Private. The public `SQLConnector` methods are thin delegations to the Rust core with the v4 signatures and docstrings; the query builders and helpers behind them (index, partition and compression helpers, temporary-table bookkeeping, `_instance.py`) are removed. |
    | `meerschaum.connectors.sql._sql`, `_create_engine`, `_uri`, `tables` | Unchanged (`read`, `exec`, `value`, `to_sql`, `.engine`, `get_tables`: SQLAlchemy, needs `meerschaum[sql]`). |
    | `meerschaum.connectors.sql._users.valid_username` | Kept at the same path as an SDK re-export. |
    | `meerschaum.connectors.sql.tables.get_tables` | Kept at the same path (SQLAlchemy `Table` objects, needs `meerschaum[sql]`). |
    | `meerschaum.core.Pipe._*` internals | Removed. |
    | `meerschaum.actions.<module>` function imports | `meerschaum.actions.actions['sync']` and friends keep their names and dispatch to Rust. |
    | `meerschaum.api.routes.*` | Removed. The routes are Rust. |
    | `meerschaum.api.dash.callbacks.*`, `meerschaum.api.dash.pages.*` | Kept (5.0.0rc2): the console is the Dash app of the Python package (`meerschaum[dash]`), ported onto the façade. |
    | `meerschaum.utils.daemon.Daemon` | Kept as a read and control façade over the Rust job supervisor (`status`, `pid`, `pause`, `quit`, `kill`, logs, properties, `get_daemons()`, ...). It no longer runs Python code: `Daemon(target=<your callable>)`, `run_daemon`, `daemon_action` and `daemon_entry` raise `NotImplementedError` naming `Job`, and the file-descriptor interception helpers are gone. Run work with `meerschaum.jobs.Job`, which keeps `start`, `stop`, `pause`, `get_logs`, `monitor_logs`. |
    | `meerschaum.utils.sql` | Kept at the same path, with every documented helper: `build_where` and `sql_item_name` delegate to the Rust core, the rest (`clean`, `dateadd_str`, `table_exists`, `get_sqlalchemy_table`, `get_table_cols_types`, `get_update_queries`, `get_create_table_queries`, `wrap_query_with_cte`, ...) keep their v4 implementations. |

- **Kept SDK names** (verified against real plugins, so implementers must keep them): `SuccessTuple`, `warn`, `error`, `info`, `dprint`, `pprint`, `print_tuple`, `print_options`, `attempt_import` (with `venv=` and `lazy=`), `import_pandas`, `pip_install`, `Venv`, `Plugin(name).module`, `from_plugin_import`, `import_plugins`, `make_connector`, `make_action`, `add_plugin_argument`, `pre_sync_hook`, `post_sync_hook`, `prompt`, `yes_no`, `wget`, `round_time`, `select_pipes`, `string_to_dict`, `items_str`, `filter_keywords`, `filter_arguments`, `parse_df_datetimes`, `query_df`, `none_if_null`, `get_current_timestamp`, `serialize_bytes`, `deserialize_bytes_string`, `serialize_geometry`, `replace_pipes_syntax`, `STOP_EVENT`, `ROOT_DIR_PATH` and `meerschaum.config.paths`, `get_plugin_config` / `write_plugin_config` (including the no-argument form), `User` and `Token` with `hash_password`, `verify_password`, `_password_hash`, `_attributes`, `_user_id`, `Token(user, label, scopes, expiration).register()` and `.get_api_key()`.

## 🔌 Plugin authors

**Data plugins (`fetch`, `sync`, `register`, `setup`), action plugins (`@make_action`, `add_plugin_argument`), hooks, library plugins, `required`, per-plugin venvs and `plugins_dir` lists are unchanged.** The plugin is still a Python module in `MRSM_PLUGINS_DIR`; `fetch` may still return a DataFrame, a GeoDataFrame, a list of dicts, a dict of lists, a JSON string, or a generator of any of those.

What is different underneath, and when it matters:

- **Plugins run in a worker process** when the host is the `mrsm` binary (CLI, jobs, server). The worker is a normal Python interpreter with the SDK loaded; `attempt_import(..., venv='myplugin')` and `Venv` activate venvs exactly as today. Module-level state in your plugin lives for the life of the worker, not of the command, which matches today's job behavior but not today's one-shot CLI behavior. Don't rely on module globals being fresh per action.
- **The working directory is not on `sys.path`.** Every Python process Meerschaum starts (workers, the API host, jobs, compose and pipeline steps, `systemd` units) runs with `-P`, so a `meerschaum.py` or a `meerschaum/` in your project directory can never replace the installed package. A plugin which imports a sibling module from the directory it is run in (not from the plugins directory) must move that module into the plugins directory or into a package; plugins are imported through the plugins directory only. v4's in-process jobs never had the working directory on the path either.
- **`@post_sync_hook` and `@pre_sync_hook`** receive the same kwargs (`success_tuple`, `sync_timestamp`, `sync_complete_timestamp`, `sync_duration`, ...) and run in the worker after each native sync.
- **Plugin manifests.** On install, setup and on any source change, Meerschaum records which decorators your plugin uses so that `--help`, tab completion and the server can start without importing Python. If your plugin registers actions or pages **conditionally** (for example, only in production), the manifest sees whatever your module registers at import time with the config present at install time. `mrsm reload` (also spelled `mrsm reload plugins`) rebuilds the manifest unconditionally and recycles the Python workers; the manifest is also rebuilt automatically when a plugin file changes, and after `install`, `upgrade`, `uninstall` and `setup` of plugins.
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

`@api_plugin` functions still receive a FastAPI `app` and can add routes, middleware, `StaticFiles` mounts and background threads. `meerschaum.api.endpoints`, `get_api_connector`, `get_cache_connector`, `production`, `debug`, `ScopedAuth`, `CurrentScopes`, `_oauth2.SECRET`, `_tokens.get_token_from_authorization`, `get_pipe` and the `models` stay. `api_plugin` returns your function unchanged, so tests that call `init_app(FastAPI())` against a bare app keep passing. `from meerschaum.api import manager` and `Depends(manager)` keep working too: `manager` is now a built-in JWT login manager with `fastapi_login.LoginManager`'s call signature (`user_loader()`, `optional`, `create_access_token()`, ...), so neither `fastapi-login` nor `python-jose` is installed (tokens are validated with PyJWT against the same secret file as the Rust server). `meerschaum.api.aiofiles`, `typing_extensions`, `importlib_metadata`, `packaging_version`, `multipart` and `uvicorn_workers` are no longer re-exported; import what you use from your plugin (declare it in `required`).

**Known difference:** your `@api_plugin` routes work, but they are not listed in the server's `/openapi.json`, `/docs` or `/redoc` (v4 listed them because one FastAPI app served everything); the Rust server documents only its own routes.

**What changed:** Meerschaum's own routes are served by the Rust server. Your routes are served by a Python **API host** that the Rust server starts when it sees a plugin needs it and reverse-proxies to. Every path the Rust server does not own, websockets included, reaches your app unchanged, so your public URLs do not move. Install `meerschaum[api]` wherever such plugins run (the Docker images already do), or leave `MRSM_NO_AUTO_INSTALL` unset and the host installs what it needs into the `mrsm` venv on its first start. With `MRSM_NO_AUTO_INSTALL=1` and the extra missing, the host exits with an `ImportError` naming `pip install "meerschaum[api]"`. The host runs inside the `mrsm` venv for its whole life, as v4's API server did.

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
      allowlist: [mcp, version]
```

The allowlist is a list of `fnmatch` patterns over the **core route groups**: `actions`, `connectors`, `index`, `jobs`, `misc`, `pipes`, `pipelines`, `plugins`, `tokens`, `users`, `version`, `engine`, `mcp` and `dash` (the console's own pages). The default is `['*']`. A core group that is not allowed is not registered, and its paths are not forwarded to the Python host.

Starting with rc6, `api.permissions.routes.disabled` explicitly disables matching route groups and overrides the allowlist. Native `/login` is enabled by default, independently of the allowlist; disable it explicitly with:

```yaml
api:
  permissions:
    routes:
      disabled: [login]
```

The disabled login route cannot reappear through host fallback. A namespaced plugin login such as `/acme/login` remains a separate route. Registering a FastAPI handler for `/login` does not replace the native route; exact core-route replacement is not supported. Custom Dash login layouts registered with `web_page('login', ...)` remain supported and are independent of login API routing. Verify your external endpoint surface after migrating Python route-stripping hooks.

### Background threads and startup hooks

`@app.on_event('startup')` and threads started inside your `@api_plugin` run in the host process, which starts once per server start and is restarted on crash. Make startup idempotent.

### Sessions from API routes

`meerschaum.api.dash.sessions` (`set_session`, `get_user_from_session`, `get_username_from_session`, `delete_session`, `is_session_active`, `session_is_admin`, `set_session_cookie`, `EXPIRES_SECONDS`) reads and writes the same session store as the console, keyed by the same `mrsm-session-id` cookie. Plugins that implement their own registration or login flows and mint console sessions keep working.

## 🎛️ Web pages and Dash plugins

The web console is **the Plotly Dash app of v4.1**, ported onto the v5 façade and served by the Python API host at the same `/dash` paths (`/dash`, `/dash/pipes`, `/dash/jobs`, `/dash/login`, ...). It needs `pip install meerschaum[dash]` (the `full` Docker image has it). The standalone binary (`cargo install meerschaum`, the `minimal` image) has no Python in it: it serves the API, `/docs`, `/engine/*`, MCP and the web terminal, and `/dash` shows a pointer page. The console adds the [`pipelines` page](/reference/pipelines/visualization/#in-the-web-console) and Parents and Children tabs on every pipe card, and the `api.permissions.routes.allowlist` route groups hide console pages as in v4. There are two ways for a plugin to add pages.

!!! note "Release candidates"
    5.0.0rc1 rendered the console from Rust and mounted one Dash app per plugin at `/dash/<plugin>/`. 5.0.0rc2 reversed both: the console is Dash again (v4's), and there is again **one** Dash app. The rc1 `/dash/<plugin>/<path>` URLs redirect to `/dash/<path>`. If you adapted a plugin to rc1 (`dash_app.get_relative_path`, `strip_relative_path`), the v4 code works again.

### Option A: keep your Dash pages

Nothing changes from v4.1. `@dash_plugin` receives **the console's own** `dash_app`, and `@web_page(...)` functions returning Dash layouts register pages in that same app at `/dash/<path>`. Core component IDs (`session-store`, `mrsm-location`, `mrsm-theme-*`) exist in the app, `index_string` and `external_stylesheets` apply to the whole console as before, and callbacks can target core pages by ID. `meerschaum.api.dash.components.build_cards_grid`, `keys`, `pipes`, `graphs` and `meerschaum.api.dash.sessions` stay available (`api.dash.callbacks` and `pages` are back as modules too). A plugin's `assets/` folder is served at `/dash/_plugin-assets/<plugin>/` and its CSS and JavaScript files are loaded by the console.

Checklist:

1. **Install `meerschaum[dash]`** wherever the server runs (and `[api]`, which it includes). Without Dash importable the host logs one warning and `/dash` shows the pointer page; plugin Dash pages are not served.
2. **Packages you declared stay yours.** `meerschaum[dash]` installs `dash`, `dash-bootstrap-components`, `dash-ace`, `dash-extensions` and `a2wsgi`. It does not install `dash-daq`, `Flask-Compress`, `terminado` or `tornado` any more (the terminal is the Rust server's): declare `dash-daq` in your plugin's `required` if you use it.
3. **`session-store` and logins work as in v4.** Callbacks that wrote `session-store` to log a user in should still call `meerschaum.api.dash.sessions.set_session`, which the console and the Rust web terminal both read.
4. **Pages for the Rust terminal and Execute.** The dashboard's Exec box, a pipe card's Manage menu and "Follow logs" run through the server (`POST /execute`, then `GET /logs/<job>/stream`) instead of v4's websocket. A plugin that reused the v4 websocket callbacks must follow the new route. The console never runs an action itself: with no server to call, or when the server refuses, it shows the server's message. `POST /execute` lets administrators run what `/actions` allowed; any other user (when `api:permissions:actions:non_admin` is true, the default; `start api --secure` turns it off) runs only what the console offers, with a `403` naming what is refused (`GET /execute/policy` lists it).
5. **Windows.** The host answers over loopback TCP with a per-start token, so the console and plugin pages load. Exec needs background jobs, which Windows still refuses.

### Option B: the page contract (recommended for new pages)

A v5 plugin page is a function that returns an **HTML fragment**. The console frames it (an iframe of the page with the console's navbar, theme and session) and serves it at `/dash/<path>`, the same for Python and Rust plugins. Charts are **Plotly figure JSON**: build the figure with `plotly.py` exactly as before, then hand the JSON to the page, which renders it with Plotly.js (the copy that ships with the `plotly` package). Interactivity is [HTMX](https://htmx.org): an element requests a fragment from one of your routes and swaps it in.

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
- **Maps:** `<div data-mrsm-map='...'>` renders GeoJSON or a layer spec with MapLibre GL JS, loaded on demand. This is the replacement for dash-leaflet.
  Maps need a browser with WebGL2 (MapLibre GL JS 6 dropped WebGL1, as did Plotly.js 4's map traces).
- **Plotly.js 4:** the console draws figures with Plotly.js 4.1, so figures written for Plotly.js 2 or 3 are adjusted before they are drawn: a string `title` becomes `title.text` (and `titlefont` becomes `title.font`), and the removed `scattermapbox`, `choroplethmapbox` and `densitymapbox` traces (and `layout.mapbox`) are renamed to `scattermap`, `choroplethmap` and `densitymap` (and `layout.map`). Use the new names in new code; `plotly.py` figures already emit `title.text`. Plotly.js 4 also changed a few defaults you may notice (`layout.geo.fitbounds` is `'locations'`, `hsv()` colors are no longer accepted), and it turns on a "Share chart" button that uploads the figure to a Plotly cloud service: the console switches it off (`showSendToCloud: false`) unless a figure's own `config` sets it.
- **Tables:** return HTML tables; the console adds sorting. Editable grids are your concern.
- **Login and session.** `login_required=True` (the default) is enforced by the console before your function runs: a request without an active `mrsm-session-id` session gets a `401`. Your function is called with **no arguments**, so read per-request values (the query string, the user) from an `@api_plugin` route that your HTMX elements call, as in the example above. An exception in the function answers `500` and prints the traceback in the host's output.
- **Theme.** The `mrsm-theme-*` CSS classes keep their names. Style against them and your fragment matches the console in both themes.
- **No CDN.** Plotly.js, MapLibre GL JS and HTMX are served by Meerschaum itself. Don't add `<script src="https://cdn...">` tags; they will be blocked by the console's content security policy and would not work on offline deployments anyway.

A function that returns a string (or a `(html, status)` pair) is a page-contract page; one that returns a Dash layout is a Dash page. The manifest records which, by calling the function once at install time. A page written this way moves to Rust without touching its chart, because the Rust `plotly` crate emits the same figure JSON (see [Rust plugins](#rust-plugins)).

## 🦀 Rust plugins

**v5 is still a Python package** (`pip install meerschaum`). Most users keep writing Python plugins, and they work unchanged. Rust plugins are an option for the native performance path (the core, kernel and engine) and for a lean image: `slim` is the base wheel on a minimal Python, and a Rust plugin needs none of the extras.

New in v5. A Rust plugin is a `cdylib` built with the `meerschaum-plugin` crate, installed into `MRSM_PLUGINS_DIR` as `<name>/plugin.toml` plus the shared library, or as a Cargo crate (`plugin.toml`, `Cargo.toml`, `src/`) that Meerschaum builds itself into `MRSM_ROOT_DIR/cache/native_build/` when `cargo` is on `PATH` (the prebuilt library is the only form that works on the `slim` image, which has no toolchain). It can implement `fetch`, `sync`, `register`, `setup`, actions, hooks, a connector, HTTP `routes` and console `pages`. Data crosses the boundary as Arrow record batches. (Continuous `subscribe` streams from a native plugin are not available yet: write a streaming connector in Python for `--listen`.)

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

!!! danger "⚠️ Open registration plus terminals for non-admins is a shell for anyone"

    With the defaults, anyone who can reach the API can register an account (`api:permissions:registration:users`) and, as a logged-in user, open the web terminal (`api:permissions:actions:non_admin`): **a shell on the server for anyone who can reach the port**. Meerschaum 4 behaves the same, and v5 keeps it. Before exposing an API, start it with `--secure` (terminals and actions for administrators only) or `--private` (authentication on every endpoint), or set `api:permissions:registration:users: false` (no self-registration) or `api:permissions:actions:non_admin: false` (administrators only). `mrsm start api` prints a warning at startup when it binds a non-loopback address with both settings on; it cannot see a reverse proxy, so a loopback bind behind a proxy exposes the same terminal **without** the warning. (`--no-auth` on a non-loopback address also prints a warning: every route, `/actions` included, is then open to anyone who can connect.)

- **Docker.** `bmeares/meerschaum` and `bmeares/meerschaum:api` keep their entrypoints, `/meerschaum/plugins`, `/meerschaum/config` and `/meerschaum/config/plugins.json`. `docker cp` of plugins into the container keeps working. The default image ships `meerschaum[full]`, so API and Dash plugins work without further installs. `bmeares/meerschaum:slim` ships only the base wheel and is the right base for Rust plugins or plugins that return dicts. As in v4, the images ship `curl` (the stack's API healthcheck runs `curl --fail http://localhost:<port>/healthcheck`), and the full image ships `git` and `ssh` (plugins may require `git+https://...` packages); the slim image has `curl` but no `git`.
- **Stack API image.** The default `api` service runs `bmeares/meerschaum:<version>-full` (for example `bmeares/meerschaum:5.0.0rc2-full`: the `full` image of the same release as the `mrsm` that writes the compose file) instead of the floating `bmeares/meerschaum:api` tag, which stays the previous major version until 5.0.0 final and is never moved by a release candidate. A root without `config/stack.yaml` follows the version on its own: the next `mrsm stack up -d` after an upgrade recreates only the `api` container (volumes and database are kept). A `stack.yaml` with a `docker-compose.yaml` section keeps its own `api` image: edit `services:api:image` with `mrsm edit config stack`. See [the stack](/reference/stack/).
- **Platforms.** Wheels are published for Linux x86_64 and arm64 (glibc `manylinux_2_28` and musl `musllinux_1_2`, so Alpine works), macOS arm64 and x86_64, and Windows x86_64. The standalone binaries are attached for Linux x86_64 and arm64, macOS arm64 and x86_64 and Windows x86_64; the macOS ones are not signed (`xattr -d com.apple.quarantine <file>` after a browser download, see [Setup](/get-started/)). The Docker images are multi-architecture (`linux/amd64`, `linux/arm64`).
- **Stack database setup.** The default `db` service mounts `<root>/db/initdb/create-extensions.sql` as a single file, `/docker-entrypoint-initdb.d/100_meerschaum.sql`, instead of mounting the `<root>/db/initdb` directory over the image's whole setup directory, so the image's own setup runs again on a new volume (`TIMESCALEDB_TELEMETRY: 'off'` takes effect, `timescaledb-tune` tunes the server, `template1` gets TimescaleDB). The script v5 writes creates each extension only when the image ships it (another PostgreSQL image no longer aborts the first start) and preloads `pg_stat_statements`. The healthcheck is `pg_isready -h 127.0.0.1 -p 5432 ...` with a 300 s `start_period`, so `db` is healthy only once the setup is done (a passing check turns it healthy at once; the long start period keeps a slow first setup, e.g. on a Raspberry Pi, from aborting `stack up`), and Grafana's datasource says PostgreSQL 18 (`postgresVersion: 1800`). See [The Database Setup](/reference/stack/#the-database-setup).
- **Existing stacks.** `mrsm stack` regenerates the compose file from the `stack` config on every run. **Without** `<root>/config/stack.yaml` (v4's `mrsm stack` never writes one; it exists only if you created it, e.g. with `mrsm edit config stack`), your stack takes the new defaults: the next `mrsm stack up -d` recreates the `db` container once (a short restart; the data volume is kept and nothing in the database changes, since Docker runs the setup only on an empty volume). Any extra scripts you put in `<root>/db/initdb/` are no longer mounted, so they would not run for a future fresh volume: move them into `create-extensions.sql`, or add your own mount in `stack.yaml`. **With** a `stack.yaml`, its `docker-compose.yaml` section is used as it is (v5's defaults fill in only a key that is missing entirely, as in v4), so its mount, healthcheck and image stay and the container is not recreated. To take the new compose defaults, edit the `db` service with `mrsm edit config stack` (the mount, the `healthcheck` test and `start_period`), or delete the `docker-compose.yaml` key from `stack.yaml` (your other `stack` keys stay) and run `mrsm stack` again. An existing `create-extensions.sql` is never rewritten: before creating a NEW database volume, delete it (or merge your changes into v5's) so that `mrsm stack` writes v5's script. Update an existing database's extensions and settings with `mrsm upgrade extensions` ([Upgrading the stack](/reference/stack-upgrades/)).
- **Bare metal.** Replace `pip install --user meerschaum==4.1.4` with `pip install --user 'meerschaum[full]>=5,<6'` (or the extras you need). The `mrsm` on your `PATH` is now a console script that runs the native CLI inside the `meerschaum` wheel (there is no separate binary package). Jobs and `systemd` units run `python -m meerschaum`, so they work from the same install.
- **NGINX / reverse proxies.** Paths do not change. The server honors `api.server.forwarded_allow_ips` (old `api.uvicorn.*` key still read). The API host listens on a Unix socket under `MRSM_ROOT_DIR`; nothing else needs to reach it.
- **`start api -p 8224 --host 127.0.0.1 -i sql:main --secure`** and friends are unchanged.
- **Webterm** is served by the Rust API server itself (no Tornado/terminado process, no separate `start webterm`): `/webterm/<session id>` and its websocket keep their paths and the terminado message format, the Dash console embeds the terminal at `/dash/webterm` and on the dashboard in an iframe (xterm.js 6.0.0 with the fit and unicode11 add-ons, served from `/webterm/assets/`, plus the mobile extra-key row), and the shell is `mrsm` on the server's instance. It keeps its tmux session model (`api.webterm.tmux.enabled`, `api.webterm.tmux.session_suffix`): each user's terminal lives in the tmux session `<user><session_suffix>`, so a reload or a second tab reattaches to it, and closing a tab never kills the session. Without `tmux`, each tab gets its own shell. `--no-webterm` and `--no-auth` still turn it off. The terminal websocket refuses cross-origin requests: a reverse proxy in front of the API must pass the original `Host` (or set `X-Forwarded-Host`) or the terminal answers 403. ⚠️ The terminal is a shell on the server, and its gate is unchanged from v4: any logged-in user may open one unless `api.permissions.actions.non_admin` is false (or the server runs with `--secure`), in which case only administrators may. Set it to false on a server whose users should not have a shell. A second login no longer fails, and the shell echoes.
- **`start gui`** opens a `pywebview` window (the `meerschaum[gui]` extra) around the console at `/dash/` instead of around the Tornado terminal: `mrsm` starts a local `start api --no-auth` for it (bound to `127.0.0.1` only, on a free port) and stops it when the window closes. ⚠️ While the window is open that console is **unauthenticated**: any process on the same machine can reach it on that loopback port, so do not use `start gui` on a shared machine. `api start` is an alias of `start api`. `login`, `os`, `sql`, `stack`, `bootstrap`, the connector, token and cache actions and `edit config --rollback` keep their prompts, messages and exit codes and no longer need a Python interpreter.
- **Plugin, package and venv actions run in `mrsm`.** `uninstall plugins|packages`, `delete plugins|venvs`, `edit plugins`, `show packages`, `verify packages|plugins|venvs`, `bootstrap plugins`, `upgrade meerschaum|packages|plugins` and `register plugins` keep their prompts, messages and exit codes. They are orchestrated by `mrsm` itself: `show packages`, `delete plugins|venvs`, `uninstall plugins`, `register plugins`, `bootstrap plugins` and `edit plugins` need no Python interpreter, `verify plugins` starts the Python worker to import each Python plugin, and the actions that install or remove packages (`install|uninstall|upgrade packages`, `upgrade meerschaum|plugins`, `verify packages`) run `uv` or `pip` as child processes against the interpreter in `MRSM_PYTHON` (else `python3`). `verify packages` no longer imports every package to read its version: it reads the versions from the installed `*.dist-info` in the `mrsm` environment and repairs an old one, and a package found in the host interpreter is counted but never changed. `edit plugins`, `bootstrap plugins`, `edit definition` and `edit config` open `$VISUAL`, else `$EDITOR`, else `vi` (v4 installed `pyvim` when none was set), and say so when the editor cannot be started. `upgrade meerschaum` upgrades the environment that `mrsm` itself was installed into (or `MRSM_PYTHON`), never a guessed `python3`, and adds no `--break-system-packages` unless you pass it. Plugin names may not contain path separators or `..`, start with `.`, or be `mrsm`, and `delete venvs` takes plain directory names only. Installing a plugin archive refuses symlink and hard-link members and writes only under the plugin's directory. `mrsm start worker` and `start|restart daemons` are not used in v5: actions run natively, so there is no CLI daemon to feed (`show daemons` answers that none are running). `MRSM_NO_AUTO_INSTALL=1` disables implicit installs only; `upgrade`, `install` and `uninstall` always run.
- **The Python package must fit the program.** The Python worker and the API host check the `meerschaum` package they would import before importing it, so a stale copy cannot run its start-up (v4 installs packages whatever `MRSM_NO_AUTO_INSTALL` says). A package of another **major** version, one that looks like v4 (no `worker` module), or one speaking another worker protocol is refused with one message (nothing is imported or installed; the message names the copy and the fix, and `mrsm doctor` shows it); a different release of the same major version starts and warns once, naming both versions. This matters when `mrsm` (`cargo install meerschaum`, a release binary, the Docker image) and the Python package come from different installs: keep them on the same release, or point `MRSM_PYTHON` at an interpreter that has the matching one. Since 5.0.0rc2 this is checked once per interpreter before a worker starts (a missing `meerschaum`, a stale one or an interpreter that cannot start fails in well under a second with the message above, not after a 60 s handshake), and a worker that never connects is stopped after 60 s (`MRSM_WORKER_STARTUP_TIMEOUT`; a timeout alone is remembered for 10 s, never longer).
- **Logs.** The Rust server's logs and the API host's output are interleaved in the same stream (the API job's logs under `mrsm start api -d`). There is no separate host log and no filter in 5.0; the host's own lines (`The API host exited ...; restarting`, `The plugin manifest changed; restarting the API host.`) come from the server. The host is started only when an installed plugin needs it and restarted with backoff if it exits.

## ✨ New in 5.0

- **The Syncing Engine and [Pipelines](/reference/pipelines/):** `mrsm sync pipes --parents [N] --children [N] --resume <run_id>`, `mrsm plan pipes` (dry run), `mrsm show graph`, `mrsm show runs` / `show run <id>`, `pipe.sync(parents=, children=)`, `mrsm.Pipeline`, the dirty-range ledger and run history on your instance, and the `/dash/pipelines` console page. Pipe parameters `schedule`, `stream`, `retry` and `min_seconds` ([reference](/reference/pipes/parameters/#schedule)).
- **Continuous mode:** `--loop` (v4's laps, plus engine runs when the selection has edges) and `--listen` with `IS_STREAMING` connectors, micro-batching and acknowledge-after-commit ([Continuous Mode](/reference/pipelines/continuous/)).
- **`mrsm.SyncResult`:** the tuple subclass `pipe.sync()` returns, with `inserted`, `updated`, `upserted`, `deleted`, `changed_intervals`, `duration`, `begin` and `end`.
- **Rust plugins**, prebuilt or as source crates, replacing a Python plugin of the same name ([Rust plugins](#rust-plugins)), and the **page contract** for console pages (`meerschaum.plugins.figure()`, `data-mrsm-figure`, `data-mrsm-map`).
- **Engine settings** under `system.engine.*` (`workers`, `python_workers`, `limits.connectors`, `limits.instances`, `auto_rebuild`, `implicit_edges`, `inclusive_cap`, `flat_ledger`, `events`, `persist_runs`, `state_flush_ms`, `run_history_days`, `debounce_seconds`, `chunk_pipelining`, `default_duration_seconds`; [Engine State](/reference/pipelines/state/#settings)) and `api.permissions.rate_limits` and `api.permissions.log_streams` (`max`, `per_user`).
- **Packaging:** one dependency-free `meerschaum` wheel with extras (its `mrsm` console script runs the native CLI), `cargo install meerschaum` for a standalone binary, and the `bmeares/meerschaum:slim` image.
- **Venv accounting:** `mrsm show venvs`, `mrsm prune venvs`, and attribution of every package installed by `attempt_import`.
- **Leases:** two jobs never sync the same pipe at once (`mrsm_pipe_leases`), with optional `acquire_pipe_lease` / `release_pipe_lease` on custom instance connectors.
- **`mrsm upgrade extensions`:** updates a PostgreSQL instance's extensions (TimescaleDB, PostGIS, the Toolkit, ...) to the versions its server ships after an image upgrade, and on the stack's database applies `TIMESCALEDB_TELEMETRY: 'off'` and offers to preload `pg_stat_statements`; `mrsm stack up`, `mrsm start api` and `mrsm upgrade mrsm` point to it when an extension lags ([Upgrading the stack](/reference/stack-upgrades/)).
- **`mrsm upgrade postgres`:** moves the stack's database to another PostgreSQL major (e.g. a stack whose `stack.yaml` pins `timescale/timescaledb-ha:pg17` to `pg18`): a full backup, then a restore into a new volume that is verified table by table before the stack switches to it; the old volume, the backup and a byte-for-byte copy of `stack.yaml` are kept, and `mrsm upgrade postgres rollback` goes back ([Upgrading the stack](/reference/stack-upgrades/#upgrading-postgresql)).
- **PostgreSQL startup safety:** `mrsm stack up` refuses a known image whose PostgreSQL major differs from an existing named volume's `PG_VERSION`. Pin the old image, start the old database, then run `mrsm upgrade postgres <major>`. `stack up --force` bypasses this check; interrupted upgrades and restores offer recovery before another run ([Upgrading the stack](/reference/stack-upgrades/#upgrading-postgresql)).
- **`mrsm backup postgres` / `mrsm restore postgres`:** an online, checksummed backup of the stack's database (nothing is stopped; `mrsm backup postgres -s 'daily' -d` schedules it, `system:backups:postgres:keep` keeps the newest N), and its restore into a new volume, verified against the backup's manifest before the stack switches to it (`mrsm restore postgres rollback` goes back) ([Backing up and restoring](/reference/stack-upgrades/#backing-up-and-restoring)).
- **Shell completions:** `mrsm install completions [bash|zsh|fish|powershell]` writes a completion script for `mrsm` and `meerschaum` (actions, subactions, flags, connector keys, jobs, plugins, files); `mrsm show completions <shell>` prints it and `mrsm uninstall completions` removes it ([Shell Completions](/reference/actions/#shell-completions)).
- **`mrsm doctor`:** a read-only health check that says what is wrong with an installation and how to fix it: the version and worker interpreter, a stale `meerschaum` copy shadowing the right one, writable directories and free space (a full `/tmp` included), the config files, the instance and its tables, DuckDB and the Oracle client, the Python extras your plugins need, stale plugin virtual environments, failed jobs, the Docker stack and the PostgreSQL extensions. Every WARN or FAIL names its fix, `--nopretty` prints one JSON document for a bug report, and the result fails only when a check FAILs ([Troubleshooting](/reference/troubleshooting/#start-here-mrsm-doctor)).

## 🧪 How to upgrade safely

1. **Try v5 in a copy.** `MRSM_ROOT_DIR=/tmp/v5-root mrsm ...` against the same instance. v5 writes no new files into an instance until an engine run happens, and engine state lives in internal `mrsm_engine_*` tables under `_mrsm_internal` (on PostgreSQL and MSSQL; on MySQL, MariaDB, SQLite, DuckDB and Oracle beside your instance tables).
2. **Lint your compose files:** `mrsm compose explain` now warns about unresolved `{{ Pipe() }}` references, `dtype:` typos (for `dtypes:`) and string-typed `tags`. It never changes behavior.
3. **Dry-run the engine:** `mrsm plan pipes -t <tag>` prints what an engine run would do without moving data.
4. **Import every plugin:** `mrsm show plugins` on v5 imports each plugin and reports `ImportError`s from removed internals, with the replacement named in the message.
5. **Diff `mrsm --help`** against 4.1 if you script around the CLI. Any difference is a bug; please report it.
6. **Start the API and open the console.** If you have `@api_plugin` or `@dash_plugin` plugins, confirm each plugin route answers (the host starts on demand; its restarts are logged by the server) and walk through each plugin page. rc1's per-plugin `/dash/<plugin>/` URLs redirect to the v4 paths.
7. **Keep 4.1 around** in a separate venv for a release cycle. Both versions read the same instance and the same root directory layout, so switching back is a `PATH` change.

## 🗑️ Removed in 5.0, at a glance

| Removed | Replacement |
|---|---|
| The Python core (`connectors/sql/_*.py`, `core/Pipe/_*.py`, `actions/*`, `api/routes/*`, `api/dash/callbacks/*`, `utils/daemon` (the `Daemon` façade stays)) | The Rust core; the façade keeps the public API |
| `--engine laps` | `--loop` |
| `pyoxidizer` builds, the portable archive | the `meerschaum` wheel, release binaries, `cargo install meerschaum` |
| Plugin middleware over core routes | `api.permissions.rate_limits`, `api.permissions.routes.allowlist`, a reverse proxy |
| Python 3.10 | 3.11+ |
| `meerschaum.utils.packages.dash` (the lazily imported `dash` module) | Declare `dash` in your plugin's `required` and call `attempt_import('dash')` |
| `SQLConnector.get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype`, `create_pipe_table_from_df` | None: the Rust core builds the DDL during `pipe.sync()` |
| `meerschaum.utils.daemon`: `Daemon(target=...)`, `run_daemon`, `daemon_action`, `daemon_entry`, `FileDescriptorInterceptor`, and the lock, pickle and file-descriptor members of `Daemon`, `RotatingFile` and `StdinFile`; `SystemdExecutor.get_job_process` | `meerschaum.jobs.Job` (jobs run in the Rust supervisor) |
| `meerschaum.actions.<module>` (all 37 action modules; the functions live on in `meerschaum.actions.actions`), `meerschaum.api.routes.*`, `api._events` / `_websockets` / `_chunks`, `meerschaum.api.dash.{websockets,sync}` (the console's job and webterm callbacks use the server's `/execute`, `/logs/<job>/stream` and the Rust terminal), `meerschaum._internal.term`, `_internal.cli`, `_internal.shell.{ShellCompleter,ValidAutoSuggest,updates}`, `_internal.arguments._parser.{parse_help,parse_version}` | `meerschaum.actions.actions[<name>]`, `meerschaum.entry([...])` and the `mrsm` binary (`MRSM_BIN` overrides it); `get_completer()` returns `None` for the standard actions and `entry()` ignores `_use_cli_daemon` / `_session_id` |
| `python-oracledb` thin mode for `oracle` instances | The Oracle client library (`libclntsh`) |
| `system:experimental:join_fetch` | Ignored with a warning |
| PyYAML / `ruamel.yaml` for the config | The native YAML codec (no package) |
| `api.uvicorn.*` | `api.server.*` (old keys read with a notice) |
| Meerschaum-installed `dask`, `rich`, `dash-ace` and the other packages listed under [Installing](#installing-and-running) | Declare them in your plugin's `required` |
