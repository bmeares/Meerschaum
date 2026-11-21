# ⬆️ Migrating to v5

Meerschaum v5 runs its core, CLI, jobs, Compose, HTTP API, and MCP server in Rust. Your existing tables, YAML configuration, Pipe definitions, and Python plugins stay compatible—check the deliberate changes below before upgrading.

## Upgrade checklist

1. Back up your configuration and database; keep your v4.1 venv for rollback.
2. Confirm **Python 3.11+** (only needed for Python plugins, the Python API, or extras).
3. Install the extras you use (`meerschaum[polars]`, `[pandas]`, `[dash]`, `[api]`, etc.).
4. Test a sync, downstream pipe, and scheduled job on a scratch `MRSM_ROOT_DIR` against a copy of your database.
5. Review [teams and permissions](#teams-access-and-permissions) if you run a shared API server.
6. Upgrade, run `mrsm doctor`, and check `mrsm show jobs` (re-enter any blank legacy command with `mrsm edit jobs NAME`).

```bash
python -m pip install --upgrade meerschaum
mrsm doctor
```

See the [v5 changelog](../changelog/v5.md) for new features and [Stack upgrades](stack/upgrades.md) for PostgreSQL maintenance.

## Upgrading an installation

**Python 3.11+ and a zero-dependency wheel.** Python 3.10, `setup.py`, and `pyoxidizer` portable archives are removed. `pip install meerschaum` ships a dependency-free maturin wheel containing the Rust core and Python SDK (`mrsm` and `python -m meerschaum` run the native CLI). For a standalone binary without Python, use `cargo install meerschaum --locked` (Rust 1.95+) or GitHub Releases.

| Extra | Installs | Needed when |
|---|---|---|
| `[polars]` | `polars` | New-root default (`dataframe: polars`) or `as_polars=True`. |
| `[pandas]` | `pandas`, `pyarrow` | Upgraded-root `dataframe: pandas` or `as_pandas=True`. |
| `[geo]` | `shapely`, `pyproj`, `geopandas`, `geoarrow-types` | Python geometry objects or GeoDataFrames. |
| `[sql]` | `SQLAlchemy`, `psycopg`, `PyMySQL` | Explicit `conn.engine` or SQLAlchemy sessions (native SQL needs none). |
| `[sql-mssql]`, `[sql-oracle]`, `[sql-duckdb]`, `[sql-cockroachdb]` | Flavor drivers | Explicit SQLAlchemy access (`[sql-duckdb]` also enables shared Python DuckDB). |
| `[api]` | `fastapi`, `uvicorn`, `a2wsgi`, `PyJWT`, `websockets`, `python-multipart` | Plugins using `@api_plugin`, `@web_page`, or Python MCP tools. |
| `[dash]` | `dash`, `dash-bootstrap-components`, `dash-ace`, `dash-extensions`, `[api]`, `pandas` | Web console (`/dash`) or `@dash_plugin` pages. |
| `[shell]` / `[gui]` / `[cli-tools]` | `ptpython` / `pywebview` / `pgcli`, `mycli`, `litecli`, `gadwall` | `mrsm python`, `mrsm start gui`, or Python SQL shells. |
| `[full]` | `pandas`, `polars`, `geo`, `sql`, `api`, `dash`, `shell` | All standard integrations (default Docker image; `:slim` ships base wheel only). |

**Self-upgrades (`mrsm upgrade mrsm`).** Upgrades the owning install (`pip`/`uv`/`pipx`, Cargo, or binary) in place without inferring the target from `MRSM_PYTHON`. Prompts are asked up front (upgrade, stack pull, stack restart; flags: `--yes`, `--noask`, `--force`, `--no-pull`, `-A --pre`, `-A --version X.Y.Z`, and `-A --break-system-packages` on PEP 668 distros).

!!! warning "Upgrading a `--user` install from 5.0.x"
    When `uv` is installed, 5.0.x's `mrsm upgrade mrsm` in a `--user` install tries to write to system site-packages. Upgrade to 5.1.0+ with pip directly:
    ```bash
    python3 -m pip install --user --upgrade meerschaum
    ```

**DuckDB and Oracle drivers.** Python hosts with `meerschaum[sql-duckdb]` and `MRSM_DUCKDB_LIB` unset share the Python DuckDB driver across Rust Pipe operations and SQLAlchemy. Standalone and base installs load `libduckdb` (v1.4+) via [`MRSM_DUCKDB_LIB`](environment.md#mrsm_duckdb_lib), `<root>/drivers/`, system paths, or checksum-verified download (`mrsm install driver duckdb`, `mrsm show drivers`). Oracle now requires Instant Client (`libclntsh`) instead of thin mode.

## Behavior changes at a glance

| Area | v4.1 | v5 |
|---|---|---|
| **YAML & config** | PyYAML tags (`!!python/object`) allowed; truncated writes | Safe native YAML (rejects `!!python/*` except `!!python/tuple`); atomic `0600` writes. `api.uvicorn.*` → `api.server.*`. |
| **Env vars** | Any `MRSM_<X>_<Y>` became a connector; `MRSM_NO_AUTO_INSTALL` still fetched `semver`/`uv` | Only built-in (`sql`, `api`, `valkey`, `plugin`, `systemd`) and `@make_connector` types become connectors. [`MRSM_NO_AUTO_INSTALL=1`](environment.md#mrsm_no_auto_install) blocks *all* implicit installs. |
| **Timeouts** | 30 s+ retries on unreachable servers | Fails fast on refused/DNS/auth errors, or after `connect_timeout` / [`MRSM_CONNECT_TIMEOUT`](environment.md#mrsm_connect_timeout) (default `5` s). Raise for serverless DBs. |
| **Python DB errors** | `read`/`exec`/`value` and `pipe.get_data`/`get_rowcount`/`get_sync_time` returned `None`/`0` | Raise DB / `5xx` errors; pass `silent=True` for v4 behavior. Unsynced pipes still read empty. |
| **DataFrames** | Always pandas; `as_dask=True` supported | Fresh roots default to **Polars**; upgraded 4.x roots auto-pin `dataframe: pandas`. Dask removed (`TypeError`—use `as_iterator=True` or `as_polars=True`). |
| **Null-like strings** | `'None'`, `'NA'`, `'nan'`, `''` stored/queried as `NULL` | Literal text in declared text and text index columns (use `None` / `'_None'` for SQL NULL; Oracle `''` stays `NULL`). |
| **Unique keys & `mrsm` pipes** | Duplicate pipe/user/plugin rows allowed; `mrsm` pipes shown | Adds unique indexes `uq_mrsm_pipes_keys`, `uq_mrsm_users_username`, `uq_mrsm_plugins_plugin_name` (`mrsm doctor` flags duplicates). `mrsm` pipes hidden unless `-c mrsm`. |
| **Debug & colors** | `dprint()` to stdout; colors when piped; `--nopretty` exited `0` | [`--debug` / `dprint()`](troubleshooting.md#debug-output) write to **stderr** (secrets redacted). Colors disable when piped (`NO_COLOR`/`FORCE_COLOR`). `--nopretty` exits `1` on failure. |
| **CLI & editors** | `--engine laps`, CLI daemons, `pyvim` fallback | `--loop` replaces `--engine laps`; CLI daemons retired; editors use `$VISUAL` → `$EDITOR` → `vi`. `mrsm python script.py [args]` runs the script. |
| **Auth & limits** | Open defaults; failed scope reads granted all; no body limit | First user is admin; self-registration off; default role `editor`. Permissions fail closed (`503`). Pre-v5 JWTs/sessions re-login once (API keys stay). Request limits enforced (`api:limits`, `413`). |

## Jobs, CLI, and Compose

- **Every root uses `systemd`.** On systemd hosts, v5 uses `systemd` for *all* roots—including Compose projects (`mrsm-<job>.service`, `mrsm-<project>.<job>.service`, `mrsm-<hash>.<job>.service`), pinning `MRSM_PYTHON`. Existing `local` jobs move on the next `compose up` or `restart jobs`; opt out with `meerschaum:executor: local`.
- **Staggered startup & job health.** Bulk starts stagger via `jobs.startup.*` (`--no-stagger` skips). `--loop` repeats *any* action and tracks laps/health ([Job health](background-jobs/health.md)).
- **Stricter jobs & API redaction.** Job names must be one path component. Remote jobs inherit the server's env (keeping Compose stamps), `GET /jobs` redacts secrets, and console workers are scoped to owner, team managers, and admins.
- **Chains vs Pipelines.** `a + b : <flags>` action chains (and saved 4.1 chain jobs) keep running; `start pipeline NAME` / `stop pipeline NAME` now manage saved Pipelines.
- **Compose & copy.** `mrsm compose explain` is side-effect free (no DBs, caches, or installs) and validates missing references; `compose up` checks job ownership first and forwards `--allow-protected`; `include:` / `parent:` support multi-project trees ([Compose](compose.md)). `copy pipes` exits `1` if any pipe fails, and `--sync-data incremental` syncs only new rows.
- **Interactive UX.** `show data` truncates like pandas (`--all` prints all); `show sizes` counts shared tables once; `show pipes` lists failing pipes; `bootstrap pipes` suggests keys with a new SQL prompt order; Ctrl-C cleanly cancels and releases leases.

## The syncing engine

v5 tracks pipe dependencies (`parents`, `children`, and `{{ Pipe() }}` references) for multi-pipe runs, dry-run plans, and resumable executions:

```bash
mrsm sync pipes -c sql:main -m daily_rollup --parents   # ancestors first
mrsm sync pipes -c sql:main -m raw_events --children    # downstream pipes
mrsm plan pipes -t reports --parents                    # dry run
mrsm sync pipes -t reports --parents --resume <run_id>  # resume failed nodes
mrsm start pipeline daily                               # background Pipeline job
```

Bare `sync pipes` keeps v4 semantics, except every sync records a status row in `mrsm_engine_sync_status` (`pipe.get_sync_status()`, `GET /engine/sync-status`). Inside engine runs (`--parents`, `--children`, `mrsm.Pipeline`, or `--loop` with edges):

- Children never sync past their parents' minimum sync time.
- Editing a pipe's `sql`, `fetch`, `dtypes`, or `columns` triggers a **rebuild** (prompts interactively; in jobs sets `rebuild_pending` without blocking unless `--yes` or `system.engine.auto_rebuild: true`).
- Scheduled children follow parent updates rather than their own timer.

See [Pipelines](pipelines/index.md), [Continuous mode](pipelines/continuous.md), and [Engine state](pipelines/state.md#settings).

## Python library and SQL connectors

Public APIs (`mrsm.Pipe`, `get_connector`, `get_config`, `pipe.sync`, `get_data`, `verify`, `filter_existing`, `enforce_dtypes`) keep their v4 signatures:

- **`mrsm.SyncResult`:** `pipe.sync()` returns a tuple subclass unpacking as `(success, message)` with `inserted`, `updated`, `upserted`, `deleted`, `changed_intervals`, and `duration`. PyArrow Tables/RecordBatches, tuples, and chunk generators all sync natively in Rust.
- **Native SQL:** `SQLConnector.read()`, `exec()`, `exec_queries()`, `value()`, `to_sql()`, and catalog helpers run in Rust without `meerschaum[sql]`. `exec()` returns `NativeResult` (`lastrowid` is `None`; `commit=False` rolls back). Keep `[sql]` for `.engine` or explicit SQLAlchemy transactions (or set `native_sql: false`). Avoid mixing `.engine`/`sqlite3` with native calls on the same SQLite/GeoPackage file.
- **DataFrames & timezones:** Override the DataFrame library with `meerschaum.yaml`'s `dataframe`, `MRSM_DATAFRAME`, or `as_polars`/`as_pandas` ([Reading data](pipes/reading.md#dataframe-library)). Pandas reads return Arrow-backed dtypes (`double[pyarrow]`, `large_string[pyarrow]`, `timestamp[ns, tz=UTC][pyarrow]`); PostgreSQL UTC tzinfo is `datetime.timezone.utc` instead of `ZoneInfo('Etc/UTC')`. Convert frames in pandas-only hooks with `meerschaum.utils.dataframe.to_pandas(df)`.
- **API & Valkey clients:** `APIConnector.make_request()` returns lightweight `ApiResponse` (no `requests` dependency); `ValkeyConnector.client` falls back to a built-in client unless `valkey` is installed.
- **New helpers:** `pipe.sync(..., replace=True)` ([Replacing a window](pipes/syncing.md#replacing-a-window)), `pipe.get_rowcount(estimate=True)`, `pipe.verify_schema()`, `mrsm.Pipeline`, and unknown-kwarg warnings in `get_data()`.

**Removed internals** (raise `ImportError` naming the replacement):

| Removed | Replacement |
|---|---|
| `meerschaum.connectors.sql._pipes`, `_users`, `_plugins`, `_fetch`, `_partition`, `_compress`, `_maintenance` | Public `SQLConnector` methods (`_sql`, `_create_engine`, `_uri`, `tables.get_tables`, `_users.valid_username` stay) |
| `SQLConnector.get_add_columns_queries`, `get_alter_columns_queries`, `get_to_sql_dtype`, `create_pipe_table_from_df` | None (Rust generates DDL internally) |
| `meerschaum.actions.<module>` and `meerschaum.api.routes.*` | `meerschaum.actions.actions['<name>']` / `mrsm.entry([...])` |
| `Daemon(target=...)`, `run_daemon`, `daemon_action`, `daemon_entry` | `meerschaum.jobs.Job` (read/control `Daemon` façade stays) |
| `meerschaum.utils.packages.dash` | List `dash` in `required` and call `attempt_import('dash')` |

`meerschaum.utils.sql` and standard plugin SDK helpers (`SuccessTuple`, `attempt_import`, `Venv`, `make_connector`, `make_action`, hooks, `User`, `Token`) are preserved.

## Teams, access, and permissions

- **Roles & registration:** The first API user is admin; self-registration is then disabled unless `api.permissions.registration.users: true`. Default role is `editor` (`api.permissions.default_role`): **Viewer** reads; **Editor** manages pipes/SQL and starts jobs; **Operator** adds actions, web terminal (`webterm:use`), and job management. Legacy `api.permissions.actions.non_admin: true` maps to Operator; `--secure` restricts actions and terminal to admins.
- **Tokens & teams:** Tokens are capped at their owner's current scopes (`scopes=[]` grants nothing; `*` is stored literally). Every user belongs to **Everyone**, connectors accept `teams: [...]`, and `access` and `locks` are now **reserved pipe parameter keys**.
- **Locks & protected connectors:** Pipe locks apply locally and cannot be bypassed by `--force`, `-y`, or `--allow-protected` (`mrsm unlock pipes --locks sync`). Connectors with `protected: true` block all writes unless `--allow-protected`, `MRSM_ALLOW_PROTECTED=1`, or `allow_protected()` is active.

See [Access and locks](pipes/access-and-locks.md), [Permissions](api-instance/permissions.md), [Teams](api-instance/teams.md), and [Tokens](api-instance/tokens.md#scopes).

## Plugin authors

Data plugins (`fetch`, `sync`, `register`, `setup`), `@make_action`, hooks, `@make_connector`, and per-plugin venvs keep their v4 interfaces. Key updates:

- **Worker processes & `-P`:** CLI and server runs execute Python plugins in a long-lived worker launched with `-P` (cwd is not on `sys.path`). Do not assume fresh module globals per command, and keep helper modules inside `MRSM_PLUGINS_DIR` or installed packages.
- **Declare dependencies:** The base `mrsm` venv no longer pre-installs `pandas`, `requests`, `dash`, `plotly`, `rich`, `dask`, etc. Declare everything in `required = [...]`. Audit and clean venvs with `mrsm show venvs` and `mrsm prune venvs --dry`; run `mrsm install required <plugin>` after upgrading Python minor versions.
- **Manifests & actions:** Decorators cache in a plugin manifest (`mrsm reload` rebuilds it). `@make_action(name='weather wait')` supports multi-word actions, and any action repeats under `--loop` automatically unless it defines a `loop` parameter.
- **Streaming & graph windows:** Graph runs may pass chunk-aligned historical `begin`/`end` windows. Custom `IS_INSTANCE = True` connectors can implement `acquire_pipe_lease` / `release_pipe_lease`. Legacy `--listen` keeps working, or switch to `IS_STREAMING = True` with `subscribe(pipe, sink)` for engine-managed micro-batching and post-commit acks:

```python
IS_STREAMING = True

def subscribe(pipe, sink, **kw):
    client.on_message = lambda m: sink.push([parse(m)], token=m.mid)
    client.connect(...)
    client.loop_forever()
```

- **API plugins (`@api_plugin`) & MCP:** Python routes and `meerschaum.mcp.tool` handlers run in a Python API host behind the Rust proxy (needs `meerschaum[api]`). Core routes live in Rust: replace core middleware with `api.permissions.rate_limits` or NGINX, and replace route-stripping hooks with `api.permissions.routes.allowlist`/`disabled` or `--public` ([Permissions](api-instance/permissions.md#the-public-preset)). Plugin routes aren't in `/openapi.json`, and `api.dash.sessions` IDs must be UUID strings. New helpers: `session_user`, `optional_session_user`, `mount_static` ([Web Pages](plugins/web-pages.md)), `meerschaum.utils.cache` ([Caching](plugins/caching.md)), and `meerschaum.testing` ([Testing](plugins/testing.md)).
- **Dash plugins (`@dash_plugin` / `@web_page`):** Requires `meerschaum[dash]` (declare `dash-daq` in `required` if used). `/dash` is now the Overview; the old dashboard is at `/dash/terminal`. Console callbacks require a signed-in session—mark public ones with `@dash_plugin(public=True)` or `@web_callback(..., public=True)` (or set `api.dash.callbacks_require_session: false`).

### Option B: the page contract (recommended for new pages)

Return an HTML fragment from `@web_page` to build console pages that work identically in Python and Rust plugins. The console frames your fragment at `/dash/<path>` and bundles Plotly.js 4 (`meerschaum.plugins.figure(fig)`), MapLibre GL JS (`<div data-mrsm-map='...'>`, requires WebGL2), and [HTMX](https://htmx.org) locally—no CDN needed.

```python
from html import escape
from fastapi.responses import HTMLResponse
from meerschaum.plugins import web_page, api_plugin, figure
import plotly.graph_objects as go

def counts_figure(site):
    df = counts_pipe.get_data(params={'site': site} if site != 'all' else None)
    return figure(go.Figure(go.Scatter(x=df['ts'], y=df['count'])))

@web_page('counts', login_required=True, page_group='Reports')
def counts_page():
    opts = ''.join(f'<option value="{escape(v)}">{escape(l)}</option>' for v, l in site_options())
    return f'''
      <select name="site" hx-get="/acme/counts/figure" hx-target="#acme-fig" hx-trigger="change">
        {opts}
      </select>
      <div id="acme-fig">{counts_figure('all')}</div>
    '''

@api_plugin
def init_api(app):
    @app.get('/acme/counts/figure', response_class=HTMLResponse)
    def counts_figure_route(site: str = 'all'):
        return counts_figure(site)
```

- `login_required=True` (default) is enforced before your function runs (`401` otherwise). Page functions take no arguments; read query params or `session_user` in `@api_plugin` routes called by HTMX.
- Use `mrsm-theme-*` CSS classes for light/dark themes, and Plotly.js 4 trace names (`scattermap`/`layout.map` instead of `scattermapbox`/`layout.mapbox`).

**Rust plugins.** Write native plugins as a single `.rs` file or Cargo `cdylib` using `meerschaum-plugin` (replaces a same-named Python plugin and runs on `:slim` images when prebuilt). See [Rust plugins](plugins/rust-plugins.md) and the [advanced guide](plugins/rust-plugins/advanced.md).

## Deployment

- **Docker & stack:** Multi-arch (`amd64`/`arm64`) `bmeares/meerschaum` (`[full]`, with `curl`/`git`/`ssh`) and `:slim` (base wheel + `curl`). `mrsm stack` pins `bmeares/meerschaum:<version>-full`, mounts `create-extensions.sql` as a single file so TimescaleDB init/tuning runs on new volumes, stores secrets in `<root>/stack/secrets/`, and verifies `PG_VERSION` before starting `db`. Update existing DBs with `mrsm upgrade extensions` or `mrsm upgrade postgres` ([Stack](stack/index.md), [Stack upgrades](stack/upgrades.md)).
- **Native HTTPS:** Pass `--keyfile privkey.pem --certfile fullchain.pem` (or `api.server.ssl_*`) to serve HTTPS via rustls with automatic 30 s / `SIGHUP` certificate reload (`308` redirect from plain HTTP, except `/healthcheck`).
- **Web terminal & GUI:** Served by the Rust API on a private tmux server under `MRSM_ROOT_DIR` (`api.webterm.tmux.config` to customize). Reverse proxies must forward `Host` / `X-Forwarded-Host` or WebSockets return `403`. `mrsm start gui` opens `pywebview` against a local `--no-auth` loopback API—avoid on shared machines.
- **Security & Windows:** Plugin archives reject symlink/hardlink members; HTTP MCP SQL blocks internal `mrsm_*` tables and SQL-text functions ([MCP security](mcp/security.md)). Windows runs the API host over loopback TCP (routes and Dash work), while background jobs (`-d`) still require Linux, macOS, Docker, or WSL.
- **Rollback:** Keep v4.1 in a separate venv—both versions share the same root and instance layout (set `meerschaum:executor: local` if moving back from systemd). Run `mrsm deduplicate pipes` to clean up legacy v4 SQLite/NULL-upsert duplicates.
