# 🌳 Environment Variables

Manage Meerschaum's paths, configuration, and runtime behavior with environment variables. Run `mrsm show environment` to list active `MRSM_` variables, or `mrsm show config` to inspect the resolved configuration.

<img src="/assets/screenshots/web-console-environment.png" alt="Meerschaum web console environment variables.">

## Summary

| Variable | Purpose | Default |
|---|---|---|
| `MRSM_ROOT_DIR` | Root directory for config, plugins, venvs, jobs, and cache. | `~/.config/meerschaum` (`%APPDATA%\Meerschaum` on Windows) |
| `MRSM_CONFIG_DIR` | Config directory (isolates secrets from the root). | `$MRSM_ROOT_DIR/config` |
| `MRSM_PLUGINS_DIR` | Plugin directory, colon-separated paths, or JSON list. | `$MRSM_ROOT_DIR/plugins` |
| `MRSM_VENVS_DIR` | Virtual environments directory. | `$MRSM_ROOT_DIR/venvs` |
| `MRSM_WORK_DIR` | Base directory when `MRSM_ROOT_DIR` is unset. | Current directory |
| `MRSM_CONFIG` | Inline JSON or simplified-dict config patch. | _(unset)_ |
| `MRSM_PATCH` | Patch applied *after* `MRSM_CONFIG`. | _(unset)_ |
| `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE` | File holding a `MRSM_CONFIG` / `MRSM_PATCH` payload. | _(unset)_ |
| `MRSM_<TYPE>_<LABEL>` | Connector URI or JSON object (e.g. `MRSM_SQL_MAIN`). | _(unset)_ |
| `MRSM_CONNECT_TIMEOUT` | First-connection timeout (seconds) for SQL, API, and Valkey. | `5` |
| `MRSM_NO_AUTO_INSTALL` | Disable all implicit package and driver installs. | _(unset)_ |
| `MRSM_NOASK` / `MRSM_NONINTERACTIVE` | Disable interactive prompts (assume defaults). | _(unset)_ |
| `MRSM_ALLOW_PROTECTED` | `1` allows writes to `protected: true` connectors. | _(unset)_ |
| `MRSM_DATAFRAME` | `polars` or `pandas` for Python APIs. | _(config value)_ |
| `MRSM_DUCKDB_LIB` | Path to `libduckdb`; forces the native C API. | _(auto)_ |
| `MRSM_BIN` | Standalone `mrsm` binary path for the Python launcher. | _(unset)_ |
| `MRSM_PYTHON` | Python interpreter for standalone binary workers/upgrades. | _(auto-detected)_ |
| `MRSM_WORKER_STARTUP_TIMEOUT` | Python worker startup timeout (seconds). | `60` |
| `MRSM_RUNTIME` | Special runtime marker (e.g. `docker`). | _(unset)_ |
| `NO_COLOR` / `FORCE_COLOR` | Disable or force terminal styles (`0`/`false`/`no` ignored for `FORCE_COLOR`). | _(unset)_ |

## `MRSM_ROOT_DIR`

Override the default root (`~/.config/meerschaum`, or `%APPDATA%\Meerschaum` on Windows) to isolate configuration, plugins, and virtual environments.

```bash
mkdir foo
MRSM_ROOT_DIR=foo mrsm show environment
```

## `MRSM_CONFIG_DIR`

Store configuration files outside `MRSM_ROOT_DIR` to separate secrets from data and venvs. Defaults to `$MRSM_ROOT_DIR/config`.

## `MRSM_PLUGINS_DIR`

Point Meerschaum at one or more custom plugin directories (handy for keeping plugins in `git`). Separate multiple paths with `:` or pass a JSON array:

```bash
MRSM_PLUGINS_DIR=plugins mrsm show plugins
export MRSM_PLUGINS_DIR='./plugins:/another/plugins/path'
export MRSM_PLUGINS_DIR='["./plugins", "/another/plugins/path"]'
```

On Windows, separate multiple paths with semicolons to preserve drive letters.

```powershell
$env:MRSM_PLUGINS_DIR = "C:\meerschaum\plugins;D:\plugins"
mrsm show plugins
```

## `MRSM_VENVS_DIR`

Place virtual environments outside `MRSM_ROOT_DIR`, e.g. to share packages across containers.

```bash
MRSM_VENVS_DIR='venvs/' mrsm show plugins
```

## `MRSM_WORK_DIR`

Base working directory under which the root directory is created when `MRSM_ROOT_DIR` is unset (mainly for containers).

## `MRSM_<TYPE>_<LABEL>`

Define a connector from the environment using a [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier) or JSON attributes (`<LABEL>` is lowercased). See [Environment Connectors](/reference/connectors/#environment-connectors).

```bash
MRSM_SQL_MAIN=postgresql://user:pass@localhost:5432/db \
MRSM_API_BAR=https://user:pass@api.example.com \
  mrsm show connectors

MRSM_SQL_FOO='{"flavor": "sqlite", "database": "/tmp/temp.db"}' \
  mrsm show connectors
```

!!! note "Only real connector types are read"
    Reserved variables (`MRSM_ROOT_DIR`, `MRSM_CONFIG`, etc.) and unknown `<TYPE>` prefixes (like `MRSM_DUCKDB_LIB`) are ignored. Only built-in types (`sql`, `api`, `valkey`, `plugin`, `systemd`) and `@make_connector` types become connectors.

## `MRSM_CONNECT_TIMEOUT`

SQL, API, and Valkey connectors fail fast on their first connection: refused connections, DNS failures, and rejected credentials error immediately, and silent hosts time out after `connect_timeout` seconds (default `5`). Override globally with `MRSM_CONNECT_TIMEOUT` (established jobs still retry):

```bash
MRSM_CONNECT_TIMEOUT=2 mrsm show pipes -i sql:main
```

## `MRSM_CONFIG`

Patch configuration inline with JSON or a simplified dictionary. Values support [`MRSM{}` config symlinks](#mrsm-config-symlinks).

```bash
MRSM_CONFIG='{"foo": {"bar": 123}}' mrsm show config foo
MRSM_CONFIG='foo:123' mrsm show config foo
MRSM_CONFIG='foo:MRSM{meerschaum:connectors:sql:main:password}' mrsm show config foo
```

## `MRSM_PATCH`

Applied *after* `MRSM_CONFIG` using the same syntax, so it can reference keys defined in `MRSM_CONFIG`:

```bash
MRSM_CONFIG='{"foo": {"bar": 123}}' \
MRSM_PATCH='baz:MRSM{foo:bar}' \
  mrsm show config baz
```

## `MRSM_CONFIG_FILE` / `MRSM_PATCH_FILE`

Read a `MRSM_CONFIG` or `MRSM_PATCH` payload from a file (relative paths resolve from cwd) to keep secrets out of `ps e`, `/proc/<pid>/environ`, and `docker inspect`:

```bash
install -m 600 /dev/null ~/.config/mrsm-patch.json
echo '{"meerschaum": {"connectors": {"sql": {"main": {"host": "db.example"}}}}}' \
  > ~/.config/mrsm-patch.json
MRSM_CONFIG_FILE=~/.config/mrsm-patch.json mrsm show connectors sql
```

- Setting both `MRSM_CONFIG` and `MRSM_CONFIG_FILE` (or the `PATCH` pair) is a startup error.
- Spawned jobs, workers, and API servers inherit the variable and read the same file.
- There is no `MRSM_<TYPE>_<LABEL>_FILE` (`MRSM_SQL_MAIN_FILE` defines `sql:main_file`). Use [`MRSM{file:...}`](#secrets-in-configuration) on the `password` attribute instead.

## `MRSM_NO_AUTO_INSTALL`

Set `MRSM_NO_AUTO_INSTALL=1` in offline or locked-down environments to block **all** implicit installs—`attempt_import()`, version checks, venv seeding (`uv venv --seed`), API host installs, and DuckDB library/extension downloads. Explicit installs (`mrsm install packages`, `pip_install()`) still run without bootstrapping `uv`, `pip`, or `wheel`.

```bash
MRSM_NO_AUTO_INSTALL=1 mrsm show pipes
```

## `MRSM_DUCKDB_LIB`

Python hosts with `meerschaum[sql-duckdb]` and `MRSM_DUCKDB_LIB` unset share the Python DuckDB driver across Rust Pipe operations and SQLAlchemy. Setting `MRSM_DUCKDB_LIB` selects the native C API and blocks mixed SQLAlchemy access (unset and restart to use both).

Standalone binaries and native C API runs locate `libduckdb` (v1.4+) in order: `MRSM_DUCKDB_LIB` → `<root>/drivers/` cache → system library path → checksum-verified download (unless `MRSM_NO_AUTO_INSTALL=1`).

```bash
MRSM_DUCKDB_LIB=/opt/duckdb/libduckdb.so mrsm show drivers
```

Manage the cached library with `mrsm install driver duckdb` and `mrsm uninstall driver duckdb`. For per-connection options the C API cannot honor (like `access_mode=READ_ONLY`), use `meerschaum[sql-duckdb]`.

## `MRSM_DATAFRAME`

Override the DataFrame library (`polars` or `pandas`) returned by Python APIs (`pipe.get_data()`, `conn.read()`, `pipe.fetch()`, `Plan.to_dataframe()`, and plugin frames) for this process and child jobs/workers. Per-call `as_polars=True` / `as_pandas=True` take precedence. See [DataFrame library](/reference/pipes/reading/#dataframe-library).

```bash
MRSM_DATAFRAME=pandas python my_report.py
```

## `MRSM_ALLOW_PROTECTED`

Connectors with `protected: true` block all writes ([Protected Connectors](/reference/connectors/#protected-connectors)). Set `MRSM_ALLOW_PROTECTED=1` for a process and its children, or pass `--allow-protected` for one command (`-y`/`--yes` never overrides protection).

```bash
MRSM_ALLOW_PROTECTED=1 mrsm start api
```

## `MRSM_NOASK` / `MRSM_NONINTERACTIVE`

Skip interactive prompts and assume defaults (same as `--noask` / `--yes`), useful in CI and automation.

```bash
MRSM_NOASK=1 mrsm bootstrap pipes
```

## `MRSM_BIN`, `MRSM_PYTHON`, and `MRSM_RUNTIME`

- **`MRSM_BIN`**: Path to a standalone `mrsm` binary for the Python launcher to exec (must match `--version`).
- **`MRSM_PYTHON`**: Python interpreter for the standalone binary's plugin workers, package installs, and `mrsm upgrade meerschaum`. Workers verify a matching `meerschaum` major version (10 s check) and time out after `MRSM_WORKER_STARTUP_TIMEOUT` (default `60` s).
- **`MRSM_RUNTIME`**: Marks special runtimes (e.g. `docker`); rarely set manually.

## Internal Variables

Set automatically by Meerschaum for jobs, `systemd`, and the API server:

| Variable | Purpose |
|---|---|
| `MRSM_SERVER_ID` / `MRSM_DAEMON_ID` / `MRSM_JOB` | API server ID, job daemon ID, and job process marker. |
| `MRSM_BIN_EXEC` / `MRSM_API_HOST` | Guards `MRSM_BIN` re-exec loops; marks the Python API host process. |
| `MRSM_CORE_TRACE` | Set to `1` to log per-stage native boundary timings to stderr. |
| `MRSM_SYSTEMD_*` | Log, stdin FIFO, result path, and delete-on-exit flags for `systemd` jobs. |
| `MRSM_TEST_FLAVORS` | Comma-separated SQL flavors for the test suite. |

---

## `MRSM{}` Config Symlinks

Reference any config key with `MRSM{key1:key2:key3}` inside config files, `MRSM_CONFIG`, connectors, or pipe parameters. References resolve at access time via `search_and_substitute_config()` in `meerschaum.config`.

```yaml
# config/connectors.yaml
sql:
  reporting:
    flavor: postgresql
    host: localhost
    database: reporting
    username: MRSM{meerschaum:connectors:sql:main:username}
    password: MRSM{meerschaum:connectors:sql:main:password}
```

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('demo', 'cfg', parameters={
    'username': 'MRSM{meerschaum:connectors:sql:main:username}',
})
print(pipe.parameters['username'])
```

!!! warning "`MRSM{}` is not `{{ Pipe() }}`"
    `MRSM{key1:key2}` references **configuration** values; `{{ Pipe('ck', 'mk', 'lk') }}` references another **pipe**. See [Pipe Parameters](/reference/pipes/parameters/).

## Secrets in configuration

Keep credentials out of YAML files and `MRSM_CONFIG` with two reserved prefixes:

| Reference | Resolves to |
|---|---|
| `MRSM{file:<path>}` | File contents minus one trailing newline (`\n` or `\r\n`). `<path>` may be absolute, start with `~`, or be relative to `MRSM_ROOT_DIR`. |
| `MRSM{env:<NAME>}` | Value of environment variable `NAME`. |

```yaml
# config/meerschaum.yaml
connectors:
  sql:
    warehouse:
      flavor: postgresql
      host: db.example
      username: reporting
      password: MRSM{file:/run/secrets/warehouse_password}
      database: warehouse
  api:
    upstream:
      host: api.example
      password: MRSM{env:UPSTREAM_PASSWORD}
```

!!! warning "Keep passwords in their own attribute"
    References inside URIs are substituted literally without percent-encoding, so `@`, `/`, or `:` will break the URI. Always use a separate `password` attribute.

- **Dynamic rotation:** File references re-read when size, mtime, or inode changes (one `stat` otherwise). New connectors (`refresh=True`) pick up rotated secrets without a restart.
- **Unreadable secrets:** Missing files or unset vars never break unrelated commands; building the affected connector errors with the key, reference, and reason—never the value.
- **Masking & config writes:** Resolved secrets print as `******` everywhere. Config writes preserve references and never persist env-only connectors, patches, or secrets.
- **Permissions:** On Unix, new config files are created `0600` and Meerschaum warns once per process if a config file with inline secrets is group/world-readable (`chmod 600`).
- **Jobs, stack, and subprocesses:** Jobs receive `MRSM_CONFIG`/`MRSM_PATCH` via owner-only `MRSM_CONFIG_FILE`/`MRSM_PATCH_FILE` files; `mrsm stack` stores secrets under `<root>/stack/secrets/` ([Credentials in files](/reference/stack/#credentials-in-files)). Non-Meerschaum helpers (editors, `pip`/`uv`, `cargo`, `docker`, etc.) strip `MRSM_CONFIG*`, `MRSM_PATCH*`, and connector vars (`meerschaum.config.environment.get_child_env()`), while Meerschaum children and `mrsm sh`/`os` keep them.

### Docker secrets and systemd credentials

```yaml
# docker-compose.yaml
services:
  api:
    image: bmeares/meerschaum
    command: start api --production
    environment:
      MRSM_SQL_MAIN: >-
        {"flavor": "postgresql", "host": "db", "username": "mrsm",
         "database": "meerschaum", "password": "MRSM{file:/run/secrets/db_password}"}
    secrets: [db_password]
secrets:
  db_password:
    file: ./db_password.txt
```

```ini
# /etc/systemd/system/mrsm-job.service
[Service]
LoadCredential=warehouse_password:/etc/meerschaum/warehouse_password
Environment=MRSM_SQL_WAREHOUSE={"flavor": "postgresql", "host": "db.example", "username": "reporting", "database": "warehouse", "password": "MRSM{file:${CREDENTIALS_DIRECTORY}/warehouse_password}"}
ExecStart=/usr/local/bin/mrsm sync pipes -i sql:warehouse --loop
```
