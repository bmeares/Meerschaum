# 👥 Teams on One Instance

Multiple teams can share one Meerschaum API instance—using the same web console, HTTP API, and MCP server while seeing only their own pipes, jobs, connectors, and plugins. An **isolated** team is walled off from **Everyone** and can get its own database on your PostgreSQL server.

See [Roles and teams](/reference/api-instance/permissions/#roles-and-teams) for roles and managers, and [Team execution environments](../team-executors.md) for separate container or remote executors. Ordinary actions work on Linux, macOS, and Windows; sandboxed host team commands and terminals require Linux 6.12+ (Landlock signal scoping).

## Create a team

Open **Users & Teams → New team** in the console or run `mrsm bootstrap teams wetlands`:

| Setting | Effect |
|---|---|
| **Hide Everyone's pipes, jobs, connectors and plugins** | Sets `isolated: true`. |
| **Storage** | Creates a **New database** or **New schema** on a PostgreSQL-family connector (`sql:main` default), or selects an **Existing instance**. |
| **Connectors / Plugins** | Grants existing connectors and installed plugins to the team. |
| **Members install, edit and upload plugins** | Sets `plugins_write: true`. |

Skip prompts with `--params`:

```bash
mrsm bootstrap teams wetlands \
  --members ann,cal --managers ann \
  --params '{"isolated": true, "database": "database", "base": "sql:main", "plugins": ["noaa"]}' \
  --yes
```

`database` accepts `database`, `schema`, or `none`; `name` and `label` override the DB/schema name and connector label. Edit attributes later via console **Settings** or `PATCH /teams/{name}/edit`.

## Isolated teams

A user is walled off when every team they belong to (besides Everyone) is isolated. Administrators and open-team members keep the standard view.

A walled-off user:

- Sees only their teams' pipes, saved Pipelines, runs, jobs, teammates, and teams (Everyone's pipes, other teams' "everyone"-shared pipes, and local jobs are hidden).
- Can use only connectors reserved for (`teams: [wetlands]`) or granted to their teams, and only plugins granted to or installed by their teams.
- Can register pipes with their teams' connectors, granted `plugin:<name>` sources, or plain non-connector labels (e.g., `manual:gauge`), and cannot chain instances (`instance:chain`).
- Runs sandboxed commands and [team terminals](#commands-of-a-team-that-runs-code) when `plugins_write` is enabled, or supported [API-only commands](../team-executors.md#commands-under-the-callers-identity) on eligible teams, plus sync/verify on editable pipes.
- Uses approved plugin MCP tools, resources, and prompts with `plugins:read` ([Plugin metadata and private handlers](../team-executors.md#plugin-metadata-and-private-handlers)).

Requests without an explicit instance default to the caller's team instance.

!!! note "Serve multi-team APIs with `--private`"
    Without `--private`, `/users` and `/plugins` answer anonymous calls (including legacy `Bearer None`). Run `mrsm start api --private`.

## A database for each team

**New database** and **New schema** require a PostgreSQL-family connector (`postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`) with role/database creation privileges. Meerschaum provisions:

- Owner role `mrsm_<team>` (non-superuser, generated password) and read-only role `mrsm_<team>_ro` for dashboards.
- The isolated database (with TimescaleDB/PostGIS if present) or schema, excluding `PUBLIC` and `mrsm__main_ro` and revoking `PUBLIC` `CONNECT` on the base database.
- A reserved `sql:<team>` connector written to owner-only `config/meerschaum.yaml` (preserving `MRSM{...}` references). Since `5.1.1`, host `MRSM_CONFIG` will not overwrite team-reserved connectors (`MRSM_PATCH` still overrides).

A separate database provides stronger isolation than a schema (which shares catalog names). No API restart is needed; keep `api:permissions:instances:allow_multiple_instances: true`.

Deleting a team keeps its database and data, re-tagging its connector as `Deleted team <name> #<id>` so future teams never inherit it. Renaming via console, `edit teams`, or `POST /teams/{name}/rename` updates reservations automatically (direct Python SDK calls do not). To intentionally claim an orphaned connector, pass `"adopt": true` (`--params '{"adopt": true}'`).

## Plugins

With `plugins_write` enabled, team members can install from the repository, scaffold templates, or upload `.py`, `.rs`, `.zip`, or `.tar.gz` archives under **Plugins → New plugin**. Uploaded plugins belong to that team.

## Team-scoped workers

Under host placement, isolated team Python code (plugin connectors, fetches, hooks, actions, jobs) runs in per-team worker pools instead of the shared pool:

- **Isolated configs:** Workers load an owner-only team config under `<root>/team_scopes/<team id>/` (`MRSM_CONFIG_FILE`, empty `MRSM_CONFIG_DIR`); team jobs store theirs in `jobs/<name>/.mrsm_config.json` with allowlisted `env` in `properties.json`. Only the team's connectors, default instance, `plugins:<name>` settings, and non-secret base keys are included (`stack`, `api:grafana`, and credential keys are withheld).
- **Live reload & version guard:** Connector or password changes recycle warm workers automatically without restarting the API. Team workers and the Python API host require `meerschaum>=5.1.0`; outdated workers abort before running code, and an outdated host returns `503` for isolated/console requests.
- **Shared API host guardrail:** Inside the shared Python API host, `get_connector()` raises `PermissionError` and `get_config()` hides other teams' connectors (a guardrail, not a process sandbox).

Configure pool limits, resource caps, and egress under `system.engine` (`mrsm edit config system`):

| Key | Default | Meaning |
|---|---|---|
| `python_team_workers` | Shared size (1–4) | Max live workers across all isolated teams. |
| `python_worker_team_idle_seconds` | `60` | Idle worker stop timeout (`0`: never). |
| `python_team_env_passthrough` | `[]` | Extra server env vars passed to teams (never `MRSM_CONFIG`/`MRSM_PATCH`/connector vars). |
| `python_team_sandbox` | `auto` | Linux sandbox mode: `auto`, `require`, `require_sockets`, or `off`. |
| `python_team_max_cpu_seconds` | unset | Hard `RLIMIT_CPU` cap (seconds, Linux). |
| `python_team_max_memory_mb` | unset | Hard `RLIMIT_AS` cap (MiB, Linux). |
| `python_team_max_file_mb` | unset | Hard `RLIMIT_FSIZE` single-file cap (MiB, Linux). |
| `python_team_max_disk_mb` | unset | Aggregate disk quota (MiB) across team temp/cache/venv/work dirs. |
| `python_team_max_processes` | unset | Hard `RLIMIT_NPROC` cap (Linux). |
| `python_team_egress` | `open` | Linux IPv4/IPv6 egress: `open`, `deny` (seccomp), or `allow_ports` (Landlock ABI 4+ TCP + seccomp). |
| `python_team_egress_ports` | `[]` | Allowed TCP ports for `allow_ports` (e.g., `[443, 5432]`). |

## Isolation and sandboxing

Three layers protect the host from isolated team processes (separate executors add container/network isolation; see [Team execution environments](../team-executors.md)):

| Layer | Protects against | Platform |
|---|---|---|
| **Team configuration** | Accessing `sql:main`, other teams' connectors, or server secrets. | All |
| **Environment allowlist** | Reading host env secrets (cloud keys, `MRSM_CONFIG`, `PG*`). | All |
| **Filesystem & socket sandbox** | Reading server/other-team files, writing shared paths, signaling server processes, or reaching local Unix sockets. | Linux (Landlock + seccomp) |

### The environment of a team's processes

Team workers and jobs start from an empty environment and receive only standard OS, locale, TLS, proxy, and Python variables, non-secret `MRSM_*` paths redirected to team directories, and explicit `python_team_env_passthrough` names. Secret `MRSM_*` variables (`MRSM_CONFIG`, `MRSM_PATCH`, `MRSM_<TYPE>_<LABEL>`) are always stripped.

<details markdown="1">
<summary>Allowlisted environment variables</summary>

- **OS & network:** `PATH`, `HOME`, `USER`, `LOGNAME`, `USERNAME`, `SHELL`, `LANG`, `LANGUAGE`, `LC_*`, `TZ`, `TZDIR`, `TMPDIR`, `TEMP`, `TMP`, `XDG_*`, `TERM`, `COLORTERM`, `NO_COLOR`, `COLUMNS`, `LINES`, `SSL_CERT_*`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, `HTTP(S)_PROXY`, `NO_PROXY`, `ALL_PROXY`, `LD_LIBRARY_PATH`, `DYLD_*`, `RUST_LOG`, `RUST_BACKTRACE`, and standard Windows paths.
- **Python:** `PYTHON*` runtime flags (`PYTHONPATH`, `PYTHONHOME`, `PYTHONUNBUFFERED`, `PYTHONUTF8`, etc.), `VIRTUAL_ENV`, `CONDA_*`.
- **Non-secret Meerschaum:** `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, `MRSM_VENVS_DIR`, `MRSM_WORK_DIR`, `MRSM_HOME`, `MRSM_PYTHON`, `MRSM_BIN`, `MRSM_WORKER_*`, `MRSM_NO_*`, `MRSM_CONNECT_TIMEOUT`, job `MRSM_DAEMON_ID`/`MRSM__COMPOSE_*`, and team scope vars (`MRSM_CONFIG_DIR`, `MRSM_CONFIG_FILE`, `MRSM_TEAM_SCOPE`, `MRSM_TEAM_FINGERPRINT`, `MRSM_TEAM_SANDBOX`).

</details>

### The filesystem sandbox

On Linux, the standalone native `mrsm` launcher applies [Landlock](https://docs.kernel.org/userspace-api/landlock.html) (plus a seccomp Unix-socket filter below ABI 9) before starting Python or a job—needing no root or container runtime and inherited by all child processes. Each team writes only to its own venv copies (`<root>/team_scopes/<id>/venvs`), temp/cache/job-work dirs, and its own SQLite/DuckDB files. Every launch verifies enforcement against `<root>/team_scopes/.sandbox_canary` (plus a socket probe under seccomp); inspect active status with `mrsm doctor`.

- **Kernel requirements:** Confined profiles (team commands, team terminals, API-only jobs) require Landlock ABI 6+ (Linux 6.12+) signal scoping plus either proven seccomp Unix-socket denial (x86-64/AArch64) or Landlock ABI 9+ (Linux 7.1+) native socket denial.
- **Modes (`python_team_sandbox`):** `auto` (sandbox where supported, warn if not), `require` (require filesystem rules), `require_sockets` (require filesystem rules, ABI 6+ signal scoping, and Unix-socket denial), or `off` (disables sandbox and refuses confined profiles).
- **Python notes:** Under seccomp, workers switch `multiprocessing` from `forkserver` to `spawn` and bind `Manager()` on loopback TCP. Connect to local databases over TCP (`127.0.0.1`), not Unix sockets.

<details markdown="1">
<summary>Landlock allowlist, ABI milestones, and internal IPC</summary>

| Paths | Access |
|---|---|
| `/usr`, `/lib*`, `/bin`, `/sbin`, `/opt`, `/nix/store`, Python prefix, `<root>/venvs/mrsm`, `uv` (excluding root/home/config) | read, exec |
| `/etc` resolver/TLS/tz/user files, `systemd-resolved`/`nscd` sockets | read, connect |
| Team config, team plugins/link dir, `<root>/.internal/plugins/__init__.py`, team job dirs | read |
| Team venvs, temp/cache dirs, `jobs/<name>/.mrsm_team_work`, team SQLite/DuckDB files | read, write |
| Standard `/dev/*`, `/dev/shm` (no list), own `/proc/<pid>`, CPU/cgroup sysfs | device / read |

- **ABIs:** 2/3/5 (`5.19`/`6.2`/`6.10`) `refer`/`truncate`/`ioctl_dev`; 6 (`6.12`) `scope_signal` & `scope_abstract_unix`; 8 (`7.0`) `all_threads`; 9 (`7.1`) native `resolve_unix`.
- **IPC:** Workers use inherited socket pairs; Python API host uses `<root>/api/host/host-<nonce>.sock` with a per-start token removed before plugin imports; job stdin FIFOs are Landlock-scoped.

</details>

### Terminals without tmux

Because `tmux` trusts any same-UID client on its socket, Meerschaum disables `tmux` for all web terminals **only** when an isolated team has `plugins_write` enabled **and** Unix sockets are not denied to team code:

| Server state | Terminals |
|---|---|
| No isolated team, or none with `plugins_write` | `tmux` enabled |
| Isolated team with `plugins_write` + Unix sockets denied (Landlock/seccomp) | `tmux` enabled |
| Isolated team with `plugins_write` + Unix sockets **not** denied | Plain single-tab shells (run long tasks as [jobs](../background-jobs/index.md)) |

### Commands of a team that runs code

Members of an isolated team with `plugins_write` enabled can run arbitrary commands (`sh`, `python`, `!command`, plugin actions) inside their team's sandbox (**team execution**) when the server runs a standalone native `mrsm` binary and all five checks hold:

1. Team is isolated with `plugins_write: true`.
2. `python_team_sandbox` is not `off`.
3. Landlock ABI 6+ scopes signals and Unix sockets are denied (Landlock ABI 9+ or seccomp).
4. The server user cannot open an uncovered Docker/Podman socket.
5. The canary probe passes and the team Python interpreter runs `meerschaum>=5.1.0`.

When active, `POST /execute`, `POST /jobs/{name}`, console **Exec**, and **New job** launch sandboxed team jobs (`require_sockets` enforced), and the terminal opens on a dedicated locked-down `tmux-t-<hash>.sock`. `POST /actions/{action}` and MCP `execute_action` refuse isolated members (use `POST /execute`). Revoking `plugins_write` or losing sandbox checks refuses new commands and closes team terminals within seconds (`mrsm doctor` reports per-team status).

### What host isolation does not stop

Host isolation leaves these gaps unless configured or moved to a separate executor ([resource and host requirements](../team-executors.md#resource-and-host-requirements)):

- **Outbound network:** Open (including internal services and `169.254.169.254`) unless `python_team_egress` is set to `deny` or `allow_ports`.
- **CPU, memory, disk:** Unbounded unless `python_team_max_*` limits or container cgroups are configured.
- **Shared connectors & shared API host:** Shared connectors grant full DB access; the shared API host uses guardrails, not a sandbox.
- **Kernel bugs & `/dev/shm` by name:** Known shared-memory names can still be opened.

## Agents and MCP

Connect AI agents via **Tokens → Connect an agent** or **New agent token** (read-only). Tokens inherit their owner's isolated team view; see [MCP](/reference/mcp/).

## Grafana

Proxy console logins to Grafana with automatic per-team organizations and read-only data sources (`mrsm_<team>_ro`):

```yaml
### `mrsm edit config api`
api:
  grafana:
    enabled: true
    root_url: https://your-api-host/grafana/
```

Run `mrsm stack up -d`. Meerschaum stores a generated admin password in `<root>/api/.grafana_admin_password`, restricts `X-WEBAUTH-USER` to the API container's `proxy_subnet` IP, and serves Grafana under `/grafana/`.

| Key | Default | Meaning |
|---|---|---|
| `url` | `http://grafana:3000` | Internal Grafana URL (override outside Stack). |
| `admin_user` / `admin_password` | `admin` / generated | Grafana admin credentials. |
| `root_url` | auto | Public URL of `/grafana/`. |
| `proxy_whitelist` / `proxy_subnet` | Stack API / `10.213.71.0/29` | Trusted proxy IP(s) and dedicated bridge network. |
| `database_url` | Connector host:port | DB `host:port` for team data sources. |

Team members become Grafana Editors, managers become Org Admins, and Meerschaum admins become Grafana Admins (isolated members are removed from Main Org; orgs track by ID across renames; deleted teams keep dashboards as `Deleted team <name> #<id>` with credentials removed). The Stack's **Meerschaum Main** data source uses read-only `mrsm__main_ro`, which never reads team schemas or databases ([Grafana's read-only data source](/reference/stack/#grafanas-read-only-data-source)).

!!! warning "Anonymous dashboards can query their org's data sources"
    When `GF_AUTH_ANONYMOUS_ENABLED` is on, signed-out visitors can run arbitrary queries against any data source in the anonymous org. Keep custom data sources there read-only and public-only.
