# 👥 Teams on One Instance

Multiple teams can share one Meerschaum API instance—using the same web console, HTTP API, and MCP server—while seeing only their own pipes, jobs, connectors, and plugins. An **isolated** team is walled off from **Everyone** and can get its own database on your PostgreSQL server.

- For roles and permissions, see [Roles and teams](/reference/api-instance/permissions/#roles-and-teams).
- For separate container or remote executors, see [Team Execution Environments](../team-executors.md).

## Create a team

Open **Users & Teams → New team** in the web console, or bootstrap a team from the CLI:

=== "CLI (interactive)"
    ```bash
    mrsm bootstrap teams wetlands
    ```

=== "CLI (non-interactive)"
    ```bash
    mrsm bootstrap teams wetlands \
      --members ann,cal --managers ann \
      --params '{"isolated": true, "database": "database", "base": "sql:main", "plugins": ["noaa"]}' \
      --yes
    ```

| Setting | What it does |
|---|---|
| `isolated: true` | Restricts team members to their teams' objects and the shared catalog on [reserved instances](#reserve-an-instance-for-teams). |
| `database` | `database` (new PostgreSQL DB), `schema` (new schema), or `none` (use an existing instance). |
| `plugins` / `connectors` | Grants existing plugins and connectors to the team. |
| `plugins_write: true` | Lets team members install, edit, and upload plugins (and run sandboxed team commands). |

Edit a team later in **Users & Teams → Settings** or via `PATCH /teams/{name}/edit`.

## Isolated teams

A user is walled off whenever every team they belong to (other than **Everyone**) is isolated. Admins and open-team members keep the normal shared view.

Walled-off members:

- See their teams' pipes, saved Pipelines, runs, jobs, and teammates, plus **Everyone** (untagged) pipes and saved Pipelines on [instances reserved for their teams](#reserve-an-instance-for-teams).
- Use only connectors reserved for (`teams: [wetlands]`) or granted to their teams, and only plugins granted to or installed by their teams.
- Run sandboxed commands and [team terminals](#commands-of-a-team-that-runs-code) when `plugins_write` is enabled, or supported [API-only commands](../team-executors.md#commands-under-the-callers-identity) without it.
- Use approved plugin MCP tools with `plugins:read` ([Plugin metadata and private handlers](../team-executors.md#plugin-metadata-and-private-handlers)).

Requests that omit `-i` / `instance` automatically default to the caller's team instance.

!!! tip "Start multi-team servers with `--private`"
    Without `--private`, `/users` and `/plugins` answer anonymous requests. Always run shared multi-team servers with:

    ```bash
    mrsm start api --private
    ```

## Reserve an instance for teams

Set `teams` on an instance connector in `config/meerschaum.yaml` (any instance type—SQL, Valkey, or custom):

```yaml
connectors:
  sql:
    observations:
      flavor: sqlite
      database: observations.db
      teams: [wetlands, water_quality]
```

On a reserved instance, **Everyone** covers members of the teams named in `teams`—including isolated teams, team terminals, team jobs, and MCP clients—without rewriting stored `access`:

- **Untagged pipes and saved Pipelines** (from `mrsm compose`, direct CLI, plugins, or ported databases) are immediately usable with no backfill.
- **Roles and per-pipe controls hold:** reserved-team managers can manage access, locks, and private untagged pipes (and return pipes to **Everyone**), while per-pipe owners, `private` visibility, `owner`-only edits, and locks work as usual. Visibility `everyone` on a team-owned pipe also covers every reserved team.
- **Live updates:** editing `teams`, renaming a team, or deleting a team (which retires its reservation) takes effect on the next request without a restart. A retired or misspelt team name grants nobody access.
- **Reservation vs. grant:** granting a connector via `connectors: [...]` lets a team use it without extending **Everyone** to its isolated members—list the team in `teams` when it should share untagged pipes.

See [Access and Locks](../pipes/access-and-locks.md#access) for per-pipe visibility and edit rules.

## A database for each team

Choosing **New database** or **New schema** works on any PostgreSQL-family connector (`postgresql`, `timescaledb`, `timescaledb-ha`, `postgis`, `citus`) with role and database creation privileges. Meerschaum provisions:

1. **Roles:** Owner role `mrsm_<team>` (non-superuser) and read-only role `mrsm_<team>_ro` for dashboards.
2. **Storage:** An isolated database (recommended) or schema, revoking `PUBLIC` and `mrsm__main_ro` access.
3. **Connector:** A reserved `sql:<team>` connector in `config/meerschaum.yaml` (preserving `MRSM{...}` references; host `MRSM_CONFIG` won't overwrite it).

No API restart is needed—just keep `api:permissions:instances:allow_multiple_instances: true`.

!!! note "Renaming and deleting teams"
    Renaming a team via the console, `mrsm edit teams`, or `POST /teams/{name}/rename` updates connector reservations automatically. Deleting a team preserves its database and re-tags the connector as `Deleted team <name> #<id>` so future teams never inherit it (pass `"adopt": true` to reclaim it deliberately).

## Plugins

When `plugins_write` is enabled, team members can install from a repository, scaffold starter files, or upload `.py`, `.rs`, `.zip`, or `.tar.gz` archives under **Plugins → New plugin**. Uploaded plugins belong to that team.

## Team-scoped workers

Under host placement, isolated team Python code runs in per-team worker pools instead of the shared pool:

- **Filtered config:** Workers load an owner-only team config containing only that team's connectors, default instance, and `plugins:<name>` settings (server secrets, `stack`, and `api:grafana` are withheld).
- **Live reload:** Updating a team's connectors or passwords automatically recycles its warm workers—no API restart required.

Tune pool size, resource limits, and Linux egress under `system:engine` (`mrsm edit config system`):

```yaml
system:
  engine:
    python_team_workers: 4
    python_team_sandbox: auto          # auto | require | require_sockets | off
    python_team_max_memory_mb: 2048
    python_team_egress: allow_ports    # open | deny | allow_ports
    python_team_egress_ports: [443, 5432]
```

??? note "All `system.engine.python_team_*` settings"

    | Key | Default | Meaning |
    |---|---|---|
    | `python_team_workers` | Shared size (1–4) | Max live workers across all isolated teams. |
    | `python_worker_team_idle_seconds` | `60` | Idle worker stop timeout (`0`: never). |
    | `python_team_env_passthrough` | `[]` | Extra env vars passed to teams (never `MRSM_CONFIG`/`MRSM_PATCH`/connector vars). |
    | `python_team_sandbox` | `auto` | Linux sandbox mode (`auto`, `require`, `require_sockets`, `off`). |
    | `python_team_max_cpu_seconds` | unset | Hard `RLIMIT_CPU` cap (seconds, Linux). |
    | `python_team_max_memory_mb` | unset | Hard `RLIMIT_AS` cap (MiB, Linux). |
    | `python_team_max_file_mb` | unset | Hard `RLIMIT_FSIZE` single-file cap (MiB, Linux). |
    | `python_team_max_disk_mb` | unset | Disk quota (MiB) across team temp/cache/venv/work dirs. |
    | `python_team_max_processes` | unset | Hard `RLIMIT_NPROC` cap (Linux). |
    | `python_team_egress` | `open` | Linux IPv4/IPv6 egress (`open`, `deny`, `allow_ports`). |
    | `python_team_egress_ports` | `[]` | Allowed TCP ports for `allow_ports`. |

## Isolation and sandboxing

Three layers protect the host from isolated team code (separate executors add container and network isolation):

| Layer | Protects against | Platform |
|---|---|---|
| **Team config** | Reading `sql:main`, other teams' connectors, or server secrets. | All |
| **Env allowlist** | Inheriting host secrets (cloud keys, `MRSM_CONFIG`, `PG*`). | All |
| **OS sandbox** | Reading server/other-team files, signaling server processes, or reaching local Unix sockets. | Linux (Landlock + seccomp) |

### The environment of a team's processes

Team workers and jobs start from a clean environment containing only standard OS/locale/TLS/Python variables, team-redirected `MRSM_*` paths, and explicit `python_team_env_passthrough` entries. Secret `MRSM_*` variables are always stripped.

<details markdown="1">
<summary>Allowlisted environment variables</summary>

- **OS & network:** `PATH`, `HOME`, `USER`, `LOGNAME`, `USERNAME`, `SHELL`, `LANG`, `LANGUAGE`, `LC_*`, `TZ`, `TZDIR`, `TMPDIR`, `TEMP`, `TMP`, `XDG_*`, `TERM`, `COLORTERM`, `NO_COLOR`, `COLUMNS`, `LINES`, `SSL_CERT_*`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, `HTTP(S)_PROXY`, `NO_PROXY`, `ALL_PROXY`, `LD_LIBRARY_PATH`, `DYLD_*`, `RUST_LOG`, `RUST_BACKTRACE`, and standard Windows paths.
- **Python:** `PYTHON*`, `VIRTUAL_ENV`, `CONDA_*`.
- **Non-secret Meerschaum:** `MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, `MRSM_VENVS_DIR`, `MRSM_WORK_DIR`, `MRSM_HOME`, `MRSM_PYTHON`, `MRSM_BIN`, `MRSM_WORKER_*`, `MRSM_NO_*`, `MRSM_CONNECT_TIMEOUT`, `MRSM_DAEMON_ID`, `MRSM__COMPOSE_*`, and team scope vars (`MRSM_CONFIG_DIR`, `MRSM_CONFIG_FILE`, `MRSM_TEAM_SCOPE`, `MRSM_TEAM_FINGERPRINT`, `MRSM_TEAM_SANDBOX`).

</details>

### The filesystem sandbox

On Linux, the standalone native `mrsm` binary applies [Landlock](https://docs.kernel.org/userspace-api/landlock.html) (and a seccomp Unix-socket filter below ABI 9) before starting Python or a job—requiring no root or container runtime. Each team writes only to its own venvs, temp/cache/work dirs, and local SQLite/DuckDB files.

Check active sandbox status on your host at any time:

```bash
mrsm doctor
```

!!! note "Use TCP for local databases"
    Because Unix sockets are denied inside the sandbox, connect to local PostgreSQL/Valkey servers over TCP (`127.0.0.1`). Confined profiles (team commands, team terminals, API-only jobs) require **Linux 6.12+** (Landlock ABI 6+) with seccomp socket denial, or **Linux 7.1+** (ABI 9+).

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
- **IPC & Python:** Workers use inherited socket pairs and switch `multiprocessing` from `forkserver` to `spawn` under seccomp; the Python API host uses `<root>/api/host/host-<nonce>.sock` with a per-start token removed before plugin imports; team workers require `meerschaum>=5.1.0`.

</details>

### Terminals without tmux

Because `tmux` trusts any same-UID client on its socket, Meerschaum disables `tmux` only when an isolated team has `plugins_write` enabled **and** Unix sockets are not denied:

| Server state | Web terminals |
|---|---|
| No isolated team, or none with `plugins_write` | `tmux` enabled |
| Isolated team with `plugins_write` + Unix sockets denied | `tmux` enabled |
| Isolated team with `plugins_write` + Unix sockets **not** denied | Plain single-tab shells (run long tasks as [jobs](../background-jobs/index.md)) |

### Commands of a team that runs code

Members of an isolated team with `plugins_write: true` can run commands (`sh`, `python`, `!command`, plugin actions) and open a dedicated **team terminal** when running on the standalone native `mrsm` binary with full Linux confinement active:

- `python_team_sandbox` is not `off`, the canary probe passes, and team Python runs `meerschaum>=5.1.0`.
- Landlock ABI 6+ scopes signals, Unix sockets are denied, and the server user cannot access an uncovered Docker/Podman socket.

When active, `POST /execute`, `POST /jobs/{name}`, console **Exec**, and **New job** launch sandboxed team jobs (`POST /actions/{action}` and MCP `execute_action` refuse isolated members—use `POST /execute`). Revoking `plugins_write` refuses new commands and closes team terminals within seconds.

### What host isolation does not stop

Host isolation leaves these gaps unless you configure caps or move to a [separate team executor](../team-executors.md#resource-and-host-requirements):

- **Outbound network:** Open (including `169.254.169.254`) unless `python_team_egress` is set to `deny` or `allow_ports`.
- **CPU, memory, disk:** Unbounded unless `python_team_max_*` or container cgroups are set.
- **Shared connectors & shared API host:** Shared connectors grant full DB access; the shared Python API host uses guardrails, not an OS sandbox.

## Agents and MCP

Connect AI agents from **Tokens → Connect an agent** or **New agent token** (read-only). Agent tokens inherit their owner's isolated team view—see [MCP](/reference/mcp/).

## Grafana

Proxy console logins to Grafana with automatic per-team organizations and read-only data sources (`mrsm_<team>_ro`):

```yaml
# mrsm edit config api
api:
  grafana:
    enabled: true
    root_url: https://your-api-host/grafana/
```

Apply with `mrsm stack up -d`. Grafana is served at `/grafana/`, team members become Grafana Editors, managers become Org Admins, and the Stack's **Meerschaum Main** data source uses read-only `mrsm__main_ro` ([Grafana's read-only data source](/reference/stack/#grafanas-read-only-data-source)).

??? note "All `api:grafana` configuration keys"

    | Key | Default | Meaning |
    |---|---|---|
    | `url` | `http://grafana:3000` | Internal Grafana URL (override outside Stack). |
    | `admin_user` / `admin_password` | `admin` / generated | Stored in `<root>/api/.grafana_admin_password`. |
    | `root_url` | auto | Public URL of `/grafana/`. |
    | `proxy_whitelist` / `proxy_subnet` | Stack API / `10.213.71.0/29` | Trusted proxy IP(s) and dedicated bridge network. |
    | `database_url` | Connector host:port | DB `host:port` for team data sources. |

!!! warning "Anonymous Grafana dashboards"
    If `GF_AUTH_ANONYMOUS_ENABLED` is on, signed-out visitors can query any data source in the anonymous org. Keep those data sources read-only and public-only.
