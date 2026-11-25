# 🚀 Team Execution Environments

Run an isolated team's jobs, plugins, virtual environments, and web terminals in its own container or remote executor while the main API keeps ownership of login, permissions, and routing.

Executor placement is optional per team—unassigned teams run on the API host ([Teams on One Instance](api-instance/teams.md)). Once you assign a separate executor, it fails closed and never falls back to the main host.

## Quick start (Stack container)

1. Point `api.yaml` at a reachable callback URL (Stack defaults to `http://api:<port>`):

```yaml
# mrsm edit config api
execution:
  require_team_executor: true
  callback_url: https://main.example:8989
```

2. Register the team with `mode: container` (or edit it in **Users & Teams → Settings**):

```json
POST /teams/wetlands/register
{
  "members": ["alice"],
  "managers": ["carol"],
  "attributes": {
    "isolated": true,
    "instance": "sql:wetlands",
    "connectors": ["sql:wetlands"],
    "plugins": [],
    "plugins_write": false,
    "execution": {
      "mode": "container",
      "egress": "proxy",
      "limits": {"memory_mb": 4096, "cpus": 2.0, "pids": 1024},
      "relays": [{"host": "database.partner.example", "port": 5432}]
    }
  }
}
```

3. Reconcile the Stack (run as UID `1000` so Compose secrets match the container user):

```bash
mrsm stack config
mrsm stack up -d
```

Meerschaum spins up a dedicated container, isolated bridge network, and private volumes (`/meerschaum` and `/var/lib/meerschaum/factory`). Grant changes apply live; image, limit, or network changes need `mrsm stack up -d`.

!!! tip "Override the executor image"
    Stack team executors default to `bmeares/meerschaum:5.1.6-full`, matching the main API release. To pin a custom image, set `executor.image` in `stack.yaml`.

    ```yaml
    # mrsm edit config stack
    executor:
      image: registry.example/meerschaum-executor:5.1.6
    ```

## Choose a deployment

| `attributes.execution.mode` | What you run | Apply changes with |
|---|---|---|
| `host` (default) | API host with Linux sandboxing | Host config |
| `container` | Meerschaum Stack + Docker Engine 28+ | `mrsm stack up -d` |
| `api` | Standalone `mrsm start executor` + `api:` connector | systemd, Kubernetes, etc. |

Both `container` and `api` require an isolated team with a registered UUID. Never mount the Docker socket or the main API root into an executor.

## Execution profiles

The main API picks a profile from the caller's permissions and records it on every job:

| Profile | Process config | Used for |
|---|---|---|
| **Team** | Team's granted connectors, credentials, and plugin settings only. | Teams authorized to run code (`plugins_write: true`). |
| **API-only** | Short-lived API credential only; no DB passwords or shared plugin config. | Authorized commands without team code-execution rights. |

Callers cannot request the **Team** profile manually, and later promotions or restarts never upgrade a recorded **API-only** job.

### Commands under the caller's identity

Through `POST /execute` or a saved API-only job, users can run:

- **Reads:** `show pipes`, `show graph`, `show runs`, `watch runs --nopretty`, and `show logs JOB --nopretty`.
- **Pipelines:** `register pipelines` and `edit pipelines` (include the current `revision`):

  ```json
  ["edit", "pipelines", "daily-stations", "--params", "{\"revision\":1,\"description\":\"Daily station readings\"}"]
  ```

- **Pipe copy:** Copy a registered pipe between SQL instances on the same API:

  ```bash
  mrsm copy pipes sql:archive -i sql:main -c manual:posted -m readings -l None --sync-data all --yes
  ```

- **Scheduled sync & verify jobs:** Submit `sync pipes` or `verify pipes` to `POST /jobs/{name}` with `--begin`, `--end`, `--schedule`/`-s`, `--jitter`, `--loop`, `--min-seconds`, or `--restart`:

  ```json
  ["verify", "pipes", "-c", "sql:main", "-m", "ticks", "-l", "None", "--schedule", "every 5 minutes"]
  ```

- **API-only terminals:** Signed-in operators (`webterm:use`, `actions:execute`, `jobs:execute`) can open an API-only web terminal on a compatible Linux host or executor.

#### Supported API-only backends

| Backend | Supported operations |
|---|---|
| **SQLite / GPKG** | Registered reads (with paging), scoped mutations, nullable column additions, atomic replacements, and direct/Pipeline sync & verify. |
| **PostgreSQL reads** | Base tables explicitly approved via `registered_source_tables` (no fetch SQL or views). |
| **MySQL / MariaDB** | Bounded appends to unpartitioned InnoDB tables (`check_existing=false`, explicit signed `BIGINT` PK, declared dtypes). |

Approve a PostgreSQL base table on its connector (for example, pipe ID `42` on `sql:water`):

```yaml
meerschaum:
  connectors:
    sql:
      water:
        registered_source_tables:
          "42":
            schema: public
            table: station_samples
```

??? note "Source broker paging limits & one-shot team plugin syncs"
    - **Source broker:** Single-shot responses cap at 100k rows / 64 MiB. Larger reads page from a frozen snapshot (2,048 rows / 8 MiB per page; max 8 snapshots, 512 MiB each, 2 GiB total; 30s idle / 5m lifetime). Exceeded limits error instead of returning partial data.
    - **Team plugin syncs:** Submit one-shot syncs to `POST /jobs/{name}` (e.g., `["sync", "pipes", "-i", "api:wetlands_writer", "-c", "plugin:collector", "-m", "stations"]`) using a team-reserved `api:` connector. Password rotation preserves the job; changing the endpoint or upstream instance requires a new job.
    - **MySQL appends:** Capped at 512 rows / 4 MiB of `BIGINT` + `BLOB` via a same-main password API connector whose DB user has `TRIGGER` privilege.

## Plugin metadata and private handlers

Plugin metadata is discovered inside a confined process (no DB credentials or network) and cached per approved source set. The main API never imports team plugin code directly—admitted API and MCP handlers run in private API-only processes with per-caller permission checks.

- **Web pages:** Team web pages render inert HTML and a safe subset of Dash/Plotly components inside an opaque iframe (no custom JS). Revocations take effect on the next visit.
- **Rust plugins:** Confined single-file `.rs` plugins need an admin-selected offline toolchain, vendor tree, and lockfile (multi-file crates are not supported). Remote executors also require a Factory signing key pinned on Main.

## Outbound connections

| `egress` | Network reach |
|---|---|
| `proxy` (default) | Main API, assigned Stack DB, HTTP (`80`) / HTTPS (`443`) via filtering proxy, and declared `relays`. |
| `none` | Main API, assigned Stack DB, and declared `relays` only. |
| `open` | Unrestricted outbound access. |

The filtering proxy blocks private, loopback, metadata, and reserved IPs. You can declare up to 64 DNS `relays` (`host` + `port`; numeric IPs are refused) and keep the real hostname for TLS verification.

!!! note "DNS changes"
    Relays pin upstream IPs during `mrsm stack up -d`. Re-run `mrsm stack up -d` if a partner database's IP changes.

## Independently managed executors

To run an executor on another server or in Kubernetes (`mode: api`):

1. Add an `api:` connector for the executor in `meerschaum.yaml` (behind TLS, with no admin credentials):

```yaml
connectors:
  api:
    wetlands_executor:
      uri: https://executor.wetlands.example:8989
```

2. Point the team at it (`"execution": {"mode": "api", "connector": "api:wetlands_executor"}`) and set `execution.callback_url` in `api.yaml`.

3. Copy the team's public verifier from `<main-root>/executors/desired/team-<UUID-without-hyphens>.json` to the executor host and start `mrsm start executor`:

```bash
sudo install -d -o mrsm-wetlands -g mrsm-wetlands -m 0700 \
    /var/lib/meerschaum-wetlands /var/lib/meerschaum-wetlands-control

sudo -u mrsm-wetlands env \
    MRSM_ROOT_DIR=/var/lib/meerschaum-wetlands \
    MRSM_EXECUTOR_BOOTSTRAP=/etc/meerschaum/wetlands-verifier.json \
    /usr/local/bin/mrsm start executor --host 0.0.0.0 --port 8989 \
    --control-root /var/lib/meerschaum-wetlands-control
```

=== "systemd"
    Run as a dedicated user with explicit `--control-root` and systemd resource/network limits. Meerschaum's systemd job backend requires **systemd 235+** (regenerate existing units after upgrading).

=== "Kubernetes"
    Run one non-root Pod per team with a read-only rootfs, separate work/control mounts, a read-only verifier mount, and NetworkPolicies (`egress`, `limits`, and `runtime` apply only to Stack Compose).

## Resource and host requirements

Stack executors run as `1000:1000` with dropped capabilities, `no-new-privileges`, private IPC, and a read-only rootfs:

| Limit | Default | Notes |
|---|---|---|
| `limits.memory_mb` | `4096` MiB | Shared by workers/builds; swap disabled. |
| `limits.cpus` / `limits.pids` | `2.0` / `1024` | CPU quota and process/thread cap. |
| Persistent disk | Unbounded | Enforce volume quotas outside Meerschaum. |

- **Docker Engine 28+** is required for isolated bridge gateways.
- **Linux 6.12+** (Landlock ABI 6+) with seccomp Unix-socket denial—or **Linux 7.1+** (Landlock ABI 9+)—is required for confined profiles under the standalone native `mrsm` binary. See [The filesystem sandbox](api-instance/teams.md#the-filesystem-sandbox).
- **Custom OCI runtime:** Set `attributes.execution.runtime` (for example `"runsc"`) to select a preconfigured runtime.

## Identity, history and current authority

Execution history is bound to owner and team UUIDs—not display names. Renaming a team keeps its history; deleting a team archives private history for owners and admins, and recreating the same name never inherits old records.

Every start or restart rechecks current membership, scopes, and grants. Executors hold only filtered config and short-lived leases (keep host clocks in sync).

!!! warning "Back up together"
    Back up the main API root (registry, signing keys, job catalog), instance database, and team volumes together. Partial restores fail closed.

## Migrate an existing deployment

1. **Back up** your database, main root, job state, and team volumes.
2. **Upgrade together:** Match main API and executor versions, and confirm Docker 28+ and Linux confinement probes pass.
3. **Configure placement:** Set `attributes.execution` (mode, limits, egress/relays) and `execution.callback_url`.
4. **Reconcile:** Run `mrsm stack up -d` (or update your systemd/Kubernetes executor and verifier).
5. **Recreate workloads:** Moving a team to a container does not migrate host jobs, venvs, or terminals—recreate them through the main API.
6. **Verify:** Test a job, plugin sync, DB connection, log stream, terminal, and revocation before widening access.
