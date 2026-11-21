# Team execution environments

An isolated team can run its jobs, engine work, plugins, virtual environments, and terminals inside a separate executor while the main API owns authentication, permissions, team administration, and routing. Ordinary reads stay on the main API, and Grafana queries the team's database with its own read-only role.

Executor placement is optional per team—unassigned teams keep host placement ([Teams on One Instance](api-instance/teams.md)). Once a separate executor is selected, an unavailable or incompatible executor fails closed and never falls back to the main host.

## Execution profiles

The main API assigns a profile from the authenticated caller and records it on every launch:

| Profile | Process configuration | Intended work |
|---|---|---|
| **Team** | Team's granted connectors, credentials, and plugin settings only. | Work authorized to run code as that team. |
| **API-only** | Logical instance names backed by a short-lived API credential; no reusable DB credentials or shared plugin settings. | Authorized commands without team code-execution rights. |

Users cannot request the **Team** profile via job properties, and promotions, manager restarts, or password rotations never upgrade a recorded **API-only** job.

- **Team plugin syncs:** Submit one-shot plugin syncs via `POST /jobs/{name}` (e.g., `["sync", "pipes", "-i", "api:wetlands_writer", "-c", "plugin:collector", "-m", "stations"]`) using a team-granted, team-reserved `api:` connector. Password rotation preserves the job; changing endpoint, account, or upstream instance requires a new job.
- **API-only source broker:** Reads route through the main API with per-call scope checks. Complete responses cap at 100k rows / 64 MiB; larger reads page from a frozen snapshot (2,048 rows / 8 MiB per page; max 8 snapshots, 512 MiB each, 2 GiB total; 30s idle / 5m lifetime). Exceeded limits or changed definitions error rather than returning partial data.

### Commands under the caller's identity

Through `POST /execute` or a saved API-only job:

- **Filtered reads:** `show pipes`, `show graph`, and `show runs` omit selectors to return visible objects; use `watch runs --nopretty` and `show logs JOB --nopretty`.
- **Pipelines:** Run `register pipelines NAME --params JSON` or `edit pipelines NAME --params JSON` (passing current `revision`):

```json
["edit", "pipelines", "daily-stations", "--params", "{\"revision\":1,\"description\":\"Daily station readings\"}"]
```

- **Explicit pipe copy:** Copy one registered pipe between SQL instances on the same main API (existing targets are skipped):

```bash
mrsm copy pipes sql:archive -i sql:main -c manual:posted -m readings -l None --sync-data all --yes
```

- **Direct sync/verify & Pipelines:** `POST /actions/sync` and `POST /actions/verify` support bounded integer-filtered SQLite syncs; `POST /pipelines/{name}/sync` and MCP `sync_pipeline` run explicit SQLite Pipelines (including `fetch.replace` parents feeding children on the same instance with matching integer axes).
- **Saved sync/verify jobs:** Submit single `sync pipes` or `verify pipes` commands to `POST /jobs/{name}` with optional `--begin`, `--end`, `--schedule`/`-s`, `--jitter`, `--loop`, `--min-seconds`, or `--restart` (requires `jobs:write`, `jobs:execute`, `pipes:write`, edit access, and `actions:execute`):

```json
["verify", "pipes", "-c", "sql:main", "-m", "ticks", "-l", "None", "--schedule", "every 5 minutes"]
```

  `GET /jobs/{name}` exposes `next_fire`, `last_fire`, `firing`, and `error`; auth failures or unresolved commit evidence stop schedules.
- **API-only terminals:** Signed-in operators (`webterm:use`, `actions:execute`, `jobs:execute`) can open an API-only web terminal on a compatible Linux host or selected Linux executor.

| Backend | Supported API-only boundary |
|---|---|
| **SQLite / GPKG** | Registered fetch reads (with paging), scoped mutations, transactional nullable column additions, same-main atomic SQLite replacement, and direct/Pipeline sync and verify. |
| **PostgreSQL reads** | Explicitly approved base tables on the same instance (no fetch SQL, views, or inherited tables) via `registered_source_tables` (below). |
| **MySQL / MariaDB appends** | Bounded appends (`check_existing=false`, ≤512 rows / 4 MiB of `BIGINT` + `BLOB`, explicit signed `BIGINT` PK, declared dtypes) to unpartitioned InnoDB tables via a same-main password API connector whose DB user has `TRIGGER` privilege. |

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

## Plugin metadata and private handlers

Plugin metadata is discovered inside a confined process (no DB credentials or network) and cached per exact approved source set. The main API reads handler metadata without importing plugin code and runs admitted API/MCP handlers in private API-only processes with per-caller permission checks.

Team web pages render inert HTML and a bounded subset of standard Dash/Plotly components and callbacks inside an opaque iframe (no arbitrary JS or active HTML); grant or session revocations take effect on the next visit.

Confined single-file Rust plugins require an admin-selected immutable offline toolchain, vendor tree, and lockfile (multi-file native packages are unsupported). Remote executors also require a Factory signing key paired with its public pin on Main.

## Choose a deployment

| `attributes.execution.mode` | Administrator provides | Apply changes with |
|---|---|---|
| `host` (default) | Host with Linux process isolation | Host config |
| `container` | Meerschaum Stack + Docker Engine 28+ | `mrsm stack up -d` |
| `api` | External native executor + main-API `api:` connector | systemd, Kubernetes, etc. |

`container` and `api` require an isolated team with a registered UUID. Never mount a Docker socket or the main API root into an executor.

Configure policy and return-path origin in `api.yaml`:

```yaml
# Main API configuration: api.yaml
execution:
  require_team_executor: true
  callback_url: https://main.example:8989
```

`callback_url` must be an HTTP/HTTPS origin without path, query, or embedded credentials. Stack executors default to `http://api:<port>`; external executors and host API-only jobs require an explicit reachable origin (loopback listener for host-only, never wildcard) and the standalone native `mrsm` binary.

## Provision a Stack executor

Stack executors require a same-release standalone native `mrsm` image (`bmeares/meerschaum:5.1.1-full` by default). Override in `stack.yaml`:

```yaml
# Stack configuration: stack.yaml
executor:
  image: registry.example/meerschaum-executor:5.1.1
```

Register the team with `POST /teams/wetlands/register` (or `PATCH /teams/wetlands/edit`, preserving existing `attributes` keys):

```json
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

Reconcile from the Stack host as UID `1000` (so file-sourced Compose secrets match the container's `1000:1000` user):

```bash
mrsm stack config
mrsm stack up -d
```

Meerschaum creates a UUID-named service, isolated bridge network (joined only by `api` and the database), and private work (`/meerschaum`) and control (`/var/lib/meerschaum/factory`) volumes. Grant updates propagate dynamically; limit/network/image changes require `mrsm stack up -d`. Deleting a team removes its service and network while retaining both volumes.

## Outbound connections

| `egress` | Network reach |
|---|---|
| `proxy` (default) | Main API, assigned Stack DB, HTTP (80) / HTTPS `CONNECT` (443) via filtering proxy, plus declared relays. |
| `none` | Main API, assigned Stack DB, and declared relays only. |
| `open` | Unrestricted bridge outbound access. |

The proxy blocks private, local, metadata, and reserved IPs. Declare up to 64 external DNS `relays` (`host` + `port`; numeric IPs and loopback are refused) and keep the original hostname with full TLS verification in your connector. Relays pin upstream IPs during `mrsm stack up -d`—re-run it after upstream DNS changes. Add external firewalls if you need to block Docker DNS forwarding.

## Independently managed executors

For `mode: api`, add an admin `api:` connector in `meerschaum.yaml` (behind a TLS reverse proxy for HTTPS; no admin passwords/tokens), set `attributes.execution` to `{"mode": "api", "connector": "api:wetlands_executor"}`, and set `execution: callback_url`:

```yaml
connectors:
  api:
    wetlands_executor:
      uri: https://executor.wetlands.example:8989
```

Install the team's public verifier from `<main-root>/executors/desired/team-<UUID-without-hyphens>.json`, create disjoint `0700` work and control directories, and launch the standalone native binary:

```bash
sudo install -d -o mrsm-wetlands -g mrsm-wetlands -m 0700 \
    /var/lib/meerschaum-wetlands /var/lib/meerschaum-wetlands-control

sudo -u mrsm-wetlands env \
    MRSM_ROOT_DIR=/var/lib/meerschaum-wetlands \
    MRSM_EXECUTOR_BOOTSTRAP=/etc/meerschaum/wetlands-verifier.json \
    /usr/local/bin/mrsm start executor --host 0.0.0.0 --port 8989 \
    --control-root /var/lib/meerschaum-wetlands-control
```

- **systemd:** Use a dedicated user, explicit `--control-root`, and systemd resource/network caps. Meerschaum's systemd job backend requires **systemd 235+** for `UnsetEnvironment=`; regenerate existing units after upgrading.
- **Kubernetes:** Run one non-root Pod per team with read-only rootfs, separate work/control mounts, read-only verifier mount, and NetworkPolicies (`egress`/`limits`/`runtime` apply only to Stack Compose).

## Resource and host requirements

Generated Stack executors run non-root (`1000:1000`) with dropped capabilities, `no-new-privileges`, private IPC, and read-only rootfs:

| Limit | Default | Notes |
|---|---|---|
| `limits.memory_mb` | `4096` MiB | Shared by workers/builds; swap disabled (`mem == memswap`). |
| `limits.cpus` / `limits.pids` | `2.0` / `1024` | CPU quota and process/thread cap. |
| Persistent disk | Unbounded | Enforce volume quotas and monitor outside Meerschaum. |

- **Docker Engine 28+** is required for isolated bridge gateway modes.
- **Linux confinement:** Requires Landlock ABI 6+ (Linux 6.12+) signal scoping plus proven seccomp Unix-socket denial (x86-64/AArch64) or Landlock ABI 9+ (Linux 7.1+) native socket denial, verified via startup probes under the standalone native `mrsm` binary (stock Linux 6.8 runs ordinary workloads, not confined profiles). See [The filesystem sandbox](api-instance/teams.md#the-filesystem-sandbox).
- **Custom runtime:** `attributes.execution.runtime` (e.g., `"runsc"`) selects a preconfigured OCI runtime; validate probes and workloads on it yourself.

## Identity, history and current authority

Execution records bind to registered owner and team UUIDs. Renaming a team preserves history; deleting a team leaves private history as an owner/admin archive, and recreating a name never inherits prior records.

Viewing history never authorizes re-running it—every start or restart rechecks original source identity, scopes, membership, and grants.

Executors hold only filtered config and short-lived credentials (control lease ~15s renewal / 90s expiry; callback creds ≤30s). Keep clocks synced; if the main API is unreachable past expiry, execution stops until the executor restarts and reauthorizes. Back up the main API root (executor registry, signing keys, job catalog), database, and team volumes together—partial restores fail closed.

## Migrate an existing deployment

1. **Back up:** Instance DB, main root, job state, and team volumes; record team UUIDs, databases, grants, and placements.
2. **Upgrade together:** Match main API and executor versions; verify native launcher, Landlock/seccomp probes, Docker 28+, and disk quotas.
3. **Configure placement:** Set `attributes.execution`, limits, egress/relays (DNS + TLS), and `execution: callback_url`.
4. **Reconcile:** Run `mrsm stack up -d` (or update systemd/Kubernetes + verifier) and regenerate scoped systemd job units.
5. **Recreate workloads:** Moving to a container does not migrate host jobs, venvs, or terminals—recreate them through the main API.
6. **Verify:** Test a job, plugin sync, DB connection, log stream, terminal, resource caps, and revocation before widening access.
