# Team execution environments

!!! warning "5.1.0 candidate draft"

    This guide describes the executor deployment contract being prepared for 5.1.0.
    API-only jobs, bounded SQLite/GPKG source paging and confined plugin discovery are
    implemented. Firefox and Chromium rendered a Team plugin's Dash page and updated its
    Plotly chart through a private callback. Chromium also checked that changes to plugin
    grants and the console session took effect on the next page visit. Signed-in users can
    open an API-only terminal on a native Linux host or its selected Linux executor.
    Several scoped backend and graph operations remain unavailable.
    Implemented paths still require validation with the final release artifacts. Candidate
    implementation does not by itself expand the commands a caller may run.

A team may run its operations in a separate executor while the main API keeps authentication,
permissions, team administration and routing. The executor owns that team's jobs, engine work,
plugins, virtual environments, work files and terminals. Ordinary reads remain on the main API.
Grafana continues to query the assigned database with its own database permissions.

Selection is optional per team. A team without an execution selection keeps host execution and
its existing isolation rules. Once a separate executor is selected, an unavailable or incompatible
executor refuses work. It never authorizes running that work on the main API host instead.

Execution placement and credential access are separate decisions. The main API chooses a
profile from the authenticated original source and records it for the launch:

| Profile | Configuration available to the process | Intended work |
|---|---|---|
| Team | Only the team's granted connectors, credentials and plugin settings | Work authorized to run code as that team |
| API-only | Logical instance names backed by a short-lived API credential; no reusable database credentials or shared plugin settings | Authorized commands whose source is not allowed to run code with the team's credentials |

Users cannot request the Team profile through a job property. A later promotion, a manager's
restart request or a credential rotation does not upgrade a recorded API-only job. API-only
commands access data through the main API, which checks current scopes and object access on
each request. A logical `sql:wetlands` instance keeps that identity without becoming a native
database connection inside the command.

A user or token authorized to run Team code can submit a one-shot plugin sync through
`POST /jobs/{name}`. Supply the Team's granted Python plugin, metric and data instance, for
example `["sync", "pipes", "-i", "api:wetlands_writer", "-c", "plugin:collector", "-m", "stations"]`.
The data connector must be granted to this Team, reserved for it in the connector's `teams`
setting, and configured with its own host, username, password and upstream `sql:` instance.
The executor performs reads and writes as that service account. Its separate callback to the
main API handles jobs and logs. Restarts recheck the original source and Team grants. Rotating
the service password preserves the job. Changing the endpoint, account or upstream instance
requires creating a new job.

API-only commands read registered SQLite/GPKG sources and explicitly approved PostgreSQL base
tables through the main API's native source broker. A complete response is limited to 100,000
rows and 64 MiB. Larger supported reads use
pages from one frozen result, with at most 2,048 rows and 8 MiB per page. Each page rechecks the
original caller, source grants and registered query definition; a credential rotation does not
create a new source identity. Unsupported sources, changed definitions and exceeded limits
return an error. A partial result is never reported as complete.

Snapshots use private main-API disk space: at most eight active snapshots, 512 MiB per snapshot
and 2 GiB in total. They expire after 30 seconds of inactivity or five minutes overall. Source
execution also has a bounded budget, so paging does not promise arbitrarily large or long-running
queries. It is separate from continuous streaming subscriptions.

### Commands under the caller's identity

Through `POST /execute` or a saved API-only job, `show pipes`, `show graph` and
`show runs` can omit selectors. The main API filters the results to the caller's visible
registrations. Use `watch runs --nopretty` for run events and
`show logs JOB --nopretty` for a named job the caller may read.

A caller with Pipeline registration or edit permissions can submit a definition through
`register pipelines NAME --params JSON` or `edit pipelines NAME --params JSON`. Supply the
current revision in the edit. For example, the following argument array changes a description
at revision 1.

```json
["edit", "pipelines", "daily-stations", "--params", "{\"revision\":1,\"description\":\"Daily station readings\"}"]
```

To copy one registered pipe between SQL instances served by the same main API, provide its
connector, metric and location explicitly. The caller needs access to both instances and
permission to read, register and write pipes. An existing destination is skipped.

```bash
mrsm copy pipes sql:archive -i sql:main -c manual:posted -m readings -l None --sync-data all --yes
```

### API-only terminals

Open **Web Terminal** in the signed-in console to run supported Meerschaum commands, such as
`show version`. The native Linux host or the team's selected Linux executor runs the terminal
in its API-only environment. The administrator must enable the web terminal and non-admin
actions. The operator needs `webterm:use`, `actions:execute` and `jobs:execute`.

The main API checks each command against the user's current permissions. Each new terminal
window uses the same restricted command environment. With tmux enabled, reconnecting to a
live session retains its windows while the original login, team and execution grants remain
valid. A change of placement or plugin grants may require a fresh terminal.

## Current candidate coverage

Selecting an executor or seeing an action in a command menu does not establish that its source,
destination or graph operation is supported. The main API still authorizes each operation and
refuses a path whose required backend checks are unavailable.

| Operation through the API-only profile | Current boundary |
|---|---|
| Registered SQL source reads, including paging | SQLite and GPKG definitions, plus explicitly approved PostgreSQL base tables on the same instance as their registration. |
| Authenticated mutations on the main API | SQLite-family paths support scoped mutations. MySQL and MariaDB support bounded appends to an existing InnoDB table through a same-main API binding. |
| Atomic replacement from a paged registered source | SQLite source and SQLite destination served by the same main API, using a registered destination in SQLite's main schema. GPKG/PostgreSQL replacement and unsupported custom observers are refused. |
| Writes through a logical API instance | A main-API-owned binding to that same main API's native instance can retain both the original caller's and configured service account's authority. A registered SQLite pipe on this path accepts typed binary data, including a new nullable BLOB column. The run records the physical pipe registration. External API forwarding without equivalent checks remains unavailable. |
| Graph and time-window execution | Direct SQLite sync and verify support explicit bounds. A saved Pipeline can sync independent SQLite targets, or sync children that share the same set of replacement parents. Other dependency layouts and implicit clears or rebuilds remain unavailable. |

The scoped PostgreSQL path currently supports reads from ordinary registered tables. TimescaleDB,
PostGIS, partitioned targets and tables with custom hooks have additional database behavior.
GPKG reserves its shared catalog and RTree tables for internal use.

### Read a registered PostgreSQL table

Register the pipe on its PostgreSQL source instance, with the same `sql:` key for the source
and the instance. Give the pipe an explicit `target` and the matching schema. In the host's
SQL connector configuration, approve the pipe's registration ID and exact physical table.
For example, registration 42 on `sql:water` can point to `public.station_samples`.

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

The PostgreSQL connector role must be able to read and lock its registrations and approved
tables on a UTF-8 primary. The API caller still needs its normal source and pipe permissions.
The broker accepts ordinary base tables with built-in column types, structured bounds and
scalar filters. It reads only the approved table. SQLite and GPKG use their registered fetch
definitions as before. PostgreSQL table reads do not accept stored fetch SQL, views, inherited
tables, or cross-instance registrations.

### Append to an existing InnoDB table

A same-main API connector can append a graph's admitted batch to an existing registered
MySQL or MariaDB InnoDB table. The launched operation must use an original user account and
the API connector must use its own configured password service. Both accounts' current
permissions and the exact pipe registration are checked on the held native transaction.

Give the pipe an explicit `target` and a caller-supplied signed `BIGINT` primary key. Declare
the dtypes for every column. This path accepts complete rows of signed `BIGINT` and
`BLOB` values in batches of up to 512 rows and 4 MiB of typed values. Set
`check_existing=false` for append semantics. The database account needs a direct
`TRIGGER` privilege on that database so the writer can verify that the target has no
triggers. This adapter applies to plain, unpartitioned tables whose schema needs no change.

### Direct SQLite sync and verify

A caller with `actions:execute`, `pipes:write` and edit access to a registered pipe can use
`POST /actions/sync` or `POST /actions/verify` to select rows from a SQLite source on the
pipe's own SQLite instance. For example, this request selects station 10 at integer ticks 2
through 4. Verification applies the same selection to each chunk.

```json
{
  "action": ["pipes"],
  "connector_keys": ["sql:main"],
  "metric_keys": ["station-samples"],
  "location_keys": ["None"],
  "mrsm_instance": "sql:main",
  "begin": 2,
  "end": 5,
  "params": {"station": 10}
}
```

Each entry in `params` matches one integer value. Multiple entries must all match. The
destination table must already contain each filter column, and the pipe must declare its
integer dtype. Use data column names made up of letters, digits and underscores. The five
catalog fields (`pipe_id`, `connector_keys`, `metric_key`, `location_key` and `parameters`)
are reserved for selecting pipe registrations.

When incoming rows introduce ordinary nullable value columns in a supported SQLite table,
the sync adds those columns and writes the rows in one transaction. Existing rows receive
SQL `NULL` for each new value unless the batch supplies an update. A failed transaction
rolls back both the column additions and the row changes.

### Run a saved SQLite Pipeline

Use `POST /pipelines/{name}/sync` or the HTTP MCP tool `sync_pipeline` to run a
registered Pipeline whose explicit targets are SQLite pipes on one instance. Main checks
the caller's `pipes:write` and `jobs:execute` permissions, each selected pipe and the saved
Pipeline revision. It records the run under one ID and gives each pipe its own data window.

For example, this request syncs both members of the saved `daily-stations` Pipeline
from integer tick 5 up to, but excluding, tick 8.

```http
POST /pipelines/daily-stations/sync
Content-Type: application/json

{"begin": 5, "end": 8, "run_id": "daily-stations-12"}
```

The REST endpoint accepts an optional run ID. The MCP tool generates the run ID and returns
it with the job name. Use a new run ID to launch the Pipeline again. The API-only profile
currently accepts the saved graph order and explicit targets with compatible bounds.
Tag-based membership, saved execution defaults and automatic parent or child expansion
require additional support.

A Pipeline may explicitly select parents and children on the same SQLite instance when every child
depends directly on every selected parent. All selected pipes must read from that instance and use
integer axes with matching units. Supply both bounds and use the default dependency options.
Each parent must have an existing target and use `fetch.replace`. A child may already contain
data. It must use normal sync, have an unchanged definition and have no saved cursor for any
selected dependency. Main waits for all parent replacements to commit and uses the lowest
parent watermark and each child's current watermark to plan the remaining work. It issues all
child windows together. For example, if two parents finish at ticks 6 and 7, a request for
ticks 5 through 7 can give a new child ticks 5 and 6. Main checks the selected dependency
layout before starting the Pipeline.

### Saved sync and verify jobs

An API-only job can run `sync pipes` or `verify pipes` against one registered pipe. Submit the
command to `POST /jobs/{name}` as a JSON array. Supply the connector and metric. Direct actions
and saved jobs use the configured instance when you omit `-i`. You can omit `-l` when the
connector and metric match exactly one registered location on that instance. Use `-l None`
to select the pipe without a location explicitly. This job syncs integer ticks from 5 up to,
but excluding, 8.

```json
["sync", "pipes", "-i", "sql:main", "-c", "sql:main", "-m", "ticks", "-l", "None", "--begin=5", "--end=8"]
```

Creating the job requires `jobs:write`, `jobs:execute` and `pipes:write`, along with permission
to edit the pipe. The server's ordinary job policy also requires the creating user to have
`actions:execute`. A caller who can see and start the job can run it again with
`POST /jobs/{name}/start`. Each start of a stopped job rechecks the original source and pipe
registration, evaluates relative bounds against the new start time and issues fresh data windows.
The job listing retains the submitted command. A revoked or expired API key, a lost permission
or a replacement pipe registration prevents a new launch.

Use `--schedule` (or `-s`) with the usual Meerschaum schedule expressions and optional
`--jitter` to run on a schedule. Use `--loop` with optional `--min-seconds` for repeated
runs or `--restart` for automatic restarts. A looping or scheduled command also requires
`actions:execute` on the submitting credential. Main records the next fire time, waits for the
previous run to finish and rechecks the original credential and data bounds before each run.
For example, submit this command to verify the pipe every five minutes.

```json
["verify", "pipes", "-c", "sql:main", "-m", "ticks", "-l", "None", "--schedule", "every 5 minutes"]
```

After Main restarts, one overdue fire resumes the existing cadence. A fire interrupted during
launch requires an explicit stop and start. Stop or pause the job to suspend the schedule and
start it to resume. `GET /jobs/{name}` returns the submitted command and the schedule's
`next_fire`, `last_fire`, `firing` and `error` fields. An authorization failure stops the
schedule. If a previous data write has unresolved commit evidence, Main also stops the schedule.
Inspect the original run and its target before starting further work.

Saved API-only pipe jobs accept a single command and optional `--begin` and `--end`. When the
creating user has selected plugin sources, Main rechecks the exact source set and its declared
CLI arguments and connector capabilities before issuing the worker's data windows. For clients
sending a job object, the standard generated label and user-job history marker are accepted.
Action chains, bulk selectors and custom job properties need separate graph capture.

## Plugin metadata and private handlers

Plugin metadata is discovered in a confined process without database credentials or network
access. Its cache belongs to the exact selected source set so each team uses its own approved
catalog. The server invokes admitted API and MCP handler declarations from supported sources in
a private API-only process. It checks the original caller's current access, including when a
code-running team supplies the source. The main API reads the handler metadata without importing
plugin code.

A read-only planner may use an already validated metadata cache. Missing or unknown connector
classification remains unavailable; it is not treated as a non-streaming or native connector.
An administrator may need to refresh confined metadata before that source can be planned.

The candidate page renderer supports inert HTML and a bounded subset of standard Dash components,
callbacks and Plotly figures in an opaque iframe. In Firefox and Chromium, entering `rows` and
pressing **Update** on a Team page drew a bar labelled `rows` with value 2. In Chromium, the
page also rendered after navigating away and returning. Removing the Team's plugin grant made
the page unavailable on the next visit. Restoring the grant restored the chart. Ending the
console session closed access on the next visit. The renderer excludes arbitrary plugin
JavaScript, active HTML and resource-bearing figure commands.

Confined single-file Rust sources require an administrator-selected immutable offline toolchain,
dependency vendor tree and lockfile. Merely installing Cargo or setting a runtime path does not
provide that capability, and private execution does not fall back to an ambient compiler.
For a remote executor, provision the Factory signing key and install its public pin on Main.
The pin identifies the Team, executor generation, SDK and compiler. Main verifies the
Factory's signed receipt for the completed metadata job before caching its declarations.
It checks the installed pin and the selected sources again when reusing the cache.
This path supports single-file Rust plugins. Native package sources remain unsupported.

## Choose a deployment

| `attributes.execution.mode` | Administrator provides | Where to apply infrastructure changes |
|---|---|---|
| `host` (default) | The existing host and its required process isolation | Host configuration |
| `container` | A Meerschaum Stack with Docker Engine 28 or newer | `mrsm stack up -d` |
| `api` | An independently managed native executor and a main-API `api:` connector | Your systemd, Kubernetes or other deployment tooling |

For `container` and `api`, the team must be isolated and have a registered UUID. Team names are
labels: a rename keeps the same executor identity and state volume. A newly registered team with
an old name receives a different identity.

The API never provisions containers itself. Do not mount a Docker socket into the API or an
executor, add a privileged provisioner, or mount the main API's root into a team executor.
Registering the team's desired settings and provisioning its environment are separate steps.

An administrator may require separate executors for isolated teams allowed to install plugins:

```yaml
# Main API configuration: api.yaml
execution:
  require_team_executor: true
```

This policy defaults to `false`. Provision and verify an executor before enabling plugin writes.
An unsupported host or an unavailable executor gives an error; changing this policy does not
create the missing infrastructure.

## Configure the return path

Jobs need an origin at which their bounded credentials can reach the main API. Set it in the
main API's trusted configuration:

```yaml
# Main API configuration: api.yaml
execution:
  callback_url: https://main.example:8989
```

Use an HTTP or HTTPS origin without a path, query, fragment or embedded credentials. The
address must be reachable from the selected execution environment and use normal TLS
verification. It is never inferred from an incoming Host header.

Generated Stack executors default to the internal `http://api:<port>` origin when this setting
is absent. Independently managed executors and API-only jobs on the main host currently need
an explicit origin. For host-only access, use the main listener's actual loopback address and
port when it is reachable from the job; do not use a wildcard bind address as a destination.

API-only host jobs also require the main API to run under the trusted standalone native `mrsm`.
A wheel-only Python API entry point may still serve ordinary API requests, but cannot admit
these confined launches. Set the callback origin and update the host launcher before migrating
jobs whose users are not allowed to run code with team credentials.

## Provision a Stack executor

Use a trusted standalone native `mrsm` of the same release as the main API. The generated
executor, HTTP proxy and destination relays all require native entry points. A Python console
script or wheel-only image cannot establish the required confinement before Python starts.

The default executor image is `bmeares/meerschaum:5.1.0rc1-full` for this release candidate, independently of a customized
`services.api.image`. For an administrator-built image, set the separate Stack setting:

```yaml
# Stack configuration: stack.yaml
executor:
  image: registry.example/meerschaum-executor:5.1.0
```

The override must contain the same-release standalone native CLI and the dependencies needed by
the team's plugins. It applies to the generated executor, proxy and relays. Empty or malformed
settings are refused. Wheel-only bootstrap and a release mismatch are refused rather than
reported ready. Choosing the override is the administrator's assertion about its contents; a
custom tag alone does not establish compatibility. The `slim` image remains a small Python image.

First configure the team's assigned database and connector grants. For example, after preparing
`sql:wetlands` and the named users, an administrator can send this JSON body to
`POST /teams/wetlands/register` on the main API using their authenticated administrator session:

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
      "limits": {
        "memory_mb": 4096,
        "cpus": 2.0,
        "pids": 1024
      },
      "relays": [
        {"host": "database.partner.example", "port": 5432},
        {"host": "database.partner.example", "port": 8883}
      ]
    }
  }
}
```

Replace the example users, connector and destinations with your own. Relay declarations grant
network reach; they do not register connectors, grant plugins or create database users. Omit
`relays` when the team needs no external non-HTTP service.

For an existing team, use `GET /teams/wetlands` and then
`PATCH /teams/wetlands/edit`. Supplying `attributes` replaces the whole attributes map: preserve
the existing connector, plugin, database and other settings you intend to keep. Team managers
may manage membership but cannot change these administrator settings. The CLI's `edit teams`
membership editor does not configure `attributes.execution`.

On the host that owns this Stack root, inspect the model and apply it:

```bash
mrsm stack config
mrsm stack up -d
```

Generated executor services run as `1000:1000`. The administrator invoking this provisioning
path must currently have UID 1000; another UID is refused before image inspection or key creation.
[File-sourced Compose secrets](https://docs.docker.com/reference/compose-file/services/#long-syntax-5)
retain their host ownership, so a Compose `uid` setting cannot repair
a differently owned private key. Keep private files restricted to their intended service identity.

Use the same root and Stack project as the main API. The main API writes the public desired
deployment into the Stack's shared deployment directory. The generated service, network and
two private volumes use the team's UUID. The executor mounts the work volume at `/meerschaum`
and the control volume at `/var/lib/meerschaum/factory`. No team service publishes a host port
by default. Of the standard Stack services, the main API and database join the team network.
Grafana and Valkey keep their existing networks.

Selecting a container before provisioning leaves team execution unavailable. Limits, networking,
runtime, image and release changes require an administrator to reconcile with `stack up`.
Current connector and plugin grants are delivered through authenticated configuration updates;
revocation does not wait for another `stack up`. A failed configuration update does not authorize
continued use of the old configuration.

Deleting a team revokes its control authority. The next `stack up` removes its owned services
and obsolete networks. Reconciliation keeps both private volumes. Prune or delete retained
volumes only after reviewing backups and retention requirements; an unrelated service or volume
is never adopted merely because its name resembles an executor's.

## Resource and host requirements

The generated service uses a non-root user, no capabilities, `no-new-privileges`, private IPC, a
read-only image filesystem and temporary writable filesystems. It keeps team work and executor
control state on separate private volumes.

| Setting | Default | What to plan for |
|---|---|---|
| `limits.memory_mb` | `4096` MiB | The total container limit; workers and plugin builds share it |
| Additional swap | None | The memory-plus-swap limit equals the memory limit; host support must enforce it |
| `limits.cpus` | `2.0` | CPU quota in CPU equivalents |
| `limits.pids` | `1024` | Processes and threads count toward the limit |
| Persistent disk | No generated quota | Set a volume/filesystem quota and monitor growth outside Meerschaum |

The work volume holds durable team files and virtual environments. The control volume holds job
records, logs and executor control state. An unbounded volume can fill a shared host disk even
when memory and process limits hold. Image caches, database storage and retained retired volumes
also need separate capacity planning.

Containers share the host kernel. Docker Engine 28 or newer is required for the generated bridge
gateway isolation; an older daemon is refused. Restricted team networks disable IPv6 in the
executor and configure [isolated bridge gateway modes](https://docs.docker.com/engine/network/port-publishing/#gateway-modes)
for IPv4 and IPv6. Docker Desktop still
depends on its Linux VM's kernel and container capabilities.

Where a launch requires process confinement, it requires an actual successful filesystem-denial
probe, Unix-socket denial and signal scoping. This needs Linux Landlock ABI 6 or newer, plus
verified seccomp socket denial on supported x86-64/AArch64 hosts, or Landlock ABI 9's native
Unix-socket restrictions. The effective kernel and container policy must allow these mechanisms.
A kernel version label alone does not prove the launch is supported.

Use the standalone native launcher for these profiles. Embedded Python SDK entry points and
wheel-only hosts fail closed when they cannot prove native launcher provenance. Setting
`MRSM_BIN` in a job does not provide that proof. Keep administrator interpreter and runtime paths
trusted; persisted team environment overrides do not choose the host's interpreter or loader.
Plugin Python starts after confinement, with each team's writable environments kept separate.

An administrator may set `attributes.execution.runtime`, for example `"runsc"`, to select a
preconfigured OCI runtime. Meerschaum does not install that runtime or certify a stronger boundary
from its name. Validate the same kernel probes and legitimate workloads with the chosen runtime.

A stock Linux 6.8 kernel lacks Landlock signal scoping. You can run Meerschaum normally on that host. To use these executor profiles, run them on a compatible Linux kernel and verify the boundary described above.

## Outbound connections

| `egress` | Generated access |
|---|---|
| `proxy` (default) | Main API, assigned Stack database, HTTP port 80 and HTTPS `CONNECT` port 443 through a public-destination filtering proxy, plus declared relays |
| `none` | Main API and assigned Stack database, plus any explicitly declared relays |
| `open` | An ordinary bridge with unrestricted outbound reach |

The HTTP proxy rejects private, local, metadata and reserved destinations, including disallowed
DNS answers, and connects to the checked address. It is not a generic TCP proxy. A client that
ignores the proxy settings does not gain an arbitrary outbound route in `proxy` mode. Database,
MQTT and other TCP clients need explicit destination relays when they are outside the attached
database service.

Relays are an administrator's explicit exception to that public-internet policy. They may reach
an approved private database or broker. Each entry authorizes one DNS hostname and port; at most
64 distinct hostname/port pairs may be declared. Review the destination's own authentication and
the data reachable through it before granting access.

Keep the original DNS hostname in the connector configuration and enable the driver's normal
TLS certificate and hostname verification. All declared ports for one hostname share one relay
service and one alias on the owning team's network. For example, both ports in the registration
body above use `database.partner.example`; the connector is not rewritten to a generated service
name. TLS remains end to end, with its original server name. Do not disable certificate checks.

The deployment host resolves the approved hostname when generating the Stack model, using a
bounded native resolver. The relay connects only to those pinned upstream addresses and its
declared ports. It does not resolve its own alias or continuously follow DNS changes. After an
upstream DNS change, regenerate and apply the model with `mrsm stack up -d`; account for the
resulting reconnection. A failed or disallowed resolution refuses generation. This is explicit
administrator reconciliation, not transparent DNS refresh.

Numeric-IP relay endpoints are refused in `proxy` and `none` modes. Use a DNS name whose
certificate matches that name; there is no verified driver-wide TLS server-name override for
isolated numeric endpoints. Loopback and other unusable relay addresses are also refused.

These settings are not a complete network data-loss boundary. Docker's DNS service may still
forward external queries. The main API and assigned database remain reachable and need their own
access controls. `open` can reach host, private-network and metadata services according to the
host's routing. Apply additional network controls outside Meerschaum when your policy requires
stricter DNS, outbound, host or cross-tenant isolation.

## Independently managed executors

Use `mode: api` when an administrator provisions the executor with systemd, Kubernetes or another
supervisor. Register an administrator-owned connector on the main API:

```yaml
# Main API configuration: meerschaum.yaml
connectors:
  api:
    wetlands_executor:
      uri: https://executor.wetlands.example:8989
```

The native gateway serves HTTP. An HTTPS connector such as the example requires an
administrator-controlled TLS proxy with normal certificate verification; starting an executor
does not configure TLS automatically. The endpoint must reach that gateway directly or through
the proxy. Do not expose the gateway's internal configured API router separately. The main API
signs control requests; ordinary account passwords or
reusable main-API administrator tokens do not belong in this connector or the team environment.

Keep the team's existing isolation and grants, and select:

```json
{
  "mode": "api",
  "connector": "api:wetlands_executor"
}
```

That object is the value of `attributes.execution`, not the full team-edit body. Configure the
[return path](#configure-the-return-path) with a main-API origin reachable from this executor.

After the main API reconciles the team, install only that team's public verifier from
`<main-root>/executors/desired/team-<team-UUID-without-hyphens>.json` into the executor's deployment.
Give each executor two private writable directories. `MRSM_ROOT_DIR` holds the team's work and
durable files. `--control-root` holds executor control state and must be an absolute directory
outside the work root. Neither directory may contain the other. Use ordinary directories with no
symbolic links in the control path, and keep their parents under administrator control.
If you install Factory response signing material, create the control directory in advance with
mode `0700` and the executor service account as its owner. The service checks that root or the
service account owns each parent directory and that only its owner can write to it.

For example, with an existing `mrsm-wetlands` service account and group, create both directories
and run the same-release standalone native binary as that account:

```bash
sudo install -d -o mrsm-wetlands -g mrsm-wetlands -m 0700 \
    /var/lib/meerschaum-wetlands /var/lib/meerschaum-wetlands-control

sudo -u mrsm-wetlands env \
    MRSM_ROOT_DIR=/var/lib/meerschaum-wetlands \
    MRSM_EXECUTOR_BOOTSTRAP=/etc/meerschaum/wetlands-verifier.json \
    /usr/local/bin/mrsm start executor --host 0.0.0.0 --port 8989 \
    --control-root /var/lib/meerschaum-wetlands-control
```

If you omit `--control-root`, the launcher uses `/var/lib/meerschaum/factory`, the generated
Stack path. That directory requires UID and GID `1000:1000` and mode `0700`. Its parent
directories must be root-owned and unwritable by group or others. Use an explicit control path
for an independently managed service account.

Bind this listener only on the intended private network, or restrict access at the deployment
boundary. The verifier is public control material for this team and generation. Private signing
keys, the main job catalog and the main API's credential files stay with the main API. A listener
awaiting signed configuration is not proof that the team is ready to execute work.

For a systemd deployment, use a dedicated service identity, a same-release native `ExecStart`,
the two environment settings above, and `--control-root` in `ExecStart`. Set the service identity
as the owner of both directories. Apply memory, CPU and task limits, filesystem restrictions and
outbound controls in the service deployment.
Any administrator-installed private Factory material must deliberately match the actual service
identity and remain protected from worker processes; a read-only mount alone does not establish
that isolation. The same ownership requirement applies to independently managed Kubernetes Pods.
Separately, scoped jobs using Meerschaum's systemd job backend require systemd 235 or newer for
the final [`UnsetEnvironment=` rules](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#UnsetEnvironment=).
Regenerate existing job units after upgrading; a previously
written unit or already-running process does not acquire the new launch contract automatically.
Do not rely on an absolute `ExecStart` alone to remove inherited dynamic-loader environment.

For Kubernetes, use a dedicated Pod per team with separate persistent mounts for its work and
control directories, writable by the Pod's service identity. Mount the team's public verifier
read-only and use the same environment settings and native command. Configure a non-root security
context, read-only image filesystem, no added capabilities, resource limits, and network policies
that provide the intended main-API, database and outbound paths. Check actual kernel/seccomp
behavior on the scheduled nodes. Kubernetes policies do not automatically reproduce the Stack's
HTTP filtering proxy and original-host destination relays. Deploy those controls explicitly where
needed.

The `egress`, `limits` and `runtime` container settings generate Stack infrastructure. `mode: api`
does not apply those settings to an external service or create its network policies for you.

## Identity, history and current authority

Execution records retain registered owner and team identities independently of display names.
Renaming a team keeps its history; deleting and registering a team with the same name does not
give the replacement team the old team's private records. Recreating a username similarly does
not transfer the former account's private host or delegated history.

The same registered owner can retain access to their own history after a password change or
team departure. Administrators and current managers of the recorded team identity retain their
authorized oversight. Deleting that team leaves its registered private history as an owner/admin
archive; it does not silently become Everyone's history. Older private records without reliable
registered provenance require administrator recovery rather than adoption by a matching name.

Reading retained history does not authorize running its command again. A new launch or restart
requires the current original source identity, scopes, team membership and grants. A manager's
ability to inspect or control a record does not replace that source with the manager's credentials.
Password, token, account, team and permission changes may therefore prevent a restart while
leaving authorized historical metadata and logs readable.

The executor receives only the team's filtered configuration and bounded, short-lived callback
credentials. The main API remains responsible for current authorization. It does not accept
executor-selected owner identities, filesystem paths or reusable global credentials. Keep clocks
synchronized; signed control and callback credentials have short expiry windows.

## Restarts, outages and retained state

Browser reconnects can reattach to a still-running authorized terminal and its tmux windows
under either profile. Recreating the executor ends its processes. The private state volume
preserves its files.

Authenticated control is renewed approximately every 15 seconds and has a 90-second lease.
Callback credentials last at most 30 seconds and are rotated under a bounded source lease.
Loss of the main API is not an unlimited offline mode: after the control or source lease expires,
execution is stopped or refused. An executor whose control lease expires must be restarted and
reauthorized before it accepts work again. A short main-API restart can only preserve work while the relevant leases and
processes remain valid; extended-outage survival is not promised.

During a controlled configuration rebuild, the executor records bounded recovery intent for work
observed running before the controlled stop. Existing job definitions can resume only after fresh
main-API authorization at the captured revision. Paused work stays stopped until an explicit
authorized start; stopped one-off jobs are not replayed. An uncertain crash during capture or stop
requires explicit recovery. Status and history reads do not restart work.

This is not an exactly-once job guarantee or a replacement for application checkpoints. Recovery
has bounded storage and may refuse when full or corrupt. Restore missing definitions only through
the main API's authorized start path; do not copy executor metadata into the main catalog. Full
main-restart, forced-crash and missing-definition acceptance remains part of candidate validation.

Back up the main API's private root, including its executor registry, signing material and canonical
job catalog, together with the instance data and each team's private work and control storage.
Losing private authority or restoring only part of this state fails closed. Restore a consistent
private backup and reconcile deliberately; generating a new key does not recover the old authority
or running work.

## Migrate an existing deployment

1. Back up the instance, main root, job state and team volumes. Record each registered team's UUID,
   assigned database, grants and execution placement.
2. Upgrade the main API and executor runtime to the same release. Verify the trusted native launcher,
   effective kernel protections, Docker version where applicable, and disk quotas before accepting
   team execution.
3. Choose each team's placement, resource limits and outbound policy. Review private relays as
   explicit network grants, arrange DNS/certificate names for isolated external services, and set
   the main API's callback origin where required.
4. Reconcile the Stack, or update the independently managed service and its public verifier. Recreate
   existing scoped systemd job units with the new environment handling before using those units.
5. Migrate files and workloads deliberately. Selecting a container does not move host jobs, virtual
   environments, plugin build products or running terminals into it. Recreate workloads through the
   authorized main API and verify database paths from inside their new execution environment.
6. Check a representative job, plugin operation, database connection, log stream and terminal;
   confirm the intended memory/PID limits, outbound behavior and revocation before widening access.

Keep retired volumes until their retention period ends. Coordinate upgrades and DNS changes with
workloads that need checkpoints, and treat infrastructure reconciliation as an administrator task.
