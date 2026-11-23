# Meerschaum glossary

The vocabulary of Meerschaum 5: what each term means here, and the module that owns it. Use these
terms in code, comments, tests, issues and docs; the words under _Avoid_ name the same thing less
precisely or collide with another term. Decisions behind the terms are in [`docs/adr/`](docs/adr/).

## Pipes and data

**Pipe**:
A named stream of time-series rows, identified by connector keys, a metric key and an optional
location key, stored in one table (its target) on an instance.
Owner: `crates/meerschaum-core/src/pipe/`; Python façade `meerschaum/core/Pipe/`.

**Instance**:
The connector that stores pipes' metadata, data and internal state (users, tokens, plugins, engine
state). A pipe's instance is where it lives, distinct from the connector it fetches from.
Owner: `crates/meerschaum-connector/src/traits/mod.rs` (`InstanceConnector`).
_Avoid_: database, backend (for the instance role).

**Connector** / **connector keys**:
A configured source or destination of data (`sql`, `api`, `valkey`, a plugin's custom type).
Connector keys are its `type:label` string (`sql:main`, `plugin:noaa`).
Owner: `crates/meerschaum-connector/`; Python façades `meerschaum/connectors/`.

**Parameters** (raw vs resolved):
A pipe's settings dictionary; every key is a public contract (`columns`, `dtypes`, `fetch`, ...).
Raw parameters are what is persisted, templates included; resolved parameters have templates and
`MRSM{}` references substituted and are used only for execution.
Owner: `crates/meerschaum-core/src/pipe/` (`raw_parameters`, `resolve_parameters_with`).

**Template**:
A `{{ Pipe(...) }}` or `{{ self.parameters[...] }}` expression inside parameters, resolved to a
table reference or a value; `MRSM{a:b}` is the config-reference counterpart.
Owner: `crates/meerschaum-core/src/template.rs`.

**Sync**:
One fetch-and-write of new rows into a pipe: fetch, enforce dtypes, filter rows already present,
write to the instance. A bare `pipe.sync()` keeps v4 semantics; a graph sync goes through the
engine.
Owner: `crates/meerschaum-engine/src/sync.rs` (with instance writes in each instance connector).

**Verify**:
Re-syncing a pipe chunk by chunk across a bounded range to repair gaps and drift.
Owner: `crates/meerschaum-cli/src/native/verify.rs`.

**Backtrack**:
The window before a pipe's sync time that a sync re-reads (`fetch.backtrack_minutes`), so late
rows are caught.
Owner: `crates/meerschaum-core/src/pipe/chunk.rs`.

**Watermark**:
A pipe's sync time (`get_sync_time()`): the newest value on its datetime axis.
Owner: `crates/meerschaum-engine/src/state.rs`.

## Syncing Engine

**Engine**:
The dependency-aware syncer: builds the pipe graph, plans each node, runs nodes in dependency
order and records runs.
Owner: `crates/meerschaum-engine/`.

**Edge** (explicit vs implicit):
A parent-to-child dependency between pipes. Explicit edges come from `parents` / `children`
parameters; implicit edges come from `{{ Pipe() }}` templates (`system.engine.implicit_edges`).
`reference` / `references` are inheritance only, never an edge.
Owner: `crates/meerschaum-engine/src/graph.rs`.

**Plan** and plan kinds:
The planner's decision for one node in a run: `skip` (nothing changed upstream), `incremental`
(sync the tail up to the parents' watermarks), `backfill` (re-sync dirty ranges), `rebuild`
(definition changed: clear and re-sync; needs confirmation), `observe` (a stream source, read
only), `root` (no parents in the selection: a v4 sync).
Owner: `crates/meerschaum-engine/src/plan.rs` (`Action`).

**Dirty range**:
An interval a parent's sync changed, written to the ledger before the data commits, which drives
`backfill` in children. Each edge's cursor records how far the child has consumed.
Owner: `crates/meerschaum-engine/src/ledger.rs`.

**Lease**:
A per-pipe lock held on the instance while a node runs, falling back to a machine-local file
lock when the instance has none.
Owner: `crates/meerschaum-engine/src/leases.rs`.

## Jobs and processes

**Job**:
A named, supervised background command (`mrsm start job`), stored in the v4 `jobs/<name>/`
layout.
Owner: `crates/meerschaum-jobs/src/job.rs`.

**Daemon**:
A job's on-disk state and its detached supervisor process.
Owner: `crates/meerschaum-jobs/src/daemon/mod.rs`, `supervisor.rs`.

**Admission gate**:
The per-root startup gate every job passes before its worker starts: file-lock slots, jitter and
load checks for bulk starts ([ADR 0005](docs/adr/0005-job-startup-admission-gate.md)).
Owner: `crates/meerschaum-jobs/src/startup.rs`.
_Avoid_: startup lock, stagger (the flag `--no-stagger` bypasses the gate).

**Compose**:
A project file declaring pipes, plugins and jobs that `mrsm compose` brings up together.
Owner: `crates/meerschaum-compose/`.

**Worker**:
A pooled Python process that runs Python plugins and connectors for the Rust host, over an Arrow
IPC protocol ([ADR 0001](docs/adr/0001-python-reached-only-through-workers-and-hosts.md)).
Owner: `crates/meerschaum-worker/`; Python side `meerschaum/worker/`.
_Avoid_: executor (for this process).

**API host**:
The one Python process (`python -m meerschaum.api.host`) that serves `@api_plugin` routes, plugin
MCP tools and the Dash console, proxied by the Rust server.
Owner: `crates/meerschaum-server/src/host/mod.rs`; Python side `meerschaum/api/host/`.

**Native**:
Implemented in Rust. A native action is a CLI action implemented in `crates/meerschaum-cli/src/native/`;
a native plugin is a Rust plugin (a single `.rs` file or a crate directory) loaded through the C
ABI of `crates/meerschaum-plugin/`.

**Façade**:
The Python class or function that adapts arguments and calls one `meerschaum._core` binding
(`mrsm.Pipe`, `SQLConnector`, `get_config`). It owns no behavior of its own.
Owner: `meerschaum/core/`, `meerschaum/connectors/`; bindings in `crates/meerschaum-py/`.

## Executors

"Executor" names five different things in the code. **The bare word means a job executor**, the
public v4 surface (`mrsm.jobs.Executor`, `--executor-keys`); qualify every other meaning.

| Term | What it is | Where |
|---|---|---|
| **Job executor** | Where a job runs: `local`, `systemd`, `api:<label>`, or a plugin's custom type | `crates/meerschaum-jobs/src/executor.rs`, `systemd.rs`, `remote.rs` |
| **Plugin executor type** | A job executor a plugin declares (`executor_types` in its manifest) | `crates/meerschaum-plugin/src/executor.rs`, `crates/meerschaum-cli/src/native_executors.rs` |
| **Engine executor** | The engine's dynamic ready queue that runs planned nodes (HEFT, leases, retries), and its node runner | `crates/meerschaum-engine/src/executor.rs`, `native.rs` (`NativeExecutor`) |
| **Team executor** | A separate process (container or remote host) running one isolated team's ordinary API, jobs and terminals under the main API's signed control | `crates/meerschaum-server/src/executor/`, `crates/meerschaum-cli/src/server/executor/` |
| `executor` modules | The main-API side (client, job catalog, contexts, stack, factory) and executor side of team executors | `crates/meerschaum-cli/src/executor/` (`client/`, `jobs/`, `profiles.rs`, `registry.rs`, `stack/`) |

## Teams and access

**Team**:
A named group of users that owns pipes, jobs, connectors and plugins; every user is also in the
implicit Everyone team.
Owner: `crates/meerschaum-core/src/access/`.

**Isolated team**:
A team (`isolated: true`) whose members see only their teams' objects and whose code runs with
the team's configuration, an environment allowlist and the filesystem sandbox
([ADR 0007](docs/adr/0007-isolated-teams-scoped-processes.md)).
Owner: `crates/meerschaum-core/src/team_scope.rs`.

**Reservation**:
The named teams listed in a connector's `teams` attribute. Beyond gating connector access, an
instance's reservation extends **Everyone** on that instance to members and managers of the
reserved teams, so untagged pipes and saved Pipelines are usable without backfilling `access`
([ADR 0011](docs/adr/0011-instance-reservations-extend-everyone.md)). Distinct from a connector
grant, which lets a team use a connector without extending Everyone on it.
Owner: `crates/meerschaum-core/src/access/rules.rs`, `crates/meerschaum-core/src/team_scope.rs`.

**Acting team**:
The one team a caller acts for on a request: never for an administrator; for a user whose teams
(other than Everyone) are all isolated, the object's team or else their primary team. The console
also narrows it with the Team switcher.
Owner: `crates/meerschaum-core/src/team_scope.rs` (`acting_team`); `meerschaum/api/dash/viewer.py`.

**Viewer**:
Two meanings, kept apart by context: the least-privileged role preset (`Role::Viewer` in
`crates/meerschaum-core/src/access/`), and the console's per-request identity of the signed-in
caller (`Viewer` in `meerschaum/api/dash/viewer.py`).

**Main API**:
The `mrsm start api` server that owns identity, authorization, routing and team administration.
Code comments call it Main.
_Avoid_: host (collides with API host and host placement).

**Host placement**:
A team's default execution mode (`execution.mode: host`): its workers, jobs and terminals run on
the main API's host inside the team sandbox.
Owner: `crates/meerschaum-core/src/execution.rs` (`Mode::Host`).
_Avoid_: host executor (there is no executor process). The API-only job runtime on the main host
is the **host API-only runtime** (`JobPlacement::HostApi`, `crates/meerschaum-cli/src/executor/host_jobs/`, `executor/host_sources.rs`).

**Execution profile**:
The credential ceiling the main API records on a launch: Team (the team's own connectors), API-only
(a short-lived main-API credential, no database credentials) or Metadata (credential-free plugin
discovery).
Owner: `crates/meerschaum-server/src/executor/environment.rs` (`Profile`).

**Sandbox**:
The Linux Landlock rules (plus a seccomp Unix-socket filter where Landlock cannot deny sockets)
that a team process applies to itself before any team code runs.
Owner: `crates/meerschaum-core/src/team_scope/sandbox.rs`; enforced at process start by
`crates/meerschaum-cli/src/team/sandbox/`.

**Team execution**:
Running arbitrary commands (`sh`, `python`, plugin actions, terminals) for a team that may write
plugins, only inside that team's sandboxed processes and only when the sandbox covers sockets and
signals ([ADR 0009](docs/adr/0009-team-execution-requires-enforced-sandbox.md)).
Owner: `crates/meerschaum-server/src/team_execution.rs`; processes in `crates/meerschaum-cli/src/team/execution.rs`.

**Execute policy**:
What a non-administrator may run through `POST /execute` and console Exec: the actions the console
offers, the pipes the command selects, and whether system commands are allowed only as team
execution.
Owner: `crates/meerschaum-server/src/execute_policy.rs`, `execute_access.rs`.

**Source fetch**:
The main API's canonical source broker: bounded, paged native SQL reads of registered pipes that
API-only work uses instead of database credentials.
Owner: `crates/meerschaum-server/src/routes/source_fetch.rs`, `crates/meerschaum-cli/src/source_fetch/`.
_Avoid_: proxy read, data relay.

**Confined compiler**:
The administrator-selected offline Rust toolchain that compiles a team's single-file native plugin
inside a confined process; no request, job property or environment can select it.
Owner: `crates/meerschaum-cli/src/executor/compiler/`, `crates/meerschaum-plugin/src/source/compiler.rs`.

**Factory**:
The administrator-installed native material in an executor image (SDK, compiler bundle) whose
manifest the main API verifies against a pinned public key before trusting its results.
Owner: `crates/meerschaum-server/src/executor/factory_*.rs`, `crates/meerschaum-cli/src/executor/factory/`.

## Release and verification

**Parity fixture**:
A JSON fixture generated by released Meerschaum 4.1.4 that a Rust test compares against; never
edited by hand.
Owner: `crates/*/tests/fixtures/gen_*.py`, `scripts/parity/`.

**Candidate**:
One full `release.yaml` run on an exact commit: wheels, standalone binaries, sdist and every test
lane. A Docker candidate is the images built from a candidate's artifacts.
Owner: `.github/workflows/release.yaml`, `scripts/ci/candidates.py`, `scripts/ci/docker_candidate.py`.

**Sealed SDK**:
A candidate's artifacts after the release gate records their checksums in the candidate manifest;
tags, publication and Docker images consume only these bytes, never a rebuild.
Owner: `scripts/ci/release_gate.py`.
