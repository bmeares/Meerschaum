# Teams and team executors: contributor map

Where the 5.1 team isolation, team execution, sandbox, source-fetch and team-executor code lives,
how a request flows through it, and which tests cover it. Read this before changing anything under
`team_scope`, `execute_policy`, `executor`, `team_*` or `source_fetch_*`. Terms are defined in
[`GLOSSARY.md`](../../GLOSSARY.md) ("Teams and access", "Executors"); the decisions are
[ADR 0007](../adr/0007-isolated-teams-scoped-processes.md),
[0008](../adr/0008-main-api-keeps-authority-over-team-executors.md) and
[0009](../adr/0009-team-execution-requires-enforced-sandbox.md). The user-facing behavior is in
`docs/zensical/reference/api-instance/teams.md` and `docs/zensical/reference/team-executors.md`;
when code and those pages disagree, the code is the fact and the page is a docs bug.

Every module named here opens with `//!` docs stating its invariants; read those before the code.

## The pieces

| Piece | Owner | What it decides |
|---|---|---|
| Team records, roles, grants, `plugins_write` | `crates/meerschaum-core/src/access.rs` | who is in which team and what the team may use |
| Execution settings (`attributes.execution`: `host` / `container` / `api`, egress, limits, relays) | `crates/meerschaum-core/src/execution.rs` | where a team's work runs; a selected remote executor never falls back to the host |
| Team scope: config, env allowlist, acting team | `crates/meerschaum-core/src/team_scope.rs`, `team_scope/environment.rs` | what a team process receives; which team a caller acts for |
| Filesystem sandbox, resource limits, egress | `crates/meerschaum-core/src/team_scope/sandbox.rs`, `sandbox/linux.rs`, `sandbox/limits.rs` | Landlock/seccomp rules, probes and canary, `RLIMIT_*` |
| API-only and source-namespace preparation | `crates/meerschaum-core/src/team_scope/api_execution.rs`, `runtime.rs`, `source_namespace.rs` | credential-free runtime paths for confined launches |
| Recorded team scopes on the server | `crates/meerschaum-cli/src/team_scopes.rs` | `<root>/team_scopes/<key>/` files jobs and workers start from |
| Team worker pools | `crates/meerschaum-worker/src/team.rs`; sessions in `crates/meerschaum-cli/src/native/session.rs` (`for_team`) | per-team Python workers, retired when the team's fingerprint changes |
| Team jobs (host placement) | `crates/meerschaum-jobs/src/config_files.rs` (`externalize_team_with`), `daemon.rs` | a team job's config file, environment and sandbox spec |
| Process-start enforcement | `crates/meerschaum-cli/src/run.rs` (entry modes), `team_sandbox/` (`python/` for interpreters), `executor/terminal_guard.rs` | the sandbox is installed before any team code or Python starts |
| Python-side scoping | `meerschaum/_internal/team_scope.py`, `meerschaum/api/host/_team_scope.py` | `get_connector` / `get_config` refusal inside workers and the shared API host (a guardrail) |
| Execute policy | `crates/meerschaum-server/src/execute_policy.rs`, `execute_access.rs`, `routes/execute.rs` | what a non-administrator may run, and whether only as team execution |
| Team execution decision | `crates/meerschaum-server/src/team_execution.rs` (`judge`, `status`); processes in `crates/meerschaum-cli/src/team_execution.rs` | whether a team may run commands on this host now |
| Access and acting team on requests | `crates/meerschaum-server/src/access/` | per-request `Access`, `acting_team` |
| Team terminals | `crates/meerschaum-server/src/webterm/team.rs`, `api_terminal.rs`, `remote.rs`; `crates/meerschaum-cli/src/executor_client/host_terminals.rs` | locked-down tmux per team, API-only and remote terminals |
| Team executor control (main side) | `crates/meerschaum-server/src/executor/` (`control.rs`, `admission.rs`, `callbacks.rs`, `environment.rs`, `context.rs`) | signed control credentials, admission, callback grants, launch profiles |
| Team executor client and job catalog (main side) | `crates/meerschaum-cli/src/executor_client/`, `executor_jobs/` (`DelegatingJobs`, `Catalog`; `schedules.rs`), `executor_registry.rs`, `server/executor/recovery.rs` | placement of each job, the canonical job catalog, schedules |
| Team executor process (executor side) | `crates/meerschaum-cli/src/server/executor/` (`mrsm start executor`), `crates/meerschaum-server/src/executor/gateway.rs`, `crates/meerschaum-cli/src/executor/contexts/` | the authenticated outer listener and the team's inner ordinary API |
| Generated stack executors | `crates/meerschaum-cli/src/executor_stack.rs`, `executor/factory/`; egress proxy `crates/meerschaum-server/src/executor/egress.rs` | `mrsm stack up` services, networks, Factory provisioning |
| Plugins under executors | `crates/meerschaum-server/src/executor/plugin_*.rs`, `plugins.rs`, `extensions.rs`; `crates/meerschaum-cli/src/executor/plugin*.rs`, `executor/extensions.rs` | credential-free discovery, sealed catalogs, private handler calls |
| Confined compiler | `crates/meerschaum-cli/src/executor/compiler/`, `crates/meerschaum-plugin/src/source/compiler.rs` | offline compilation of single-file native plugins |
| Source fetch (canonical source broker) | `crates/meerschaum-server/src/routes/source_fetch.rs`, `crates/meerschaum-cli/src/source_fetch_backend/`, `source_fetch_postgres.rs`, `source_fetch_snapshot/`, `crates/meerschaum-connector-api/src/source_fetch.rs` | bounded native SQL reads for API-only work |
| Console | `meerschaum/api/dash/viewer.py` (`Viewer`, Team switcher), `access.py`, `_native_execution.py`; `crates/meerschaum-web/src/extension_pages.rs`, `stream.rs`, `grafana.rs` | per-session identity, team plugin pages, job-log visibility, Grafana orgs |
| Team databases | `crates/meerschaum-connector-sql/src/team_database.rs`, `crates/meerschaum-py/src/team_api.rs` | `bootstrap teams` database/schema and roles |

In `meerschaum-cli` the file layout follows the module tree: `executor/` holds the crate-internal
executor machinery, the public `executor_*` modules sit beside it, and each directory's `mod.rs`
says what its children are for.

## Flow: a console action to a running command

1. **Identity.** The console resolves a `Viewer` (`meerschaum/api/dash/viewer.py`) and, when the
   user switched teams, narrows it to the acting team. The Rust side derives the same per request
   (`crates/meerschaum-server/src/access/`, `acting_team`).
2. **Policy.** Exec in the terminal asks `POST /execute/validate`; New job and pipe management call
   `POST /execute` or `POST /jobs/{name}` (`routes/execute.rs`, `routes/jobs.rs`). For a
   non-administrator, `execute_policy::check_command` and `execute_access::check` run, then
   `may_run_system_commands` answers `Anywhere`, `TeamOnly { team }` or no. `TeamOnly` marks the job
   `team_execution` (`mark_team_execution`).
3. **Placement.** The job backend of `mrsm start api` is `DelegatingJobs`
   (`crates/meerschaum-cli/src/executor_jobs/`, wired in `crates/meerschaum-cli/src/server/mod.rs`).
   It consults the job catalog: a team with `execution.mode: host` uses the local backend; a team
   with a container or remote executor gets a catalog record, an execution profile (Team or
   API-only) and a signed launch through `executor_client::Controller`. Lookup or transport
   failure never selects the local backend.
4. **Host placement.** The local backend writes the team's config, allowlisted environment and
   sandbox spec into the job directory (`meerschaum-jobs/src/config_files.rs`), and the supervisor
   starts `mrsm`. `run.rs` calls `team_sandbox::enforce_if_team_process` before anything else, so
   the command runs confined; Python started from it goes through `team_sandbox/python/`.
5. **Team executor.** The executor's outer listener (`server/src/executor/gateway.rs`) verifies the
   signed control request with the team's public verifier and builds the inner ordinary API from
   the signed configuration only (`cli/src/server/executor/`). The executor runs the job with its
   own local backend; it reports status and logs back, and reaches the main API only through
   short-lived callback credentials (`server/src/executor/callbacks.rs`). API-only work reads data
   through source fetch instead of holding database credentials.
6. **Terminals** follow the same split: a host team terminal is a sandboxed shell on a per-team
   tmux socket (`webterm/team.rs`); API-only and remote terminals use short-lived owner-bound
   leases (`server/src/executor/terminal.rs`, `webterm/remote.rs`).

## Trust boundaries

- **Main API ↔ team process (host placement).** Everything a team process receives comes from
  `team_config` / `team_env` and the sandbox spec; nothing of the server's environment passes by
  default. The process confines itself at start (`run.rs` order matters: the trusted job lifecycle
  wrapper runs before confinement, every other marked process after).
- **Main API ↔ team executor.** The executor is untrusted: its replies are bounded and never
  reflected (`executor_client/configuration_diagnostics.rs`), it holds no signing key, and it cannot pick
  a job's owner, team, command, retention or paths (`executor_jobs/mod.rs` module docs).
- **Shared Python API host.** One process serves every team; scoping there is a guardrail
  (`meerschaum/api/host/_team_scope.py`), not a sandbox. Do not move team-trusted work into it.
- **Administrator-only inputs.** Compiler policy, Factory material and executor images are read
  from administrator installation paths (`executor/compiler/provisioning.rs`,
  `server/src/executor/factory_*.rs`); no request, job property or environment selects them.

A change that adds an input crossing one of these lines (a new field in a signed context, a new
environment variable for team processes, a new callback route) is a trust-boundary change: say so
in the PR and add a refusal test for the hostile case.

## Tests

Rust suites run as `cargo test -p <crate> --test <suite> -- <module>::`.

| Area | Tests |
|---|---|
| Team scope, env allowlist, sandbox rules | inline `crates/meerschaum-core/src/team_scope/tests.rs`, `team_scope/sandbox/tests.rs`; `crates/meerschaum-core/tests/integration/` (`team_reservations`, `team_resource_limits`) |
| Execute policy, team execution decision, executor admission, callbacks, source ceilings | `crates/meerschaum-server/tests/executors_host_suite/` |
| Access matrix, pipe-operation admission, job access | `crates/meerschaum-server/tests/routes_access_suite/` |
| Web terminals | `crates/meerschaum-server/tests/webterm_suite/`, `crates/meerschaum-cli/tests/host_api_terminals.rs` |
| Host placement end to end (team execution, sandbox probe, team jobs, restricted webterm) | `crates/meerschaum-cli/tests/host_api_team_suite/` |
| Team executor process end to end (`mrsm start executor`, delegated jobs, recovery, plugins) | `crates/meerschaum-cli/tests/executor_suite/` |
| Team sessions and worker retirement | `crates/meerschaum-cli/tests/team_sessions.rs` |
| Plugin discovery on the main host | `crates/meerschaum-cli/tests/host_plugin_discovery.rs` |
| Generated stack containers (Docker 28+, `--ignored`, image named by `MRSM_TEST_EXECUTOR_IMAGE`) | `crates/meerschaum-cli/tests/executor_container.rs`; run by `scripts/ci/docker_candidate.py` |
| Confined compiler (`--ignored`, compiler root from `MRSM_TEST_EXECUTOR_COMPILER_*`) | `crates/meerschaum-cli/tests/native_source_compiler.rs` |
| Source fetch on PostgreSQL (disposable database) | `crates/meerschaum-cli/tests/registered_pg_source.rs` |
| Python scoping in workers and the API host | `tests/test_team_workers.py`, `tests/test_team_sandbox_multiprocessing.py`, `tests/test_sql_main_sandbox.py`, `tests/api_host/test_team_*.py`, `tests/api_host/test_exec_security.py`, `tests/api_host/test_server_client_and_acting_team.py` |

Sandbox tests need Linux with Landlock; kernel support decides which of them run (`mrsm doctor`
prints what this host enforces).
