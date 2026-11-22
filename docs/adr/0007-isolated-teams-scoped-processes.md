# 0007. Isolated teams run in scoped processes under the server's user

Status: accepted (5.1). Sources: `crates/meerschaum-core/src/team_scope.rs` (module docs),
[Teams on One Instance](../zensical/reference/api-instance/teams.md) ("Isolation and sandboxing").

## Context

Several teams share one API server. An isolated team's members can install plugins, so their
Python code must not read other teams' connectors, the server's configuration or its environment.
Giving each team its own operating-system user would need root to provision.

## Decision

An isolated team's code runs only in processes started for that team (its own worker pool and its
jobs), as the server's operating-system user, with three layers:

1. **Team configuration**: a generated config holding only the team's connectors and settings,
   through an owner-only `MRSM_CONFIG_FILE` and an empty `MRSM_CONFIG_DIR`. Every other connector
   is `null`; `stack`, `api:grafana` and credential-named values are withheld.
2. **Environment allowlist**: the process starts from an empty environment plus named variables,
   never the server's environment minus a denylist.
3. **Filesystem sandbox** (Linux): Landlock rules, plus a seccomp Unix-socket filter where needed,
   applied by the process itself before any team code runs. Mode `python_team_sandbox`
   (`auto`, `require`, `require_sockets`, `off`).

Who a request acts for is decided by `acting_team`: never an administrator, never several teams
at once.

## Consequences

- On macOS and Windows the first two layers apply and the sandbox does not; features that need
  the sandbox (team execution, confined profiles) refuse there.
- The shared Python API host enforces team scoping as a guardrail, not as a sandbox.
- Changing what reaches a team process means changing `team_config` / `team_env`; review it as a
  trust-boundary change.
