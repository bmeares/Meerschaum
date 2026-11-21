# 0009. Team execution runs only where the sandbox covers sockets and signals

Status: accepted (5.1). Sources: `crates/meerschaum-server/src/team_execution.rs` (module docs),
`crates/meerschaum-server/src/execute_policy.rs`,
[Teams on One Instance](../zensical/reference/api-instance/teams.md) ("Commands of a team that runs code").

## Context

A team with `plugins_write` can already run any Python in its own processes, so refusing it shell
commands buys nothing. A command run with the server's credentials, in the server's process or
next to a reachable Unix socket (tmux, a container runtime) would escape the team's scope.

## Decision

A member of such a team may run commands (`sh`, `python`, `!command`, plugin actions, terminals)
only as team execution: a process started like the team's jobs, in sandbox mode
`require_sockets`, and only when every condition holds: the sandbox mode is not `off`; Unix
sockets are denied (Landlock ABI 9 or the verified seccomp filter) and signals are scoped
(Landlock ABI 6); no container-runtime socket the server's user can open is left uncovered; and a
probe process of that team proved the sandbox and a `meerschaum` that applies team scoping.
`judge` names the first failed condition; anything else answers no.

## Consequences

- macOS, Windows and older kernels never offer team execution.
- Losing a condition or revoking `plugins_write` refuses new commands and closes team terminals.
- Web terminals fall back to plain shells without tmux while an isolated team may write plugins
  and sockets are not denied.
