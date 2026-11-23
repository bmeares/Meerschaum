# 0011. Instance reservations extend Everyone on reserved instances

Status: accepted (5.1.3). Sources: `crates/meerschaum-core/src/access/rules.rs`,
`crates/meerschaum-server/src/access/`,
[Teams on One Instance](../zensical/reference/api-instance/teams.md) ("Reserve an instance for teams"),
[Access and Locks](../zensical/reference/pipes/access-and-locks.md) ("Access").

## Context

An administrator gives an isolated team its own instance by reserving a connector (`teams: [...]`).
Pipes and saved Pipelines registered outside the API's new-pipe stamping—ported databases,
`mrsm compose`, direct CLI use, or plugins—carry no stored `access` team, meaning **Everyone**.
Under [ADR 0007](0007-isolated-teams-scoped-processes.md), members of an isolated team (along with
team-bound execution principals and administrators narrowed via the console Team switcher) are not
in Everyone, leaving those untagged objects invisible except to a global administrator.

Defaulting untagged pipes to one team on single-team reserved instances was rejected: widening a
reservation to a second team would hide existing pipes again, multi-team reserved instances would
stay broken, "no team" would become impossible to express on a single-team instance, and reads
would raise persistence ambiguity.

## Decision

On an instance whose connector has a `teams` reservation, **Everyone** covers the members of the
reserved teams in addition to its ordinary audience:

- For an Everyone pipe or saved Pipeline on a reserved instance, a principal's effective role is
  the highest of their ordinary role in Everyone and their role in any reserved team. Isolated
  members gain member access; reserved-team managers gain manager access (including access edits,
  locks, private Everyone pipes, and returning a pipe to Everyone).
- A principal bound to one team (team execution) is permitted Everyone objects when the instance's
  reservation names its bound team, evaluated at the team-boundary check before membership.
- Visibility `everyone` on a reserved instance includes members of the reserved teams even when
  the pipe belongs to another team.

The reservation is read verbatim from the deciding process's live connector `teams` attribute at
decision time. It is never cached in the pipe index and never checked against registered teams, so
empty, Everyone-only, misspelt, or retired reservation entries extend nothing.

Core access rules (`crates/meerschaum-core/src/access/rules.rs`) require the instance's reservation
as an input, with context-free forms removed so every caller—server routes, SQL guard, engine graph
and history, MCP tools, CLI, Python bindings, and the web console—applies one rule. Stored pipe
`access` and registration stamps are never written or backfilled. Connector grants, the connector
gate, API new-pipe stamping, executor placement, and team rename/delete maintenance remain
unchanged.

## Consequences

- **Security invariant:** A principal gains access under this rule only if they are a member of a
  team named in the reservation of the instance the pipe lives on, or are bound to such a team.
- **Trust boundary:** Nothing new crosses a trust boundary. Team processes already receive their
  connectors' `teams` attributes in generated team configuration under
  [ADR 0007](0007-isolated-teams-scoped-processes.md).
- Editing a connector's `teams` reservation, renaming a team, or deleting a team (which retires its
  reservation) takes effect on the next request without a restart or pipe backfill.
- Instances without a reservation, deployments without teams, and teams that reach a connector only
  through a grant (without being named in its reservation) behave unchanged.
