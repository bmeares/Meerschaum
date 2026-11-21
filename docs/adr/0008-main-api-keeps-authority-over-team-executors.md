# 0008. The main API keeps all authority; team executors are untrusted and fail closed

Status: accepted (5.1). Sources: `crates/meerschaum-server/src/executor/mod.rs`, `control.rs`,
`crates/meerschaum-core/src/execution.rs`, [Team execution environments](../zensical/reference/team-executors.md).

## Context

An isolated team may run its jobs, plugins and terminals in a separate container or on a remote
host (`execution.mode: container | api`). That process runs team code, so it may be compromised.

## Decision

- The main API alone holds identity, authorization, team administration, routing and the Ed25519
  control signing key. A team executor receives a public verifier, its own filtered configuration
  and short-lived, request-bound credentials; it can never mint control credentials or choose a
  job's owner, team, command or paths.
- Executor responses are untrusted input: bounded, validated and never reflected verbatim.
- Selection is explicit. A selected executor that is unavailable, unhealthy or misconfigured is an
  error; it never falls back to running on the main host.
- Every start or restart rechecks the original source identity, scopes, membership and grants;
  history never authorizes a re-run.

## Consequences

- If the main API is unreachable past the credential expiry, an executor stops work until it
  re-authorizes.
- Back up the main API root (executor registry, signing keys, job catalog) with the team volumes;
  partial restores fail closed.
- Code on the executor side treats every main-API input as signed data and every executor reply,
  on the main side, as hostile.
