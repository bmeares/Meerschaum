# 0010. A flag modifies an action; new behavior is a new action

Status: accepted (5.0). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) D26, §4.3.

## Context

v4 grew flags that turned one action into another. Each such flag made `--help`, completion and
the access checks of `POST /execute` reason about an action whose meaning depends on its flags.

## Decision

On the CLI, a flag alters an action's behavior or supplies its input, and never turns it into
another action. New functionality gets its own action or subaction (`verify schema`,
`start pipeline`, `stop pipeline`; action chains serialize under a hidden `chain` action).

## Consequences

- Review rejects a new flag that adds an operation; propose a subaction instead.
- The frozen part of `--help` is the argument surface, not its text.
- The execute policy can judge a command by its action and subaction words
  (`crates/meerschaum-server/src/execute_policy.rs`).
