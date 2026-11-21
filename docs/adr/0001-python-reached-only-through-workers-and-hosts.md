# 0001. Python is reached from Rust only through workers and hosts

Status: accepted (5.0). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) §3 D12, §4.4.

## Context

Meerschaum 4 was a Python program. Version 5 moves `Pipe`, config, templating, connectors, the
engine, jobs, the server and the CLI to Rust, while plugins, custom connectors, API plugins, Dash
pages and MCP tools stay Python.

## Decision

The language bridge is inverted. The Python `Pipe`, connectors and `get_config` are façades over
`meerschaum._core`. Rust reaches Python-only code (plugins, `@make_connector` connectors, actions,
hooks, `@api_plugin` routes, plugin MCP tools) only through the pooled Python worker
(`crates/meerschaum-worker`) and the API host (`crates/meerschaum-server/src/host.rs`). Rust never
calls a Python implementation of something it implements itself.

## Consequences

- A façade method adapts arguments and calls one `_core` binding; an instance without a usable
  Rust driver is an error naming the cause, never a silent Python fallback.
- A result that differs from v4 is fixed at the boundary (`crates/meerschaum-py/src/pandas_bridge.rs`)
  or in the kernel and logged in `crates/meerschaum-py/PARITY.md`.
- Isolated teams get their own worker pools (`crates/meerschaum-worker/src/team.rs`) because the
  worker is the only place their Python runs.
