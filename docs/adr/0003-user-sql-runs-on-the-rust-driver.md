# 0003. User SQL from Python runs on the Rust driver on every flavor

Status: accepted (5.0). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) §3 D16, §6.2.

## Context

Plugins call `SQLConnector.read`, `exec`, `exec_queries`, `value` and `to_sql` directly. Routing
those through SQLAlchemy would keep a second, implicit SQL execution path beside the Rust driver.

## Decision

These methods and the catalog helpers use the Rust driver on every SQL flavor. SQLAlchemy
constructs are compiled at the boundary. Explicit `.engine`, caller-provided SQLAlchemy
connections and helpers that return SQLAlchemy objects remain available with `meerschaum[sql]`.

## Consequences

- The base wheel runs user SQL without SQLAlchemy (see [ADR 0004](0004-zero-dependency-base-wheel.md)).
- SQLite-family façade calls share the core SQLite library; DuckDB's transport selection is a
  documented exception for shared database instances (§6.2), not a second `Pipe` implementation.
