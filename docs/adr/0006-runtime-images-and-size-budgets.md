# 0006. Runtime image variants and size budgets

Status: accepted (5.0). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) D25, §12.1.

## Context

One engine ships through two entry points (the PyPI wheel and the standalone binary), and users
need images with and without Python and with and without build toolchains.

## Decision

Docker publishes `rust` (native binary, no Python), `rust-full` (plus Cargo/rustc and native build
tools), `slim` / `minimal` (the base wheel only) and `full` / `latest` / deprecated `api` (native
binary, the Python SDK with every extra and both toolchains). Uncompressed budgets: 250 MB for
`rust` and `slim`, 1600 MB for `rust-full`, 2500 MB for `full`. Containers are upgraded by
replacing images; self-upgrade resolves the installation from its launcher.

## Consequences

- `scripts/docker/size_gate.sh` enforces the budgets on every release build; a passing size gate
  does not replace the offline façade smoke.
- All variants share configuration and plugin formats.
- Team executor images are the `full` image by default ([ADR 0008](0008-main-api-keeps-authority-over-team-executors.md)).
