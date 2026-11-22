# 0004. The base wheel has no runtime dependencies

Status: accepted (5.0). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) §3 D20, §12.1.

## Context

v4 installed a large shared dependency set (and plugins added to it through `attempt_import`).
Most of those packages served code that moved to Rust.

## Decision

`pip install meerschaum` depends on nothing. Data, SQL, GIS, API and Dash libraries are extras;
packages used only by code that moved to Rust are deleted rather than made optional.
`attempt_import` keeps `venv='mrsm'` as its default, with installs attributed to the requesting
plugin and prunable.

## Consequences

- Python code imports heavy packages lazily through `attempt_import`, never at module level.
- `pyproject.toml` extras and `meerschaum/utils/packages/_packages.py` must agree;
  `scripts/check_packaging.py` enforces it and `--write` regenerates `requirements/<extra>.txt`.
- The `slim` image installs only the base wheel and is smoke-tested offline
  (`scripts/docker/smoke_facade.py`).
