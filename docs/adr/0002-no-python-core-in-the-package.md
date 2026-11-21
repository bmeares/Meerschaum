# 0002. No Python core in the package; released v4 is the parity oracle

Status: accepted (5.0). Source: [`docs/design/v5.0.0.md`](../design/v5.0.0.md) §3 D14, §10.2.

## Context

Keeping the v4 Python core behind a switch (`MRSM_CORE=python`) through 5.x would have given a
fallback and an in-package oracle, at the cost of two implementations of every behavior.

## Decision

The Python core is removed in 5.0. Parity is measured against released `meerschaum==4.1.4`
installed in a separate venv (`.venv-ref`, built by `scripts/parity/setup_reference.sh`), which
generates the fixtures under `crates/*/tests/fixtures/`.

## Consequences

- `meerschaum._core` is required: an unbuilt tree raises `CoreNotBuiltError`; a deleted v4 module
  raises `ModuleNotFoundError` naming its replacement (`meerschaum/_internal/_removed.py`).
- Fixtures are generated, never hand-edited, and CI's `parity` job regenerates them all.
- Recordings of the deleted parity-phase Python mode (`tests/oracle/`) cannot be re-recorded.
