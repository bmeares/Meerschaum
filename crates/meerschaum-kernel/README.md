# meerschaum-kernel

Dataframe coercion, filtering, serialization, Arrow transport, and geometry operations.
Depends only on core among workspace crates; Polars features come from the workspace definition.
See `../README.md` and `../../docs/design/v5.0.0.md` for the compatibility contract.

`pipe_enforce_dtypes` updates authoritative `meerschaum:dtype` tags after successful conversions,
including bytes to geometry. A successful conversion clears the old database type tag; a failed
cast must not be labeled as the requested dtype. Geometry conversion itself replaces stale bytes
metadata so Arrow export and special-column detection agree with the converted values.

Integral floats outside `[-2**63, 2**63)` stay floating-point when pandas cannot convert them to
int64 without loss. The released-v4 generator `tests/fixtures/gen_big_floats.py` records the boundary
cases; `big_floats` tests cover document inference, casting, enforcement and filtering.

Use fresh temporary `MRSM_ROOT_DIR` and empty `MRSM_PLUGINS_DIR` for every test and reference run.
Run `cargo test -p meerschaum-kernel`, crate-scoped fmt/clippy, and the generators through
`scripts/parity/run.sh`. `field_tags` verifies metadata and Arrow geometry extension export.
