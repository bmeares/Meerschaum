# meerschaum-core

> **Internal crate.** `meerschaum-core` is an implementation detail of [Meerschaum](https://meerschaum.io)
> and may change in any minor release. Depend on the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate instead.

Leaf crate for pipe metadata, config, templates, dtype vocabulary, and shared Python behavior.
See `../README.md` for layering and `../../docs/design/v5.0.0.md` for the compatibility contract.

`Pipe::parameters()` and `param()` share a lazy resolved snapshot. Consequently `target()`,
columns, dtypes, indices, chunk settings and fetch definitions see self templates and `MRSM{}`
config substitutions. Mutating parameters invalidates the snapshot. `raw_parameters()` and
`to_state()` retain unresolved metadata for persistence, dependency extraction and definition hashes.

For registered references, load their state into a `TemplateResolver`, implement `table_reference`
with the instance's SQL quoting/schema rules, then call `resolve_parameters_with(resolver, config)`
before using the pipe in a run. That snapshot feeds every accessor without changing their signatures.
Core cannot load instance metadata itself: connector dependencies point toward core, not away from it.
After mutating parameters or changing registered reference state, resolve again. Template errors retain
the original text, matching v4. Warnings are returned by explicit resolution. Default lazy access
has no warning sink. Resolution depth is bounded at 32; v4 uses Python's recursion limit.

The config registry adds these v5 defaults under `system.engine` (outside the embedded v4 snapshot):

| Key | Default | Meaning |
| --- | --- | --- |
| `python_workers` | `min(cpu_count, 4)` (at least one) | Shared Python worker pool size |
| `inclusive_cap` | `true` | Include the parent watermark by adding one axis tick to the exclusive cap |
| `flat_ledger` | `true` | Record flat-run writes in the ledger without writing cursors or hashes |

Normal config file and environment overrides apply. `version::version_ge` implements PEP 440
ordering, including epochs, local versions, prereleases, development and post releases.
Invalid versions compare false; `version::parse_version` exposes parse errors.

For an already loaded config, `Config::apply_environment_patches(env)` applies `MRSM_CONFIG`
then `MRSM_PATCH`, and `Config::apply_environment_uris(env)` patches connector attributes while
preserving other top-level keys used by `MRSM{}` symlinks. Both accept an explicit environment
and record malformed input in `take_warnings()`. These methods respect `allow_writes` for config
files. Initialization retains Python's URI-first order and its historical replacement of the
config with only `meerschaum` when applying each URI. Compose instead replaces `MRSM_CONFIG`
with its project block, applies `MRSM_PATCH`, then URI connectors and file connector overrides.

Tests must run with fresh `MRSM_ROOT_DIR` and empty `MRSM_PLUGINS_DIR`. Generate the parameter
matrix using `scripts/parity/run.sh crates/meerschaum-core/tests/fixtures/gen_parameter_resolution.py`.
It uses in-memory instance stubs and never connects to a database. See `PARAMETER_CALL_SITES.md`
for the audited consumer locations and required integration changes in other crates.

`debug::dprint!` prints debug output: a `tracing` `DEBUG` event targeted at the caller's module,
emitted only while `debug::enabled()` (`--debug`, the shell's `debug` toggle, or a Python binding
called with `debug=True`), so the arguments are not even formatted otherwise. `debug::scope` (one
`--debug` action) and `debug::guard` (one `debug=True` call) are counted, so overlapping holders
leave the switch off once the last one ends; `debug::without_scopes` makes `scope` a no-op on a
thread (the API server's action requests) and `debug::take_over` hands an enclosing `--debug` to the
shell's toggle. Redact secrets with `debug::redact_uris` (URI passwords, secret query parameters,
quoted secret keys in JSON or `repr` text, secret flags inside a word) and `debug::is_secret_key`
before printing them. The CLI and the Python
extension own the subscribers that print these events.
