# Raw and resolved parameters

A pipe's parameters exist in two forms, and every consumer picks one on purpose
(`crates/meerschaum-core/src/pipe/mod.rs`):

| Accessor | Returns | Use it for |
|---|---|---|
| `Pipe::raw_parameters()` | the parameters as stored, with `{{ Pipe() }}` and `MRSM{}` references unexpanded | persistence (register, edit, forward to another instance), definition hashes, dependency extraction, merging loaded metadata |
| `Pipe::parameters()` | the resolved parameters; resolves self references and config lazily | execution: fetch, sync, dtype enforcement, display |
| `Pipe::resolve_parameters_with(resolver, config)` | resolves references that need an instance (other pipes' registered state) and caches the result for `parameters()` | once at the entry of a sync or fetch, before the first `parameters()` call |

Rules for consumers:

- **Persist raw, execute resolved.** Writing `parameters()` back to an instance replaces a
  reference with its value at that moment, and the pipe stops following its parent.
- **Resolve before the first read.** A lazy `parameters()` call resolves a cross-pipe reference
  without the instance and caches that answer, so an entry point that has an instance calls
  `resolve_parameters_with` with an instance-backed `TemplateResolver`
  (`crates/meerschaum-core/src/template.rs`) first.
- **Changing parameters drops the resolved copy.** `parameters_mut()` and `set_parameters()`
  clear the cache; call `resolve_parameters_with` again after registered state changes.
- **The engine hashes and scans raw parameters** (`crates/meerschaum-engine/src/graph.rs`), so a
  definition hash does not change when a referenced value does.
- **The kernel receives dtype contexts**, never a pipe's metadata.

List the consumers with `rg 'raw_parameters\(\)|\.parameters\(\)' crates/*/src`. The matrix of
resolution cases is `tests/fixtures/gen_parameter_resolution.py` in this crate.
