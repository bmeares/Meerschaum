# Pipe concepts the façade preserves

The v4 behavior of pipes, parameters, syncs and config that v5 keeps observable. Read this before
changing `Pipe`, a connector, the sync path or a façade. Term definitions are in
[`GLOSSARY.md`](../../GLOSSARY.md); the full compatibility contract is §11 of
[`docs/design/v5.0.0.md`](../design/v5.0.0.md).

**`SuccessTuple`** (`(bool, str)`) is the return type of actions, pipe methods and connector
methods. Rust returns `SyncResult` (`crates/meerschaum-core/src/success.rs`); Python's
`mrsm.SyncResult` is a `tuple` subclass, so `success, msg = pipe.sync()` keeps unpacking.

**Identity.** `Pipe(connector_keys, metric_key, location_key=None, instance=...)`; `pipe.target`
defaults to `{connector_keys with ':'→'_'}_{metric_key}[_{location_key}]`. Constructing a `Pipe`
never touches the instance. Engine state keys on
`pipe_ref` = `"{instance}|{connector_keys}|{metric_key}|{location_key}"`.

**`parameters`** is a live dict (assign then `pipe.edit()`, or `update_parameters({...}, persist=True)`),
and every key is part of the compatibility contract (§11.1): `columns` (`datetime`, `id`, `primary`,
`value`), `dtypes` (Meerschaum dtype strings incl. every `geometry[...]` notation, or pandas dtypes;
role-keyed entries resolve through `columns`), `indices`, `tags`, `target`, `upsert`, `static`,
`autoincrement`, `autotime`, `null_indices`, `mixed_numerics`, `enforce`, `precision`, `fetch`
(`backtrack_minutes`, `definition`; top-level `sql` is the same), `verify` (`chunk_minutes` and
aliases, `bound_days`), `hypertable` / `hypercore` / `compress`, `reference(s)` (inheritance only,
not a graph edge), `parents` / `children` (engine edges, may carry `lookback_*` / `lookahead_*` /
`axis`), `schedule`, `min_seconds`, `retry`, `stream`, `access`, `locks`.

**Sync flow** (`pipe.sync(df=None, begin='', end=None, ...)`): fetch, `enforce_dtypes`,
`filter_existing` (backtrack window + `filter_unseen_df`), `instance.sync_pipe`. `begin=''` = sync
time minus backtrack; `None` = unbounded. A bare `pipe.sync()` keeps v4 semantics (no graph, no
capping); `pipe.sync(parents=..., children=...)` and `mrsm.Pipeline(...)` run the engine (children
capped at `min(parent watermarks)`; definition changes mean `rebuild`, which needs `--yes` /
`auto_rebuild`).

**`params` filter**: value = `=`, list = `IN`, `_` prefix negates, `None` / `'None'` = `IS NULL`,
`'_None'` = `IS NOT NULL`, a dict compares JSON text. With `pipe.enforce=True` unknown columns are
dropped from `params` on SQL pipes.

**Templating.** `{{ Pipe('ck', 'mk', 'lk', instance=...) }}` resolves to the table reference;
attribute chains and `{{ self.parameters['x'] }}` into values; `MRSM{a:b:c}` reads config at access
time. Implicit `{{ Pipe() }}` references are engine edges (`system.engine.implicit_edges`). Raw
(persisted) parameters keep the templates; resolved parameters are for execution
(`crates/meerschaum-core/README.md` describes the resolution API).

**Connectors and plugins.** Custom connectors via `@make_connector` (`IS_INSTANCE`,
`IS_THREAD_SAFE`, `IS_STREAMING` + `subscribe`, optional lease methods). Python connectors and
plugins are reached from Rust only through the worker
([ADR 0001](../adr/0001-python-reached-only-through-workers-and-hosts.md)). Rust plugins are single
`.rs` files or native directories in the plugins dir.

**Config.** `get_config('a', 'b')` over YAML under `MRSM_ROOT_DIR/config/`; `MRSM_CONFIG` (JSON)
patches config; engine keys live under `system.engine.*`. `MRSM_PYTHON` picks the interpreter
workers run with (preflighted once; it must import a `meerschaum` of the same major).
`connect_timeout` / `MRSM_CONNECT_TIMEOUT` bound the first connection of a command.
