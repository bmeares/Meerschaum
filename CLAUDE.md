# CLAUDE.md

Guidance for Claude Code working on **Meerschaum 5** (the `v5` branch). The design is
`docs/design/v5.0.0.md` (read §4, §5, §6.1, §7, §8, §9, §11, §12, §14 and decisions D12/D14/D16/D20
before changing architecture); build, CI and Docker are in `docs/design/v5-build.md`; crate conventions
and layering are in `crates/README.md`; the user-facing story is `docs/zensical/reference/migrating-to-v5.md`.

## What v5 is

- **A Rust core.** `Pipe`, parameters, config, `{{ Pipe() }}` templating, the Arrow/Polars data kernel,
  the `sql:` / `valkey:` / `api:` connectors, the dependency-aware Syncing Engine, jobs, compose, the
  `axum` HTTP API + MCP server and the `mrsm` CLI/shell are Rust crates under `crates/`. There is one
  implementation and no Python fallback.
- **Python is the SDK, the façade and one extension host.** The `meerschaum` wheel = the `_core` PyO3
  extension + a façade (`mrsm.Pipe`, connectors, `get_config`, actions dispatch) + the plugin SDK
  (`attempt_import`, `Venv`, `@make_action`, hooks, `warn`/`error`/`info`/`dprint`) + the **API host**
  (`python -m meerschaum.api.host`: `@api_plugin` routes, plugin MCP tools and the one Plotly Dash console
  at `/dash`, proxied by the Rust server over a Unix socket, or loopback TCP + per-start token on Windows)
  + the Python worker (`python -m meerschaum.worker`, which runs Python plugins for the Rust host).
- **Public surfaces do not change** (§11): pipe keys and every `parameters` key, config YAML layout,
  `MRSM_SQL_<LABEL>` env connectors, instance tables, the job directory layout, compose files, every HTTP
  route, and the Python public API. `SQLConnector.read/exec/value/to_sql/.engine/get_tables()` stay
  SQLAlchemy (D16, extra `meerschaum[sql]`). The base wheel has **no runtime dependencies** (D20).
- **`meerschaum._core` is required.** An unbuilt tree raises `CoreNotBuiltError` naming
  `scripts/rust/build_py.sh`. A deleted v4 module raises a `ModuleNotFoundError` naming its replacement
  (`meerschaum/_internal/_removed.py`: add an entry when you delete a module).
- **One wheel, one CLI.** The `mrsm` / `meerschaum` console scripts and `python -m meerschaum` call
  `_native.main(argv)` (`_meerschaum_mrsm.py` -> `meerschaum/__main__.py` -> `_internal/binary.py`), which
  runs the whole CLI inside the extension; `cargo install meerschaum` builds the same CLI as a standalone
  binary (`meerschaum_cli::run_main`); `MRSM_BIN` makes Python exec a standalone binary instead. Anything
  that re-launches `mrsm` (jobs, compose, systemd units, the web terminal) asks
  `meerschaum_core::self_exe::self_command()`. `import meerschaum` is eager except in the CLI process,
  where it is lazy (PEP 562, `_internal/_facade.py`). `meerschaum.entry([...])` runs the native dispatcher
  in-process.
- **v4 is the reference** only through the `.venv-ref` parity fixtures and the recordings in
  `tests/oracle/` (`tests/_oracle.py`).

## Isolation rules (read before running anything)

- **Always set a scratch root and plugins dir.** Every build (debug and release) uses the real default
  root (`~/.config/meerschaum`) when `MRSM_ROOT_DIR` is unset; nothing refuses it for you. Use
  `MRSM_ROOT_DIR=./test_root` or a fresh directory under `~/.cache/mrsm-<you>/` (not `/tmp`: it is a
  small tmpfs), which must exist, and `MRSM_PLUGINS_DIR=./tests/plugins` (or an empty directory). Delete
  scratch roots by literal path when done.
- **`PYTHONPATH=$(pwd)` for every Python invocation.** A stale `meerschaum` under `~/.local` shadows the
  working tree; check `meerschaum.__file__` when a result looks impossible.
- **Parity generators never use the ambient interpreter.** `scripts/parity/run.sh` runs
  `.venv-ref/bin/python -s -E` (released `meerschaum==4.1.4`, built by `scripts/parity/setup_reference.sh`);
  cross-implementation Rust tests use `MRSM_TEST_PYTHON` (default `.venv-ref/bin/python`).
- **Never set `CARGO_TARGET_DIR`.** Every crate shares `target/` (Polars/Arrow compile once). Several
  sessions may share the tree: concurrent builds block on the directory lock, `scripts/rust/build_py.sh`
  takes its own lock (`target/.build-py.lock`; never wrap it in `flock`), and never `git stash` /
  `checkout` / `restore` / `clean` / `reset`.
- **Verify on a fresh root, like CI** (`git clean -ffdx`): tests that need plugin venvs create them in
  fixtures. Leave no processes behind (`pgrep -af 'target/debug/mrsm|meerschaum'`).
- **Never run two `scripts/test.sh` invocations (or `test.sh` + an `api`-flavor pytest) at once**: they
  share `test_root` and the test API on port 8989.

## Commands

```bash
export PATH="$HOME/.cargo/bin:$PATH"       # rustup toolchain from rust-toolchain.toml
export CARGO_INCREMENTAL=0                 # what CI uses; optional locally
```

### Build

| Command | What it does |
|---|---|
| `cargo build -p meerschaum --bin mrsm` | the debug standalone `mrsm` at `target/debug/mrsm` (what `cargo install meerschaum` builds; `tests/console` runs it). `cargo build -p meerschaum-cli` builds only `target/debug/mrsm-cli` for that crate's integration tests |
| `scripts/rust/build_py.sh [--debug \| --profile ci]` | builds the extension with maturin into `meerschaum/_core/_native.abi3.so` (release by default, ~10 min; `--profile ci` takes 2 to 4 min and is what to use for everyday Python test runs). Needs `maturin` and Python >= 3.11 (`PYTHON=...`; keep it stable between runs or Polars rebuilds) |
| `scripts/rust/clean_py.sh [--cargo]` | removes the built extension |
| `scripts/mrsm.sh <args>` | `python -m meerschaum` against `./test_root` + `./tests/plugins` |

### Checks (CI; `scripts/rust/ci.sh <step>` reproduces each job)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
scripts/rust/ci.sh doc           # rustdoc with -D warnings (broken intra-doc links; docs.rs builds after publish)
cargo test -p <crate>            # `cargo test --workspace` is the CI job (CI uses nextest + `cargo test --doc`)
scripts/rust/ci.sh               # fmt clippy test build
scripts/rust/ci.sh parity        # regenerate every crates/*/tests/fixtures/gen_*.py with v4.1.4 and diff
scripts/parity/check_fixture_hygiene.sh [PATH ...]   # before committing fixtures or any public file
python scripts/check_packaging.py            # versions, console scripts, extras, lockstep pins, crates.io metadata
python scripts/check_packaging.py --write    # regenerate requirements/<extra>.txt and crates/meerschaum-cli/src/extras.rs
scripts/rust/ci.sh frontcrate | package | publish-dry-run   # see crates/README.md "Publishing"
```

Crate integration tests skip unless their service URI is set (an empty value counts as set):
`MRSM_TEST_PG_URI`, `MRSM_TEST_TIMESCALE_URI`, `MRSM_TEST_POSTGIS_URI`, `MRSM_TEST_TIMESCALE_HA_URI`
(`timescaledb-ha://`), `MRSM_TEST_MYSQL_URI`, `MRSM_TEST_MARIADB_URI`, `MRSM_TEST_MSSQL_URI`,
`MRSM_TEST_ORACLE_URI` (`--features oracle`, needs Instant Client), `MRSM_TEST_GPKG_DIR`,
`MRSM_TEST_DUCKDB_PATH` / `MRSM_TEST_DUCKDB_LIB`, `MRSM_TEST_VALKEY_URI`, `MRSM_TEST_MQTT_URI`,
`MRSM_TEST_PYTHON` / `MRSM_TEST_KEEP=1`. The databases come from `tests/docker-compose.yaml`; host ports
are in `tests/connectors.py` and `crates/meerschaum-connector-sql/README.md`. `MRSM_DUCKDB_LIB` pins the
native DuckDB library for a process (otherwise it is found in `<root>/drivers`, the system loader, or
downloaded on first use); see the connector README before changing DuckDB loading.

### Python tests (pytest; need the built extension)

```bash
mkdir -p test_root   # the root must exist; use absolute paths (compose and spawned processes change directory)
MRSM_TEST_FLAVORS=sqlite PYTHONPATH=$(pwd) MRSM_ROOT_DIR=$(pwd)/test_root MRSM_PLUGINS_DIR=$(pwd)/tests/plugins \
  python -m pytest tests/ -q -p no:cacheprovider \
    --ignore=tests/test_jobs.py --ignore=tests/test_mcp.py --ignore=tests/test_listen_shutdown.py \
    --ignore=tests/api_host --ignore=tests/console

./scripts/test.sh db "" <pytest-args>            # full harness: Docker DBs, test API on 8989, --ff, -n=auto
MRSM_TEST_FLAVORS=sqlite ./scripts/test.sh db ""
```

- A few tests use a standalone `mrsm` (`MRSM_TEST_BIN`, else the newest `target/{release,debug}/mrsm`
  outside CI; `tests/_binary.py` checks its version and age).
- Tests needing the extension fail (never skip) when it is not built.
- `MRSM_PLUGINS_DIR` must be `./tests/plugins` (synthetic `stress` and `w7_*` plugins live there).
- **Flavor keys != SQL flavor names**: `timescaledb`, `timescaledb_ha`, `postgis`, `mariadb`, `mysql`,
  `mssql`, `oracle`, `sqlite`, `api`, `valkey` (no `postgresql`). `test.sh` defaults to `api,timescaledb`,
  needs `pytest-xdist` and `pytest-cache`, and exits before pytest on an unknown key.
- Oracle recordings (`tests/oracle/*.json`) replay the deleted parity-phase Python mode and cannot be
  re-recorded; pandas-major-dependent tests list themselves in `pandas_dependent`.
- `tests/console` (Playwright, opt-in with `MRSM_TEST_CONSOLE=1`) starts real `mrsm start api` servers on
  random ports with the `w7_*` plugins; it needs `target/debug/mrsm`, `meerschaum[dash,sql,pandas]` and
  Chromium (`tests/console/build_native.sh` builds the native page plugin). `tests/api_host` runs in process.
- Tests that wait for jobs started by `compose up` or a bulk start pass `--no-stagger` (see Gotchas).

### Other

```bash
python scripts/bench/run.py --label <name>       # sync benchmarks, v4.1.4 vs v5 (release binary; results in scripts/bench/results/)
scripts/transition/run.sh [--flavors ...]        # v4 -> v5 upgrade harness (README there)
./scripts/zensical.sh                             # serve docs/zensical/; ./scripts/pdoc.sh builds the Python API reference
./scripts/build.sh                                # docs, check_packaging --write, wheel + sdist (never upload its wheel)
```

`MRSM_CORE_TRACE=1` prints per-stage timings of each boundary call to stderr. Releases follow
`.claude/skills/release/SKILL.md`.

## Architecture map

### Crates (`crates/`, edition 2024; the workspace version in the root `Cargo.toml` is the single source of truth)

Dependencies point down only; `meerschaum-core` is a leaf; `cli` and `py` are the top layer.

| Crate | Owns |
|---|---|
| `meerschaum` | the published **front crate**: `pub use` of the public surface behind features (`sql`, `cli` default; `valkey`, `api`, `engine`, `server`, `plugin`, `full`) + `src/main.rs`, the `mrsm` binary. Every other crate is internal (`meerschaum-py` is `publish = false`) |
| `meerschaum-core` | `Pipe`, config registry + `MRSM{}` symlinks + `static.toml`, dtype vocabulary, template parser, users/tokens/plugins types, `SuccessTuple`/`SyncResult`, `version_ge`, shared Python-behavior helpers (`pyfmt`, `Value`, `shlex`), `self_exe` |
| `meerschaum-kernel` | Arrow/Polars `Frame`: `enforce_dtypes`, `filter_unseen_df` (`merge_dtype.rs`), `query_df`, special columns (json, uuid, numeric, bytes, geometry as GeoArrow WKB) |
| `meerschaum-connector` | `Connector` / `InstanceConnector` traits, `DataQuery` / `SyncOptions`, users/plugins/tokens over internal pipes, cross-pipe template resolution |
| `meerschaum-connector-sql` | `SqlConnector`: per-flavor query builders (`queries/`, golden-tested), `sqlx`, `tiberius`, `oracle` and a runtime-loaded `duckdb` driver; instance methods, partitioning, compression, maintenance, leases, `sync_pipe_inplace`, `facade.rs` |
| `meerschaum-connector-valkey` / `-api` | `ValkeyConnector` (`fred`); the `APIConnector` client (`reqwest`) + route/auth/JWT models |
| `meerschaum-engine` | graph, planner (`skip`/`incremental`/`backfill`/`rebuild`/`observe`/`root`), executor (HEFT, leases, retries), dirty-range ledger, edge cursors, run history (`mrsm:engine_*` pipes), `--loop` / `--listen`, events, Gantt data |
| `meerschaum-worker` | Python worker pool + msgpack/Arrow-IPC protocol, `PythonConnector`, callbacks, supervised streams, plugin manifest (`schema/manifest.schema.json`) |
| `meerschaum-jobs` / `-compose` | daemon supervision (v4 `jobs/<name>/` layout), `-s` schedules, startup gate; the compose model, `.env`, subactions, `explain` |
| `meerschaum-server` | `axum` API (every v4 route + `/engine/*` + `POST /execute`), OAuth2/JWT/tokens, rate limits, route allowlists, the API-host proxy (`web.rs`), MCP (`/mcp` + stdio) |
| `meerschaum-web` | web terminal (`/webterm/...`, vendored xterm.js), job-log SSE, native plugin pages (`/_mrsm/native-pages`), console sessions |
| `meerschaum-plugin` | native plugin SDK: `Plugin` trait, `export_plugin!`, C ABI (`mrsm_plugin_v1`, Arrow C stream), loader, discovery, single-file `.rs` plugins |
| `meerschaum-cli` | the CLI library: clap spec/argparse parity, dispatch, native actions (`src/native/`), the reedline shell (`src/shell/`), `start api`/`start mcp`, venv accounting, engine views, self-upgrade |
| `meerschaum-py` | PyO3 bindings -> `meerschaum._core`; `PARITY.md` logs every Python-vs-Rust discrepancy, `PERF.md` the boundary benchmarks |

### Python package (`meerschaum/`)

| Path | Role |
|---|---|
| `_core/` | stdlib-only loader for `_native.abi3.so` (`AVAILABLE`, `LOAD_ERROR`, `CoreNotBuiltError`) |
| `_internal/` | `binary.py` (CLI entry, `MRSM_BIN`, `self_command()`), `_facade.py`, `entry.py`, `_removed.py`, `arguments/`, `shell/` |
| `core/Pipe/` | the façade `Pipe` over `_core.Pipe`; `_live_parameters.py` provides the live `pipe.parameters` views |
| `core/Pipeline/` | `mrsm.Pipeline`, `Plan`, `RunResult`, `pipe.sync(parents=, children=)`, event decoding |
| `connectors/` | façades; `SQLConnector._rust_method()` builds one `_core.SqlConnector` per connector (rebuilt after a fork) |
| `plugins/` | SDK decorators, `_page.py` (page-contract figures) |
| `worker/`, `api/host/`, `api/dash/` | the Python worker; the FastAPI host; the Dash console (`build_dash_app()` builds the one app; `tests/console` drives it) |
| `api/`, `actions/`, `utils/`, `config/` | what is left of the v4 packages: SDK helpers and façades over `_core`. See "Removed in 5.0" in the migration guide |

### Façade pattern

Façade methods call one `_core` binding each and keep only argument adaptation, caches and the
pandas/Polars boundary in Python. An instance without a usable Rust driver is an error naming the cause,
never a silent fallback. `SQLConnector._rust_method` raises `AttributeError` for a name the extension does
not bind. A result that differs from v4 is a parity bug: fix it at the boundary
(`src/pandas_bridge.rs` dtype hints) or in the kernel, and add a row to `PARITY.md`.

### Parity fixtures

- `crates/*/tests/fixtures/*.json` are **generated by released v4.1.4** through
  `scripts/parity/run.sh crates/<crate>/tests/fixtures/gen_<x>.py`; never hand-edit them; commit the
  generator next to the fixture. CI's `parity` job regenerates them all and fails on any diff. Exception:
  the engine planner fixtures are written by hand from the spec (v4 has no planner).
- Fixture data is synthetic. Generators run with a fresh root, an empty plugins dir and no inherited
  `MRSM_*` variables (v4 reads every `MRSM_<TYPE>_<LABEL>` as a connector); scrub pids, timestamps, paths
  and credentials at the source.
- `// PARITY:` marks a v4 quirk reproduced on purpose, with the Python line it comes from.

## Pipe concepts the façade preserves

**`SuccessTuple`** (`(bool, str)`) is the return type of actions, pipe methods and connector methods:
return `(False, "reason")`, never raise from action-level code. Rust returns `SyncResult`
(`meerschaum-core/src/success.rs`); Python's `mrsm.SyncResult` is a `tuple` subclass, so
`success, msg = pipe.sync()` keeps unpacking. Use `warn` / `error` / `info` / `dprint`, not `print()`.

**Identity.** `Pipe(connector_keys, metric_key, location_key=None, instance=...)`; `pipe.target` defaults
to `{connector_keys with ':'→'_'}_{metric_key}[_{location_key}]`. Constructing a `Pipe` never touches the
instance. Engine state keys on `pipe_ref` = `"{instance}|{connector_keys}|{metric_key}|{location_key}"`.

**`parameters`** is a live dict (assign then `pipe.edit()`, or `update_parameters({...}, persist=True)`),
and every key is part of the compatibility contract (§11.1): `columns` (`datetime`, `id`, `primary`,
`value`), `dtypes` (Meerschaum dtype strings incl. every `geometry[...]` notation, or pandas dtypes;
role-keyed entries resolve through `columns`), `indices`, `tags`, `target`, `upsert`, `static`,
`autoincrement`, `autotime`, `null_indices`, `mixed_numerics`, `enforce`, `precision`, `fetch`
(`backtrack_minutes`, `definition`; top-level `sql` is the same), `verify` (`chunk_minutes` and aliases,
`bound_days`), `hypertable` / `hypercore` / `compress`, `reference(s)` (inheritance only, not a graph
edge), `parents` / `children` (engine edges, may carry `lookback_*` / `lookahead_*` / `axis`), `schedule`,
`min_seconds`, `retry`, `stream`.

**Sync flow** (`pipe.sync(df=None, begin='', end=None, ...)`): fetch, `enforce_dtypes`,
`filter_existing` (backtrack window + `filter_unseen_df`), `instance.sync_pipe`. `begin=''` = sync time
minus backtrack; `None` = unbounded. A bare `pipe.sync()` keeps v4 semantics (no graph, no capping);
`pipe.sync(parents=..., children=...)` and `mrsm.Pipeline(...)` run the engine (children capped at
`min(parent watermarks)`; definition changes mean `rebuild`, which needs `--yes` / `auto_rebuild`).

**`params` filter**: value = `=`, list = `IN`, `_` prefix negates, `None` / `'None'` = `IS NULL`,
`'_None'` = `IS NOT NULL`, a dict compares JSON text. With `pipe.enforce=True` unknown columns are dropped
from `params` on SQL pipes.

**Templating.** `{{ Pipe('ck', 'mk', 'lk', instance=...) }}` resolves to the table reference; attribute
chains and `{{ self.parameters['x'] }}` into values; `MRSM{a:b:c}` reads config at access time. Implicit
`{{ Pipe() }}` references are engine edges (`system.engine.implicit_edges`). Raw (persisted) parameters
keep the templates; resolved parameters are for execution.

**Connectors and plugins.** Custom connectors via `@make_connector` (`IS_INSTANCE`, `IS_THREAD_SAFE`,
`IS_STREAMING` + `subscribe`, optional lease methods). Python connectors and plugins are reached from Rust
**only** through the worker; Rust never calls a Python implementation of something it implements itself
(D12). Rust plugins are single `.rs` files or native directories in the plugins dir.

**Config.** `get_config('a', 'b')` over YAML under `MRSM_ROOT_DIR/config/`; `MRSM_CONFIG` (JSON) patches
config; engine keys live under `system.engine.*`. `MRSM_PYTHON` picks the interpreter workers run with
(preflighted once; it must import a `meerschaum` of the same major). `connect_timeout` /
`MRSM_CONNECT_TIMEOUT` bound the first connection of a command.

## Conventions

### Rust

- Match the surrounding code; `cargo fmt --all`; clippy-clean with `-D warnings` on `--all-targets`.
  No `unsafe` outside `meerschaum-plugin` and `meerschaum-py` without a `// SAFETY:` comment.
- Work inside your crate; edit the root `Cargo.toml` only for a `[workspace.dependencies]` entry two or
  more crates need. Shared Python-behavior helpers belong in `meerschaum-core`.
- `pyo3`, `arrow` and `polars` are workspace dependencies; upgrade them together. The Python boundary is
  our own PyCapsule code (`ffi_import.rs`, `ffi_export.rs`, `polars_bridge.rs`), not pyo3-arrow/pyo3-polars.
- Reproduce observable v4 behavior, quirks included; mark deliberate quirks `// PARITY:`. Rust-only
  behavior gets a crate test; behavior visible from Python also gets a pytest test.
- Integration tests isolate themselves: temporary `MRSM_ROOT_DIR`, empty `MRSM_PLUGINS_DIR`, unique table
  prefixes (`_mrsm_rs_test_*`, random connector labels), cleanup afterwards.
- Default every dependency and vendored asset to its newest stable release.

### Python

- The public API is frozen (§11.1). New Python code goes in the SDK / façade / host surfaces.
- `SuccessTuple` returns, `warn`/`error`/`info`/`dprint`, `attempt_import` for anything heavy (never a
  module-level import of pandas/polars/SQLAlchemy/Dash), `filter_arguments` / `filter_keywords` when
  calling plugin code, duck typing for pipes and users.
- The base wheel stays dependency-free: `pyproject.toml` extras and
  `meerschaum/utils/packages/_packages.py::extras` must agree (`check_packaging.py`); regenerate
  `requirements/<extra>.txt` with `--write`.

### Commits and public files

Emoji + imperative summary, as in `git log --oneline` (`🦀` release commits, `💚` CI/harness, `📝` docs,
`🧪` tests, `🐛` fixes, `✨` features, `🛡️` / `🔒` safety and security). Run
`scripts/parity/check_fixture_hygiene.sh` on any fixture directory or public file you touched: code,
fixtures and docs must never name private deployments, their organizations, people, home directories,
hostnames or credentials. Host-specific release settings live in the gitignored `.env`.

## Gotchas (verified)

- **Jobs start through a per-root admission gate** (D23, `meerschaum-jobs/src/startup.rs`): file-lock
  slots, random jitter and load/memory waits for bulk starts (`compose up`, `restart jobs`, several
  `start jobs`). `--no-stagger` (or `jobs.startup.enabled: false`) bypasses it; a job "stuck at starting"
  for 10 to 30 s is the gate.
- **`SqlDriver::ready()` must stay tiny**: its slow path is `Box::pin(...)`; an inline body overflowed the
  2 MiB tokio stack in debug builds. `driver::future_size_tests` guards it.
- **Empty frames export no Arrow batches**: the boundary must import an empty batch carrying the schema.
- **Polars at the Arrow boundary**: `Null` columns export `n_buffers = 1` (repaired in `ffi_import.rs`);
  Polars inputs go through `DataFrame.to_arrow()` when pyarrow is present; `ffi_import.rs` calls the
  private C stream callbacks through a `#[repr(C)]` mirror (see `crates/meerschaum-py/README.md`).
- **Dtype hints restore pandas backends** (`pandas_bridge.rs`, table in `PARITY.md`): the kernel erases
  `int64` vs `Int64` vs `int64[pyarrow]`; special columns stay `object`.
- **Forked job daemons and Tokio**: the extension's runtime is keyed by PID; never `block_on` an inherited
  runtime.
- **`get_config` is hot** (~35 calls per sync chunk): the binding reads the live Python dict and caches
  nothing so `set_config` / env patches are visible at once. Keep it that way.
- **Partitioned tables fold the datetime column into the primary key**, so an upsert's conflict target
  must include `dt_col` there.
- **MariaDB `get_sync_time(params=...)` on a partitioned table** uses `MAX`/`MIN` (an optimizer bug
  returns zero rows with `ORDER BY ... LIMIT 1`).
- **`get_chunk_bounds(align=True)`** (verify, ledger grid) anchors edges to the Unix epoch so they match
  partition boundaries; elsewhere it defaults to `align=False`.
- **The API server caches `Pipe` objects** (`pipes_dict`); `APIConnector.sync_pipe` clears the shared
  column-type cache after every sync. The server refuses `mrsm` connector keys on the generic routes.
- **`clean()` in `meerschaum.utils.sql` is only a keyword blocklist**: bind user values; use `clean()` for
  internal names.
- **A connector patch in `MRSM_CONFIG` sits under `meerschaum.connectors.<type>.<label>`**: a top-level
  `connectors` key is ignored, so `sql:main` keeps pointing at `localhost:5432`. Check the resolved port
  before mutating anything.
- **CI keeps `target/` on the self-hosted runners**; `scripts/ci/changes.py` decides which jobs a push
  runs, and a dispatch or `v5*` tag runs everything (`docs/design/v5-build.md` "CI: what runs when").
- **Read pytest summaries carefully**: "N passed, 2 warnings" is not "2 failed"; the two `test_worker.py`
  codec tests are order-dependent.
