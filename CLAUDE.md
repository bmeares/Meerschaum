# CLAUDE.md

Instructions for coding agents working on **Meerschaum 5** (the `main` branch). `AGENTS.md` is a
symlink to this file. Before changing architecture, read `docs/design/v5.0.0.md` (§4, §5, §6.1, §7,
§8, §9, §11, §12, §14) and [`docs/adr/`](docs/adr/); build, CI and Docker are in
`docs/design/v5-build.md`; crate layering is in `crates/README.md`; the user-facing story is
`docs/zensical/reference/migrating-to-v5.md`.

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
  route, and the Python public API. `SQLConnector.read/exec/exec_queries/value/to_sql()` and catalog
  helpers use Rust on every flavor (D16). Explicit `.engine`, caller-provided SQLAlchemy connections
  and helpers returning SQLAlchemy `Table` objects (`get_tables`) need `meerschaum[sql]`.
  The base wheel has **no runtime dependencies** (D20).
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
- **Isolated teams** (5.1) run their code in scoped, sandboxed processes on the API host or in
  separate team executors under the main API's signed control. Before touching `team_scope`,
  `execute_policy`, `executor*`, `team_*` or `source_fetch_*`, read
  [`docs/agents/teams-and-executors.md`](docs/agents/teams-and-executors.md).
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
- **Check the resolved `sql:main` before mutating anything.** Its default is `localhost:5432`, which
  can be a live database. A connector patch in `MRSM_CONFIG` must sit under
  `meerschaum.connectors.<type>.<label>`; a top-level `connectors` key is ignored.
- **Parity generators never use the ambient interpreter.** `scripts/parity/run.sh` runs
  `.venv-ref/bin/python -s -E` (released `meerschaum==4.1.4`, built by `scripts/parity/setup_reference.sh`);
  cross-implementation Rust tests use `MRSM_TEST_PYTHON` (default `.venv-ref/bin/python`).
- **Never set `CARGO_TARGET_DIR`.** Every crate shares `target/` (Polars/Arrow compile once). Several
  sessions may share the tree: concurrent builds block on the directory lock, `scripts/rust/build_py.sh`
  takes its own lock (`target/.build-py.lock`; never wrap it in `flock`), and never `git stash` /
  `checkout` / `restore` / `clean` / `reset`.
- **Qualify one checkout at a time.** Local worktrees sharing a physical target can reuse another
  checkout's workspace artifacts even after serialized builds. At a checkout transition,
  invalidate the workspace packages in the affected profile with `cargo clean -p` before
  counting results; preserve third-party caches. Record the source commit, profile and binary
  hashes. Never clean a target while another owner is building or testing.
- **Verify on a fresh root, like CI** (`git clean -ffdx`): tests that need plugin venvs create them in
  fixtures. Leave no processes behind (`pgrep -af 'target/debug/mrsm|meerschaum'`).
- **Never run two `scripts/test.sh` invocations (or `test.sh` + an `api`-flavor pytest) at once**: they
  share `test_root` and the test API on port 8989.

## Commands

`cargo` comes from rustup (`export PATH="$HOME/.cargo/bin:$PATH"`); `rust-toolchain.toml` pins the
toolchain.

| Command | What it does |
|---|---|
| `cargo build -p meerschaum --bin mrsm` | the debug standalone `mrsm` at `target/debug/mrsm` (what `cargo install meerschaum` builds; `tests/console` runs it). `cargo build -p meerschaum-cli` builds only `target/debug/mrsm-cli` for that crate's integration tests |
| `scripts/rust/build_py.sh [--debug \| --profile ci]` | builds the extension with maturin into `meerschaum/_core/_native.abi3.so` (release by default, ~10 min; `--profile ci` takes 2 to 4 min and is what to use for everyday Python test runs). Needs `maturin` and Python >= 3.11 (`PYTHON=...`; keep it stable between runs or Polars rebuilds) |
| `scripts/mrsm.sh <args>` | `python -m meerschaum` against `./test_root` + `./tests/plugins` |
| `scripts/rust/ci.sh <step>` | reproduces each CI job; its header lists the steps. Before pushing: `ci.sh guardrails fmt clippy doc` (`guardrails` runs `scripts/check_architecture.py` and the hygiene scan; `fmt` also runs `scripts/check_packaging.py`), plus `MRSM_PACKAGE_ALLOW_DIRTY=1 ci.sh package` after adding or moving files under `crates/` |
| `cargo test -p <crate> --test <suite> -- <module>::` | one module of a crate's integration suite (`ls crates/<crate>/tests` lists the suites) |
| `scripts/rust/ci.sh parity` | regenerates every `crates/*/tests/fixtures/gen_*.py` with v4.1.4 and fails on any diff |
| `python scripts/check_packaging.py --write` | regenerates `requirements/<extra>.txt` and `crates/meerschaum-cli/src/extras.rs` |
| `scripts/parity/check_fixture_hygiene.sh [--public] [PATH ...]` | before committing fixtures; `--public PATH ...` for docs and code (it also scans `*.md` and `*.py`) |

Python tests need the built extension and an existing root (use absolute paths: compose and spawned
processes change directory); `MRSM_PLUGINS_DIR` must be `./tests/plugins`:

```bash
mkdir -p test_root
MRSM_TEST_FLAVORS=sqlite PYTHONPATH=$(pwd) MRSM_ROOT_DIR=$(pwd)/test_root MRSM_PLUGINS_DIR=$(pwd)/tests/plugins \
  python -m pytest tests/ -q -p no:cacheprovider \
    --ignore=tests/test_jobs.py --ignore=tests/test_mcp.py --ignore=tests/test_listen_shutdown.py \
    --ignore=tests/api_host --ignore=tests/console

./scripts/test.sh db "" <pytest-args>            # full harness: Docker DBs, test API on 8989, --ff, -n=auto
```

Service URIs, flavor keys, standalone-binary and console tests: [`docs/agents/testing.md`](docs/agents/testing.md).
Benchmarks, the v4-to-v5 transition harness and docs builds are under `scripts/` (`bench/`,
`transition/`, `zensical.sh`, `build.sh`; never upload `build.sh`'s wheel). `MRSM_CORE_TRACE=1`
prints per-stage timings of each boundary call to stderr.

## Architecture map

### Crates (`crates/`, edition 2024; the workspace version in the root `Cargo.toml` is the single source of truth)

Dependencies point down only; `meerschaum-core` is a leaf; `cli` and `py` are the top layer.

| Crate | Owns |
|---|---|
| `meerschaum` | the published **front crate**: `pub use` of the public surface behind features (`sql`, `cli` default; `valkey`, `api`, `engine`, `server`, `plugin`, `full`) + `src/main.rs`, the `mrsm` binary. Every other crate is internal (`meerschaum-py` is `publish = false`) |
| `meerschaum-core` | `Pipe`, config registry + `MRSM{}` symlinks + `static.toml`, dtype vocabulary, template parser, users/tokens/plugins types, access (teams, roles, locks), team execution settings, team scope + sandbox (`team_scope/`), `SuccessTuple`/`SyncResult`, `version_ge`, shared Python-behavior helpers (`pyfmt`, `Value`, `shlex`), `self_exe` |
| `meerschaum-kernel` | Arrow/Polars `Frame`: `enforce_dtypes`, `filter_unseen_df` (`merge_dtype.rs`), `query_df`, special columns (json, uuid, numeric, bytes, geometry as GeoArrow WKB) |
| `meerschaum-connector` | `Connector` / `InstanceConnector` traits, `DataQuery` / `SyncOptions`, users/plugins/tokens over internal pipes, cross-pipe template resolution |
| `meerschaum-connector-sql` | `SqlConnector`: per-flavor query builders (`queries/`, golden-tested), `sqlx`, `tiberius`, `oracle` and a runtime-loaded `duckdb` driver; instance methods, partitioning, compression, maintenance, leases, `sync_pipe_inplace`, team databases, `facade.rs` |
| `meerschaum-connector-valkey` / `-api` | `ValkeyConnector` (`fred`); the `APIConnector` client (`reqwest`) + route/auth/JWT models, source-fetch client |
| `meerschaum-engine` | graph, planner (`skip`/`incremental`/`backfill`/`rebuild`/`observe`/`root`), executor (HEFT, leases, retries), dirty-range ledger, edge cursors, run history (`mrsm:engine_*` pipes), `--loop` / `--listen`, events, Gantt data |
| `meerschaum-worker` | Python worker pool (shared and per-team) + msgpack/Arrow-IPC protocol, `PythonConnector`, callbacks, supervised streams, plugin manifest (`schema/manifest.schema.json`) |
| `meerschaum-jobs` / `-compose` | daemon supervision (v4 `jobs/<name>/` layout), job executors, `-s` schedules, startup gate, team job environments; the compose model, `.env`, subactions, `explain` |
| `meerschaum-server` | `axum` API (every v4 route + `/engine/*` + `POST /execute`), OAuth2/JWT/tokens, rate limits, route allowlists, execute policy, team execution, team-executor control (`executor/`), the API-host proxy (`web.rs`), web terminal, MCP (`/mcp` + stdio) |
| `meerschaum-web` | web terminal page and assets (`/webterm/...`, vendored xterm.js), job-log SSE, native plugin pages (`/_mrsm/native-pages`), console sessions, Grafana team organizations |
| `meerschaum-plugin` | native plugin SDK: `Plugin` trait, `export_plugin!`, C ABI (`mrsm_plugin_v1`, Arrow C stream), loader, discovery, single-file `.rs` plugins and their confined compilation |
| `meerschaum-cli` | the CLI library: clap spec/argparse parity, dispatch, native actions (`src/native/`), the reedline shell (`src/shell/`), `start api`/`start mcp`/`start executor`, team executor client and process (`executor/`, `server/executor/`), team scopes, sandbox enforcement and execution (`team/`), source fetch (`source_fetch/`), venv accounting, engine views, self-upgrade |
| `meerschaum-py` | PyO3 bindings -> `meerschaum._core`; `PARITY.md` logs every Python-vs-Rust discrepancy, `PERF.md` the boundary benchmarks |

### Python package (`meerschaum/`)

| Path | Role |
|---|---|
| `_core/` | stdlib-only loader for `_native.abi3.so` (`AVAILABLE`, `LOAD_ERROR`, `CoreNotBuiltError`) |
| `_internal/` | `binary.py` (CLI entry, `MRSM_BIN`, `self_command()`), `_facade.py`, `entry.py`, `_removed.py`, `team_scope.py`, `arguments/`, `shell/` |
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

## Read when it applies

- [`GLOSSARY.md`](GLOSSARY.md): domain terms and the five meanings of "executor"; name things with these terms.
- [`CODING_STANDARDS.md`](CODING_STANDARDS.md): before writing or reviewing a diff.
- [`docs/adr/`](docs/adr/): decisions to build on, not reopen; flag any change that contradicts one.
- [`docs/agents/pipe-concepts.md`](docs/agents/pipe-concepts.md): before changing `Pipe`, parameters, sync, connectors, templating or config behavior.
- [`docs/agents/gotchas.md`](docs/agents/gotchas.md): before changing the SQL connector, the Python boundary, the server's pipe caches or job startup.
- `scripts/check_architecture.py`: when `ci.sh guardrails` fails; its docstring explains each rule.
- `.claude/skills/docs/SKILL.md`: before editing `docs/zensical/` or release notes.
- `.claude/skills/release/SKILL.md`: before staging or cutting a release.
- Issues live on GitHub (`gh`): [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md), labels in
  [`docs/agents/triage-labels.md`](docs/agents/triage-labels.md); glossary and ADR upkeep in [`docs/agents/domain.md`](docs/agents/domain.md).

## Commits and public files

Emoji + imperative summary, as in `git log --oneline` (`🦀` release commits, `💚` CI/harness, `📝` docs,
`🧪` tests, `🐛` fixes, `✨` features, `🛡️` / `🔒` safety and security). Run
`scripts/parity/check_fixture_hygiene.sh` on any fixture directory, and with `--public` on any other file you touched: code,
fixtures and docs must never name private deployments, their organizations, people, home directories,
hostnames or credentials. Host-specific release settings live in the gitignored `.env`.
