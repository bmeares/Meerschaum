# CLAUDE.md

Guidance for Claude Code working on the **v5** branch of Meerschaum. The authoritative design is
`docs/design/v5.0.0.md` (read §4, §5, §6.1, §7, §8, §9, §11, §12, §14 and decisions D12/D14/D16/D20
before touching anything); build/CI/Docker details are in `docs/design/v5-build.md`; crate conventions
and layering are in `crates/README.md`; the user-facing story is `docs/zensical/reference/migrating-to-v5.md`.

## What v5 is

- **A Rust rewrite, clean break.** The core (`Pipe`, parameters, config, `{{ Pipe() }}` templating),
  the data kernel (Arrow/Polars dtype enforcement and the `filter_existing` diff), the `sql:` / `valkey:` /
  `api:` connectors, the dependency-aware Syncing Engine, jobs, compose, the `axum` HTTP API + MCP server,
  and the `mrsm` CLI/shell are Rust crates under `crates/`. The web console is **not** Rust (D21): it is
  v4's Plotly Dash app ported onto the façade (`meerschaum/api/dash`, extra `meerschaum[dash]`), served by
  the API host; the Rust server keeps the web terminal, the job-log stream and a pointer page.
  No Python fallback ships in 5.0 and there is no v6 "to finish the port".
- **Python stays as SDK + façade + one extension host.** The `meerschaum` wheel = the `_core` PyO3
  extension + a façade (`mrsm.Pipe`, connectors, `get_config`, actions dispatch) + the plugin SDK
  (`attempt_import`, `Venv`, `@make_action`, hooks, `warn`/`error`/`info`/`dprint`, ...) + the
  **API host** (`python -m meerschaum.api.host`: `@api_plugin` routes, plugin MCP tools and the **one Dash
  console app** at `/dash` that plugin `@dash_plugin` / Dash `@web_page` pages register into; proxied by the
  Rust server, started only when a plugin needs it or the console is importable, over a Unix socket or, on
  Windows, loopback TCP with a per-start token) + the Python worker (`python -m meerschaum.worker`, what the Rust host runs plugins in).
- **Public surfaces do not change** (§11): pipe keys and every `parameters` key, config YAML layout,
  `MRSM_SQL_<LABEL>` env connectors, instance tables, the job directory layout, compose files, every HTTP
  route, and the Python public API. `SQLConnector.read/exec/value/to_sql/.engine/get_tables()` stay
  SQLAlchemy (D16, extra `meerschaum[sql]`). The base wheel has **no runtime dependencies** (D20); pandas,
  polars, geo, sql, api, dash, shell, cli-tools, gui and full are extras.
- **Where the branch is now.** After D14 (wave 16): the v4 Python core and the development gate that
  routed it to Rust are gone, so there is one implementation and tests run once. `meerschaum._core`
  is **required** (an unbuilt tree raises `CoreNotBuiltError`, naming `scripts/rust/build_py.sh`).
  A removed v4 module raises a `ModuleNotFoundError` naming its replacement
  (`meerschaum/_internal/_removed.py`: add an entry when you delete a module). **One PyPI wheel, one CLI
  program** (D8 revised): the `mrsm` / `meerschaum` console scripts and `python -m meerschaum` call
  `_native.main(argv)` (`_meerschaum_mrsm.py`, a top-level module in the wheel, -> `meerschaum/__main__.py` -> `_internal/binary.py`), which runs the whole CLI inside
  the extension (real fds, real signals, GIL released; `meerschaum_cli::run_main` is shared with the
  standalone binary); `cargo install meerschaum` builds that same binary from the front crate's `cli`
  feature; `MRSM_BIN` makes Python exec a standalone binary instead. Everything that re-launches `mrsm`
  (job supervisors and workers, compose, `systemd` units, pipeline steps, the web terminal) asks
  `meerschaum_core::self_exe::self_command()`: `[current_exe]` standalone, `[sys.executable, "-P", "-m",
  "meerschaum"]` in the extension (`-P`: the cwd never shadows the package; workers and the API host spawn
  with `-P` too). `import meerschaum` stays EAGER (threads importing `meerschaum.*` while others touch
  `mrsm.Pipe` must not deadlock) except in the CLI process (`sys._meerschaum_cli`, set by `_meerschaum_mrsm`,
  or `python -m meerschaum` seen in `sys.orig_argv`), where it is lazy (PEP 562, `_internal/_facade.py`
  holds the imports) so `mrsm` does not pay for the façade. `meerschaum.entry([...])` does not
  start a process: it runs the same native dispatcher in-process (`_core.entry`). There is no Python CLI. v4 stays the oracle only through the
  `.venv-ref` parity fixtures and the recordings in `tests/oracle/` (`tests/_oracle.py`).

## Isolation rules (read before running anything)

- **Always set a scratch root and plugins dir**: `MRSM_ROOT_DIR=./test_root` (or a fresh `mktemp -d`
  under `~/.cache/mrsm-<you>/`, **not** `/tmp`: it is a tmpfs near its inode limit) and
  `MRSM_PLUGINS_DIR=./tests/plugins` (or an empty temp dir). Never touch the real `~/.config/meerschaum`.
  Delete scratch roots by literal path when done.
  `cargo` processes get the tracked throwaway root `.mrsm-cargo-root` from `.cargo/config.toml` unless
  `MRSM_ROOT_DIR` is already set.
- **Debug builds refuse the default root.** `target/debug/mrsm` (and `maturin develop` builds) exit
  with `DefaultRootRefused` when `MRSM_ROOT_DIR` is unset. Do **not** bypass this with
  `MRSM_ALLOW_DEFAULT_ROOT`; set `MRSM_ROOT_DIR` instead. Release builds are unaffected.
- **`PYTHONPATH=$(pwd)` for every Python invocation.** A stale `meerschaum` under `~/.local` shadows
  the working tree under a bare `python -m pytest`; it silently lacks new modules and behaves like an
  older release. Check `meerschaum.__file__` first when a result looks impossible.
- **Parity generators never use the ambient interpreter.** `scripts/parity/run.sh` runs
  `.venv-ref/bin/python -s -E` (released `meerschaum==4.1.4`) and refuses anything else; cross-impl
  Rust tests run `MRSM_TEST_PYTHON` (default `.venv-ref/bin/python`) with `-E`, so that interpreter
  must itself have `meerschaum` installed (`scripts/parity/setup_reference.sh` builds it).
- **Never set `CARGO_TARGET_DIR`.** Every crate shares `target/` (Polars/Arrow compile once; disk on
  the self-hosted runner is limited). Concurrent builds block on the directory lock; batch them.
  Several agents may share the tree: `scripts/rust/build_py.sh` takes the tree's own lock
  (`target/.build-py.lock`) and waits for it, so never wrap it in `flock` yourself; and never
  `git stash` / `checkout` / `restore` / `clean` / `reset`.
- **Verify on a fresh root, like CI.** CI starts from `git clean -ffdx`: a test that passes only
  because `./test_root` already holds a plugin venv or cache is broken. Tests that need plugin venvs
  create them in fixtures. Leave no processes behind (`pgrep -af 'target/debug/mrsm|meerschaum'`).
- **Never run two `scripts/test.sh` invocations (or `test.sh` + an `api`-flavor pytest) at once**:
  they share `test_root` and the test API on port 8989, and the contention looks like real bugs
  (`Connection refused`, pool exhaustion).

## Commands

```bash
export PATH="$HOME/.cargo/bin:$PATH"       # rustup toolchain from rust-toolchain.toml (stable + rustfmt + clippy)
export CARGO_INCREMENTAL=0                 # what CI uses; optional locally
```

### Build

| Command | What it does |
|---|---|
| `cargo build -p meerschaum --bin mrsm` | the debug standalone `mrsm` at `target/debug/mrsm` (the front crate's `cli` feature, what `cargo install meerschaum` builds; `tests/console` runs it). Oracle needs Instant Client (`libclntsh`) only at runtime. `cargo build -p meerschaum-cli` builds only the internal `target/debug/mrsm-cli` its integration tests run (named differently so `--workspace` never writes two `mrsm`) |
| `cargo build --release -p meerschaum --bin mrsm` | the release binary (`scripts/bench/run.py` uses it; the Docker image and the wheel run the CLI from the extension instead) |
| `scripts/rust/build_py.sh [--debug \| --profile ci]` | builds `crates/meerschaum-py` with maturin and drops `meerschaum/_core/_native.abi3.so` into the working tree (release by default, ~10 min, atomic swap; the `meerschaum` package does not work without it). `--profile ci` (Cargo `[profile.ci]`: no LTO, opt-level 1) takes 2 to 4 minutes and is what the pytest CI job builds and what to use for everyday Python test runs; only `--release` artifacts ship. Needs `maturin` (`uv tool install maturin`) and a Python >= 3.11 (`PYTHON=...`). Keep `PYTHON` stable between runs or Polars rebuilds. It takes the tree lock `target/.build-py.lock` itself (concurrent builds would corrupt each other); do not wrap it in `flock`. |
| `scripts/rust/clean_py.sh [--cargo]` | removes the built extension |
| `cargo build -p meerschaum-plugin --example native_demo` | the example C-ABI plugin |
| `scripts/mrsm.sh <args>` | `python -m meerschaum` against `./test_root` + `./tests/plugins` (the façade launcher: the CLI runs from the built extension; an explicit `MRSM_BIN` is accepted only if its `--version` equals the extension's and, in CI, it is newer than the Rust sources) |

### Checks (what CI runs; `scripts/rust/ci.sh <step>` reproduces each job)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings      # CI: `cargo clippy --all-targets -- -D warnings`
cargo test -p <crate>            # one crate; `cargo test --workspace` is the CI job
scripts/rust/ci.sh               # = fmt clippy test build (test = cargo nextest when installed + `cargo test --doc`; MRSM_CI_ACCEL=1 enables sccache/nextest locally)
scripts/rust/ci.sh parity        # regenerate every crates/*/tests/fixtures/gen_*.py with v4.1.4 and diff
scripts/parity/check_fixture_hygiene.sh [PATH ...]   # run before every commit that touches fixtures (and on any new public file)
python scripts/check_packaging.py            # Cargo version (`5.0.0-rc.6` <-> PEP 440 `5.0.0rc6`) == _version.py; console scripts `mrsm`/`meerschaum`; no `dependencies`, no second wheel; extras == _packages.py == requirements/ == extras.rs; lockstep workspace dep versions; crates.io metadata
scripts/rust/ci.sh frontcrate                # the `meerschaum` front crate: no default features, each feature alone, `full` tests (CI job `frontcrate`)
scripts/rust/ci.sh package                   # cargo package --workspace --no-verify + contents/size checks (CI job `package`); `publish-dry-run` also builds the tarballs (it mutates the shared `target/` and `$CARGO_HOME`, see `crates/README.md` "Publishing"; publish from Linux/macOS because LICENSE/NOTICE are symlinks)
python scripts/check_packaging.py --write    # regenerate requirements/<extra>.txt and crates/meerschaum-cli/src/extras.rs
```

Crate integration tests skip unless their service URI is set (an empty value counts as set):

| Variable | Used by |
|---|---|
| `MRSM_TEST_PG_URI`, `MRSM_TEST_TIMESCALE_URI`, `MRSM_TEST_POSTGIS_URI` | `meerschaum-connector-sql` drivers/instance tests, engine crash-restart (`w9_engine` schema) and lease tests (`w7_engine`) |
| `MRSM_TEST_TIMESCALE_HA_URI` | `meerschaum-connector-sql/tests/w17c_geometry_srid.rs`: TimescaleDB-HA with PostGIS, using the `timescaledb-ha://` scheme |
| `MRSM_TEST_MYSQL_URI`, `MRSM_TEST_MARIADB_URI`, `MRSM_TEST_MSSQL_URI`, `MRSM_TEST_ORACLE_URI` | `meerschaum-connector-sql` (MSSQL is the default `mssql` feature; `--features oracle` is opt-in) |
| `MRSM_TEST_GPKG_DIR`, `MRSM_TEST_DUCKDB_PATH` | GeoPackage / DuckDB file flavors (empty = system temp dir) |
| `MRSM_DUCKDB_LIB` | Explicitly pins the native DuckDB C API for this process (library >= 1.4; pinned download 1.5.6). Standalone / base installs find it in `<root>/drivers`, the system loader, or download on first use unless `MRSM_NO_AUTO_INSTALL`. In Python, leave unset with `meerschaum[sql-duckdb]` installed to share the public Python driver between Rust Pipe operations and SQLAlchemy. A native-pinned Python process refuses SQLAlchemy DuckDB access: unset the override and restart to use both safely. `MRSM_TEST_DUCKDB_LIB` supplies native-only test fixtures without pinning SDK tests. |
| `MRSM_TEST_VALKEY_URI` | `meerschaum-connector-valkey`, engine `leases_backends` |
| `MRSM_TEST_MQTT_URI` (e.g. `mqtt://127.0.0.1:1893`) | `meerschaum-engine/tests/mqtt_stream.rs` (disposable Mosquitto, QoS 1) |
| `MRSM_TEST_PYTHON`, `MRSM_TEST_KEEP=1` | cross-implementation tests in `meerschaum-connector-sql` (keep the databases for inspection) |

The test databases come from `tests/docker-compose.yaml` (`docker compose -f tests/docker-compose.yaml up -d postgresql postgis mariadb mysql ...`);
their host ports are in `tests/connectors.py` and `crates/meerschaum-connector-sql/README.md`.

### Python tests (pytest)

```bash
# sqlite; the suite runs once. Needs the built extension (jobs and the CLI run from it); a few tests also use a standalone `mrsm` (`MRSM_TEST_BIN`, else the newest `target/{release,debug}/mrsm` outside CI; `tests/_binary.py` checks its `--version` and age and fails a stale one in CI) and skip without one. `scripts/mrsm.sh` / `test.sh` run `mrsm` through the built extension, never a `target/` binary; the pytest workflow builds the binary in the job (`scripts/rust/ci.sh bin-debug`) and passes `MRSM_TEST_BIN`.
MRSM_TEST_FLAVORS=sqlite PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
  python -m pytest tests/ -q -p no:cacheprovider \
    --ignore=tests/test_jobs.py --ignore=tests/test_mcp.py --ignore=tests/test_listen_shutdown.py \
    --ignore=tests/api_host --ignore=tests/console

# a single file / test
MRSM_TEST_FLAVORS=sqlite PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \
  python -m pytest tests/test_pipeline.py::test_parents_true_syncs_the_ancestors_first -v

# the full harness (Docker, starts the test API on 8989, hardcodes --ff and -n=auto)
./scripts/test.sh db "" <pytest-args>
MRSM_TEST_FLAVORS=sqlite ./scripts/test.sh db ""
```

- Tests that used to compare the Python and Rust implementations replay recordings of the
  parity-phase Python mode (`tests/_oracle.py`, type-tagged JSON `tests/oracle/<module>.json`, keyed
  by node id, read without `pickle`). They are not v4.1.4 output and cannot be re-recorded (the code
  that produced them is deleted); a test whose data depends on the pandas major lists itself in
  `pandas_dependent` and skips under another major.
- Tests needing the extension (`test_pipeline.py`, `test_core_pipe.py`, `test_d14_bindings.py`, ...)
  fail when it is not built (a broken build must not skip); build it first.
- `MRSM_PLUGINS_DIR` must be `./tests/plugins`: `test_maintenance.py`, `test_verify.py`, `test_worker.py`
  and the engine tests need the synthetic `stress` plugin there (an empty plugins dir makes them fail
  for harness reasons, not façade bugs); `test_page_figure.py` and `tests/console` use the `w7_*` plugins.
- **Flavor keys != SQL flavor names.** `tests/connectors.py` defines `timescaledb`, `timescaledb_ha`,
  `postgis`, `mariadb`, `mysql`, `mssql`, `oracle`, `sqlite`, `api`, `valkey`. There is no `postgresql`
  key (the PostgreSQL-family connector is `postgis`); an unknown key resolves to an empty set and
  `test.sh` exits before pytest runs. Default set for `test.sh`: `api,timescaledb`.
- `scripts/test.sh` requires `pytest-xdist` and `pytest-cache`; `-p no:cacheprovider` breaks its `--ff`.
  For non-default pytest flags call `python -m pytest` directly with the `MRSM_*` variables exported.

### Console (Playwright) and API host tests

```bash
cargo build -p meerschaum --bin mrsm                         # tests/console uses MRSM_TEST_BIN or target/debug/mrsm (version and age checked); the interpreter needs meerschaum[dash,sql,pandas] + Playwright's Chromium
cargo build -p meerschaum-plugin && tests/console/build_native.sh <plugins-dir>   # the synthetic native page plugin
MRSM_TEST_CONSOLE=1 PYTHONPATH=$(pwd) python -m pytest tests/console -v
PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins python -m pytest tests/api_host -v
```

`tests/console` is opt-in (skipped without `MRSM_TEST_CONSOLE=1`): it starts real `mrsm start api`
servers (the Rust server + the Python host serving the **Dash console**) on random high ports with a
temporary root, a SQLite instance and the `w7_pyfig` / `w7_dashapp` / `w7_native_fig` plugins, and drives
the Dash pages, the pipelines page, Exec through `POST /execute`, page-contract pages (in their frame),
the webterm iframe and the pointer page with Chromium. It never uses 8989 or a container.
`tests/api_host` covers the host and the console's support code in process (no browser). Tests that wait
for jobs started through `compose up` or a bulk start must pass `--no-stagger` (the startup gate adds up to
10 s of jitter there, see Gotchas).

### Parity fixtures, benchmarks, docs, release build

```bash
scripts/parity/setup_reference.sh                       # .venv-ref/: released meerschaum==4.1.4 + pinned data libs (scripts/parity/requirements.txt)
scripts/parity/run.sh crates/<crate>/tests/fixtures/gen_<x>.py [args]   # regenerate one fixture with the reference
python scripts/bench/run.py --label <name>              # fixed sync benchmark set, v4.1.4 vs v5 (needs the release binary; results in scripts/bench/results/)
scripts/transition/run.sh [--flavors sqlite,postgis,...] [--wheel <whl>]   # v4 -> v5 upgrade harness: state created by released 4.1.4, continued by the tree (README there; uv + network; touches only trn* objects)
./scripts/zensical.sh                                   # serve docs/zensical/; ./scripts/pdoc.sh builds the Python API reference (PDOC_ALLOW_EXEC=1)
./scripts/build.sh                                      # clean, docs, check_packaging --write, `python -m build` for both maturin projects, wheel checks
```

`MRSM_CORE_TRACE=1` prints per-stage timings of each boundary call (and SQLite staged-update stages) to stderr.

## Architecture map

### Crates (`crates/`, Cargo workspace, edition 2024, version `5.0.0-rc.6` is the single source of truth)

Dependencies point down only; `meerschaum-core` is a leaf; `cli` and `py` are the top layer no library
depends on (the front crate's default `cli` feature pulls `cli` in for the `mrsm` binary only). Verify with `cargo tree -p <crate> -e normal --depth 1`.

| Crate | Owns |
|---|---|
| `meerschaum` | the **front crate** (Polars model) published to crates.io: thin `pub use` of the curated public surface behind features (`sql` and `cli` default, `valkey`, `api`, `engine`, `server`, `plugin`, `full`); no logic except `src/main.rs`, the `mrsm` binary of `cargo install meerschaum` (`required-features = ["cli"]`; library users use `default-features = false`). Every other crate is internal (`meerschaum-py` is `publish = false`) |
| `meerschaum-core` | `Pipe` (keys, raw vs resolved parameters, `target`), config registry + `MRSM{}` symlinks + `static.toml` (D15, the old `STATIC_CONFIG`), dtype vocabulary, `{{ Pipe() }}` template parser, users/tokens/plugins types, `SuccessTuple`/`SyncResult`, PEP 440 `version_ge`, shared Python-behavior helpers (`pyfmt`, `Value`, `shlex`) |
| `meerschaum-kernel` | Arrow/Polars `Frame`: `enforce_dtypes`, `filter_unseen_df` (pandas merge-dtype rules in `merge_dtype.rs`), `query_df`, special-column detection/serialization (json, uuid, numeric, bytes, geometry as GeoArrow WKB) |
| `meerschaum-connector` | the `Connector` / `InstanceConnector` traits, `DataQuery` / `SyncOptions`, generic users/plugins/tokens over internal pipes, cross-pipe template resolution |
| `meerschaum-connector-sql` | `SqlConnector`: per-flavor query builders (`queries/`, golden-tested), `sqlx` (PostgreSQL family, MySQL family, SQLite/GeoPackage), `tiberius` (mssql), `oracle`, and `duckdb` (a loader for DuckDB's own shared library: `driver/duckdb/`, `duckdb_lib/`; no DuckDB code is compiled in) drivers; instance methods with the v4 names; partitioning, compression, maintenance, leases (`mrsm_pipe_leases`), `sync_pipe_inplace`, `facade.rs` helpers for the bindings |
| `meerschaum-connector-valkey` / `meerschaum-connector-api` | `ValkeyConnector` over `fred`; the `APIConnector` client (`reqwest`) plus shared route/auth/JWT models |
| `meerschaum-engine` | graph (parents/children/`{{ Pipe() }}` edges), planner (`skip`/`incremental`/`backfill`/`rebuild`/`observe`/`root`), executor (HEFT priorities, leases, retries), dirty-range ledger + edge cursors + run history as `mrsm:engine_*` internal pipes, continuous mode (`--loop` laps, `--listen` streams), events, Gantt data |
| `meerschaum-worker` | the Python worker pool + msgpack/Arrow-IPC wire protocol, `PythonConnector` adapter, callbacks (`pipe.sync()` from plugin code), supervised streams, the plugin manifest (`<root>/cache/plugins_manifest.json`, schema in `schema/manifest.schema.json`) |
| `meerschaum-jobs` / `meerschaum-compose` | daemon supervision with v4's `jobs/<name>/` layout, `-s` schedule grammar, laps, per-root scheduler; the compose model, `.env`/envyaml, subactions, `explain` lint |
| `meerschaum-server` | the `axum` API (every v4 route + `/engine/*` + `POST /execute`), OAuth2/JWT/tokens, rate limits, allowlist route groups, the API-host proxy (Unix socket, or TCP + token on Windows) that forwards `/dash*` to the host when it reports `console.mounted` (`web.rs`, else the static pointer page for `pip install meerschaum[dash]`), MCP (`/mcp` + stdio) with federation of plugin tools |
| `meerschaum-web` | what is left of the web side (D21: the console is Dash, not Rust): the web terminal (`/webterm/<session-id>` + vendored xterm.js at `/webterm/assets/*`), the job-log SSE stream (`GET /logs/{name}/stream`), native plugin pages served as fragments to the Dash console (`/_mrsm/native-pages`), console sessions (`mrsm-session-id`, Valkey or the `mrsm_sessions` pipe) |
| `meerschaum-plugin` | the native plugin SDK: `Plugin` trait + `export_plugin!`, the C ABI (`mrsm_plugin_v1`, Arrow C stream), loader, discovery (`<name>/plugin.toml` + `lib<name>.so`), native manifest cache |
| `meerschaum-cli` | the CLI library (internal; `run_main()` is the process entry shared by the `meerschaum` front crate's `mrsm` binary and `_native.main`; its own `mrsm-cli` bin runs its integration tests): clap spec/argparse parity, dispatch, every native action (`src/native/`), the reedline shell with prompt_toolkit parity (`src/shell/`), `start api`/`start mcp` (`server.rs`), venv accounting (`show`/`prune venvs`), engine views (`plan pipes`, `show graph`, `show runs`) |
| `meerschaum-py` | PyO3 bindings -> `meerschaum._core` (`src/lib.rs`; `main_api.rs` = `_native.main`, the `mrsm` console script's entry; kernel, core, `SqlConnector`, `Pipe`, engine, verify and D14 APIs); `PARITY.md` logs every python-vs-rust discrepancy and its resolution, `PERF.md` the boundary benchmarks |

### Python package after v5 (`meerschaum/`)

| Path | Role |
|---|---|
| `_core/` | loader package (`__init__.py`, stdlib only, lazy PEP 562 load of `_native.abi3.so`; `AVAILABLE`, `LOAD_ERROR`, `CoreNotBuiltError`); the extension is required |
| `_internal/` | `binary.py` (the `mrsm` entry: `_native.main` in-process, `MRSM_BIN` exec, `self_command()`), `_facade.py` (the façade imports `meerschaum/__init__` runs on first use in the CLI process), `entry.py` (`entry()` calls `_core.entry`, the in-process dispatcher), `_removed.py` (the `ModuleNotFoundError` finder naming replacements of deleted modules), `arguments/`, `shell/` |
| `core/Pipe/` | the façade `Pipe`; the parameter store is `_core.Pipe` and `_live_parameters.py` provides the live `pipe.parameters` views (`LiveDict`/`RawDict`); methods call the `_core.pipe_*` bindings |
| `core/Pipeline/` | `mrsm.Pipeline`, `Plan`, `RunResult`, `pipe.sync(parents=, children=)`, event decoding (`_events.py`), calls into `_core` (`_engine.py`). Rust-only: needs the built extension |
| `connectors/` | façades; `SQLConnector._rust_method()` builds one `_core.SqlConnector` per Python connector (rebuilt when the PID changes); `read/exec/value/to_sql/.engine` stay SQLAlchemy |
| `plugins/` | the SDK decorators, `_page.py` (`figure()` / `figure_json()` for the page contract) |
| `worker/` | `python -m meerschaum.worker`: the v4 plugin machinery driven over the worker protocol |
| `api/host/` | `python -m meerschaum.api.host`: the FastAPI host for `@api_plugin`, plugin MCP tools, and `host/dash.py` (mounts the one console app at `/dash`, reports `GET /_mrsm/host/status`, frames page-contract pages, redirects rc1's `/dash/<plugin>/` URLs) |
| `api/dash/` | the Plotly Dash console, v4's pages/callbacks/components ported onto the façade (`build_dash_app()` builds the ONE app lazily; `_support.py` replaces deleted v4 internals; `execute.py` = Exec through the server; `engine.py` + `pages/pipelines.py` = the v5 pipelines page; `sessions.py` = plugin-facing session helpers). Plugins register into it; `tests/console` drives it |
| `api/`, `actions/`, `utils/`, `config/` | what is left of the v4 packages: SDK helpers and façades over `_core` (`config` is the live dict plus `_core.get_config`; `utils.daemon.Daemon` and `jobs.Job` read and control the Rust supervisor; `actions.actions[<name>]` dispatches to the binary; `api/` keeps `host/` and the helpers the host uses). Everything else was deleted: see the D14 table at the end of `crates/meerschaum-py/PARITY.md` and "Removed in 5.0" in the migration guide |

### Façade pattern

Façade methods call one `_core` binding each (`_core.pipe_get_data(...)`, `_core.sync_pipe(...)`,
`SQLConnector._rust_method(name)` over one `_core.SqlConnector` per Python connector), and keep only
argument adaptation, caches and the pandas/Polars boundary in Python. There is no Python fallback:
an instance without a usable Rust driver is an error naming the cause (`oracle` needs `libclntsh`),
never a silent v4 read. `SQLConnector._rust_method` raises `AttributeError` for a name the extension
does not bind (`read`, `exec`, ... stay SQLAlchemy and raise an `ImportError` naming `meerschaum[sql]`
when the extra is missing). A result that differs from v4 is a parity
bug: fix it at the boundary (`src/pandas_bridge.rs` dtype hints) or in the kernel, and add a row to
`PARITY.md` (the `.venv-ref` fixtures are the reference for v4.1.4; `tests/oracle/` holds recordings of the
parity-phase Python mode).

### Parity fixtures

- `crates/*/tests/fixtures/*.json` are **generated by the released v4.1.4** through
  `scripts/parity/run.sh crates/<crate>/tests/fixtures/gen_<x>.py`; never hand-edit them. CI's
  `parity` job regenerates every `gen_*.py` and fails on any diff or untracked file. Commit the
  generator next to its fixture. Exception: the engine planner fixtures
  (`crates/meerschaum-engine/tests/fixtures/*.json`) are written by hand from the spec, as their
  README says, because v4 has no planner.
- Fixture data must be synthetic. Generators run with a fresh temporary root, an empty plugins dir and
  no inherited `MRSM_*` variables (v4 reads every `MRSM_<TYPE>_<LABEL>` as a connector); servers they
  start use `env -i`. Scrub pids, timestamps, paths and minted credentials at the source.
- `// PARITY:` marks a v4 quirk reproduced on purpose, with the Python line it comes from.

## Pipe concepts the façade preserves

**`SuccessTuple`** (`Tuple[bool, str]`) is still the universal return type for actions, pipe methods
and connector methods: return `(True, "...")` / `(False, "reason")`, never raise from action-level code.
The Rust side returns `SyncResult` (`meerschaum-core/src/success.rs`): `(success, message)`-compatible
plus `inserted` / `updated` / `upserted` / `duration` / `begin` / `end`; the Python `pipe.sync()`
returns `mrsm.SyncResult` (`meerschaum/core/Pipe/_sync.py`), a `tuple` subclass of the same shape, so
`success, msg = pipe.sync()` keeps unpacking. Use `warn` / `error` / `info` /
`dprint(msg, debug=debug)` from `meerschaum.utils.warnings`, not `print()`.

**Identity.** `Pipe(connector_keys, metric_key, location_key=None, instance=...)`. `pipe.target` defaults
to `{connector_keys with ':'→'_'}_{metric_key}[_{location_key}]`; SQL instances use `schema.target`.
Constructing a `Pipe` never touches the instance. `__eq__` / `__hash__` by keys; `str(pipe)` and pickling work.
Engine state keys on `pipe_ref` = `"{instance}|{connector_keys}|{metric_key}|{location_key}"`, never on `pipe.id`.

**`parameters`** is a live dict (assign, then `pipe.edit()`; or `update_parameters({...}, persist=True)`).
Every key is part of the compatibility contract (§11.1):

| Key | Purpose |
|---|---|
| `columns` | role -> column: `datetime` (drives incremental sync), `id`, `primary`, `value` |
| `dtypes` | Meerschaum dtype strings: `datetime`, `int`, `numeric`, `uuid`, `json`, `bytes`, `geometry[...]` (every notation: `geometry[EPSG:4326]`, `geometry[MULTIPOLYGON, 6570]`, bare `geometry`), or any pandas dtype string; role-keyed (`dtypes: {datetime: ...}`) resolves through `columns` |
| `indices` / `indexes`, `tags`, `target`, `upsert`, `static`, `autoincrement`, `autotime`, `null_indices`, `mixed_numerics`, `enforce`, `precision` | as in v4 |
| `fetch` | `backtrack_minutes`, `definition` (SQL metadefinition); `sql` at the top level is the same |
| `verify` | `chunk_minutes` (default 43200) and aliases (`chunk_hours`/`days`/`weeks`/`years`/`seconds`, `chunk_range` on an integer axis), `bound_days`; also the native-partition width |
| `hypertable`, `hypercore`, `compress` | TimescaleDB hypertable / columnstore; native range partitioning on PG/MySQL/MSSQL (default `True` on a datetime axis) |
| `reference` / `references` | parameter inheritance only; **not** a graph edge |
| `parents` / `children` (`parent` / `child`) | graph edges for the engine; entries may carry `lookback_*` / `lookahead_*` / `axis` (stripped before the entry becomes a `Pipe`) |
| `schedule`, `min_seconds`, `retry`, `stream` | engine: per-root cadence in `--loop`, child rate limit, `{max, backoff_seconds}`, micro-batch triggers `{idle_ms, max_latency_ms, max_rows, max_bytes}` |

**Sync flow** (`pipe.sync(df=None, begin='', end=None, ...)`): fetch (`pipe.connector.fetch`, or the
SQL metadefinition `WITH "definition" AS (...) SELECT * ... WHERE dt >= begin - backtrack AND dt < end`),
`enforce_dtypes`, `filter_existing` (backtrack window + `filter_unseen_df`), `instance.sync_pipe`.
`begin=''` = sync time minus backtrack; `None` = unbounded. A bare `pipe.sync()` keeps v4 semantics
(R1: no graph, no capping). `pipe.sync(parents=True|N, children=True|N)` and `mrsm.Pipeline(...)`
run the engine: children are capped at `min(parent watermarks)`, definition changes (`sql`, `fetch`,
`dtypes`) mean `rebuild` (needs `--yes` / `auto_rebuild`, else `rebuild_pending`), ledger entries drive
`backfill`. CLI: `sync pipes --parents/--children/--resume <run_id>`, `plan pipes`, `show graph`,
`show runs`, `show run <id>`, `--loop --min-seconds N` (= v4 laps for a flat selection), `--listen`.

**`params` filter** (`get_data`, `get_rowcount`, `get_sync_time`, `clear`, `get_pipes`, `query_df`): value
= `WHERE col = v`; list = `IN`; `_` prefix negates (`'_red'`, `['_a','_b']` = `NOT IN`); `None` / `'None'` =
`IS NULL`, `'_None'` = `IS NOT NULL`; a dict value compares the JSON text. With `pipe.enforce=True`
(default) unknown columns are dropped from `params` on SQL pipes.

**Templating.** `{{ Pipe('ck', 'mk', 'lk', instance=...) }}` resolves to the table reference; attribute
chains (`{{ Pipe(...).columns['datetime'] }}`) and `{{ self.parameters['x'] }}` into values;
`MRSM{a:b:c}` reads config at access time. Implicit `{{ Pipe() }}` references are engine edges
(`system.engine.implicit_edges`, default on); a self-reference is ignored. Raw (persisted) parameters
keep the templates; resolved parameters are for execution.

**Connectors.** `Connector` -> `InstanceConnector` -> `SQLConnector` / `APIConnector` / `ValkeyConnector`;
custom ones via `@make_connector` (`IS_INSTANCE`, `IS_THREAD_SAFE`, `IS_STREAMING` + `subscribe(pipe, sink)`,
optional `acquire_pipe_lease` / `release_pipe_lease`, `REQUIRED_ATTRIBUTES`). Python connectors and
plugins are reached from Rust **only** through the worker adapter; Rust never calls a Python
implementation of something it implements itself (D12). Plugins: `fetch` / `sync` / `register` /
`setup` in `MRSM_PLUGINS_DIR`, `required` lists, per-plugin venvs, `@make_action`, `add_plugin_argument`,
`@pre_sync_hook` / `@post_sync_hook`, `@api_plugin`, `@dash_plugin`, `@web_page` (returns HTML = page
contract, framed by the console; returns a Dash layout = a page of the one console app),
`meerschaum.mcp.tool` / `resource`.

**Config.** `get_config('a', 'b')` over YAML under `MRSM_ROOT_DIR/config/`; `MRSM_SQL_<LABEL>` /
`MRSM_API_<LABEL>` URIs define connectors; `MRSM_CONFIG` (JSON) patches config in tests; the engine's
keys live under `system.engine.*` (`workers`, `python_workers`, `limits.connectors`, `auto_rebuild`,
`implicit_edges`, `inclusive_cap`, `flat_ledger`, `state_flush_ms`, `run_history_days`,
`debounce_seconds`, `chunk_pipelining`, `default_duration_seconds`). `MRSM_PYTHON` picks the
interpreter the binary runs workers with (`tests/console` sets it to `sys.executable`). Before the first
worker starts the binary preflights the interpreter once (`python -P -c`, 10 s: it must import a
`meerschaum` of the same major); a refusal is one message naming the interpreter and the fix, remembered
for seconds-to-minutes, and a worker has `MRSM_WORKER_STARTUP_TIMEOUT` seconds (default 60) to connect.
`connect_timeout` (connector attribute; `MRSM_CONNECT_TIMEOUT` overrides, default 5 s) bounds the first
connection of a command to a SQL/API/Valkey server, and a refused connection, unknown host or bad
credentials fail at once (`SqlDriver::ready()`; keep its slow path boxed, see Gotchas).

## Conventions

### Rust

- Match the surrounding code; `cargo fmt --all`; clippy-clean with `-D warnings` on `--all-targets`
  before you finish (CI blocks on both). No `unsafe` outside `meerschaum-plugin` and `meerschaum-py`
  without a `// SAFETY:` comment.
- One crate per directory; work inside your crate. Edit the root `Cargo.toml` only to add a
  `[workspace.dependencies]` entry that two or more crates need; crate-private deps go in the crate.
  Shared Python-behavior helpers belong in `meerschaum-core`, never copied into another crate.
- `pyo3`, `arrow` and `polars` are workspace dependencies (0.30, 60 and 0.55); upgrade them together
  (only one pyo3 can link libpython, RecordBatches cross crates). No pyo3-arrow / pyo3-polars: the
  Python boundary is our own PyCapsule code (`ffi_import.rs`, `ffi_export.rs`, `polars_bridge.rs`).
  `extension-module` is an opt-in crate feature so `cargo test -p meerschaum-py` links libpython.
- Reproduce observable v4 behavior, quirks included (§4.4 "What the façade must preserve"); where v4
  is wrong, keep it and mark `// PARITY:` with the Python line. Rust-only behavior gets a crate test;
  behavior visible from Python also gets a pytest test.
- Every integration test isolates itself: fresh temporary `MRSM_ROOT_DIR`, empty `MRSM_PLUGINS_DIR`,
  unique table prefixes (`_mrsm_rs_test_*`, random `rs<id>` connector tags, `w<N>_` schemas), and
  cleanup afterwards. Tests must never read a developer's real root or connectors.

### Python

- The public API is frozen (§11.1); the v4 internal modules are deleted (D14). New Python code goes in
  the SDK/façade/host surfaces, never back into a v4 core path.
- `SuccessTuple` returns, `warn`/`error`/`info`/`dprint`, `attempt_import` for anything heavy (never a
  module-level import of pandas/polars/SQLAlchemy/Dash), `filter_arguments` / `filter_keywords` when
  calling plugin code, duck typing for pipes and users (no `isinstance` checks on user objects).
- The base wheel must stay dependency-free: `pyproject.toml` extras and
  `meerschaum/utils/packages/_packages.py::extras` must agree (`check_packaging.py` enforces it);
  `requirements/<extra>.txt` are generated with `--write`, not edited.
- Stale-install check: assert `meerschaum.__file__` is in the repo whenever a result makes no sense.

### Commits

Emoji + imperative summary, as in `git log --oneline`: `🦀 v5 wave N: ...` for wave merges,
`💚` CI/harness fixes, `📝` docs, `🧪` tests, `🐛` fixes, `✨` features, `🛡️` / `🔒` safety and security.
Agents working in a wave do not commit; the orchestrating session commits each wave. Run
`scripts/parity/check_fixture_hygiene.sh` on any fixture directory or public file you touched before
it is committed: `CLAUDE.md`, READMEs, fixtures and docs must never name private deployments, their
organizations, home directories, hostnames or credentials.

## Gotchas (verified)

- **Jobs start through a per-root admission gate** (D23, `meerschaum-jobs/src/startup.rs`): file-lock slots
  under `<root>/jobs/.startup/`, random jitter (`jobs.startup.jitter_seconds`, 30) and load/memory waits for
  bulk starts (`compose up`, `restart jobs`, `start jobs` of several, systemd restarts). Compose sets
  `MRSM_JOB_START_KIND=bulk` on its child `mrsm`; an explicit single `start job x` skips jitter but takes a
  slot; **`--no-stagger` bypasses the gate**. Tests that start jobs and wait for them use `--no-stagger`
  (or `jobs.startup.enabled: false`); a worker "stuck at starting" for ~10-30 s is the gate, not a hang.
- **`SqlDriver::ready()` (the fail-fast connect gate) must stay tiny.** Its slow path is
  `Box::pin(self.establish_first(..))`: an inline body made every driver future 8 KB and overflowed the
  2 MiB tokio worker stack in debug builds (`sync pipes`). `driver::future_size_tests` guards it.

- **Empty frames export no Arrow batches.** A zero-row pandas/Polars frame's `__arrow_c_stream__` has
  no batches; the boundary (`frame_from_arrow`) must import an empty batch carrying the schema or
  every column disappears (`TypeError: missing column dt`).
- **Polars quirks at the Arrow boundary** (`crates/meerschaum-py/README.md`): `Null` columns export
  `n_buffers = 1`, which arrow-rs (through 60) rejects (repaired in `ffi_import.rs`); older Polars
  `Extension` string columns export a `Utf8View` layout arrow-rs 56 panicked on, so Polars inputs
  still go through `DataFrame.to_arrow()` when pyarrow is there; arrow 60 made the C stream
  callbacks private, so `ffi_import.rs` calls them through a `#[repr(C)]` mirror.
- **Dtype hints restore pandas backends.** The kernel erases `int64` vs `Int64` vs `int64[pyarrow]`;
  `pandas_bridge.rs` restores what v4 returns per function (table in `PARITY.md`). Datetime targets
  always take `to_pandas_dtype(typ)`; special columns (`json`/`uuid`/`numeric`/`bytes`) stay `object`.
- **Forked job daemons and Tokio.** The extension's runtime is keyed by PID and `SQLConnector._rust_method`
  rebuilds `_core.SqlConnector` after a fork; never `block_on` an inherited runtime.
- **Dask gives lazy scalars.** Dask inputs are computed at the boundary (`from_pandas(sort=False)`);
  Dask is deleted in 5.0 (D20), do not add Dask paths.
- **`get_config` is hot.** The façade calls it ~35 times per sync chunk; the Rust binding has a
  read-only fast path over the live `meerschaum.config.config` dict and caches nothing, so
  `set_config` / `replace_config` / env patches are visible on the next call. Keep it that way.
- **Partitioned tables fold the datetime column into the primary key** (TimescaleDB hypertables and
  native PG/MySQL/MSSQL partitions, `hypertable=True` default), so an upsert's conflict target must
  include `dt_col`; a non-partitioned `primary`-keyed upsert keeps the primary-key-only target.
- **MariaDB `get_sync_time(params=...)` on a partitioned table** uses `MAX`/`MIN` instead of
  `ORDER BY ... LIMIT 1` (a MariaDB optimizer bug returns zero rows); flavor-gated, keep it when porting.
- **`get_chunk_bounds(align=True)`** (used by `verify()` and the ledger grid) anchors edges to the
  Unix epoch so they coincide with partition boundaries; it defaults to `align=False` elsewhere.
- **The API server caches `Pipe` objects** (`pipes_dict`): `get_pipe(refresh=False)` returns the
  shared object, `refresh=True` a throwaway one. `DELETE /pipes/{ck}/{mk}/{lk}/cache` clears the
  shared object's column-type cache; `APIConnector.sync_pipe` calls it after every sync. The server
  refuses `mrsm` connector keys over the generic routes; engine state uses `/engine/*`.
- **`clean()` in `meerschaum.utils.sql` is a keyword blocklist only** (`;`, `--`, `drop`, `union`, ...);
  use bound parameters for user values and `clean()` only for internal names. `dateadd_str`
  validates `datepart` against a whitelist after the `isinstance(begin, int)` early return. Queries
  built with `.format()` escape names with `s.replace("'", "''")`.
- **A connector patch in `MRSM_CONFIG` / `MRSM_PATCH` sits under `meerschaum.connectors.<type>.<label>`**:
  a top-level `connectors` key is silently ignored, so `sql:main` keeps pointing at `localhost:5432`
  (on a developer machine that may be a real database). Check the resolved port before mutating anything.
- **v4 reads every `MRSM_<TYPE>_<LABEL>` variable as a connector**: an inherited `MRSM_CI_MIN_FREE_GB`
  once became a `ci:min_free_gb` connector in a recorded fixture. Drop unrelated `MRSM_*` variables
  when spawning reference or test processes.
- **CI keeps `target/` on the self-hosted runners.** `rust.yaml` and `pytest.yaml` both check out with `clean: false` and
  `git clean -ffdx -e /target -e /.venv-ref -e /.venv-test` (no trailing slash: a symlinked `target` survives); `test.sh` does
  not touch `target/`. What runs on a push is decided by `scripts/ci/changes.py` (docs-only: nothing heavy; Python-only: pytest
  only; parity only for generators/fixtures; frontcrate/package/duckdb only for their inputs; a dispatch or `v5*` tag runs
  everything, including the release-profile `build`). Make the `summary` job of each workflow the required check. The pytest
  job builds the extension with `--profile ci`; wheels, binaries and Docker stay `--release`. Details: `docs/design/v5-build.md`
  "CI: what runs when" (also how to add runner instances).
- **Read pytest summaries carefully**: "N passed, 2 warnings" is not "2 failed"; the two
  `test_worker.py` codec tests are order-dependent (pass alone, fail after the suite).
