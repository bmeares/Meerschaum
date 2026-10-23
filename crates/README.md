# Meerschaum v5 Rust workspace

The design is `docs/design/v5.0.0.md`. Read it before touching a crate. The crate map is §4.1; the
port table with line counts per Python area is §6.1; the compatibility contract (release gate) is §11.

## Conventions (binding for humans and agents)

- **One crate per directory under `crates/`.** Work inside your crate. Do not edit another crate or
  the root `Cargo.toml` except to add a `[workspace.dependencies]` entry when two or more crates need
  the same dependency; crate-private dependencies go in the crate's own `Cargo.toml`.
- **Shared target dir** (`.cargo/config.toml`). Never set `CARGO_TARGET_DIR`; disk on the self-hosted
  runner is limited. Builds from concurrent sessions may block on the directory lock; batch your builds.
- **Parity is measured against the Python implementation in this repo** (v4.1.4 on this branch). To
  produce golden fixtures, run the Python code directly:

  ```bash
  PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins python -c '...'
  ```

  Always set `PYTHONPATH=$(pwd)`: a stale `meerschaum` is installed under `~/.local` and shadows the
  working tree otherwise. Check `meerschaum.__file__` if a result looks impossible.
  Commit fixtures under `crates/<crate>/tests/fixtures/` as JSON with the generating script next to them.
- **Rules of the port:** reproduce observable behavior, including quirks the corpora depend on (§4.4
  "What the façade must preserve", §6.2, §6.3). Where v4 behavior is a bug, keep it and leave a
  `// PARITY:` comment pointing at the Python line, so the parity gate can carry a snapshot explanation.
- **Python stays runnable throughout.** The Python package on this branch keeps working as v4.1 until a
  façade module is switched to `_core` behind tests. Do not delete Python code in this phase.
- **Before you finish:** `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test -p <crate>`.
  No `unsafe` outside `meerschaum-plugin` and `meerschaum-py` without a `// SAFETY:` comment.
- **Do not commit.** The orchestrating session commits each wave.
- **Never name the private corpora or their organizations** in code, comments, fixtures or docs.
  Fixture data must be synthetic.

## Crate layering

Dependencies point down only. No cycles; `meerschaum-core` is a leaf. Verify with
`cargo tree -p <crate> -e normal --depth 1`; CI will diff this table against it.

```
  meerschaum (front crate)         thin re-exports behind features; depends on everything below, and
                                   on `cli` (default feature) only for its `mrsm` binary
  cli ◀── py                       top layer; no library depends on them
   │       │
   ▼       ▼
  server ─▶ c-api ─▶ connector ─▶ kernel ─▶ core
  c-valkey ─▶ c-api     ▲
  c-sql ───────────────┘           (c-sql, c-valkey, c-api, server, engine all sit on connector;
                                    c-* = meerschaum-connector-sql / -valkey / -api)
  engine ─▶ connector              (SQLite-backed tests use connector-sql as a dev-dependency)
  jobs, compose ─▶ core
  worker ─▶ connector              (Python worker pool + adapter; used by engine/jobs/server/cli)
  web ─▶ server, plugin ─▶ kernel + core   (web = the terminal, the job-log stream and native plugin pages;
                                    the console itself is the Dash app of the Python package, D21)
```

| Crate | Normal workspace dependencies |
|---|---|
| `meerschaum` | core, kernel, connector; optional: connector-sql, connector-valkey, connector-api, engine, server, plugin (features); `cli` (default): the CLI library, for its `mrsm` binary only (`src/main.rs`; the library surface never touches it) |
| `meerschaum-core` | none |
| `meerschaum-kernel` | core |
| `meerschaum-connector` | core, kernel |
| `meerschaum-connector-sql` | connector, core, kernel |
| `meerschaum-connector-api` | connector, core, kernel |
| `meerschaum-connector-valkey` | connector-api (wire helpers only, no default features), connector, core, kernel |
| `meerschaum-server` | connector-api, connector, core, kernel |
| `meerschaum-engine` | connector, core, kernel (dev: connector-sql) |
| `meerschaum-jobs` | core |
| `meerschaum-compose` | core |
| `meerschaum-worker` | connector, core, kernel |
| `meerschaum-cli` | compose, connector, connector-api, connector-sql, connector-valkey, core, engine, jobs, kernel, plugin, server, web, worker |
| `meerschaum-py` | cli, compose, connector, connector-api, connector-sql, connector-valkey, core, engine, jobs, kernel, worker (not published) |
| `meerschaum-web` | server, connector, connector-valkey, core, kernel |
| `meerschaum-plugin` | connector, core, kernel |

Shared Python-behavior helpers (`pyfmt`, `Value`, dtype vocabulary, `shlex`) belong in `meerschaum-core`;
a copy in another crate is a bug to file, not a pattern to follow.

### Job executors: client injection (`jobs -> api` is not an edge)

`meerschaum-jobs` stays on `meerschaum-core` only, yet it has three executors. The `api:<label>` and
`systemd` executors reach the outside world through traits the jobs crate owns, and the host injects the
implementations through `JobsContext`:

| Executor | Trait (in `meerschaum-jobs`) | Implemented by | Installed with |
|---|---|---|---|
| `api:<label>` | `remote::RemoteJobs` (a *blocking* client of one endpoint, every read fallible) and `remote::RemoteJobsResolver` (keys -> shared client) | `meerschaum-cli` (`native/remote_jobs.rs`) over `meerschaum_connector_api::APIConnector`, blocking on the `Session`'s one Tokio runtime | `JobsContext::with_remote` |
| `systemd` | `runner::ProcessRunner` (`systemctl --user`, process state) | `runner::SystemRunner` (real); `runner::FakeSystemctl` (tests) | `JobsContext::with_process_runner` |

A context without a resolver answers every remote operation with an explicit error, and a failed remote
read is never "no jobs": decisions (listing, ownership, replacement) use the executor's `try_*` methods.
The wire protocol lives only in `meerschaum-connector-api` (`client/jobs.rs`, including the strict `try_*` reads);
the server side of the same routes is `meerschaum-server`'s `JobsBackend`, implemented over the local
executors in `meerschaum-cli` (`jobs_backend.rs`). Hosts other than the CLI (the Python extension) install
their own resolver or none.

### The config registry (`meerschaum_core::config`)

`ConfigState` owns every config rule (file selection, YAML/JSON read and write, defaults, `MRSM{}` substitution and
restoration, `MRSM_CONFIG` / `MRSM_PATCH` / `MRSM_<TYPE>_<LABEL>`, backups, `sync_files`) over the lossless
`ConfigValue`; `Config` is the lock-wrapped registry the binary and the server use (JSON-like `get`/`get_with`). The
Python extension (`meerschaum-py/src/config_api.rs`) owns the one live `dict` (`meerschaum.config.config`) because
Python code mutates the dictionaries `get_config` returns; each call loads the keys it needs into a throwaway
`ConfigState`, runs the rule, and writes back only the changed top-level keys. Never add a second config store.

## Publishing (crates.io)

Every crate except `meerschaum-py` (`publish = false`: the PyO3 extension ships in the `meerschaum`
PyPI wheel, not on crates.io) is published, in lockstep at the workspace version.

- **`meerschaum` is the front crate** (`crates/meerschaum`, the Polars model): thin `pub use` of the
  curated surface plus cargo features (`sql` default, `valkey`, `api`, `engine`, `server`, `plugin`,
  `full`, `oracle`; DuckDB is part of `sql`: its library is loaded at run time). It holds no logic; its `lib.rs` says which items are semver-stable.
  Every other `meerschaum-*` crate is internal and says so in its `lib.rs` and README (`meerschaum-plugin`,
  the native plugin SDK, is the one other public crate).
- **Versions.** Cargo `5.0.0-rc.9` <-> PEP 440 `5.0.0rc9` (use the dot form: Cargo sorts `rc.10` after
  `rc.2` but `rc10` before `rc2`). Internal dependencies are `version = "=<ver>"` plus `path` in the root
  `[workspace.dependencies]`; `scripts/check_packaging.py` checks the lockstep and the publish metadata.
- **Package contents** are an explicit `include` per crate (`src/**`, the assets the crate embeds with
  `include_str!`/`include_bytes!`, the README); no tests, fixtures or benches are shipped.
  `scripts/rust/ci.sh package` (CI job `package`) packages the workspace and checks that and the sizes.
- **`scripts/rust/ci.sh publish-dry-run` mutates shared state.** It builds into the shared `target/`
  (the temporary registry `target/package/tmp-registry` and compiled units of the `meerschaum-*` crates)
  and extracts the overlay crates into `$CARGO_HOME/registry/src/-<hash>/`. Registry crates are fingerprinted
  by name and version only, so the step first deletes the previous run's overlay registry, its extracted
  sources and its compiled units, then rebuilds all 15 crates (tens of minutes). Run it when nothing else is
  building in the tree.
- **Publish from Linux or macOS.** `crates/<crate>/LICENSE` and `NOTICE` are symlinks to the root files; a
  checkout without symlink support (Windows by default) would package the link text instead of the license.
  `scripts/rust/check_packages.py` (run by `ci.sh package`) only checks that the files exist in the archive.
- **`ci.sh frontcrate`** (CI job `frontcrate`) compiles `meerschaum` with no default features, with each of
  `sql valkey api engine server plugin` alone, and tests it with `full`: the workspace only builds the
  features together.
- **The sqlx `[patch]` in the root `Cargo.toml` is ignored by crates.io**: published SQL connectors build
  against plain sqlx 0.9.x until 0.9.1 ships (5.0.0 does not wait for it). The real `cargo publish` is run by the maintainer, never by CI,
  and never without the maintainer's explicit authorization.
- **The publish command** is `cargo publish --workspace --exclude meerschaum-py --locked`: it publishes the 15
  crates in dependency order and waits for the index between them (`meerschaum-py` is `publish = false`).
  Run `scripts/rust/ci.sh package` and then `scripts/rust/ci.sh publish-dry-run` first. A published version can
  only be yanked (`cargo yank --version <v> <crate>`), never replaced, so a fix is a new lockstep version.
- **Order, for recovery after a partial failure** (publish the missing crates one by one with `cargo publish -p
  <crate> --locked`; crates of one level are independent except for the dev-dependencies noted below): 1 `meerschaum-core`; 2 `meerschaum-kernel`,
  `meerschaum-jobs`, `meerschaum-compose`; 3 `meerschaum-connector`; 4 `meerschaum-connector-sql`,
  `meerschaum-connector-api`, `meerschaum-plugin`, `meerschaum-worker`; 5 `meerschaum-connector-valkey`,
  `meerschaum-engine`, `meerschaum-server`; 6 `meerschaum-web`; 7 `meerschaum-cli`; 8 `meerschaum`.
  Within a level publish `meerschaum-connector-sql` before `meerschaum-worker` (level 4) and
  `meerschaum-connector-valkey` before `meerschaum-engine` (level 5): `-worker`, `-engine` and `-web` dev-depend on
  them and `cargo publish` verifies the dev-dependencies. Recompute with `cargo metadata --no-deps` if a dependency changes.
- **Installing.** `cargo install meerschaum --locked` (Rust 1.95 or newer, the workspace `rust-version`): without `--locked`
  Cargo may resolve newer dependencies than the tested ones (on Windows a newer `aws-lc-sys` than our
  prebuilt-NASM pin). A pre-release is ignored unless `--version 5.0.0-rc.9` is given.
  The full release procedure (PyPI, crates.io, tag, GitHub release, Docker) is
  `.agents/skills/release/SKILL.md`, section "Meerschaum 5.x".
  sqlx 0.9.0 no longer sets `TCP_NODELAY` (upstream #4335, fixed by #4336) and has no API to set it, so
  `crates/meerschaum-connector-sql` prepares small MySQL/MariaDB statements once per connection instead of
  closing them after each use (`Backend::PERSISTENT_STATEMENTS`, 32 cached statements per connection).
  What is left on a crates.io build is one ~40 ms wait for each empty read (zero rows) on MySQL/MariaDB over a
  network, plus statements over 8 KiB or 128 parameters; PostgreSQL is not changed by the mitigation (its
  statements are not closed without a reply). Measured on this repo's test containers (instance suites, 8 test
  threads), connections opened for `instance_sync` / `instance_dtypes` on MySQL: 314 / 493 unmitigated, 46 / 67
  mitigated, 45 / 67 with the `[patch]`; wall time of `instance_dtypes`: 2.0 s, 1.7 s, 1.1 s (MariaDB 3.5 s, 2.0 s,
  0.9 s). 5.0.0 ships on sqlx 0.9.0 (wheels, Docker images and release binaries use the `[patch]`); once sqlx 0.9.1
  is on crates.io, a 5.0.x release deletes the `[patch]` section and keeps or drops the mitigation
  (it is harmless on a fixed sqlx).

## Building

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build                 # whole workspace
cargo test -p meerschaum-core
cargo run -p meerschaum --bin mrsm -- --help    # the standalone `mrsm` (`cargo install meerschaum` builds it)
```

The PyO3 extension (`crates/meerschaum-py`) is built with maturin into `meerschaum/_core`; see that
crate's README once it exists.
