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
  cli ◀── py                       top layer; nothing depends on them
   │       │
   ▼       ▼
  server ─▶ api ─▶ connector ─▶ kernel ─▶ core
  valkey ─▶ api        ▲
  sql ────────────────┘            (sql, valkey, api, server, engine all sit on connector)
  engine ─▶ connector              (SQLite-backed tests use sql as a dev-dependency)
  jobs, compose ─▶ core
  worker ─▶ connector              (Python worker pool + adapter; used by engine/jobs/server/cli)
  web, plugin                      (stubs; web will depend on server, plugin on kernel + core)
```

| Crate | Normal workspace dependencies |
|---|---|
| `meerschaum-core` | none |
| `meerschaum-kernel` | core |
| `meerschaum-connector` | core, kernel |
| `meerschaum-sql` | connector, core, kernel |
| `meerschaum-api` | connector, core, kernel |
| `meerschaum-valkey` | api (wire helpers only, no default features), connector, core, kernel |
| `meerschaum-server` | api, connector, core, kernel |
| `meerschaum-engine` | connector, core, kernel (dev: sql) |
| `meerschaum-jobs` | core |
| `meerschaum-compose` | core |
| `meerschaum-worker` | connector, core, kernel |
| `meerschaum-cli` | compose, core (sql, engine, jobs, server are added as their actions are wired) |
| `meerschaum-py` | cli, core, kernel, sql |
| `meerschaum-web`, `meerschaum-plugin` | not started |

Shared Python-behavior helpers (`pyfmt`, `Value`, dtype vocabulary, `shlex`) belong in `meerschaum-core`;
a copy in another crate is a bug to file, not a pattern to follow.

### Job executors: client injection (`jobs -> api` is not an edge)

`meerschaum-jobs` stays on `meerschaum-core` only, yet it has three executors. The `api:<label>` and
`systemd` executors reach the outside world through traits the jobs crate owns, and the host injects the
implementations through `JobsContext`:

| Executor | Trait (in `meerschaum-jobs`) | Implemented by | Installed with |
|---|---|---|---|
| `api:<label>` | `remote::RemoteJobs` (a *blocking* client of one endpoint, every read fallible) and `remote::RemoteJobsResolver` (keys -> shared client) | `meerschaum-cli` (`native/remote_jobs.rs`) over `meerschaum_api::APIConnector`, blocking on the `Session`'s one Tokio runtime | `JobsContext::with_remote` |
| `systemd` | `runner::ProcessRunner` (`systemctl --user`, process state) | `runner::SystemRunner` (real); `runner::FakeSystemctl` (tests) | `JobsContext::with_process_runner` |

A context without a resolver answers every remote operation with an explicit error, and a failed remote
read is never "no jobs": decisions (listing, ownership, replacement) use the executor's `try_*` methods.
The wire protocol lives only in `meerschaum-api` (`client/jobs.rs`, including the strict `try_*` reads);
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

## Building

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cargo build                 # whole workspace
cargo test -p meerschaum-core
cargo run -p meerschaum-cli -- --help
```

The PyO3 extension (`crates/meerschaum-py`) is built with maturin into `meerschaum/_core`; see that
crate's README once it exists.
