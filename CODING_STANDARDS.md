# Coding standards

The judgement calls a reviewer applies to a Meerschaum diff, and that an author applies before
asking for review. Mechanical rules live in checks; this file lists them once and then covers what
no check can decide. Terms follow [`GLOSSARY.md`](GLOSSARY.md); decisions follow
[`docs/adr/`](docs/adr/). Crate layering and per-crate conventions are in
[`crates/README.md`](crates/README.md).

## Enforced by checks (fix the finding, do not argue with the check)

- `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings`
  (workspace lints in the root `Cargo.toml`).
- `scripts/rust/ci.sh doc`: rustdoc with `-D warnings`.
- `scripts/check_architecture.py` (runs in `ci.sh guardrails`): its docstring lists each rule.
- `scripts/check_packaging.py`: versions, lockstep pins, console scripts, extras.
- `scripts/parity/check_fixture_hygiene.sh [PATH ...]`: private names, captured environments and
  credential shapes in recorded fixtures (it skips `*.md` and `*.py`). `--public PATH ...` scans docs
  and code, Markdown and Python included, for private names, home directories and credential shapes.

## Compatibility

- **Public surfaces are frozen** (§11 of `docs/design/v5.0.0.md`): pipe keys and every
  `parameters` key, config layout, `MRSM_<TYPE>_<LABEL>` connectors, instance tables, the job
  directory layout, compose files, HTTP routes, CLI actions and flags, the Python public API, and
  message text that tests or `crates/meerschaum-py/PARITY.md` pin.
- **Reproduce observable v4 behavior, quirks included.** Mark a deliberate quirk `// PARITY:` with
  the v4 Python line it comes from. A Python result that differs from v4 is fixed at the boundary
  or in the kernel, with a row in `PARITY.md`. Changing v4 behavior on purpose needs the
  maintainer's approval first.
- **Behavior visible from Python gets a pytest test as well as a crate test.**

## Architecture

- **Dependencies point down** (`crates/README.md` "Crate layering"). Shared Python-behavior helpers
  belong in `meerschaum-core`; a second copy of a helper in another crate is a bug to fix, not a
  convenience.
- **One implementation.** A façade calls one `_core` binding and keeps only argument adaptation,
  caches and the pandas/Polars boundary. Rust reaches Python only through the worker and the API
  host ([ADR 0001](docs/adr/0001-python-reached-only-through-workers-and-hosts.md)). A missing
  driver or capability is an error naming the cause, never a silent fallback.
- **Delete what you replace.** No compatibility shims, re-export aliases or dead wrappers for
  internal names; internal crates may change in any minor release.
- **Edit the root `Cargo.toml` only for a `[workspace.dependencies]` entry** two or more crates need.

## Safety and robustness

- **Bound every caller-controlled range and resource**: row counts, byte sizes, time windows,
  recursion, retries, queue lengths. An unbounded loop or allocation driven by input is a defect
  even when today's callers are polite.
- **Fail closed** on unreadable, missing or malformed auth, session, grant or policy state: deny
  and say why.
- **Make multi-step registrations atomic**: a failure part-way leaves either the old state or the
  new one, never a half-registered object.
- **Never expand secrets into persisted config.** `MRSM{...}` references and secret files stay
  references when written back.
- **Trust boundaries** (team processes, team executors, the shared API host; see
  [`docs/agents/teams-and-executors.md`](docs/agents/teams-and-executors.md)): a new input that
  crosses one is called out in the PR and gets a test for the hostile case. Treat executor replies
  and plugin output as untrusted data.
- No `unsafe` outside `meerschaum-plugin` and `meerschaum-py` without a `// SAFETY:` comment.

## Errors and output

- Actions, pipe methods and connector methods return a `SuccessTuple` / `SyncResult`:
  `(False, "reason")` instead of raising from action-level code.
- Python reports through `warn` / `error` / `info` / `dprint`; Rust debug output goes through
  `meerschaum_core::debug::dprint!` (stderr). Plain `print` / `println!` is for command output only.
- Messages name what failed and what to do next, in the user's terms.

## Tests

- **A behavior fix comes with a test that fails without it.** Say in the PR how you saw it fail.
- **Tests isolate themselves**: a temporary `MRSM_ROOT_DIR`, an empty `MRSM_PLUGINS_DIR`, unique
  table prefixes (`_mrsm_rs_test_*`, random connector labels), cleanup afterwards. They never
  assume a shared session, instance or job list starts empty.
- **Bound edge-case runs** that loop, spawn or allocate (`timeout`, a memory cap); a run killed by
  the bound is a finding, not noise.
- New Rust integration tests are modules in an existing suite unless they change process-wide
  state ([`docs/agents/testing.md`](docs/agents/testing.md)).

## Python

- New Python code goes in the SDK, façade and host surfaces; the public API is frozen.
- Heavy packages (pandas, Polars, SQLAlchemy, Dash, ...) load through `attempt_import`, never at
  module level ([ADR 0004](docs/adr/0004-zero-dependency-base-wheel.md)).
- Call plugin code through `filter_arguments` / `filter_keywords`; use duck typing for pipes and
  users rather than `isinstance`.

## Dependencies

- **Default every dependency and vendored asset to its newest stable release**, and upgrade them
  routinely.
- `pyo3`, `arrow` and `polars` are workspace dependencies upgraded together. The Python boundary is
  our own PyCapsule code (`ffi_import.rs`, `ffi_export.rs`, `polars_bridge.rs`), not
  pyo3-arrow/pyo3-polars.

## Product rules

- **A flag modifies an action's behavior or inputs; new functionality is a new action or
  subaction** ([ADR 0010](docs/adr/0010-flags-modify-actions.md)).
- **Console pages carry no helper captions or subtitles.** When a page needs one to be understood,
  fix the design (D26).
- **Public files never name private deployments, their organizations, people, home directories,
  hostnames or credentials.** Fixture data is synthetic.

## Names and comments

- **Names and comments use domain terms** from `GLOSSARY.md` and describe what the code does.
  Development-history tags (release-candidate numbers, wave or work-item IDs, PR numbers, "review
  finding") never appear in code, comments, test names or file names; the history lives in git.
  A `// PARITY:` comment naming the v4 source line is the one reference to another version.
- Qualify "executor" unless you mean a job executor (`GLOSSARY.md` "Executors").
