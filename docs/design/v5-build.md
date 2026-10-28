# Meerschaum v5: build, CI and Docker

Companion to `docs/design/v5.0.0.md` (§10.2 reference harness, §12.1 size gate, §13 milestones, §15 risks).
Conventions for the crates themselves are in `crates/README.md`.

## Build matrix

| Workflow | Ordinary pushes and PRs | Full validation |
|---|---|---|
| `rust.yaml` | Linux fmt, Clippy, rustdoc and broad Rust tests; affected feature/package/driver checks | Nightly and reusable full call: every platform, reference regeneration, all feature/package checks |
| `pytest.yaml` | One CI-profile wheel and debug CLI, then core/Postgres and affected backend lanes | Nightly: every lane; release call: install the already-built shipping wheel and CLI |
| `wheels.yaml` | No automatic build | Manual/reusable: selected release wheel matrix, standalone binaries and sdist |
| `wheel-platform.yaml` | Called by the wheel matrix | Each platform builds, immediately runs its own compatible smokes, then stages a validated artifact |
| `docs.yaml` | Strict build when docs inputs change; hosted runners only | Included in the release gate; uploads a website preview |
| `release.yaml` | No branch/PR build | One manual full candidate; tag pushes only verify an existing successful candidate |
| `docker-build.yaml` | No automatic build | Explicit manual build/publish; size and offline smoke gates retained |

Build the website locally with `bash scripts/docs.sh --site-only`. It uses a cached, isolated
tool environment from `docs/requirements.txt`; the caller's Zensical installation is unchanged.
Without the flag, the script also regenerates API/MCP/llms/changelog output using a scratch
configuration and empty plugins directory for framework imports.

Prereleases retain Linux x86_64 glibc/musl wheels, one standalone CLI and the sdist. Finals add
Linux arm64, both macOS architectures and Windows x86_64: seven ABI3 wheels and five binaries.
The optional Linux cp314t wheel is disabled by default; enable `experimental` to build it. It
receives a publishable artifact name only after its own free-threaded smoke succeeds.

## CI: what runs when

`scripts/ci/changes.py` classifies the actual checkout diff. PRs retain their synthetic merge
revision; artifacts never substitute the branch head. Unknown diffs choose full validation.
Non-Rust fixtures run tests/reference regeneration without unnecessary formatting and lint jobs.
Documentation changes build the docs without compiling the runtime.

Python PRs keep core/SQLite/API-host checks and PostgreSQL-family coverage, adding affected driver
lanes. Changes to the harness, manifests, dependency lock or workflows select every lane. Full
nightly/release calls cannot select a partial matrix. `rust / summary`, `pytest / summary` and
`docs / summary` cover failures and cancellations; self-hosted jobs refuse fork PR code.

### Artifact reuse

The development producer runs before any database service starts. It builds one complete wheel
with Cargo's `ci` profile and one debug CLI. Release tests instead consume `wheels-ubuntu-latest`
and the Linux standalone binary from the release workflow, eliminating a second optimized build.
Shipping artifacts always use `release`; the CI profile never passes the publication gate.

`scripts/ci/artifacts.py` records the actual commit/tree, profile, target, features, compiler,
lock/toolchain hashes and every payload checksum. Consumers reject unlisted files, symlinks,
wrong profiles and stale revisions before using a binary. Downloaded mtimes are irrelevant.
Tests install the complete wheel into their venv. Source-reading tests also receive its complete
native payload, including bundled libraries, while tracked Python files must match the wheel.
Dependency-free installed-wheel smokes remain separate from backend/extras tests.

### Isolated backend lanes

Each lane owns a scratch root, Compose project, volumes, loopback-mapped database ports and API
port. `scripts/ci/services.py` records ownership before startup and cleans only that project,
including after cancellation. Existing shared test services and production services are unrelated.
All unparameterized tests run once; parameterized cases are assigned by backend. A collection
proof rejects missing, repeated, unknown or conflicting cases before the full run.

| Lane | Workers |
|---|---:|
| Core / SQLite / API-host | 4 |
| PostgreSQL family and API | 2 |
| MySQL / MariaDB | 1 |
| MSSQL / Valkey | 1 |
| Oracle | 8 |

The combined budget is 16 workers. Oracle gets more slots because it dominated recorded call
time; MySQL and Valkey retain smaller budgets. `scripts/ci/shards.py` is the shared source of
truth. Test evidence artifacts retain per-lane collection reports, the union proof and ownership.
Rust tests retain their own shared-fixture lock and a bounded nextest budget.

### Release candidate workflow

```bash
gh workflow run release.yaml --ref v5
gh run list --workflow release.yaml --limit 3 --json databaseId,headSha,status,conclusion
```

One run builds shipping artifacts and runs Rust, all Python lanes and the strict docs build.
The final gate verifies required platforms and seals every artifact digest in
`release-candidate-manifest`. It never publishes. To verify the candidate again without building:

```bash
gh workflow run release.yaml --ref v5 -f candidate_run_id=RUN_ID
```

A future `v5*` tag must match the source version and point to an exact commit with a successful,
unexpired sealed candidate. Tag validation downloads those same bytes; it does not rebuild or
publish. Registry uploads, GitHub releases and Docker publication remain separately authorized
release steps.

### Build caches

Each runner keeps its own physical Cargo target; no targets or build locks are merged. Hosted
caches remain keyed by target/profile and save on `main` and `v5`. Self-hosted runners preserve
`target`, `.venv-ref` and `.venv-test`; sccache adds a bounded shared dependency cache. The disk
guard measures the resolved target filesystem and its actual size, including symlinked targets.
`cargo-nextest` still runs all selected Rust tests, followed by doctests. Tool downloads remain
version/checksum pinned; mold remains opt-in.

### Parallel self-hosted runners

One runner serializes every job; three runner instances on this host let `clippy`, `test` and `pytest` run side by side.
Do not register or start them from CI or from an agent: the maintainer applies this once.

Layout. Each runner has its own directory and therefore its own workspace, `target/`, `.venv-ref`, `.venv-test` and `test_root`
(nothing is shared between workspaces except what is listed below). Keep them on the big disk: the root filesystem may have
little free space and one workspace's `target/` is about 50 GB. `BIG` below is a directory there.

```bash
# 1. Runner software and directories (as the user that runs the existing runner)
BIG=/path/on/the/big/disk
mkdir -p "$BIG/actions-runners/ci-2" "$BIG/actions-runners/ci-3" "$BIG/mrsm-ci"
RUNNER_VERSION="$(gh api repos/actions/runner/releases/latest --jq '.tag_name | ltrimstr("v")')"
for n in ci-2 ci-3; do
  cd "$BIG/actions-runners/$n"
  curl -fsSL -o runner.tar.gz "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
  tar xzf runner.tar.gz && rm runner.tar.gz
  TOKEN="$(gh api -X POST repos/<owner>/<repo>/actions/runners/registration-token --jq .token)"   # needs admin rights; valid 1 hour
  ./config.sh --unattended --url https://github.com/<owner>/<repo> --token "$TOKEN" \
    --name "$n" --labels mrsm-ci --work _work --replace
  # Per-runner environment (read by every job): the shared tool/cache directory on the big disk, and the sccache cap.
  printf 'MRSM_CI_CACHE=%s/mrsm-ci\nSCCACHE_CACHE_SIZE=20G\n' "$BIG" >> .env
done
# The existing runner (~/actions-runner) keeps working as is. Give it the same two lines in ~/actions-runner/.env.
# (`runs-on: self-hosted` matches every registered runner; `--labels mrsm-ci` is for a future `runs-on: [self-hosted, mrsm-ci]`.)
```

```ini
# ~/.config/systemd/user/mrsm-actions-runner@.service   (user level: needs `loginctl enable-linger`, no sudo; replace <BIG>)
[Unit]
Description=GitHub Actions runner %i
After=network-online.target docker.service

[Service]
Type=simple
WorkingDirectory=<BIG>/actions-runners/%i
ExecStart=<BIG>/actions-runners/%i/run.sh
Restart=always
RestartSec=10
KillMode=process
KillSignal=SIGTERM
TimeoutStopSec=5min
# The host may also run production services: CI yields CPU and is capped.
Nice=10
CPUQuota=1600%

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now mrsm-actions-runner@ci-2 mrsm-actions-runner@ci-3
systemctl --user status 'mrsm-actions-runner@*' --no-pager
# The original runner (~/actions-runner) runs the same way from mrsm-actions-runner-main.service
# (WorkingDirectory=%h/actions-runner, ExecStart=%h/actions-runner/run.sh), so all runners survive a reboot.
gh api repos/<owner>/<repo>/actions/runners --jq '.runners[] | [.name, .status, .busy] | @tsv'
```

Moving a workspace's `target/` off the root filesystem (optional, while its runner is stopped and idle):
`mv <workspace>/target "$BIG/runner-target" && ln -s "$BIG/runner-target" <workspace>/target`
(the workflows' `git clean -e /target` keeps a symlink; `.cargo/config.toml`'s `target-dir = "target"` follows it).

Isolation between concurrent jobs:

| Shared resource | Handling |
|---|---|
| `tests-*` containers (`tests/docker-compose.yaml`), the test API on port 8989, the `mrsm` Docker stack, port 8000, the production processes | only `pytest.yaml`'s `tests` job touches the test containers and 8989, and it carries `concurrency: {group: mrsm-test-services, cancel-in-progress: false}`: one at a time per repository, whichever runner takes it (a queued second run waits; a newer one replaces a waiting one). The Docker release workflow also uses the daemon, with its own builders and isolated smoke containers; it never uses 8989, 8000 or the deployed stack. |
| `target/`, `.venv-ref`, `.venv-test`, `test_root`, the build lock `target/.build-py.lock` | per workspace (per runner): no sharing, no locking between runners |
| the compile work of dependencies | shared through sccache (one server per user, same `SCCACHE_DIR`): a second runner's cold build is mostly cache hits |
| `~/.cargo` (registry, git checkouts), the `rustup` toolchain | cargo and rustup take their own locks; safe |
| `${MRSM_CI_CACHE}/duckdb`, `/bin`, `/sccache` | written by atomic `mv` (`duckdb_lib.sh`, `ci_tools.sh`); sccache is concurrent by design |
| ports of the Rust tests | tests bind random free ports; the optional `MRSM_TEST_*_URI` services are not set in CI |
| disk | each runner's `disk-guard` measures its own filesystem; sccache is capped at 20 GB; GitHub-hosted legs use no local disk |

A label-based alternative for the same isolation is to give one runner the label `mrsm-pytest` and set the `tests` job to
`runs-on: [self-hosted, mrsm-pytest]`; the concurrency group needs no runner changes and is what the workflow uses.
A new runner starts with a cold `target/` (the first job there compiles everything once; sccache serves the dependencies).

## Running CI locally

`scripts/rust/ci.sh` runs exactly the workflow's commands. With no arguments it runs `fmt clippy test build`.

```bash
scripts/rust/ci.sh setup          # rustup (installed non-interactively if missing) + rust-toolchain.toml
scripts/rust/ci.sh fmt clippy     # any subset, in order
scripts/rust/ci.sh check          # the hosted legs
scripts/rust/ci.sh disk-guard     # cargo clean if < MRSM_CI_MIN_FREE_GB (default 10) GiB free
scripts/rust/ci.sh parity         # reference harness (below)
```

CI also sets `CARGO_INCREMENTAL=0`, which only changes how workspace crates are cached, not the results.
On a self-hosted runner `ci.sh` also turns on sccache and nextest (see "CI: what runs when"); to try them locally:
`MRSM_CI_ACCEL=1 scripts/rust/ci.sh test` (installs the pinned tools under `$MRSM_CI_CACHE`, default `~/.cache/mrsm-ci`).

## Disk on the self-hosted runner

A runner (`~/actions-runner`, workspace `_work/<repo>/<repo>`) is persistent, so its `target/` is the build cache; sccache adds a
host-wide dependency cache on top (see "CI: what runs when"). There is no `actions/cache` on the self-hosted legs and no second
target directory. Self-hosted checkouts use `clean: false` followed by
`git clean -ffdx -e /target -e /.venv-ref -e /.venv-test`; **neither `rust.yaml` nor `pytest.yaml` wipes `target/`**. Expect a
runner's `target/` to hold debug, clippy, `ci` and release artifacts for the whole graph (tens of GB once Polars and Arrow are in the
graph; the `ci` layer is about 2 GB, the dispatch-only release layer the largest), in addition to the development checkout's own
`target/`. `disk-guard` runs before the clippy, test, frontcrate and test-artifact jobs: below 10 GB free it first drops the `release` and `ci`
layers (`cargo clean --release`, `cargo clean --profile ci`) and only then runs a full `cargo clean`. `pytest.yaml` preserves the same
`target/`, `.venv-ref/` and `.venv-test/` caches and builds the native extension with the interpreter used by its test script (the
test venv is only reinstalled when its dependency hash changes). Maturin runs from an isolated tool environment under `RUNNER_TEMP`.
The hosted legs use `Swatinem/rust-cache`, which stores its cache with GitHub, not on this host.

## Reference harness (§10.2)

Golden fixtures are generated by the **released** `meerschaum==4.1.4`, never by the working tree.

```bash
scripts/parity/setup_reference.sh            # .venv-ref/: uv venv --seed (Python 3.14) + meerschaum[core,formatting]==4.1.4
                                             # + pinned data libraries from scripts/parity/requirements.txt
scripts/parity/run.sh crates/meerschaum-kernel/tests/fixtures/gen_dtypes.py
scripts/rust/ci.sh parity                    # all generators, then fails on any diff or untracked file in the fixture dirs
```

`run.sh` never uses the ambient interpreter. It starts `.venv-ref/bin/python -s -E`, so the stale
`~/.local` install and `PYTHONPATH` are both ignored. It imports `meerschaum` from `.venv-ref` and
asserts the version is 4.1.4 *before* the generator runs. The generator then runs as `__main__` from
the repository root, the same as `python gen_x.py`. Because the reference package is already in
`sys.modules`, a generator that adds the repository to `sys.path` still gets 4.1.4.

`run.sh` also sets these variables:

- `MRSM_ROOT_DIR=.venv-ref/mrsm_root`, so v4's own venvs stay separate from `test_root`.
- `MRSM_PLUGINS_DIR=tests/plugins`.
- `UV_PYTHON` points at the reference interpreter. v4 creates venvs with a bare `uv venv`, which
  would otherwise pick uv's preferred Python.
- `MRSM_PARITY_REFERENCE=4.1.4`. A generator that asserts it imported the repository's `meerschaum`
  must accept the reference when this variable is set.

The data libraries (pandas, numpy, pyarrow, shapely, pyproj, geopandas, pyogrio, geoarrow-types, polars,
pytz) are pinned in `scripts/parity/requirements.txt` to the versions the committed fixtures were made
with. Change a pin only together with regenerating every fixture. `.venv-ref/` takes about 750 MB, is
git-ignored, and persists on the runner.

## Python packaging (D8, D9, D20)

There is no `setup.py`, `setup.cfg` or `MANIFEST.in` any more. One distribution is built with maturin (D8, revised 2026-10-17: there is no `meerschaum-cli` wheel):

| Distribution | Source | Contents |
|---|---|---|
| `meerschaum` | root `pyproject.toml` (maturin mixed project, `abi3-py311`, module `meerschaum._core._native`) | The Python SDK and façade plus the Rust extension, which holds the whole core and the CLI. The `mrsm` and `meerschaum` console scripts (`_meerschaum_mrsm:main`, a top-level module shipped in the wheel) call `_native.main(argv)`, so there is no `mrsm` binary in the wheel. `dependencies` is empty; every heavier package is an extra (`pandas`, `polars`, `geo`, `sql`, `sql-mssql`, `sql-oracle`, `sql-duckdb`, `sql-cockroachdb`, `api`, `dash`, `shell`, `cli-tools`, `gui`, `full`). |

`scripts/check_packaging.py` verifies that the Cargo workspace version, the console scripts and the extras in `pyproject.toml` agree with `meerschaum/utils/packages/_packages.py`, and (`--wheels dist`) that the wheel has no dependencies and no `mrsm` binary. `.github/workflows/wheels.yaml` builds wheels on GitHub-hosted runners. Prereleases select Linux x86_64 only (glibc and musl, plus opt-in experimental cp314t), plus one Linux x86_64 CLI binary and the source distribution. Final releases use the complete matrix (Linux x86_64 and arm64 for glibc and musl, macOS arm64 and x86_64, Windows x86_64; Windows arm64 is not built) and smoke-tests it in a fresh venv (`mrsm --version`, `mrsm show connectors`, `python -m meerschaum --version`, a `sync` of a tiny pipe on SQLite, a job round trip, and the Python API through `scripts/docker/smoke_facade.py` with `MRSM_NO_AUTO_INSTALL=1`, a scratch SQLite instance and pip/uv shims that fail the step if anything tries to install). A build-job step checks the platform tag (Linux wheels must be `manylinux_2_28` or older, or `musllinux_1_2` or older for the musl ones, and carry the expected architecture, or PyPI rejects them) and fails a wheel over 95 MiB (PyPI refuses 100 MiB). The standalone binary is `cargo install meerschaum` (the front crate's default `cli` feature; library users use `default-features = false`). `scripts/rust/build_py.sh` still builds the in-tree extension for development. The Docker `slim` image installs the base wheel only; the default image installs `meerschaum[full]`. Current size and functional gates are recorded below, rather than inferred from the scaffold measurements.

## Docker targets and the size gate (§12.1)

| `docker buildx build ...` | Image | Contents |
|---|---|---|
| `.` / `--target full` | Complete runtime and build environment | Native CLI, base wheel + all extras, Cargo/rustc, C/C++ tools and Python development headers. |
| `--target slim` | Small Python runtime | Base wheel, no extras or compiler; also tagged minimal. |
| `--target rust` | Native runtime | Native binary, OS libraries, no Python or compiler. |
| `--target rust-full` | Native development | Native binary + Cargo/rustc and C/C++ tools, no Python. |

The Fedora 44 `rust-builder` stage builds the wheel with maturin and the repository's Rust
toolchain. BuildKit cache mounts share the Rust artifacts between builds. The `py-slim` and
`py-full` stages install into a prefix before copying into the runtime installroots.
Slim trims development headers; full retains them for building dependencies. Full still explicitly includes PyYAML while public Python config I/O
requires it; that is an outstanding D20 dependency, not part of the declared extras.

`scripts/docker/buildx.sh [--push] [--v4-only] [--platform P] [--arch-tags] [--merge]` builds the following images:

- `minimal` on `--target slim`, retained as a compatibility tag.
- `full` on `--target full`.
- `slim`, tagged `<version>-slim`, plus `slim` for releases.
- `rust` and `rust-full`, with matching version-qualified and stable floating tags.

Pre-releases (`dev`, `rc`, `aN`, `bN`) get only version-qualified tags. The script refuses to push the
Rust targets for a version below 5. The legacy `--v4-only` option selects default-target images and omits the slim/native targets; the current
Dockerfile still installs the v5 wheels.
Docker release builds run on the existing **self-hosted Linux x86_64 runners**. Prereleases
select only `linux/amd64`; final releases also select `linux/arm64`.
`scripts/ci/release_matrix.py` reads the Cargo version and selects the Docker, wheel, binary and
matching smoke matrices on release candidate dispatches. Source distributions and crates.io packages
remain platform independent. The complete wheel and binary matrix is retained for final releases.

Each Docker architecture uses its own named BuildKit cache (`meerschaum-release-<arch>`), preserved
between jobs with `keep-state: true`. The builder is limited to 16 CPUs and 64 GiB RAM, with cache
GC budgets of 100 GB maximum use and 30 GB minimum free space. The orchestration uses the host's Python 3.11 or newer (no downloaded Ubuntu interpreter); the images build their own Python.
The workflow serializes runs and architecture jobs because runners share a daemon and loaded image tags. It uses a temporary Docker
configuration so registry login and builder selection do not change the user's configuration.
The normal test-suite containers, deployed services and other builders are left alone.

AMD64 runs natively. ARM64 uses QEMU (registered only for the ARM64 job); its first Rust build will be
slower, while later builds can reuse its own cache. Both architectures load their images and pass the
same size, CLI, API and offline facade checks before upload. The explicit `--platform` and
`DOCKER_DEFAULT_PLATFORM` make builds and smoke containers use the selected architecture.
`--push --arch-tags` uploads architecture-qualified tags; `--merge` joins only the selected release
platforms, and CI checks every published variant's manifest. Prereleases never move floating tags.
`tests/test_docker_buildx.py` checks command plans; `tests/test_workflows_packaging.py` checks both
release policies and that each smoke job has a corresponding artifact.
`scripts/docker/size_gate.sh [--warn-only] [--slim-max 250] [--full-max 2500] [--rust-max 250] [--rust-full-max 1600] [--slim-image I] [--full-image I]`
measures the uncompressed size from `docker image inspect` in decimal MB and fails over the limit.
D25 expands the image matrix: rust/slim are limited to 250 MB, rust-full to 1600 MB, and full to 2500 MB because full now includes both language build toolchains.
Comparisons use exact bytes, with rounding only for display.
In `docker-build.yaml` the gate defaults to `enforce`. A passing size gate alone does not
establish dependency-free operation: run `scripts/docker/smoke_facade.py` in the installed
image with isolated root and plugins directories and networking disabled. It checks the native
CLI, config lookup, password hashing, public YAML config I/O and list-of-dicts Pipe sync/read,
and rejects dependency-install requests. Any failed operation is a release blocker.

Historical measurements on 2026-10-12, built from the committed scaffold (limits at that time):

| Image | Size | Limit |
|---|---|---|
| `slim` | 86.4 MB | 200 MB |
| `bmeares/meerschaum:full` (v4.0.0, current) | 1868.1 MB | 900 MB |

The `mrsm` binary was still a 338 kB scaffold for the slim measurement. The scaffold-era budget for
the binary, `_core` and any Python additions is the remaining ~110 MB. A first attempt on
`fedora-minimal:44` came to 199.2 MB before any real binary existed, which is why `slim` uses an
installroot rootfs.

## Crate names on crates.io (§15)

`scripts/rust/check_crate_names.sh [--markdown] [NAME ...]` checks crates.io. The lookup is canonical,
so `-` and `_` count as the same name. Results on 2026-10-12:

| Crate | Status |
|---|---|
| `meerschaum` | free |
| `meerschaum-core` | free |
| `meerschaum-kernel` | free |
| `meerschaum-connector-sql` | free |
| `meerschaum-connector-valkey` | free |
| `meerschaum-connector-api` | free |
| `meerschaum-engine` | free |
| `meerschaum-jobs` | free |
| `meerschaum-compose` | free |
| `meerschaum-server` | free |
| `meerschaum-web` | free |
| `meerschaum-plugin` | free |
| `meerschaum-cli` | free |
| `mrsm` | free |

`meerschaum-py` is also free (it is `publish = false`: the extension ships in the PyPI wheel). `meerschaum-connector` and `meerschaum-worker` are free too (checked 2026-10-17). `meerschaum` is the front crate (`crates/meerschaum`, thin re-exports behind cargo features, plus the `mrsm` binary behind its default `cli` feature: `cargo install meerschaum`; `meerschaum-cli` is the internal library it calls); the other crates are internal. crates.io has no reservation mechanism. Claiming a name means
publishing a version from the owner's account. Nothing is published until the maintainer authorizes it (the
procedure and the order of the 15 crates are in `.agents/skills/release/SKILL.md` and `crates/README.md`
"Publishing"); the first publication is `5.0.0-rc.1` (PyPI `5.0.0rc1`).

## Release procedure (5.x)

`.agents/skills/release/SKILL.md`, section "Meerschaum 5.x", is the ordered runbook: bump the version (Cargo
`[workspace.package]` and the lockstep `=` pins, `Cargo.lock`, `meerschaum/config/_version.py`, literals),
`scripts/check_packaging.py`, `scripts/rust/ci.sh package` and `publish-dry-run`, dispatch `release.yaml` on the exact release commit, wait for its complete sealed candidate, download that run's verified wheels and sdist (`gh run download`), `twine check`, then the
steps that need explicit authorization: PyPI `twine upload`, `cargo publish --workspace --exclude meerschaum-py`,
the annotated tag (`v5.0.0rc1`) and a GitHub pre-release, and (separately) Docker. Release candidates are cut from the
`v5` branch (no PR to `main`); `main` is merged at 5.0.0 final. The PyPI classifier is `Development Status ::
4 - Beta` for a pre-release and `5 - Production/Stable` for 5.0.0 final. A pre-release gets only version-qualified
Docker tags. The wheels uploaded are the CI artifacts of the release commit, never a local `python -m build` wheel
(`linux_x86_64`, which PyPI rejects). The sdist needs a Rust toolchain and network access to crates.io and GitHub
(the sqlx `[patch]` points at a git revision).

## Fixture hygiene

Recorded fixtures are scanned by `scripts/parity/check_fixture_hygiene.sh` (run by the parity job after
the reference regeneration, and by hand before any commit that touches `crates/*/tests/fixtures`). It
fails on captured process environments, Claude Code or desktop session variables, home directories,
the maintainer's handle in a path or e-mail, private project names and credential shapes. Python
helpers under `fixtures/` are excluded because they name the things they scrub. Generators that start
a Python reference server or daemon must do so with `env -i` plus only `PATH`, a temporary `HOME`,
`MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR` and `PYTHONHASHSEED=0`, and must replace minted credentials, pids,
timestamps and absolute paths with placeholders before writing.

## Active v5 hardening work order (2026-10-15)

The initial `d50d5698` pytest CI run later completed with 1,726 passed, 363 skipped and two
failures: the external-uv Rust subprocess had no compiled `_core`, and ENV-001 reproduced the
known Python GeoArrow registry issue. Pytest CI now builds `_core` before starting integration
services or tests; the native password facade also requires that prerequisite in Python mode.
This setup repair adds no fallback or test skips. ENV-001 remains a separate test-environment issue.

Implementation must preserve Python plugin compatibility while moving execution into Rust.
Track behavioral findings in the [bug and behavior review ledger](../../crates/meerschaum-py/PARITY.md#bug-and-behavior-review-ledger).
Parity is evidence for review, not a requirement to preserve every v4 bug: intentional changes
to observable v4 behavior and corresponding reference-test rewrites require maintainer approval.
Record the trigger, before/after behavior, compatibility impact, evidence and decision before
changing reference expectations. A proposed fix is not an approved deviation.

| Work | Acceptance criteria | Status |
|---|---|---|
| Parallel metadata actions, starting with `show partitions`, `show rowcounts` and `show sizes` | Bounded concurrency only for supported connectors; explicit single-worker mode; deterministic output and preserved per-pipe errors; measured overlapping reads and serial fallback tests | Implemented for capable native SQL connectors; overlap and exact output tests pass. Python/custom, API and Valkey connectors remain serial |
| Concurrent engine-state initialization | Independent connectors can initialize tables and missing schemas concurrently; interrupted or failed initialization is recoverable; existing indexes are preserved; warm writes retain their cost | PostgreSQL coordination and missing-index recovery implemented; 940 SQL tests, focused engine regressions and 11 browser tests pass. CockroachDB coordination remains open |
| Shared environment config layers | Compose reuses core parsing while preserving file/environment/patch precedence and config references | Implemented; core and compose parity tests pass |
| Python bridge lifecycle and bootstrap | Repeated worker interrupts remain usable; venv locks release on failure; external uv works in an isolated interpreter | Implemented; focused lifecycle, worker, venv and package tests pass in both core modes |
| pandas string dtype parity | Preserve Python merge results across pandas 2/3, string storage backends, null and empty deltas | Implemented; expanded core suites pass with pandas 2.3.3 and 3.0.3 in both modes. Wave 11 extends coverage to object dtype inference and special-value null sentinels; see the ledger |
| Remaining CLI output parity | Restore pre/post-sync hook result lines and distinguish datetime partition durations from integer-axis intervals | Implemented; hook transcripts match released v4 and typed partition metadata distinguishes datetime durations without changing the public Python metadata shape |
| Maintainer behavior review | Decide whether connector overrides should stop mutating shared v4 static defaults; review any intentional behavior changes and associated reference expectations together | V4-001 isolation fix and V5-007 supported Polars Object input approved on 2026-10-15; regression tests retain unrelated connector defaults and reject unsupported objects |
| Native Compose execution | Route all lifecycle subactions through native jobs, connectors and engine; preserve ownership and project configuration | Implemented and exercised with real disposable jobs and syncs; fresh plugin setup still reaches Python core and remains a D14 gap |
| D14 / D20 | Remove Python core and runtime dependency fallbacks while preserving the facade and plugin SDK | Password hashing now delegates unconditionally to Rust; metadata lookup uses the standard library; synchronous SQLite no longer requests an async driver. Pipe/config/actions migration remains incomplete |
| Release-candidate gates | Remaining deployment replays, full flavor gates, packaging/image limits and Python-core removal | Six-flavor facade suites, scoped private replays and image size checks passed. Installed-package functional gates and D14/D20 removal remain blockers; current CLI benchmarks are complete |

Metadata benchmark: `show partitions --nopretty` over 64 PostgreSQL pipes, each with four
daily partitions and 64 rows, took a median 0.625 seconds with one worker and 0.192 seconds
with four (3.26× speedup). Two warmups per setting preceded eight paired rounds with
alternating order; all 20 runs produced identical output. Timing includes CLI startup and
output capture. This measures loopback PostgreSQL with warm caches, without injected latency;
it is not a network-latency projection. The disposable database was removed afterward.
[Recorded samples](../../scripts/bench/results/2026-10-15-w10-metadata.json).

The rebuilt release CLI also completed the SQLite/PostGIS sync, incremental-sync and chain
benchmark matrix against v4.1.4: all 26 implementation/case entries passed their row-count
checks, and v5 was faster in every paired case. This covers 10k, 100k and 1M syncs, 100k and
1M incremental syncs, and three-pipe chains; graph-mode chains have no direct v4 counterpart.
The disposable benchmark schema was removed afterward.
[CLI report and samples](../../scripts/bench/results/2026-10-15-w10-hardening-cli.md).

Wave 10 validation (historical; superseded for changed paths by Wave 11 below): workspace formatting, all-target Clippy and Rust tests passed
(1,552 tests; three ignored subprocess entrypoints exercised by their parent tests).
After the final boundary guards, PyO3 Clippy and all 16 unit tests passed again, followed by
194 standalone core tests with one existing xfail in each pandas 2.3.3/3.0.3 × Python/Rust
combination and 100 affected dataframe/filtering/dtype integration tests. The main SQLite
Python suites passed separately: Rust mode 892 passed/12 skipped; Python mode 872 passed/
31 skipped/one explicit local GeoArrow exclusion (ENV-001). All 11 browser tests passed.
Packaging and fixture/changed-file hygiene checks passed. Reference fixtures and existing
parity expectations were not rewritten. Those separate suite runs did not resolve V4-001's combined collection-order failure.
The maintainer subsequently approved the isolation fix implemented in Wave 11.


## Wave 11 continuation (2026-10-15)

The maintainer raised the slim-image size budget to 250 MB; the full-image budget remains
900 MB. No runtime contents need to be removed solely to meet the former 200 MB limit.

The maintainer approved two behavior changes: per-connector SQL engine overrides now copy
shared defaults (V4-001), and the existing native Polars input path preserves supported Object
strings, UUIDs, bytes and Decimals while rejecting unsupported arbitrary objects (V5-007).
Neither approval permits other observable v4 changes or weakening reference assertions.

Compose lifecycle execution now uses native jobs and engine operations with project-scoped
worker environments. Hook result output and datetime partition labels match released-reference
behavior. Strict dataframe tests cover full-input dtype inference, special-value null sentinels,
partial dtype maps and empty deltas. Plugin pages registered during Dash initialization are
collected after initialization, fixing empty plugin apps discovered in private replays.

D14 and D20 remain release blockers. Password hashing and verification now use Rust without a
Python implementation. Standard-library distribution metadata replaces installer subprocesses,
and synchronous SQLite/GeoPackage engines no longer request an async driver. These changes do
not remove the remaining Python Pipe sync/fetch/verify branches, config read/write/sync logic,
action fallbacks, formatting/job dependencies or fresh-plugin setup paths. A passing suite in
Rust mode does not establish that all Python core execution has been removed.

Private API-host replays used copied plugins, scratch roots, synthetic metadata and restricted
network access. Across the two deployments, all 23 and four page routes and their page-router
callbacks passed, including session propagation. Native secure-server checks passed for login,
user lookup, console sessions, scoped-token reads and denied writes, revocation, redirects and
MCP tool visibility. This does not establish all domain-data callbacks, middleware semantics,
deployment-data geospatial exports or deployment from the final images. Three unchanged
deployment export helpers were separately exercised with synthetic geometries: released v4
and v5 matched GeoJSON output and GeoPackage row, CRS and geometry readback. The transport
call was recorded locally; no remote export destination or original dataset was used. Raw
deployment artifacts stay private.

The initial commit's hosted wheel matrix (`d50d5698`) passed on Linux, Windows, macOS and
experimental free-threaded Python. Self-hosted Rust and pytest jobs remained queued while the
runner was offline; local gates use separate disposable database endpoints and do not establish
remote CI success.


Local validation: workspace formatting and all-target Clippy passed; workspace tests passed
1,564 tests, with three ignored subprocess entrypoints exercised by their parent tests. Final
password binding checks additionally match all 12 released-reference edge cases exactly
(results, exception types/messages and warning counts). Core password tests cover 37 synthetic
legacy digest/alias vectors, and the Python facade checks both core modes. The dataframe/core
matrix passes 300 tests with one existing xfail in each pandas 2.3.3/3.0.3 × Python/Rust run,
including 106 strict boundary regressions. All 11 browser tests and nine API-host tests passed.


The private Compose replay exercised 35 configuration documents in native v5 and installed
v4.1.4, preserving definitions for metadata checks and substituting synthetic SQLite operations
and jobs for execution. Lifecycle statuses and synthetic row snapshots match, including the
reference failures for empty-project and duplicate-definition cleanup. A shared two-project
workflow preserves data until the last owner is removed. One initial subprocess failed while
the debug binary was being relinked; fresh full-lifecycle and unchanged-state reruns passed, and
the original failure remains in the private report. Subsequent checks use an immutable binary
copy. This validates Compose structure and lifecycle, not original production fetches or jobs.


Final slim image size is 200,384,701 bytes (200.38 MB), below the approved 250 MB limit.
Its installed-package smoke ran read-only with networking and automatic installs disabled.
Native CLI version, config lookup and password operations passed; public config writes failed
because PyYAML is absent, and dict-based Pipe sync reached missing SQLAlchemy, typing and
formatting dependencies (including `typing_extensions`, `pygments` and `rich`).
The smoke correctly exits nonzero. These are D14/D20 release blockers, not an invitation to add
those dependencies to the base wheel or to ship the remaining Python core fallback.


The full image is 844,052,775 bytes (844.05 MB), below its unchanged 900 MB limit. Both exact
byte-size gates pass. The final SDK and CLI wheels were exported and installed together into a
clean offline environment. Installed-wheel and image functional checks remain failures:

| Environment | Native CLI / config lookup / passwords | Public config roundtrip | Dict-based Pipe roundtrip | API host |
|---|---|---|---|---|
| Clean base-wheel environment | Pass | Fails: missing YAML support | Fails: remaining Python dependencies | Not a base-wheel extra |
| Slim image | Pass | Fails: missing YAML support | Fails: remaining Python dependencies | Not a base-image extra |
| Full image | Pass | Pass | Fails: implicit `semver>=3.0.2` install request | Fails: same install request; also warns about missing `uv` |

Image probes used disabled networking, read-only runtime filesystems and writable scratch roots
and home directories. Automatic install hooks were denied. The final full-image results use a
writable scratch home, correcting an initial harness permission error. No packages or images
were published. Size and packaging metadata success do not override these functional failures.


Final review also confirmed V4-002 in installed released v4.1.4: dependency-version prechecks
can call `pip_install` directly despite `MRSM_NO_AUTO_INSTALL`. The image smoke intercepted
this request before execution; source inspection confirms the direct installer path. This
inherited behavior needs maintainer review before repair, including the result returned when
a version parser is unavailable. It was not introduced by the metadata lookup change.

The full Python-mode facade suite passed against SQLite, PostGIS, TimescaleDB, MariaDB,
MySQL and MSSQL: 1,725 passed, 40 skipped, one existing xfail and the explicit local GeoArrow
exclusion (ENV-001). This combined run includes core and dataframe-boundary tests and retains
the original static-default assertions. The self-hosted runner subsequently came online and
was busy; the initial commit's Rust and pytest jobs were still queued at that check.


An initial combined Rust-mode run found three generic Arrow roundtrip fixture failures
(1,742 other tests passed). Pure PyArrow reproduces the same promotion from raw binary to
GeoArrow after extension registration. The opaque-metadata fixture now uses a deliberately
unregistered synthetic name; exact schema/data assertions and the real GeoArrow integration
test are unchanged. The same-process geometry-plus-core check passes 195 tests with one
existing xfail; this is test isolation, not a changed v4 behavior expectation.


Final password error-path review found and repaired unsupported-digest exception and validation
ordering differences. All 30 bounded released-v4 cases match the rebuilt extension exactly
(exception type/message and warnings), and 12 facade tests pass across both core modes.
Supported hashes use Rust exclusively. Unknown digest names consult standard-library lookup
metadata for platform-specific errors; no password, salt or PBKDF2 work is delegated to Python.
The final affected PyO3 Clippy check and extension build pass.


The final combined Rust-mode facade suite passes: **1,755 passed, 21 skipped, one existing
xfail**, across SQLite, PostGIS, TimescaleDB, MariaDB, MySQL and MSSQL. It includes the rebuilt
password binding and the isolated Arrow fixture, with no deselections. The Python-mode full
run passed 1,725 tests (40 skipped, one existing xfail, explicit ENV-001 exclusion); the final
password and fixture changes additionally passed focused checks in both modes. Final PyO3
unit tests pass all 16 cases. Both rebuilt images match the final wheel extension after the
existing stripping step and match all 30 password error oracle cases.
