# Meerschaum v5: build, CI and Docker

Companion to `docs/design/v5.0.0.md` (§10.2 reference harness, §12.1 size gate, §13 milestones, §15 risks).
Conventions for the crates themselves are in `crates/README.md`.

## Build matrix

| Workflow / job | Runner | Command (`scripts/rust/ci.sh <step>`) | Runs when | Blocks? |
|---|---|---|---|---|
| `rust` / `changes` | GitHub-hosted | `scripts/ci/changes.py`: which jobs this diff needs | always | n/a |
| `rust` / `fmt` | GitHub-hosted | `cargo fmt --all --check` | Rust inputs | yes |
| `rust` / `clippy` | self-hosted | `cargo clippy --all-targets -- -D warnings` | Rust inputs | yes |
| `rust` / `test` | self-hosted | debug `_core` extension, `cargo nextest run --workspace` (all tests, one process each), `cargo test --workspace --doc` | Rust inputs, or the Python the cargo tests read / import | yes |
| `rust` / `frontcrate` | self-hosted | the `meerschaum` front crate's feature matrix | front crate / manifests, dispatch, tag | yes |
| `rust` / `package` | self-hosted | `check_packaging.py`, `cargo package --workspace --no-verify` + checks | manifests / packaging files, dispatch, tag | yes |
| `rust` / `build` | self-hosted | `cargo build --release -p meerschaum --bin mrsm` (the standalone `mrsm` that `cargo install meerschaum` builds; uploads it) | dispatch / tag only | yes |
| `rust` / `parity` | self-hosted | regenerate `crates/*/tests/fixtures/gen_*.py` with v4.1.4, diff | generators, fixtures, `scripts/parity/`, dispatch, tag | yes (optional job) |
| `rust` / `check (ubuntu-latest)` | GitHub-hosted | `cargo check --workspace` | Rust inputs | yes |
| `rust` / `check (windows-latest, macos-latest)` | GitHub-hosted | `cargo check --workspace` | Rust inputs | no (`continue-on-error`, §15) |
| `rust` / `duckdb (...)` | GitHub-hosted (Linux arm64, macOS, Windows x64 and arm64) | `ci.sh duckdb-platform` | the DuckDB loader, dispatch, tag | yes (`windows-11-arm`: no) |
| `rust` / `summary` | GitHub-hosted | fails if a job that ran failed or was cancelled | always | **the required check** |
| `wheels` (`wheels.yaml`) | GitHub-hosted, manual dispatch / `v5*` tag only | Prereleases: Linux x86_64 only; finals: maturin-action builds one `cp311-abi3` wheel on Linux x86_64 and Linux arm64 (`ubuntu-24.04-arm`, native `manylinux_2_28`), the same two as `musllinux_1_2` (cross-built in the `rust-musl-cross` containers on `ubuntu-latest`, smoke-tested in Alpine, the aarch64 one natively on the arm runner), macOS arm64 and x86_64 (`macos-15-intel`) and Windows x86_64, the sdist, an experimental `cp314t` Linux wheel, and the standalone `mrsm` release binary of five targets (Linux ones in `manylinux_2_28`, glibc floor 2.28; macOS arm64 and x86_64; Windows x86_64); a wheel tag/size guard and a smoke test per OS (see below) | dispatch / `v5*` tag | yes (`cp314t` is `continue-on-error`) |
| `buildx` (`docker-build.yaml`) | Self-hosted Linux x86_64 (AMD64 native; ARM64 under QEMU for finals), manual | `scripts/docker/buildx.sh` per architecture, then `size_gate.sh`; a publish pushes `<version>-<tag>-<arch>` images and the `publish` job joins them (`buildx.sh --merge`) | dispatch only | size gate enforces by default |
| `pytest` / `changes`, `tests`, `summary` | GitHub-hosted, self-hosted, GitHub-hosted | `scripts/rust/build_py.sh --profile ci` (the quick `ci` Cargo profile), then `scripts/test.sh db` | Python, tests, harness or Rust inputs; a push to `v5` runs the `sqlite,timescaledb` flavors, a dispatch and a PR run `all` | `pytest / summary` is the required check |

`rust.yaml` runs on pushes to `v5`, on `v5*` tags, on pull requests and on manual dispatch; `pytest.yaml` on pushes to
`v5` (and `main`/`staging`), pull requests and dispatch; `wheels.yaml` only on dispatch and `v5*` tags; `docker-build.yaml`
only on dispatch. What a push actually runs is decided by the `changes` job ("CI: what runs when" below), not by
`paths:` filters. `clippy`, `test`, `frontcrate`, `package` and `build` wait for `fmt` (a few seconds on a hosted runner) so a
formatting slip costs no self-hosted time. Self-hosted jobs are skipped for pull requests from forks, so untrusted code never runs on
the maintainer's host. Those PRs still get the hosted `fmt` and `check` legs.

## CI: what runs when

Goal: a push never waits on CI between commits (about 8 to 12 minutes on warm caches, jobs running side by side on
the host's runners) while a release is as rigorous as before. Four mechanisms:

1. **Change detection** (`scripts/ci/changes.py`, the `changes` job of `rust.yaml` and `pytest.yaml`; pinned by
   `tests/test_ci_changes.py`). It diffs the push (`before..after`) or the pull request (`base..HEAD`) and turns jobs on from the
   changed paths. A dispatch, a tag, a schedule or a diff it cannot compute (new branch, error) turns **everything** on.

   | Change | Jobs that run |
   |---|---|
   | docs only (`docs/**`, root `*.md`, `.claude/**`, `.agents/**`, `scripts/bench/**`, licenses) | none (`changes` and `summary` only) |
   | Python only (`meerschaum/**`, `tests/**`, harness scripts) | `pytest` (sqlite + timescaledb flavors) |
   | Python the cargo tests read or import (`meerschaum/worker/**`, `_core`, `_internal`, `utils/packages`, `api/dash/execute.py`, `pyproject.toml`, `requirements/**`) | `pytest`, `test` |
   | Rust (`crates/**`, `Cargo.*`, toolchain, `.cargo/**`, `scripts/rust/**`, `scripts/ci/**`) | `fmt`, `clippy`, `test`, `check` x3, `pytest` |
   | generators, fixtures, plugins a generator loads, `scripts/parity/**` | also `parity` (v4.1.4 produces the fixtures; no Rust source can change the result, so Rust changes alone skip it) |
   | `crates/meerschaum/**` or any `crates/*/Cargo.toml` | also `frontcrate` |
   | manifests, crate READMEs/licenses, `check_packaging.py`, extras | also `package` |
   | `crates/meerschaum-connector-sql/**`, `duckdb_lib.sh` | also `duckdb` (4-OS matrix) |
   | dispatch, `v5*` tag, schedule, unknown diff | everything, including `build` (release binary) and `parity` |
   | a file no rule claims | `rust`, `test`, `pytest` (the safe default) |

   The rule tables are the lists at the top of `scripts/ci/changes.py`: widen one there (for example parity on kernel / core /
   connector changes) and add the case to `tests/test_ci_changes.py`.
2. **Required status**: each workflow ends in an always-running `summary` job that fails only when a job that ran failed or was
   cancelled. Skipped jobs do not block it. Make `rust / summary` and `pytest / summary` the required checks, never the
   individual jobs.
3. **A quick profile for development builds.** Cargo's `ci` profile (`Cargo.toml`: inherits `release`, no LTO, opt-level 1,
   256 codegen units, line tables only) builds the pytest job's `_core` extension (`scripts/rust/build_py.sh --profile ci`;
   a profile is the only addition, `--release` stays the default of the script). Measured on this host (40 cores): cold
   3 min 24 s, after touching `meerschaum-core` (everything rebuilds) 1 min 50 s, against 9 min 54 s for the release profile in CI;
   `target/ci` is 2.2 GB. The suite passes identically (1602 passed, 170 skipped on the sqlite flavor with the `ci` extension). Nothing that ships uses it:
   the wheels, the Docker images and the release binaries are `--release`, and `tests/test_workflows_packaging.py` fails if a
   release path names the `ci` profile. `build_info()['profile']` still says `release` (it reports Cargo's `PROFILE`).
4. **Release builds only on dispatch / tag.** `wheels.yaml` (no pull-request trigger any more) and `docker-build.yaml` run on
   dispatch (wheels also on `v5*` tags); the `rust` dispatch / tag also runs `build`, `parity`, `frontcrate`, `package` and the DuckDB matrix.
   `pytest` on a dispatch runs every flavor.

Build accelerators (self-hosted Linux runners; GitHub-hosted legs use `Swatinem/rust-cache`, pinned by commit, restored on every
run and saved from `v5` only, in the wheels, `check` and `duckdb` jobs):

- **warm `target/`**: self-hosted checkouts use `clean: false` and `git clean -ffdx -e /target -e /.venv-ref -e /.venv-test`
  (no trailing slash: it also protects a `target` that is a symlink to another disk). `pytest.yaml` and `rust.yaml` both keep it;
  neither wipes `target/` any more. The disk guard drops the `release` and `ci` layers before a full `cargo clean`.
- **sccache** (`RUSTC_WRAPPER`): a local on-disk cache shared by every runner of the host, 20 GB cap
  (`SCCACHE_CACHE_SIZE`), in `${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/sccache`. It makes a second runner's cold `target/`, a
  `cargo clean`, or a re-run of a job fast: dependencies compile once per host, not once per workspace.
- **cargo-nextest**: the `test` step runs `cargo nextest run --workspace --no-fail-fast` and then `cargo test --workspace --doc`
  (nextest does not run doc tests). Settings in `.config/nextest.toml`. Measured on this host: 2939 tests in 84 s of test time
  (2 m 44 s including the compile), all passing, against 10 m 20 s for `cargo test --workspace` in the last CI run.
  Without nextest installed `ci.sh test` falls back to `cargo test --workspace` (`MRSM_CI_NEXTEST=0` forces it).
- **mold** is opt-in (`MRSM_CI_MOLD=1`): since Rust 1.90 rustc links with lld on x86_64 Linux, and linking the 280 MB debug `mrsm`
  takes 1.3 s with lld, 0.45 s with mold and 12 s with GNU ld. A saving of under a second per binary does not pay for the cold rebuild that
  a `RUSTFLAGS` change forces, so it is off. Enabling it needs nothing but the variable (the pinned binary is downloaded).
- The tools are installed by `scripts/rust/ci_tools.sh` into `${MRSM_CI_CACHE:-$HOME/.cache/mrsm-ci}/bin` (user level, pinned versions
  and SHA-256, no sudo), once, by `ci.sh` itself the first time a self-hosted job runs. `ci.sh` enables them only when
  `RUNNER_ENVIRONMENT=self-hosted` (set by the runner) or `MRSM_CI_ACCEL=1`; locally and on hosted runners nothing changes.
  `ci.sh env` prints the `export` lines for workflow steps that run cargo or maturin themselves.

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
| `target/`, `.venv-ref`, `.venv-test`, `test_root`, `.mrsm-cargo-root`, the build lock `target/.build-py.lock` | per workspace (per runner): no sharing, no locking between runners |
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
`target/`. `disk-guard` runs before the clippy, test, frontcrate and build jobs: below 10 GB free it first drops the `release` and `ci`
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

`scripts/check_packaging.py` verifies that the Cargo workspace version, the console scripts and the extras in `pyproject.toml` agree with `meerschaum/utils/packages/_packages.py`, and (`--wheels dist`) that the wheel has no dependencies and no `mrsm` binary. `.github/workflows/wheels.yaml` builds wheels on GitHub-hosted runners. Prereleases select Linux x86_64 only (glibc, musl and experimental cp314t), plus one Linux x86_64 CLI binary and the source distribution. Final releases use the complete matrix (Linux x86_64 and arm64 for glibc and musl, macOS arm64 and x86_64, Windows x86_64; Windows arm64 is not built) and smoke-tests it in a fresh venv (`mrsm --version`, `mrsm show connectors`, `python -m meerschaum --version`, a `sync` of a tiny pipe on SQLite, a job round trip, and the Python API through `scripts/docker/smoke_facade.py` with `MRSM_NO_AUTO_INSTALL=1`, a scratch SQLite instance and pip/uv shims that fail the step if anything tries to install). A build-job step checks the platform tag (Linux wheels must be `manylinux_2_28` or older, or `musllinux_1_2` or older for the musl ones, and carry the expected architecture, or PyPI rejects them) and fails a wheel over 95 MiB (PyPI refuses 100 MiB). The standalone binary is `cargo install meerschaum` (the front crate's default `cli` feature; library users use `default-features = false`). `scripts/rust/build_py.sh` still builds the in-tree extension for development. The Docker `slim` image installs the base wheel only; the default image installs `meerschaum[full]`. Current size and functional gates are recorded below, rather than inferred from the scaffold measurements.

## Docker targets and the size gate (§12.1)

| `docker buildx build ...` | Image | Contents |
|---|---|---|
| `.` (default, last stage) | `full` with selected extras | Installs the locally built maturin wheel; `dep_group=full` (default), `api` or `minimal` selects the extras. Entrypoint `mrsm`. |
| `--target full` | full runtime | Fedora 44 installroot plus the wheel, selected extras, plugin-install tooling and interactive tools. Entrypoint `mrsm`. |
| `--target slim` | base runtime | Fedora 44 installroot plus the base wheel, with no package manager or compiler. Entrypoint `mrsm`. |

The Fedora 44 `rust-builder` stage builds the wheel with maturin and the repository's Rust
toolchain. BuildKit cache mounts share the Rust artifacts between builds. The `py-slim` and
`py-full` stages install into a prefix and trim development files before copying into the
runtime installroots. Full still explicitly includes PyYAML while public Python config I/O
requires it; that is an outstanding D20 dependency, not part of the declared extras.

`scripts/docker/buildx.sh [--push] [--v4-only] [--platform P] [--arch-tags] [--merge]` builds the following images:

- `minimal` on the default target with no selected extras.
- `full` on `--target full`.
- `slim`, tagged `<version>-slim`, plus `slim` for releases.

Pre-releases (`dev`, `rc`, `aN`, `bN`) get only version-qualified tags. The script refuses to push the
Rust targets for a version below 5. The legacy `--v4-only` option selects default-target images and omits `slim`; the current
Dockerfile still installs the v5 wheels.
Docker release builds run on the existing **self-hosted Linux x86_64 runners**. Prereleases
select only `linux/amd64`; final releases also select `linux/arm64`.
`scripts/ci/release_matrix.py` reads the Cargo version and selects the Docker, wheel, binary and
matching smoke matrices, on both dispatches and tags. Source distributions and crates.io packages
remain platform independent. The complete wheel and binary matrix is retained for final releases.

Each Docker architecture uses its own named BuildKit cache (`meerschaum-release-<arch>`), preserved
between jobs with `keep-state: true`. The builder is limited to 16 CPUs and 64 GiB RAM, with cache
GC budgets of 100 GB maximum use and 30 GB minimum free space. The workflow serializes runs and
architecture jobs because runners share a daemon and loaded image tags. It uses a temporary Docker
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
`scripts/docker/size_gate.sh [--warn-only] [--slim-max 250] [--full-max 900] [--slim-image I] [--full-image I]`
measures the uncompressed size from `docker image inspect` in decimal MB and fails over the limit.
The maintainer raised the slim limit to 250 MB on 2026-10-15; full remains 900 MB.
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
`scripts/check_packaging.py`, `scripts/rust/ci.sh package` and `publish-dry-run`, dispatch `wheels`, `rust` and
`pytest` on the release commit, download that run's wheels and sdist (`gh run download`), `twine check`, then the
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
