# Meerschaum v5: build, CI and Docker

Companion to `docs/design/v5.0.0.md` (§10.2 reference harness, §12.1 size gate, §13 milestones, §15 risks).
Coding conventions are in `CODING_STANDARDS.md`; crate layering and crates.io publishing in `crates/README.md`.

## Build matrix

| Workflow | Ordinary pushes and PRs | Full validation |
|---|---|---|
| `rust.yaml` | Linux fmt, Clippy, rustdoc and broad Rust tests; affected feature/package/driver checks | Nightly and reusable full call: every platform, reference regeneration, all feature/package checks |
| `pytest.yaml` | One CI-profile wheel and debug CLI, then core/Postgres and affected backend lanes | Nightly: every lane and console browser tests; release call: test the shipping wheel and CLI with browsers required |
| `wheels.yaml` | No automatic build | Manual/reusable: selected release wheel matrix, standalone binaries and sdist |
| `wheel-platform.yaml` | Called by the wheel matrix | Each platform builds, immediately runs its own compatible smokes, then stages a validated artifact |
| `docs.yaml` | Strict build when docs inputs change; hosted runners only | Included in the release gate; uploads a website preview |
| `release.yaml` | Definition changes run a metadata check | One manual full candidate; tag pushes only verify an existing successful candidate |
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

`rust.yaml`'s hosted `guardrails` job runs on every push and pull request whatever changed:
`scripts/check_architecture.py`, then `scripts/parity/check_fixture_hygiene.sh` over the changed
tracked files (every tracked file for a dispatch or an unknown diff). Locally:
`scripts/rust/ci.sh guardrails`.

The isolated-member token scenario runs on the MSSQL/Valkey lane using an owned Valkey,
a separate API instance and a team data instance. Changes to token, team, instance and cache
handling select that lane. The full console browser suite runs in the core lane on nightly and release candidates. The pinned
Playwright package and Chromium must launch successfully; unavailable browser dependencies fail
these required runs. Their native plugin fixture travels as a separate verified artifact from
the existing CLI producer, tied to that CLI's bytes and current SDK. Consumers need no cached
SDK or Rust compiler. Ordinary PRs retain the faster API-host coverage and skip fixture builds.

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

Branch pushes and same-repository PRs may reuse a successful full release candidate when its
Git tree exactly matches the checked-out tree (the PR merge tree for a PR). The proof requires
the same repository, a successful release workflow, every mandatory gate and an unexpired
artifact catalog. A changed base, expired payload or unavailable proof runs the ordinary checks.
Shipping artifacts still require the exact commit, profile and every payload digest.

With the repository variable `CI_LOCAL_ARTIFACTS=true`, `pytest.yaml` hands the verified test wheel
and CLI from the `artifacts` job to the test jobs through `$MRSM_CI_CACHE/pytest-artifacts/`
(`scripts/ci/local_artifacts.py`) instead of a GitHub artifact round trip (about 178 MiB each way
over the self-hosted host's uplink). Every runner of the run must share that host and cache
directory. Reused artifacts (`artifacts_run_id`) and the `release` profile always use GitHub
artifacts, and `prepare_pytest.sh` verifies both paths identically. Unset, nothing changes.

When a candidate for the same commit is already queued or running, the hosted `changes` job of
`rust.yaml` and `pytest.yaml` waits for it (at most 75 minutes; the job's timeout is 90) instead
of competing with it for the self-hosted runners, then applies the same proof. A failed,
cancelled or late candidate, or any API error, runs the ordinary checks. Only a candidate that
exists when `changes` starts is waited for, so dispatch it right after the push. A PR whose merge
tree differs from its head commit's tree cannot reuse that candidate and does not wait.

### Isolated backend lanes

Each lane owns a scratch root, Compose project, volumes, loopback-mapped database ports and API
port. `scripts/ci/services.py` records ownership before startup and cleans only that project,
including after cancellation. Existing shared test services and production services are unrelated.
All unparameterized tests run once; parameterized cases are assigned by backend. A collection
proof rejects missing, repeated, unknown or conflicting cases before the full run.

| Lane | Workers | xdist scheduler | Measured release run |
|---|---:|---|---:|
| Core / SQLite / API-host / console browsers | 4 | `loadfile` | 20.9 min with `load` |
| Oracle | 8 | `load` | 7.7 min |
| MSSQL / Valkey | 1 | `load` | 5.7 min |
| PostgreSQL family and API | 2 | `load` | 3.0 min |
| MySQL / MariaDB | 1 | `load` | 1.9 min |

The combined budget is 16 workers. Oracle gets more slots because it dominated recorded call
time; MySQL and Valkey retain smaller budgets. `scripts/ci/shards.py` is the shared source of
truth. Test evidence artifacts retain per-lane collection reports, the union proof and ownership.
Rust tests retain their own shared-fixture lock and a bounded nextest budget.

Lanes are listed longest first and the matrix runs at most three at once (`max-parallel: 3`, one
per self-hosted runner). Matrix jobs are created in that order, so the core lane takes the first
free runner instead of queueing behind short lanes (it queued 5 to 17 minutes when it was listed last).
Raise `max-parallel` with the runner count. The core lane uses `--dist loadfile`: xdist's default
`load` hands each worker one contiguous chunk of the collection and never rebalances it, so the
whole browser suite ran on one worker for 18 to 20 minutes while the other three finished in 8
to 11. Grouping by file keeps each module's order and module-scoped fixtures (Playwright, the
browser) on one worker; the summed test time is about 46 minutes, about 12 per worker.

Full release and nightly runs also execute native account and copy checks against fresh PostgreSQL,
MySQL, MariaDB and Valkey fixtures (`scripts/ci/native_backends.py`). They reuse the compiled
workspace tests, use bounded containers and random loopback ports, and retain results and cleanup
receipts. Database-creation privileges exist only in these disposable fixtures. Ordinary PRs
run the Rust suite without contacting an ambient Valkey service.

### Release candidate workflow

```bash
git push origin main && gh workflow run release.yaml --ref main   # dispatch at once (push/PR checks then wait for it)
gh run list --workflow release.yaml --limit 3 --json databaseId,headSha,status,conclusion
```

Push and PR checks of the same commit wait for this candidate and reuse it ("Artifact reuse"),
so they need neither cancelling nor a later re-run. If one of them had already started its
ordinary checks before the dispatch existed, cancel it and `gh run rerun` it: the new attempt
waits for the candidate. (A PR run still supersedes the push run of the same commit through the
shared `auto` concurrency group; re-running that push run later costs one hosted minute.)

One run builds shipping artifacts and runs Rust, all Python lanes and the strict docs build.
The final gate verifies required platforms and seals every artifact digest in
`release-candidate-manifest`. It never publishes. To verify the candidate again without building:

```bash
gh workflow run release.yaml --ref main -f candidate_run_id=RUN_ID
```

A `v5*` tag must match the source version and point to an exact commit with a successful,
unexpired sealed candidate. Tag validation downloads those same bytes; it does not rebuild or
publish. Registry uploads, GitHub releases and Docker publication remain separately authorized
release steps.

Docker candidates take `candidate_run_id=SDK_RUN_ID` and consume that run's verified manylinux
wheel and standalone CLI; nothing is compiled. Dispatch the Docker candidate as soon as the SDK
candidate succeeds; it can overlap the other pre-publication steps. How the images are built,
tested and promoted is in "Docker publication" below.

### Build caches

Each runner keeps its own physical Cargo target; no targets or build locks are merged. Hosted
caches remain keyed by target/profile and save on `main` only. Self-hosted runners preserve
`target`, `.venv-ref` and `.venv-test`; sccache adds a bounded shared dependency cache. The disk
guard measures the resolved target filesystem and its actual size, including symlinked targets.
`cargo-nextest` still runs all selected Rust tests, followed by doctests. Tool downloads remain
version/checksum pinned; mold remains opt-in.

Local worktrees that share one physical target need one active source checkout for qualification.
When changing checkouts, invalidate only the workspace packages in the affected Cargo profile
with `cargo clean -p` before counting test results. The build lock serializes writes but does
not prevent reuse of another checkout's workspace artifacts. Keep third-party dependencies
cached and record the tested commit, profile and binary hashes. Runner targets remain separate.

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
| Backend containers and API processes | Each Python lane owns a unique Compose project, volumes, random loopback database ports, API port and scratch configuration. Ownership checks protect cleanup. The deployed Stack and fixed service ports are untouched. |
| `target/`, `.venv-ref`, `.venv-test`, `test_root`, the build lock `target/.build-py.lock` | per workspace (per runner): no sharing, no locking between runners |
| the compile work of dependencies | shared through sccache (one server per user, same `SCCACHE_DIR`): a second runner's cold build is mostly cache hits |
| `~/.cargo` (registry, git checkouts), the `rustup` toolchain | cargo and rustup take their own locks; safe |
| `${MRSM_CI_CACHE}/duckdb`, `/bin`, `/sccache` | written by atomic `mv` (`duckdb_lib.sh`, `ci_tools.sh`); sccache is concurrent by design |
| ports of the Rust tests | tests bind random free ports; the optional `MRSM_TEST_*_URI` services are not set in CI |
| disk | each runner's `disk-guard` measures its own filesystem; sccache is capped at 20 GB; GitHub-hosted legs use no local disk |
| `${MRSM_CI_CACHE}/native-api-venv-<key>` | one per requirements, interpreter (version and ABI) and ISO week; created once under `flock`, never cleared in place; keys older than 15 days are pruned (`ci.sh native-api-venv`) |
| `${MRSM_CI_CACHE}/maturin-<version>-<abi>` | one per exact maturin and interpreter ABI, created once under `flock` (`ci.sh maturin`) |
| `${MRSM_CI_CACHE}/pytest-artifacts/<run>-<attempt>` | only with `CI_LOCAL_ARTIFACTS=true` ("Artifact reuse"): written under a temporary name and renamed, removed after two days |

A new runner starts with a cold `target/` (the first job there compiles everything once; sccache serves the dependencies).

## Running CI locally

`scripts/rust/ci.sh` runs exactly the workflow's commands. With no arguments it runs `guardrails fmt clippy test build`.

```bash
scripts/rust/ci.sh setup          # rustup (installed non-interactively if missing) + rust-toolchain.toml
scripts/rust/ci.sh guardrails     # architecture rules + hygiene scan of tracked files (stdlib Python, seconds)
scripts/rust/ci.sh fmt clippy     # any subset, in order (fmt also runs scripts/check_packaging.py)
scripts/rust/ci.sh check          # the hosted legs
scripts/rust/ci.sh disk-guard     # cargo clean if < MRSM_CI_MIN_FREE_GB (default 10) GiB free
scripts/rust/ci.sh parity         # reference harness (below)
```

CI also sets `CARGO_INCREMENTAL=0`, which only changes how workspace crates are cached, not the results.
On a self-hosted runner `ci.sh` also turns on sccache and nextest (see "CI: what runs when"); to try them locally:
`MRSM_CI_ACCEL=1 scripts/rust/ci.sh test` (installs the pinned tools under `$MRSM_CI_CACHE`, default `~/.cache/mrsm-ci`).
Nextest runs at most two tests from the API host and team executor suites at once. The Rust test
step reaps its own detached children on exit or cancellation and fails if any survive a completed run.

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

Local source builds compile on Fedora 44 with the image's pinned Rust toolchain ("Base image and
toolchain pins"): `native-builder` builds the standalone `mrsm` and `rust-builder` the wheel, both
from `source-builder`. Release builds select the verified SDK context instead (`sdk_source`,
`--sdk-artifacts`). Layer order keeps slow inputs cached across releases: the Cargo registry and
`target/` live in BuildKit cache mounts (`mrsm-v5-cargo-*`), DNF and pip downloads in
per-architecture cache mounts, and the confined compiler snapshot is built without the workspace
manifests. `py-slim` (the wheel alone, the volatile layer) and `py-full-deps` (the extras, cached
until `requirements/*.txt` change) install into prefixes that the final stages copy. Slim trims
development headers; full keeps them for building dependencies. PyYAML arrives only as a
dependency of the extras.

`slim` and `rust` drop the CA trust sources and the OpenSSL FIPS provider for their size budgets
(add a CA by appending to `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem` or setting
`SSL_CERT_FILE`); `full` and `rust-full` keep both, so `update-ca-trust` works and OpenSSL starts on
FIPS hosts. No image contains a package manager. `smoke_image.sh` checks that the native Factory
manifest of `full`, `rust` and `rust-full` describes exactly the files of the final image
(`write_native_factory_manifest.py --verify-export` over `docker export`).

The final stages use `COPY --link`, so identical copies (`mrsm`, the wheel, the compiler snapshot)
have one layer digest in every image. The two `--chown` copies into the user's home directory are
the exception: a linked layer carries its own root-owned parent directories and would replace the
home directory's ownership. The images run as `MRSM_IMAGE_UID:MRSM_IMAGE_GID`; CI pins `1000:1000`
whatever account the runner uses (hosted runners are uid 1001), and `docker_candidate.py` refuses a
candidate with any other user. A local `buildx.sh` defaults to the caller's ids so bind mounts match.

`scripts/docker/buildx.sh [--push] [--v4-only] [--platform P] [--arch-tags] [--merge] [--candidate REPO --metadata-dir DIR] [--cache-ref REF]` builds the following images:

- `minimal` on `--target slim`, retained as a compatibility tag.
- `full` on `--target full`.
- `slim`, tagged `<version>-slim`, plus `slim` for releases.
- `rust` and `rust-full`, with matching version-qualified and stable floating tags.

Pre-releases (`dev`, `rc`, `aN`, `bN`) get only version-qualified tags. The script refuses to push the
Rust targets for a version below 5. The legacy `--v4-only` option selects default-target images and
omits the slim/native targets; the current Dockerfile still installs the v5 wheels. Prereleases
select only `linux/amd64`; final releases also select `linux/arm64`.
`scripts/ci/release_matrix.py` reads the Cargo version and selects the Docker, wheel, binary and
matching smoke matrices on release candidate dispatches. Source distributions and crates.io packages
remain platform independent. The complete wheel and binary matrix is retained for final releases.
`--candidate` pushes to a staging repository and `--cache-ref` imports and exports a registry build
cache (both in "Docker publication"); `--merge` joins only the selected release platforms, and CI
checks every published variant's manifest. Prereleases never move floating tags.
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

## Docker publication

Two candidate runs of 5.1.2 passed every gate (results at the end of this section). Rehearse the
first publication through this workflow to verify GitHub's registry access and the Docker Hub copy.

It exists because the self-hosted host uploads at under 1 MB/s: promoting 5.1.1 from that host took
3 h 28 min for amd64 and 2 h 06 min for arm64. Image bytes now move between registries, and the
self-hosted host only downloads.

`docker-build.yaml`, dispatched with `candidate_run_id=SDK_RUN_ID` (a successful `release.yaml` run):

1. `buildx` (one job per architecture, GitHub-hosted `ubuntu-24.04` and `ubuntu-24.04-arm`, no
   QEMU) frees disk, logs in to GHCR with the workflow token and runs
   `buildx.sh --candidate ghcr.io/<owner>/<repo>/candidate --cache-ref ghcr.io/<owner>/<repo>/buildcache`.
   Each image is pushed by digest without provenance or SBOM (one image manifest), pulled back by
   that digest, and the smokes, the size gate and the offline facade smoke run on those bytes.
   `docker_candidate.py record` writes the `docker-candidate-<arch>` receipt (digest and config
   digest per group). The cache imports every target's `buildcache:<target>-<arch>` and exports its
   own (`mode=max`), so unchanged OS, dependency and compiler-snapshot layers keep their digests.
2. `executors` (self-hosted amd64, the one job that compiles Rust) pulls the amd64 `full` digest
   and runs the installed Team executor suite; its receipt is `docker-executors-amd64`. It pushes
   nothing.

Rehearse with the same SDK run plus `rehearse=true image_run_id=DOCKER_RUN_ID`. With publication
authorized, use `publish=true` in place of `rehearse=true`:

3. `promote` (hosted, per architecture) verifies the sealed SDK again, fetches both receipts from
   the candidate run, checks that the digests are still staged, then copies exactly those digests to
   `bmeares/meerschaum:<version>-<group>-<arch>` registry to registry
   (`imagetools create --prefer-index=false`) and checks digest, config, platform, revision and user.
4. `publish` (hosted) assembles indexes from the receipt digests on Docker Hub and checks that
   every platform points to the exact tested manifest. With `publish=true`, it moves release
   aliases only after every versioned index passes. Rehearsals write tags with a
   `-check-<run>-<attempt>` suffix and stop after verifying the indexes.

Publication never rebuilds. A missing staged digest stops it (exit code 2): dispatch a new candidate
run, which reruns every gate, and publish that one. Only `buildx` may write packages; the Docker Hub
secrets appear only in the promotion and index jobs. Repository variable `DOCKER_RUNNER=self-hosted` moves
`buildx` back to the self-hosted runners (persistent per-architecture builders limited to 16 CPUs,
64 GiB and a 100 GB cache; arm64 under QEMU; no registry cache). That fallback still stages on GHCR,
so it uploads every image from the slow host.

### Base image and toolchain pins

Every Fedora stage starts from `ARG fedora_image=fedora:44@sha256:...`. The image's Rust is
`ARG rust_toolchain` (an exact release, not `rust-toolchain.toml`'s `stable` channel), installed by
`rustup-init` `ARG rustup_version`, checked against `rustup_sha256_amd64` / `rustup_sha256_arm64`.
Nothing follows a moving tag or channel, so cached layers and the digests registries deduplicate
change only when these lines change. Refresh them deliberately (monthly, for a CVE in the base, or
for a new Rust release), knowing that every OS, dependency and snapshot layer is rebuilt and users
pull it again:

```bash
docker buildx imagetools inspect fedora:44 | sed -n 's/^Digest: *//p'      # fedora_image digest
curl -fsSL https://static.rust-lang.org/dist/channel-rust-stable.toml | sed -n '/^\[pkg.rustc\]/,/^version/p'
curl -fsSL https://static.rust-lang.org/rustup/release-stable.toml           # rustup_version
curl -fsSL https://static.rust-lang.org/rustup/archive/<version>/{x86_64,aarch64}-unknown-linux-gnu/rustup-init.sha256
```

### What the first candidate runs confirmed

Two runs of the same 5.1.2 commit and SDK candidate, the second against the cache the first filled:

| | Cold cache | Warm cache |
|---|---|---|
| Whole run | 21 min | 14 min |
| `buildx` job (amd64 / arm64) | 9.5 / 10 min | 4 / 3 min |
| "Build Docker images" step (amd64 / arm64) | 6.5 / 8 min | 1.7 / 2.2 min |
| `executors` (self-hosted) | 11 min | 9 min |

- **Packages.** The first `GITHUB_TOKEN` push created the `candidate` and `buildcache` packages, and
  `executors` pulled the `full` digest with `packages: read`. Both packages were created **public**:
  anyone can pull a candidate tag (`<version>-<group>-<arch>-<run>-<attempt>`) before the release.
  Make them private in the package settings if that matters; nothing in the workflow depends on
  anonymous access.
- **Digests.** `--metadata-file` wrote both digests, each a single OCI image manifest, and the
  receipts carry them. On the self-hosted daemon the pulled image's `Id` equals the receipt's config
  digest, and its user is `1000:1000`.
- **Cache.** Every cache export succeeded. The warm run reused every layer: all four images on both
  architectures have identical layer digests in the two runs. The image digests still differ (one
  `history[].created` timestamp in the config), so a receipt names the images of its own run only.
  Both runs used the same SDK; reuse of the OS, dependency and compiler layers across two versions
  is first measured at the next release.
- **Shared layers** (amd64, compressed): the `mrsm` layer (44 MB) is in `full`, `rust` and
  `rust-full`, the compiler layer (228 MB) in `full` and `rust-full`, and the Python base layer
  (52 MB) in `slim` and `full`: 1,022 MB of distinct layers for 1,398 MB of images.
- **Disk.** At least 100 GB free after the builds on both hosted architectures.
- **arm64** builds and smokes natively, including the native Factory manifest check, and the pinned
  `rustup-init` and toolchain install on both.

### What the first rehearsal and publish confirmed

Confirmed on `v5.1.2` (`93015f5f`, SDK run `45048246322`, Docker candidate `45053779525`):

- **Hosted rehearsal (`rehearse=true`, run `45055564988`, ~2.5 min total):** `promote (amd64)` (1m15s) and `promote (arm64)` (1m41s) read the staged GHCR digests with `packages: read`, copied them registry-to-registry to Docker Hub's `-check-*` architecture tags without rebuilding, and `publish` (41s) verified the multi-arch indexes against the tested receipts.
- **Hosted publication (`publish=true`, run `45055882594`, ~2 min total):** `promote (amd64)` (1m3s), `promote (arm64)` (1m0s), and `publish` (56s) promoted all versioned and floating release aliases (`latest`, `full`, `slim`, `minimal`, `rust`, `rust-full`, `api`) after digest verification — down from 5 h 34 min on the self-hosted runner for 5.1.1.
- **Note on GHCR package visibility:** If the `candidate` and `buildcache` GHCR packages are switched from public to private in GitHub package settings, confirm on the next candidate run that `executors` and `promote` still pull with `packages: read`.

## Crate names on crates.io (§15)

The 15 published crates (the `meerschaum` front crate included) were claimed by publishing
`5.0.0-rc.1`; `meerschaum-py` is `publish = false` (the extension ships in the PyPI wheel). The
publication order is in `.claude/skills/release/SKILL.md` and `crates/README.md` "Publishing".
`scripts/rust/check_crate_names.sh` checks whether a new crate name is free on crates.io (the
lookup is canonical: `-` and `_` count as the same name).

## Release procedure (5.x)

`.claude/skills/release/SKILL.md`, section "Meerschaum 5.x", is the ordered runbook: bump the version (Cargo
`[workspace.package]` and the lockstep `=` pins, `Cargo.lock`, `meerschaum/config/_version.py`, literals),
`scripts/check_packaging.py`, `scripts/rust/ci.sh package` and `publish-dry-run`, dispatch `release.yaml` on the exact release commit, wait for its complete sealed candidate, download that run's verified wheels and sdist (`gh run download`), `twine check`, then the
steps that need explicit authorization: PyPI `twine upload`, `cargo publish --workspace --exclude meerschaum-py`,
the annotated tag (`v5.1.0rc1`) and a GitHub release, and (separately) Docker. Releases are cut from
`main`. The PyPI classifier is `Development Status :: 5 - Production/Stable`. A pre-release gets only version-qualified
Docker tags. The wheels uploaded are the CI artifacts of the release commit, never a local `python -m build` wheel
(`linux_x86_64`, which PyPI rejects). The sdist needs a Rust toolchain and network access to crates.io and GitHub
(the sqlx `[patch]` points at a git revision).

## Fixture hygiene

Recorded fixtures are scanned by `scripts/parity/check_fixture_hygiene.sh` (run by the parity job after
the reference regeneration, by the `guardrails` job over changed files, and by hand before any commit
that touches `crates/*/tests/fixtures`). It fails on captured process environments, Claude Code or
desktop session variables, home directories, the maintainer's handle in a path or e-mail, private
project names and credential shapes. By default it skips `*.py` (generators name what they scrub) and
`*.md`. `--public [PATH ...]` (default: every tracked file) scans docs and code, Markdown and Python
included, with every pattern except the captured-environment variable names, which code and docs
mention legitimately. Each allowed exception is one pattern in one file, listed in the script.
Generators that start
a Python reference server or daemon must do so with `env -i` plus only `PATH`, a temporary `HOME`,
`MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR` and `PYTHONHASHSEED=0`, and must replace minted credentials, pids,
timestamps and absolute paths with placeholders before writing.

## Intentional changes to v4 behavior

Parity is evidence for review, not a requirement to preserve every v4 bug. Intentional changes to
observable v4 behavior, and the reference-test rewrites that go with them, require maintainer
approval. Record the trigger, before/after behavior, compatibility impact, evidence and decision
in the [bug and behavior review ledger](../../crates/meerschaum-py/PARITY.md#bug-and-behavior-review-ledger)
before changing reference expectations. A proposed fix is not an approved deviation.


## Acceptance evidence for user-facing changes

Write down a user's action and its observable result before reviewing the implementation.
For a regression, run the same scenario on the previous release and the candidate. For a
new feature, temporarily change a decision the test depends on and verify the expected
failure. Exercise combinations implicated by previous incidents explicitly. Use pairwise
selection to bound the remaining role, store, platform and existing-state combinations.

A required service-backed check must reach the assertion against its owned service.
Record the tested artifact revision, service version and final observable value in the
job output. Record optional skips separately. A process exit before the assertion, an
unexplained upgrade difference or a missing required backend fails the release check.
