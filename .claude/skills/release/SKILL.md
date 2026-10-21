---
name: release
description: Meerschaum release process. Two sections - 5.x (the Rust/maturin train cut from the v5 branch - version bump across Cargo and Python, CI wheels for the exact commit, sdist, PyPI, crates.io workspace publish, Docker, tag and GitHub pre-release) and v4.x (dev to main PR, build.sh, twine). Use when cutting or staging a Meerschaum release or preparing a release commit.
---

# Meerschaum 5.x release process (the `v5` branch; rc and final)

**Nothing in this section publishes by itself, and nothing may be published without the user's explicit
authorization for that exact step** (PyPI, crates.io, DockerHub, a pushed git tag, a GitHub release). Steps
that publish are marked **[needs explicit user authorization]**. For 5.0.0rc1 the user authorized PyPI,
crates.io and a GitHub pre-release; DockerHub is a separate authorization. Agents stage and verify; the
maintainer (or an agent the maintainer tells to) runs the marked steps.

State at the end of an rc: one commit on `v5` (no PR to `main`; `main` is merged only for 5.0.0 final)
carrying the bumped versions; that commit's CI green; `dist/` holding the wheels built by CI for that
commit plus the sdist; the packages on PyPI and crates.io; the annotated tag `v5.0.0rc1` and a GitHub
pre-release. Tag spelling: `v<PEP 440 version>`, i.e. `v5.0.0rc1`, `v5.0.0` (the `v5*` tag trigger of
`wheels.yaml` matches it, and a tag push only builds wheels, it never publishes).

Versions have two spellings that must agree (`scripts/check_packaging.py` enforces it): Cargo
`5.0.0-rc.1` (use the dot form; Cargo sorts `rc.10` after `rc.2` but `rc10` before `rc2`) and PEP 440
`5.0.0rc1`. A final is `5.0.0` in both. Pre-release installs need `pip install --pre meerschaum` (or
`meerschaum==5.0.0rc1`) and `cargo install meerschaum --locked --version 5.0.0-rc.1` (cargo never picks a
pre-release without the explicit version). `rc2` is `5.0.0-rc.2` / `5.0.0rc2`.

**Platform policy (releases after rc3): prereleases publish Linux x86_64 artifacts only.**
This applies to Docker (`linux/amd64`), Python wheels (glibc and musl, plus experimental cp314t if
its checks pass), and the standalone CLI binary (`x86_64-unknown-linux-gnu`). The sdist and all
15 crates.io packages remain platform independent. Final releases retain the complete platform
matrix below. `scripts/ci/release_matrix.py` reads the Cargo version for both workflows, including
manual dispatches; do not manually re-enable other prerelease architectures.

Final-release wheel platforms (`.github/workflows/wheels.yaml`, GitHub-hosted runners only): Linux x86_64 and
Linux arm64 (`manylinux_2_28`, native `ubuntu-24.04-arm`), the same two for musl libc (`musllinux_1_2`, built
in the `rust-musl-cross` containers on `ubuntu-latest`, the aarch64 one smoke-tested natively on
`ubuntu-24.04-arm` in an Alpine container), macOS arm64, macOS x86_64 (`macos-15-intel`), Windows x86_64, all one
`cp311-abi3` wheel per platform, plus one experimental free-threaded `cp314t` Linux x86_64 wheel and the
sdist. Standalone `mrsm` binaries (the `cargo install meerschaum` program) are built for five targets
(`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` in `manylinux_2_28` containers, so the glibc floor is 2.28;
`aarch64-apple-darwin` and `x86_64-apple-darwin`; `x86_64-pc-windows-msvc`) as GitHub release assets, not on PyPI.
**Not built:** Windows arm64, Linux arm64 free-threaded, musl standalone binaries. Docker builds run
on self-hosted Linux x86_64 runners: AMD64 natively, ARM64 under QEMU for final releases. Each has a
persistent BuildKit cache. `buildx.sh --merge` joins the selected platforms (5.7).

## 5.1 Prepare the release commit

All on the `v5` branch; do not commit or tag as an agent unless the maintainer says so.

```bash
export PATH="$HOME/.cargo/bin:$PATH" CARGO_INCREMENTAL=0
OLD_CARGO=5.0.0-a1  OLD_PEP=5.0.0a1        # the current values (Cargo.toml / _version.py)
NEW_CARGO=5.0.0-rc.1 NEW_PEP=5.0.0rc1      # final: 5.0.0 and 5.0.0
```

1. **Bump the version everywhere it is a literal** (the Cargo workspace version is the single source of
   truth; the internal `=` pins must equal it):

   ```bash
   sed -i "s/$OLD_CARGO/$NEW_CARGO/g" Cargo.toml        # [workspace.package] + the 14 `=` pins
   sed -i "s/$OLD_PEP/$NEW_PEP/g" meerschaum/config/_version.py
   cargo update --workspace                              # Cargo.lock: only the 16 workspace entries change
   git diff --stat Cargo.lock                            # expect exactly the 16 `version =` lines
   ```

   Then list the remaining literals and fix the ones that are not history (a manifest fixture's
   `meerschaum_version`, the example plugins, prose that states the current version):

   ```bash
   git grep -nF -e "$OLD_CARGO" -e "$OLD_PEP" -- . \
     ':!scripts/bench/results' ':!CHANGELOG.md' ':!docs/zensical/news/changelog.md' ':!Cargo.lock' \
     ':!scripts/check_packaging.py' ':!crates/meerschaum-py/src/build_info.rs' ':!docs/design/v5.0.0.md' \
     ':!.agents' ':!.claude' ':!docs/zensical/llms-full.txt'
   ```

   At 5.0.0-a1 this listed `tests/plugins/w7_native_fig/plugin.rs`, `crates/meerschaum-plugin/examples/native_demo.rs`
   and `native_probe.rs` (`"meerschaum_version"` literals), `CLAUDE.md` ("version `5.0.0-a1` is the single source of
   truth"), `crates/meerschaum-py/README.md` and `docs/design/v5-build.md`. `build_info.rs` and
   `check_packaging.py` only use the strings as examples and test vectors; leave them. Historical
   benchmark results keep the version they were measured with.
2. **PyPI classifier.** `pyproject.toml` carries `Development Status :: 4 - Beta` for every pre-release.
   **For 5.0.0 final switch it back to `Development Status :: 5 - Production/Stable`** (and remove the comment
   above it). Also re-read `requires-python` (`>=3.11`), the `Programming Language :: Python :: 3.x`
   classifiers (3.11 to 3.15; the smoke matrix covers 3.11 and 3.14), and `license = "Apache-2.0"` with
   `license-files` (PEP 639; `twine check` validates it).
3. **Changelog.** Edit `docs/zensical/news/changelog.md` (source of truth) in the style of the v4 section
   below, then run `./scripts/docs.sh` (it regenerates root `CHANGELOG.md`, which is committed; `./scripts/build.sh`
   also runs it). The maintainer owns the 5.0.0 wording.
4. **Local checks.**

   ```bash
   python3 scripts/check_packaging.py          # versions, console scripts, extras, lockstep pins, crates.io metadata
   scripts/rust/ci.sh package                   # cargo package --workspace --no-verify + size/content checks
   scripts/parity/check_fixture_hygiene.sh      # on any touched public file or fixture directory
   ```

   `scripts/rust/ci.sh publish-dry-run` builds every tarball against crates.io (no `[patch]`, plain sqlx)
   and mutates the shared `target/` and `$CARGO_HOME`: run it yourself when nothing else is building (tens
   of minutes), after `package`. It is the only check that the published crates compile against the
   released sqlx, so do it before step 5.5.
5. Commit on `v5` (`🦀 v5.0.0rc1: ...`), push the branch: **[needs explicit user authorization to push]**.

## 5.2 CI on the release commit

Dispatch the workflows on the branch and wait for the commit's runs. `wheels.yaml` and `docker-build.yaml` run only on
dispatch (`wheels.yaml` also on a `v5*` tag push), never on a push or pull request. `rust.yaml` and `pytest.yaml` also run on
pushes to `v5`, but a push runs only the jobs its diff needs (`scripts/ci/changes.py`; pytest runs just the sqlite and timescaledb
flavors); a **dispatch or a tag runs every job** (release-profile `build`, `parity`, `frontcrate`, `package`, the 4-OS DuckDB matrix;
pytest: all flavors), so the dispatched runs below are the release gate and the push runs are not a substitute for them. Check
`gh run list` before dispatching a duplicate:

```bash
SHA=$(git rev-parse HEAD)
gh workflow run wheels.yaml --ref v5         # builds wheels + sdist, smoke-tests them (see 5.3)
gh workflow run rust.yaml --ref v5           # everything: fmt, clippy, test (nextest), build (release), parity, frontcrate, package, check, duckdb matrix
gh workflow run pytest.yaml --ref v5 -f profile=release   # self-hosted: builds the release _core (the default `ci` profile is quick and nothing ships from it), all flavors (check no local test.sh is running)
gh run list --branch v5 --limit 6 --json databaseId,workflowName,status,conclusion,headSha
```

Every run must have `headSha == $SHA` and `conclusion == success`. The self-hosted jobs share this host's
containers, ports and `target/`: never run a local `scripts/test.sh` or `cargo` build at the same time.
`wheels.yaml` selects the prerelease subset or the full final-release matrix. Full matrix jobs: `packaging metadata`; `build (ubuntu-latest | ubuntu-24.04-arm | ubuntu-latest, musllinux x86_64 | ubuntu-latest, musllinux aarch64 | macos-latest | macos-15-intel |
windows-latest)`, which run `check_packaging.py --wheels dist` and a tag/size guard (Linux wheels must be
`manylinux_2_28` or older (musl: `musllinux_1_2` or older) and the right architecture, a wheel over 95 MiB fails, PyPI refuses 100 MiB);
`build (sdist)`; `binary (<target>)` x5 (a release `mrsm` per platform, glibc floor check, `--version` and
`show version` smoke, uploaded as `mrsm-<version>-<target>[.exe]` plus a `.sha256`); `smoke (...)` on all five runner images (Linux x86_64 and arm64, macOS arm64 and Intel, Windows) and Python 3.11 and 3.14, and `smoke (musllinux <arch>, Alpine)` (install into a fresh venv,
`mrsm` console scripts, a SQLite sync, a job round trip, the Python API smoke with no install allowed,
no leaked dependencies); the experimental `build (... cp314t ...)` and `smoke (free-threaded, py3.14t)`.
`docker-build.yaml` (buildx with the size gate) is separate, see 5.7.

## 5.3 Collect the artifacts of that exact commit

Use a fresh directory (never the repository's `dist/`, which can hold older files):

```bash
RUN=<the wheels run id>   # gh run list --workflow wheels.yaml --branch v5 --limit 3
gh run view "$RUN" --json headSha,conclusion          # headSha must equal $SHA, conclusion success
REL=$(mktemp -d "$HOME/.cache/mrsm-release-5.0.0rc1.XXXX"); mkdir "$REL/dl" "$REL/dist"
gh run download "$RUN" --dir "$REL/dl"                # one subdirectory per artifact
find "$REL/dl" -name '*.whl' -o -name '*.tar.gz' | grep -v cp314t | xargs -I{} cp {} "$REL/dist/"
ls -l "$REL/dist"
```

For a prerelease, expect two abi3 wheels (`manylinux_2_28_x86_64`, `musllinux_1_2_x86_64`)
and the sdist: three files, or four with a passing experimental cp314t wheel. Expect only the
`x86_64-unknown-linux-gnu` standalone binary and its checksum.

For a final release, expect: `meerschaum-<ver>-cp311-abi3-manylinux_2_28_x86_64.whl`, `...manylinux_2_28_aarch64.whl`,
`...musllinux_1_2_x86_64.whl`, `...musllinux_1_2_aarch64.whl`, `...macosx_*_arm64.whl`, `...macosx_*_x86_64.whl`,
`...win_amd64.whl` and `meerschaum-<ver>.tar.gz` (eight files).

The standalone binaries are in the same download (`mrsm-<ver>-<target>[.exe]` in `$REL/dl/mrsm-<ver>-<target>/`,
with a `.sha256` each). They are release assets of the GitHub pre-release (5.6), never uploaded to PyPI.
The artifact zip drops the executable bit:

```bash
mkdir "$REL/bin"; cp "$REL"/dl/mrsm-*/mrsm-* "$REL/bin/"
(cd "$REL/bin" && find . -maxdepth 1 -type f ! -name '*.sha256' -exec chmod +x {} + && cat *.sha256 > SHA256SUMS && sha256sum -c SHA256SUMS)
ls -l "$REL/bin"       # rc: 1 binary + checksum; final: 5 binaries + checksums; SHA256SUMS
```

**The free-threaded `cp314t` wheel** (`wheels-ubuntu-latest-cp314t`) is uploaded only if both its `build`
entry and `smoke (free-threaded, py3.14t)` passed on this commit; both are `continue-on-error`, so check
the jobs, not the workflow conclusion:

```bash
gh run view "$RUN" --json jobs -q '.jobs[] | select(.name | test("cp314t|free-threaded")) | [.name, .conclusion] | @tsv'
```

If both say `success`, `cp $REL/dl/wheels-ubuntu-latest-cp314t/*.whl $REL/dist/`; otherwise leave it out
and say so in the release notes.

Then verify the set:

```bash
python3 scripts/check_packaging.py                    # run from the checkout at $SHA
for w in "$REL"/dist/meerschaum-*.whl; do d=$(mktemp -d "$REL/one.XXXX"); cp "$w" "$d/" \
  && python3 scripts/check_packaging.py --wheels "$d" || break; done        # the checker wants exactly one wheel per directory
python3 -m venv "$REL/tw" && "$REL/tw/bin/pip" install -U twine && "$REL/tw/bin/twine" check "$REL"/dist/*
```

The sdist built by CI is the one to upload (`maturin sdist --out <dir>` or `python -m build --sdist`
locally give the same file; a local `python -m build` wheel is `linux_x86_64` and PyPI rejects it, so never
upload anything from `scripts/build.sh`'s `dist/`). The sdist excludes `crates/meerschaum-py/tests/**`
and has no `rust-toolchain.toml` (the manifests carry `rust-version = "1.95"`: read it from `Cargo.toml`, it moves with the toolchain). Building from it needs a
Rust toolchain and network access to crates.io **and to GitHub**: the root `Cargo.toml` `[patch.crates-io]`
points sqlx at a git revision. A pre-release `pip install` of a wheel never compiles anything.

## 5.4 PyPI **[needs explicit user authorization]**

```bash
source .env     # PYPI_TOKEN (never printed, never committed)
TWINE_USERNAME=__token__ TWINE_PASSWORD="$PYPI_TOKEN" "$REL/tw/bin/twine" upload "$REL"/dist/*
```

Optional dress rehearsal on TestPyPI first (a separate token): `twine upload --repository testpypi ...`
(the project name may be taken there). Upload is irreversible for a given filename: PyPI never lets a
file name be reused, even after a delete; a bad rc is yanked (web UI) and replaced by `rc2`. Verify from a
clean venv: `pip install --pre --no-cache-dir meerschaum==5.0.0rc1 && mrsm --version`.

## 5.5 crates.io **[needs explicit user authorization]**

Publish from Linux or macOS (the crates' `LICENSE`/`NOTICE` are symlinks). `meerschaum-py` is
`publish = false` (the extension ships in the PyPI wheel). Log in once (`cargo login`, token never
printed), then **primary command**, which publishes the 15 crates in dependency order and waits for the
index between them:

```bash
cargo publish --workspace --exclude meerschaum-py --locked
```

Fallback and recovery: `cargo publish` of a version that exists fails, so after a partial failure publish
the remaining crates one by one in this order (each level only needs the levels above it; crates in one
level can go in any order except where a dev-dependency says otherwise, below), waiting until `cargo search`/the index shows the previous level:

| Level | Crates |
|---|---|
| 1 | `meerschaum-core` |
| 2 | `meerschaum-kernel`, `meerschaum-jobs`, `meerschaum-compose` |
| 3 | `meerschaum-connector` |
| 4 | `meerschaum-connector-sql` (first: `-worker` and `-web` dev-depend on it), `meerschaum-connector-api`, `meerschaum-plugin`, `meerschaum-worker` (after `-connector-sql`) |
| 5 | `meerschaum-connector-valkey` (first: `-engine` dev-depends on it), `meerschaum-engine` (after `-connector-valkey`), `meerschaum-server` |
| 6 | `meerschaum-web` |
| 7 | `meerschaum-cli` |
| 8 | `meerschaum` (the front crate) |

```bash
cargo publish -p meerschaum-core --locked      # then the next one, ...
```

Consequences to state before publishing: (1) crates.io ignores the root `[patch.crates-io]`, so the
published SQL connector crates build against plain sqlx 0.9.x (not the pinned git revision) until sqlx 0.9.1
ships: `scripts/rust/ci.sh publish-dry-run` is the check that this compiles. (2) Publishing cannot be undone:
a version can only be yanked (`cargo yank --version 5.0.0-rc.1 <crate>`), never reused, so a fix is `rc.2`
for all crates (lockstep `=` pins). (3) `cargo install meerschaum --version 5.0.0-rc.1 --locked` builds the
`mrsm` binary from the front crate. (4) CI never runs `cargo publish`.

## 5.6 Tag and GitHub pre-release **[needs explicit user authorization]**

After PyPI and crates.io succeeded, on the exact release commit:

```bash
git tag -m "v5.0.0rc1" v5.0.0rc1 "$SHA"      # -m required: git config forces annotated tags
git push origin v5.0.0rc1                     # triggers wheels.yaml on the tag (builds only)
mapfile -t release_assets < <(find "$REL/bin" -maxdepth 1 -type f ! -name '*.sha256' -print)
gh release create v5.0.0rc1 --prerelease --verify-tag --title "v5.0.0rc1" --notes-file <notes> \
  "${release_assets[@]}"
```

The selected binaries (one for a prerelease, five for a final; built on the release commit by `wheels.yaml`) and `SHA256SUMS` are attached as the release
assets. Say in the notes that the macOS binary is not signed or notarized (Gatekeeper asks once:
`xattr -d com.apple.quarantine mrsm-...`, documented under Setup in the docs), that `cargo install meerschaum --locked` (Rust 1.95 or newer) and
`pip install meerschaum` are the supported installs, and that the Linux binaries need glibc 2.28 or newer.

Notes = the changelog notes of this version with the heading demoted to one `#`. For an rc **always**
pass `--prerelease` (and not `--latest`). Do not `git push origin --tags` (it would push unrelated tags).

## 5.7 Docker **[needs explicit user authorization, separate from 5.4 to 5.6]**

Docker images are built and pushed **from the maintainer's build host** (this repository's checkout at the
release commit) or from `docker-build.yaml`; the production VPS is no longer used for releases (it ran out
of memory on the native Rust build). Prerequisites: `docker login -u bmeares` on the build host with a
Docker Hub personal access token (Read & Write; run it in a real terminal: `docker login` refuses a non-TTY,
or use `--password-stdin` from a file only the user can read), and, for the workflow route, the same token in
the `DOCKER_PASSWORD` repository secret (`gh secret set DOCKER_PASSWORD`; `DOCKER_USERNAME` is `bmeares`).
Agents never read, print or copy the token; check the login with `docker info | grep Username`.

**Build host route (release candidates: `linux/amd64` only).** Build and load, gate, then push exactly the
images that passed, by their version-qualified tags:

```bash
git rev-parse HEAD                                   # must be the release commit ($SHA), clean tracked tree
./scripts/docker/buildx.sh                           # --load for the host's platform (linux/amd64), smoke-tested
./scripts/docker/size_gate.sh --slim-image bmeares/meerschaum:<ver>-slim --full-image bmeares/meerschaum:<ver>-full --slim-max 250 --full-max 900
# the façade smoke inside the slim image (as below), must exit 0 with install_requests []
for t in full minimal slim; do docker push bmeares/meerschaum:<ver>-$t; done
docker buildx imagetools inspect bmeares/meerschaum:latest   # unchanged by an rc
```

The build only creates its own images; it never touches other containers on the host (the build host may run
production services: never `docker system prune` or remove images you did not create). Multi-architecture
images for final releases (adding `linux/arm64`) come from the self-hosted workflow route below.

**Workflow route (self-hosted).** Dispatch `docker-build.yaml` with `publish: true` to use
repository Docker Hub secrets. The workflow derives its matrix from the Cargo version: AMD64 only
for a prerelease; AMD64 and ARM64 for a final release. All image builds run on the existing
self-hosted Linux x86_64 runners; ARM64 uses QEMU and may take much longer on its first build.

The workflow serializes Docker release jobs and runs, keeps separate named BuildKit caches for each
architecture, and limits each builder to 16 CPUs / 64 GiB RAM. Builder state persists across runs;
its cache GC is capped at 100 GB with 30 GB minimum free space. Docker configuration is private to
the job, so publishing does not change the user's login or selected builder. Do not prune the shared
Docker daemon or interfere with unrelated containers, images or builders.

Each job loads and smoke-tests its images, enforces size limits, and runs the offline facade smoke
before pushing its architecture-qualified tags. The publish job merges only the selected release
platforms and verifies every variant's manifest. These checks are mandatory; a failed facade smoke
blocks publication. Prereleases publish only version-qualified tags and never move `latest`,
`full`, `slim`, `minimal` or `api`.

```bash
# Capture the current latest digest before dispatch; verify it is unchanged for a prerelease.
docker buildx imagetools inspect bmeares/meerschaum:latest
gh workflow run docker-build.yaml --ref v5 -f publish=true -f size_gate=enforce
# Wait for every job to succeed on the exact release SHA, then inspect minimal/full/slim tags.
docker buildx imagetools inspect bmeares/meerschaum:<ver>-full
docker buildx imagetools inspect bmeares/meerschaum:latest
```

The `-amd64` / `-arm64` tags of selected architectures stay in the registry. `tests/test_docker_buildx.py`
and `tests/test_workflows_packaging.py` verify prerelease/final selection, matching smoke jobs and
manifest command plans.

## 5.8 Docs

`./scripts/docs.sh` (also run by `./scripts/build.sh`) rebuilds the site docs and regenerates root
`CHANGELOG.md`. The public docs site is still rebuilt where it is served (the docs part of the v4 step 7
below; never its Docker part, which 5.7 replaces). Commit the regenerated `CHANGELOG.md` and `requirements/*.txt`
in the release commit (5.1), not after the tag.

## 5.9 Final release (5.0.0) differences

- Version `5.0.0-rc.N` becomes `5.0.0` in Cargo/`_version.py` (and `5.0.0` PEP 440); classifier back to
  `5 - Production/Stable`; changelog final.
- The release is cut from `main`: open the PR `v5` to `main` (gitmoji title, changelog-verbatim body), CI
  green, merge, then run 5.2 to 5.6 on the merged commit. The tag is `v5.0.0` and the GitHub release is
  created without `--prerelease` (with `--latest`).
- `pip install meerschaum` (no `--pre`) and `cargo install meerschaum --locked` now resolve to 5.0.0.
- Docker: only now do `latest`, `full`, `minimal`, `slim` and `api` move (`buildx.sh --merge`, run by the `publish` job of
  `docker-build.yaml`, does it for a non-pre-release version): needs its own authorization and a passing size gate and façade smoke.
- Re-run the full gate: all flavors, parity, packaging, `publish-dry-run`.

## 5.10 Checklist (5.x)

- [ ] Versions bumped (Cargo, lockstep pins, Cargo.lock, `_version.py`, literals), classifier right for rc/final
- [ ] `check_packaging.py`, `ci.sh package`, `publish-dry-run`, fixture hygiene pass
- [ ] `wheels`, `rust`, `pytest` runs green on the release commit (`headSha` checked)
- [ ] `dist/`: rc: 2 Linux x86_64 abi3 wheels; final: 7 platform wheels (+ `cp314t` only if build and smoke passed) + sdist from that run, `twine check` ok
- [ ] **PyPI** uploaded (authorized), verified in a clean venv with `--pre`
- [ ] **crates.io** `cargo publish --workspace --exclude meerschaum-py` done (authorized), `cargo install meerschaum --locked --version ...` verified
- [ ] **Tag** `v5.0.0rc1` pushed and **GitHub pre-release** created with the selected binaries + `SHA256SUMS` attached (authorized)
- [ ] **Docker** (separately authorized), built and pushed from the build host (or `docker-build.yaml` for multi-arch): version-qualified tags only for an rc; `latest` unchanged
- [ ] Docs rebuilt

---

# Meerschaum 4.x release process (legacy: v4.x releases from `dev` to `main`)

**This section is for 4.x releases only.** 5.x uses the section above (the `v5` branch, CI-built wheels,
crates.io): do not use `scripts/build.sh` + `twine upload dist/*` for 5.x (it builds one local
`linux_x86_64` wheel that PyPI rejects).

Follow these steps in order. The finished state: version bumped in `meerschaum/config/_version.py`, PR merged into `main` with CI passing, git tag pushed, GitHub release created, package on PyPI, Docker images on DockerHub, docs rebuilt on prod.

## 1. Bump version

Edit `meerschaum/config/_version.py` (`__version__ = "X.Y.Z"`).

## 2. Update the changelog

Edit `docs/zensical/news/changelog.md` (the source of truth — root `CHANGELOG.md` is generated from it by `scripts/docs.sh`, which `scripts/build.sh` calls). Follow the existing style exactly:

- New `### vX.Y.Z` heading under the current release-cycle `##` section, above the previous version.
- One bullet per change: `- **Bolded title ending with a period.**` followed by **two trailing spaces** (markdown line break), then an indented explanation paragraph on the next line.
- Code blocks and examples are fine inside a bullet's explanation (indented to match).

Do NOT hand-edit root `CHANGELOG.md`; it's overwritten by the build.

## 3. Stage the release branch and PR

`dev` is recreated fresh from `main` for each release:

```bash
git checkout main && git pull
git branch -D dev
git checkout -b dev
# ...commit release changes (version bump, changelog, fixes)...
```

Before the final commit, run `./scripts/build.sh` — it may update `requirements/*.txt` and regenerate `CHANGELOG.md`; commit those too.

Push and open a PR from `dev` to `main`:

- **Title:** relevant gitmoji + version + short summary, e.g. `🐛 v3.4.6 Derive venvs path from root when MRSM_VENVS_DIR is unset`.
- **Body:** the changelog notes for this version verbatim, except the version heading uses one `#` instead of `###`.

## 4. CI

The PR triggers `.github/workflows/pytest.yaml` on a self-hosted runner (`~/actions-runner/run.sh` on this machine). It runs `./scripts/test.sh db` with `MRSM_TEST_FLAVORS=all` against the databases in `tests/docker-compose.yaml`.

If tests fail unexpectedly (stale DB state), reset the test databases:

```bash
cd tests/
docker compose down -v
docker compose up -d
```

Never run two test.sh invocations concurrently (shared `test_root` + API port 8989).

## 5. Merge, build, publish to PyPI

Once CI is green, mark the PR ready (release PRs often start as drafts) and merge:

```bash
gh pr ready <number>
gh pr merge <number> --squash
git checkout main && git pull
./scripts/build.sh          # cleans, builds docs, regenerates requirements, builds wheel + sdist into dist/
```

Publish (token in `.env` as `PYPI_TOKEN`):

```bash
source .env
TWINE_USERNAME=__token__ TWINE_PASSWORD="$PYPI_TOKEN" twine upload dist/*
```

## 6. Tag and GitHub release

```bash
git tag -m "vX.Y.Z" vX.Y.Z   # -m required: git config forces annotated tags
git push origin main --tags
```

Create a GitHub release for the tag (`gh release create vX.Y.Z --title "<same as PR title>" --notes-file <notes>`). The release description = the PR body = the changelog notes, with the version heading demoted to a single `#`.

## 7. Docker images and docs

Docker images are built and pushed from the build host (logged in to Docker Hub with a personal access token,
see 5.7), not from the VPS: `./scripts/docker/buildx.sh --push` from a checkout of the release tag.

The docs site is still rebuilt on the VPS that serves it:

```bash
ssh -p 2269 meerschaum@mrsm.io
cd ~/docs/Meerschaum
git pull
./scripts/build.sh                  # rebuild on Ubuntu/older Python (catches last-minute bugs); calls docs.sh which rebuilds the website docs
```

Optionally run `./scripts/setup.sh` (locally and on the VPS) to ensure latest dev packages before building.

## Checklist

- [ ] `meerschaum/config/_version.py` bumped
- [ ] `docs/zensical/news/changelog.md` updated (style matched)
- [ ] `./scripts/build.sh` run before final commit (requirements + CHANGELOG.md regenerated)
- [ ] PR `dev` → `main`, gitmoji title, changelog-verbatim body, CI green, merged
- [ ] Wheel/sdist built from `main` and uploaded to PyPI via twine
- [ ] `vX.Y.Z` tag pushed; GitHub release created with same notes
- [ ] Docker images built and pushed from the build host
- [ ] Docs rebuilt on prod VPS
