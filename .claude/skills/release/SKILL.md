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

Platforms of the wheels (`.github/workflows/wheels.yaml`, GitHub-hosted runners only): Linux x86_64 and
Linux arm64 (`manylinux_2_28`, native `ubuntu-24.04-arm`), macOS arm64, Windows x86_64, all one
`cp311-abi3` wheel per platform, plus one experimental free-threaded `cp314t` Linux x86_64 wheel and the
sdist. Standalone `mrsm` binaries (the `cargo install meerschaum` program) are built for the same four targets
(`x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` in `manylinux_2_28` containers, so the glibc floor is 2.28;
`aarch64-apple-darwin`; `x86_64-pc-windows-msvc`) as GitHub release assets, not on PyPI. **Deferred (not built):** macOS x86_64, musllinux, Windows arm64, Linux arm64 free-threaded. Docker
images are `linux/amd64` only (`scripts/config.sh` `platforms`); arm64 images are deferred.

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

Dispatch the workflows on the branch and wait for the commit's runs (`wheels.yaml` only runs on dispatch, a
`v5*` tag push or a pull request touching packaging files; `rust.yaml` also runs on every push to `v5`, so
check `gh run list` before dispatching a duplicate):

```bash
SHA=$(git rev-parse HEAD)
gh workflow run wheels.yaml --ref v5         # builds wheels + sdist, smoke-tests them (see 5.3)
gh workflow run rust.yaml --ref v5           # self-hosted: fmt, clippy, test, build, parity, frontcrate, package
gh workflow run pytest.yaml --ref v5         # self-hosted: builds _core, runs the flavors (check no local test.sh is running)
gh run list --branch v5 --limit 6 --json databaseId,workflowName,status,conclusion,headSha
```

Every run must have `headSha == $SHA` and `conclusion == success`. The self-hosted jobs share this host's
containers, ports and `target/`: never run a local `scripts/test.sh` or `cargo` build at the same time.
`wheels.yaml` jobs: `packaging metadata`; `build (ubuntu-latest | ubuntu-24.04-arm | macos-latest |
windows-latest)`, which run `check_packaging.py --wheels dist` and a tag/size guard (Linux wheels must be
`manylinux_2_28` or older and the right architecture, a wheel over 95 MiB fails, PyPI refuses 100 MiB);
`build (sdist)`; `binary (<target>)` x4 (a release `mrsm` per platform, glibc floor check, `--version` and
`show version` smoke, uploaded as `mrsm-<version>-<target>[.exe]` plus a `.sha256`); `smoke (...)` on all four OSes and Python 3.11 and 3.14 (install into a fresh venv,
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

Expected: `meerschaum-<ver>-cp311-abi3-manylinux_2_28_x86_64.whl`, `...manylinux_2_28_aarch64.whl`,
`...macosx_*_arm64.whl`, `...win_amd64.whl` and `meerschaum-<ver>.tar.gz` (five files).

The standalone binaries are in the same download (`mrsm-<ver>-<target>[.exe]` in `$REL/dl/mrsm-<ver>-<target>/`,
with a `.sha256` each). They are release assets of the GitHub pre-release (5.6), never uploaded to PyPI.
The artifact zip drops the executable bit:

```bash
mkdir "$REL/bin"; cp "$REL"/dl/mrsm-*/mrsm-* "$REL/bin/"
(cd "$REL/bin" && chmod +x mrsm-*-linux-gnu mrsm-*-apple-darwin && cat *.sha256 > SHA256SUMS && sha256sum -c SHA256SUMS)
ls -l "$REL/bin"       # 4 binaries, 4 .sha256, SHA256SUMS
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
gh release create v5.0.0rc1 --prerelease --verify-tag --title "v5.0.0rc1" --notes-file <notes> \
  "$REL"/bin/mrsm-*-linux-gnu "$REL"/bin/mrsm-*-apple-darwin "$REL"/bin/mrsm-*.exe "$REL"/bin/SHA256SUMS
```

The four binaries (built on the release commit by `wheels.yaml`) and `SHA256SUMS` are attached as the release
assets. Say in the notes that the macOS binary is not signed or notarized (Gatekeeper asks once:
`xattr -d com.apple.quarantine mrsm-...`), that `cargo install meerschaum --locked` (Rust 1.95 or newer) and
`pip install meerschaum` are the supported installs, and that the Linux binaries need glibc 2.28 or newer.

Notes = the changelog notes of this version with the heading demoted to one `#`. For an rc **always**
pass `--prerelease` (and not `--latest`). Do not `git push origin --tags` (it would push unrelated tags).

## 5.7 Docker **[needs explicit user authorization, separate from 5.4 to 5.6]**

Locally or from `docker-build.yaml` (`workflow_dispatch`, `publish: true` pushes with the repository's
DockerHub secrets). `scripts/docker/buildx.sh` builds `minimal`, `full` and `slim` for `linux/amd64`; **a
pre-release (`rc`, `aN`, `bN`, `dev`) gets only version-qualified tags** (`bmeares/meerschaum:5.0.0rc1-minimal`,
`-full`, `-slim`) and never moves `latest`, `full`, `slim`, `minimal` or `api`. Before pushing:

```bash
./scripts/docker/buildx.sh                         # --load, no push
./scripts/docker/size_gate.sh --slim-image bmeares/meerschaum:5.0.0rc1-slim --full-image bmeares/meerschaum:5.0.0rc1-full --slim-max 250 --full-max 900
# the façade smoke inside the slim image (no network, read-only, no implicit installs, scratch root/plugins)
docker run --rm --network none --read-only --tmpfs /scratch -e MRSM_NO_AUTO_INSTALL=1 \
  -e MRSM_ROOT_DIR=/scratch/root -e MRSM_PLUGINS_DIR=/scratch/plugins \
  -v "$PWD/scripts/docker/smoke_facade.py:/smoke_facade.py:ro" --entrypoint sh bmeares/meerschaum:5.0.0rc1-slim \
  -c 'mkdir -p /scratch/root /scratch/plugins && exec python -I /smoke_facade.py'      # must exit 0, install_requests []
./scripts/docker/buildx.sh --push                  # the authorized step
docker buildx imagetools inspect bmeares/meerschaum:latest   # still the previous release: unchanged
```

Any failed check of `smoke_facade.py` is a release blocker. Multi-arch images (`linux/arm64`) are deferred
(`scripts/config.sh`).

## 5.8 Docs

`./scripts/docs.sh` (also run by `./scripts/build.sh`) rebuilds the site docs and regenerates root
`CHANGELOG.md`; on the production VPS the docs rebuild is the v4 step 7 below (docs only: skip its Docker
part for an rc unless 5.7 was authorized). Commit the regenerated `CHANGELOG.md` and `requirements/*.txt`
in the release commit (5.1), not after the tag.

## 5.9 Final release (5.0.0) differences

- Version `5.0.0-rc.N` becomes `5.0.0` in Cargo/`_version.py` (and `5.0.0` PEP 440); classifier back to
  `5 - Production/Stable`; changelog final.
- The release is cut from `main`: open the PR `v5` to `main` (gitmoji title, changelog-verbatim body), CI
  green, merge, then run 5.2 to 5.6 on the merged commit. The tag is `v5.0.0` and the GitHub release is
  created without `--prerelease` (with `--latest`).
- `pip install meerschaum` (no `--pre`) and `cargo install meerschaum --locked` now resolve to 5.0.0.
- Docker: only now do `latest`, `full`, `minimal`, `slim` and `api` move (`buildx.sh --push` does it for a
  non-pre-release version): needs its own authorization and a passing size gate and façade smoke.
- Re-run the full gate: all flavors, parity, packaging, `publish-dry-run`.

## 5.10 Checklist (5.x)

- [ ] Versions bumped (Cargo, lockstep pins, Cargo.lock, `_version.py`, literals), classifier right for rc/final
- [ ] `check_packaging.py`, `ci.sh package`, `publish-dry-run`, fixture hygiene pass
- [ ] `wheels`, `rust`, `pytest` runs green on the release commit (`headSha` checked)
- [ ] `dist/`: 4 platform wheels (+ `cp314t` only if build and smoke passed) + sdist from that run, `twine check` ok
- [ ] **PyPI** uploaded (authorized), verified in a clean venv with `--pre`
- [ ] **crates.io** `cargo publish --workspace --exclude meerschaum-py` done (authorized), `cargo install meerschaum --locked --version ...` verified
- [ ] **Tag** `v5.0.0rc1` pushed and **GitHub pre-release** created with the 4 binaries + `SHA256SUMS` attached (authorized)
- [ ] **Docker** (separately authorized): version-qualified tags only for an rc; `latest` unchanged
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

## 7. Prod VPS: Docker images and docs

```bash
ssh -p 2269 meerschaum@mrsm.io
cd ~/docs/Meerschaum
git pull
./scripts/docker/buildx.sh --push   # build multi-arch Docker images and push to DockerHub
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
- [ ] Docker images built and pushed from prod VPS
- [ ] Docs rebuilt on prod VPS
