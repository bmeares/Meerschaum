---
name: release
description: Meerschaum release process — bump version, update changelog, stage dev→main PR, run CI, publish to PyPI, tag, GitHub release, build/push Docker images, rebuild docs on prod VPS. Use when cutting a new Meerschaum release or preparing a release PR.
---

# Meerschaum Release Process

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
