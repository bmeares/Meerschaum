#!/usr/bin/env python3
"""Hand the pytest wheel and CLI from the artifacts job to the test jobs through a host directory.

The self-hosted runners of one host share a cache directory (`$MRSM_CI_CACHE`). With the repository
variable `CI_LOCAL_ARTIFACTS` set to `true`, `.github/workflows/pytest.yaml` stores the verified test
artifacts there instead of sending them to GitHub and back over the host's slow uplink:

    python scripts/ci/local_artifacts.py publish --source "$CI_ARTIFACT_DIR" --store DIR
    python scripts/ci/local_artifacts.py fetch --store DIR --run RUN --output "$PYTEST_ARTIFACT_DIR"

`publish` verifies each kind's provenance (`artifacts.py`: commit and tree, profile, target, every
payload checksum, no unverified file), copies the kinds into `<store>/<run>.tmp-*` and renames that
to `<store>/<run>` (`<run>` = run ID and attempt), so a reader never sees a partial directory. It then
removes run directories older than two days. `fetch` copies one run's directory into the job's
workspace after checking that each manifest names this repository's run; `prepare_pytest.sh` then
verifies the copies exactly as it verifies downloaded GitHub artifacts. Reused artifacts
(`artifacts_run_id`) and release profiles always use GitHub artifacts.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.ci import artifacts  # noqa: E402

# The kinds the tests consume: the directory name, the artifact kind and whether every run has it.
KINDS = (('wheel', 'wheel', True), ('binary', 'binary', True), ('console-native-fixture', None, False))
MAX_AGE_SECONDS = 2 * 24 * 3600


def _run_key(run_id: str, attempt: str) -> str:
    if not (run_id.isdigit() and attempt.isdigit()):
        raise ValueError('The run ID and attempt must be numbers.')
    return f'{run_id}-{attempt}'


def _check_run(manifest_path: Path, run_id: str) -> None:
    manifest = json.loads(manifest_path.read_text())
    actions = manifest.get('actions', {})
    if (actions.get('run_id') != run_id
            or actions.get('repository') != os.environ.get('GITHUB_REPOSITORY', actions.get('repository'))):
        raise ValueError(f'{manifest_path} was built by another workflow run.')


def publish(source: Path, store: Path, run_id: str, attempt: str, *, profile: str,
            now: float | None = None) -> Path:
    key = _run_key(run_id, attempt)
    for name, kind, required in KINDS:
        manifest = source / name / 'provenance.json'
        if not manifest.exists():
            if required:
                raise ValueError(f'The {name} artifact is missing from {source}.')
            continue
        # The console fixture is bound to the binary's manifest; prepare_pytest.sh verifies that pair.
        if kind is not None:
            artifacts.verify_manifest(manifest, kind=kind, profile=profile)
            _check_run(manifest, run_id)
    store.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination = store / key
    if destination.exists():
        raise ValueError(f'{destination} already exists.')
    staging = Path(tempfile.mkdtemp(prefix=f'{key}.tmp-', dir=store))
    try:
        for name, _, _ in KINDS:
            if (source / name).is_dir():
                shutil.copytree(source / name, staging / name, symlinks=True)
        os.rename(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    prune(store, keep=destination, now=now)
    return destination


def prune(store: Path, *, keep: Path, now: float | None = None) -> list[Path]:
    """Remove run directories (and abandoned staging directories) older than two days."""
    removed = []
    cutoff = (time.time() if now is None else now) - MAX_AGE_SECONDS
    for entry in store.iterdir():
        if entry == keep or not entry.is_dir() or entry.is_symlink():
            continue
        if not entry.name.split('.tmp-', 1)[0].replace('-', '').isdigit():
            continue
        if entry.stat().st_mtime < cutoff:
            shutil.rmtree(entry, ignore_errors=True)
            removed.append(entry)
    return removed


def fetch(store: Path, run_id: str, attempt: str, output: Path) -> Path:
    source = store / _run_key(run_id, attempt)
    if not source.is_dir() or source.is_symlink():
        raise ValueError(f'No local test artifacts for this run at {source}.')
    if output.exists() and any(output.iterdir()):
        raise ValueError(f'{output} is not empty.')
    output.mkdir(parents=True, exist_ok=True)
    for name, kind, required in KINDS:
        if not (source / name).is_dir():
            if required:
                raise ValueError(f'The {name} artifact is missing from {source}.')
            continue
        if kind is not None:
            _check_run(source / name / 'provenance.json', run_id)
        shutil.copytree(source / name, output / name, symlinks=True)
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('publish', 'fetch'):
        command = sub.add_parser(name)
        command.add_argument('--store', type=Path, required=True)
        command.add_argument('--run', default=os.environ.get('GITHUB_RUN_ID', ''))
        command.add_argument('--attempt', default=os.environ.get('GITHUB_RUN_ATTEMPT', '1'))
    sub.choices['publish'].add_argument('--source', type=Path, required=True)
    sub.choices['publish'].add_argument('--profile', default='ci')
    sub.choices['fetch'].add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'publish':
            path = publish(args.source, args.store, args.run, args.attempt, profile=args.profile)
            output = os.environ.get('GITHUB_OUTPUT')
            if output:
                with open(output, 'a', encoding='utf-8') as handle:
                    handle.write(f'attempt={args.attempt}\n')
        else:
            path = fetch(args.store, args.run, args.attempt, args.output)
    except (ValueError, OSError, artifacts.ArtifactError) as error:
        print(f'local_artifacts.py: {error}', file=sys.stderr)
        return 1
    print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
