#!/usr/bin/env python3
"""Seal a fully tested release candidate, or verify its unchanged artifacts on a tag.

This script never publishes. It rejects missing platforms, development profiles,
failed checks and a candidate built from a different actual commit/tree.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sys
import tomllib

_spec = importlib.util.spec_from_file_location('ci_artifacts', Path(__file__).with_name('artifacts.py'))
artifacts = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(artifacts)
REPO = Path(__file__).resolve().parents[2]
CHECKS = {'wheels', 'rust', 'python', 'docs'}


def expected_catalog(repo: Path = REPO) -> tuple[dict, dict]:
    version = tomllib.loads((repo / 'Cargo.toml').read_text())['workspace']['package']['version']
    convert = runpy.run_path(str(REPO / 'scripts/check_packaging.py'))['cargo_to_pep440']
    matrices = runpy.run_path(str(REPO / 'scripts/ci/release_matrix.py'))['release_matrices'](version)
    required = {'sdist': {'kind': 'sdist', 'target': 'source'}}
    optional = {}
    for key, target in [('wheels', required), ('experimental_wheels', optional)]:
        for wheel in matrices[key]['include']:
            name = wheel.get('artifact', 'wheels-' + wheel['os'])
            target[name] = {'kind': 'wheel', 'target': wheel['target']}
    for binary in matrices['binaries']['include']:
        name = f'mrsm-{convert(version)}-{binary["target"]}'
        required[name] = {'kind': 'binary', 'target': binary['target'], 'program': name + binary.get('ext', '')}
    return required, optional


def _payload(directory: Path, repo: Path) -> dict:
    required, optional = expected_catalog(repo)
    present = {p.name for p in directory.iterdir() if p.is_dir()}
    if missing := required.keys() - present:
        raise artifacts.ArtifactError(f'Missing required release platforms/artifacts: {sorted(missing)}')
    if unexpected := present - (required.keys() | optional.keys()):
        raise artifacts.ArtifactError(f'Unexpected release artifacts: {sorted(unexpected)}')
    if any(p.is_file() or p.is_symlink() for p in directory.iterdir()):
        raise artifacts.ArtifactError('Release artifacts must have their own named directories.')
    files = {}
    for name in sorted(present):
        expected = (required | optional)[name]
        manifest_path = directory / name / 'provenance.json'
        manifest = artifacts.verify_manifest(manifest_path, profile='release', kind=expected['kind'],
                                              target=expected['target'], repo=repo)
        if expected['kind'] == 'binary' and expected['program'] not in manifest['files']:
            raise artifacts.ArtifactError(f'Missing standalone program for {name}')
        if expected['kind'] == 'sdist' and len([n for n in manifest['files'] if n.endswith('.tar.gz')]) != 1:
            raise artifacts.ArtifactError('Expected exactly one source distribution.')
        for path in [manifest_path, *[directory / name / file for file in manifest['files']]]:
            files[path.relative_to(directory).as_posix()] = artifacts.sha256(path)
    return files


def create(directory: Path, output: Path, checks: dict, *, repo: Path = REPO) -> dict:
    if set(checks) != CHECKS or any(result != 'success' for result in checks.values()):
        raise artifacts.ArtifactError(f'A release candidate requires every full gate to succeed: {checks}')
    record = {
        'schema': 1,
        'source': artifacts.checkout(repo),
        'checks': checks,
        'run_id': os.environ.get('GITHUB_RUN_ID', ''),
        'repository': os.environ.get('GITHUB_REPOSITORY', ''),
        'files': _payload(directory, repo),
    }
    output.write_text(json.dumps(record, sort_keys=True, indent=2) + '\n')
    return record


def verify(directory: Path, manifest: Path, *, repo: Path = REPO) -> dict:
    record = json.loads(manifest.read_text())
    if record.get('schema') != 1 or record.get('source') != artifacts.checkout(repo):
        raise artifacts.ArtifactError('Release candidate was validated at a different commit/tree.')
    if set(record.get('checks', {})) != CHECKS or any(x != 'success' for x in record['checks'].values()):
        raise artifacts.ArtifactError('Release candidate has incomplete or failed checks.')
    if record.get('files') != _payload(directory, repo):
        raise artifacts.ArtifactError('Release artifacts differ from the fully tested candidate.')
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('create', 'verify'))
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--checks-json', default='{}')
    args = parser.parse_args()
    try:
        if args.command == 'create':
            create(args.artifacts, args.manifest, json.loads(args.checks_json))
        else:
            verify(args.artifacts, args.manifest)
    except (artifacts.ArtifactError, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f'release gate: {exc}\n')
    print('Release candidate verified; nothing published.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
