#!/usr/bin/env python3
"""Select bounded Python test lanes; full/nightly/release validation always runs every lane."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tomllib

REPO = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('ci_changes', Path(__file__).with_name('changes.py'))
changes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(changes)

# One shared worker budget, weighted by the measured Oracle-heavy call time.
LANES = {name: lane['workers'] for name, lane in runpy.run_path(str(Path(__file__).with_name('shards.py')))['SHARDS'].items()}
BACKENDS = {
    'postgres': ('postgres', 'timescale', 'postgis', 'citus', 'cockroach'),
    'mysql': ('mysql', 'mariadb'),
    'mssql-valkey': ('mssql', 'valkey', 'redis'),
    'oracle': ('oracle',),
}


def plan(files: list[str], *, full: bool = False, requested: str = '') -> dict:
    """Keep ordinary PRs on core/Postgres plus affected drivers; unknown inputs fail closed."""
    flags = changes.classify(files)
    enabled = full or flags['pytest']
    selected = set(LANES) if full else ({'core', 'postgres'} if enabled else set())
    for path in files:
        normalized = path.removeprefix('./')
        if normalized.startswith(('.github/workflows/', 'scripts/ci/', 'scripts/parity/')) or normalized in (
            'Cargo.toml', 'Cargo.lock', 'pyproject.toml', 'rust-toolchain.toml',
            'scripts/test.sh', 'scripts/mrsm.sh', 'tests/docker-compose.yaml', 'tests/conftest.py',
            'tests/connectors.py',
        ):
            selected.update(LANES)
        elif not any(test(normalized) for test in (
            changes._is_ignored, changes._is_rust, changes._is_rust_test, changes._is_pytest,
            changes._is_parity, changes._is_frontcrate, changes._is_package, changes._is_duckdb,
        )):
            selected.update(LANES)
        for lane, names in BACKENDS.items():
            if any(name in normalized.lower() for name in names) and not changes._is_ignored(normalized):
                selected.add(lane)
    if requested and requested != 'all':
        explicit = set(requested.split(','))
        if not explicit <= LANES.keys():
            raise ValueError(f'Unknown shard(s): {sorted(explicit - LANES.keys())}')
        if full and explicit != set(LANES):
            raise ValueError('Full validation must include every test lane.')
        selected = explicit
        enabled = True
    matrix = {'include': [{'shard': lane, 'workers': count} for lane, count in LANES.items() if lane in selected]}
    # An empty matrix is rejected by Actions even when the consumer is skipped.
    if not matrix['include']:
        matrix['include'] = [{'shard': 'core', 'workers': LANES['core']}]
    return {'pytest': enabled, 'full': full, 'matrix': matrix}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event', default=os.environ.get('GITHUB_EVENT_NAME', ''))
    parser.add_argument('--before', default='')
    parser.add_argument('--after', default='HEAD')
    parser.add_argument('--base', default='')
    parser.add_argument('--full', action='store_true')
    parser.add_argument('--shards', default='')
    parser.add_argument('--files', help='Read paths from a file, or - for stdin')
    parser.add_argument('--profile', choices=('ci', 'release'), default='ci')
    args = parser.parse_args()
    full = args.full or args.event in ('schedule', 'workflow_call')
    try:
        if args.files:
            files = (sys.stdin.read() if args.files == '-' else Path(args.files).read_text()).splitlines()
        elif args.event in ('workflow_dispatch', 'schedule', 'workflow_call'):
            files = []
        else:
            files = changes.changed_files(args.base if args.event == 'pull_request' else args.before, args.after)
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f'Cannot determine the diff ({exc}); choosing full validation.', file=sys.stderr)
        files, full = [], True
    # Manual runs default to all lanes; a specific lane is useful for diagnosis.
    if args.event == 'workflow_dispatch' and args.shards in ('', 'all'):
        full = True
    try:
        result = plan(files, full=full, requested=args.shards)
    except ValueError as exc:
        parser.exit(2, f'{exc}\n')
    version = tomllib.loads((REPO / 'Cargo.toml').read_text())['workspace']['package']['version']
    convert = runpy.run_path(str(REPO / 'scripts/check_packaging.py'))['cargo_to_pep440']
    result.update(
        wheel_artifact='wheels-ubuntu-latest' if args.profile == 'release' else 'pytest-wheel',
        binary_artifact=f'mrsm-{convert(version)}-x86_64-unknown-linux-gnu' if args.profile == 'release' else 'pytest-binary',
        binary_profile='release' if args.profile == 'release' else 'debug',
    )
    lines = [f'{name}={json.dumps(value, separators=(",", ":")) if not isinstance(value, str) else value}'
             for name, value in result.items()]
    print('\n'.join(lines))
    if output := os.environ.get('GITHUB_OUTPUT'):
        with open(output, 'a') as stream:
            stream.write('\n'.join(lines) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
