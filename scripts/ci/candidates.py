#!/usr/bin/env python3
"""Fetch trusted Actions candidates and reuse full checks for an identical PR tree."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

spec = importlib.util.spec_from_file_location('candidate_release_gate', Path(__file__).with_name('release_gate.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def api(path: str):
    return json.loads(subprocess.check_output(['gh', 'api', path], text=True))


def repository() -> str:
    name = os.environ['GITHUB_REPOSITORY']
    if not re.fullmatch(r'[\w.-]+/[\w.-]+', name):
        raise ValueError('Invalid Actions repository.')
    return name


def trusted_run(run_id: str, workflow: str, *, commit: str | None = None) -> dict:
    if not str(run_id).isdigit():
        raise ValueError('Use a numeric Actions run ID.')
    repo = repository()
    run = api(f'repos/{repo}/actions/runs/{run_id}')
    if (run['conclusion'] != 'success' or run['event'] != 'workflow_dispatch'
            or run['path'].split('@')[0] != f'.github/workflows/{workflow}'
            or run.get('head_repository', {}).get('full_name') != repo
            or (commit is not None and run['head_sha'] != commit)):
        raise ValueError('The candidate must be a successful trusted run at the requested source.')
    return run


def available(run_id: str) -> set[str]:
    names = set()
    page = 1
    while True:
        rows = api(f'repos/{repository()}/actions/runs/{run_id}/artifacts?per_page=100&page={page}')['artifacts']
        names.update(row['name'] for row in rows if not row['expired'])
        if len(rows) < 100:
            return names
        page += 1


def download(run_id: str, output: Path, *names: str) -> None:
    output.mkdir(parents=True, exist_ok=True)
    command = ['gh', 'run', 'download', str(run_id), '--repo', repository(), '--dir', str(output)]
    for name in names:
        command.extend(['--name', name])
    subprocess.run(command, check=True)


def reuse(head: str, output: Path) -> dict | None:
    if not re.fullmatch('[0-9a-f]{40}', head):
        raise ValueError('Use the full PR head SHA.')
    runs = api(f'repos/{repository()}/actions/workflows/release.yaml/runs?head_sha={head}'
               '&event=workflow_dispatch&status=success&per_page=100')['workflow_runs']
    for run in runs:
        try:
            trusted_run(str(run['id']), 'release.yaml', commit=head)
            names = available(str(run['id']))
            required, _ = gate.expected_catalog()
            if not (required.keys() | {'release-candidate-manifest'}) <= names:
                continue
            directory = output / str(run['id'])
            download(str(run['id']), directory, 'release-candidate-manifest')
            manifest = directory / 'release-candidate.json'
            record = gate.equivalent(manifest)
            if (record.get('run_id') != str(run['id']) or record.get('repository') != repository()
                    or record['source']['commit'] != head):
                continue
            return record
        except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
            continue
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--head-sha', required=True)
    parser.add_argument('--github-output', type=Path, required=True)
    args = parser.parse_args()
    # An unavailable candidate falls back to the ordinary checks, never to a skip.
    try:
        with tempfile.TemporaryDirectory(prefix='mrsm-candidate-') as temp:
            record = reuse(args.head_sha, Path(temp))
    except (ValueError, OSError, KeyError, subprocess.SubprocessError):
        record = None
    with args.github_output.open('a') as stream:
        stream.write(f'reused={str(record is not None).lower()}\n')
        stream.write(f'candidate_run_id={record["run_id"] if record else ""}\n')
    if record:
        url = f'https://github.com/{repository()}/actions/runs/{record["run_id"]}'
        message = f'Full release checks reused for identical tree {record["source"]["tree"]}: {url}'
    else:
        message = 'No complete, current candidate matches this merge tree; ordinary checks run.'
    print(message)
    if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(summary).open('a') as stream:
            stream.write(message + '\n')


if __name__ == '__main__':
    main()
