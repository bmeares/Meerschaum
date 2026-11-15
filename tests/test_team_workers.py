#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Team-scoped workers (5.1), the Python package's side: a process started with the environment the
API server gives an isolated team's worker (built by the extension) holds only the team's
connectors, and the proof it sends in its handshake says so. The Rust side (a worker that cannot
prove it is stopped before running any team code) is tested in `meerschaum-worker`.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

PROBE = r'''
import json
import meerschaum as mrsm
from meerschaum import _core
from meerschaum._internal import team_scope
raw = _core.get_config('meerschaum', 'connectors', 'sql', warn=False) or {}
out = {
    'report': team_scope.hello_report(),
    'defined': sorted(label for label, attrs in raw.items() if attrs),
}
for keys in ('sql:main', 'sql:drylands', 'sql:wetlands'):
    try:
        conn = mrsm.get_connector(keys)
        out[keys] = 'built' if conn is not None else 'undefined'
    except Exception as e:
        out[keys] = type(e).__name__
print('JSON:' + json.dumps(out))
'''


@pytest.fixture
def team_root(tmp_path):
    root = tmp_path / 'root'
    (root / 'config').mkdir(parents=True)
    config = root / 'config' / 'meerschaum.yaml'
    config.write_text(
        "connectors:\n"
        "  sql:\n"
        "    main:\n"
        f"      flavor: sqlite\n      database: {root / 'main.db'}\n      password: server-pw\n"
        "    wetlands:\n"
        f"      flavor: sqlite\n      database: {root / 'wetlands.db'}\n"
        "      password: w-pw\n      teams: [wetlands]\n"
        "    drylands:\n"
        f"      flavor: sqlite\n      database: {root / 'drylands.db'}\n"
        "      password: d-pw\n      teams: [drylands]\n"
    )
    config.chmod(0o600)
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    return root, plugins


def test_a_team_worker_process_holds_only_the_teams_connectors(team_root):
    from meerschaum import _core
    root, plugins = team_root
    launching = {
        'PATH': os.environ.get('PATH', ''),
        'HOME': os.environ.get('HOME', ''),
        'PYTHONPATH': str(REPO),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_SQL_ENVDB': 'postgresql://u:env-pw@db/env',
        'TPR_SENTINEL_CLOUD_KEY': 'cloud-secret',
    }
    env = _core.access_team_process_env(str(root), 'wetlands', ['sql:wetlands'], launching)
    assert env['MRSM_TEAM_SCOPE'] == 'wetlands'
    assert 'MRSM_SQL_ENVDB' not in env
    assert 'TPR_SENTINEL_CLOUD_KEY' not in env
    result = subprocess.run(
        [sys.executable, '-c', PROBE],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        cwd=str(root),
    )
    assert result.returncode == 0, result.stderr[-6000:]
    line = [line for line in result.stdout.splitlines() if line.startswith('JSON:')][-1]
    out = json.loads(line[len('JSON:'):])
    assert out['defined'] == ['wetlands'], out
    assert out['report']['team'] == 'wetlands'
    assert out['report']['connectors'] == ['sql:wetlands']
    assert out['report']['fingerprint'] == env['MRSM_TEAM_FINGERPRINT']
    assert out['sql:wetlands'] == 'built'
    for other in ('sql:main', 'sql:drylands'):
        assert out[other] != 'built', out


def test_the_handshake_announces_the_team_capability():
    from meerschaum._internal import team_scope
    assert team_scope.CAPABILITY == 'team_scope'
    ### Outside a team's process: no proof to give.
    assert os.environ.get('MRSM_TEAM_SCOPE') or team_scope.hello_report() is None
