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


### 5.1: the filesystem sandbox. The worker's host program runs this prelude (the embedded
### `_mrsm_sandbox()`), which loads `team_sandbox.py` by its path and restricts the process before
### the package is imported; the probe then does what a team's plugin code might.
SANDBOX_PRELUDE = r'''
import importlib.util, os, sys
spec = importlib.util.find_spec('meerschaum')
path = os.path.join(os.path.dirname(spec.origin), '_internal', 'team_sandbox.py')
boot_spec = importlib.util.spec_from_file_location('_mrsm_team_sandbox_boot', path)
module = importlib.util.module_from_spec(boot_spec)
boot_spec.loader.exec_module(module)
module.boot()
'''

SANDBOX_PROBE = r'''
import json, os, socket, ssl, sys, tempfile, threading
results = {}
def attempt(label, fn):
    try:
        value = fn()
        results[label] = 'ok' if value is None else value
    except PermissionError:
        results[label] = 'denied'
    except Exception as e:
        results[label] = f'error: {type(e).__name__}: {e}'
def read(path):
    with open(path, 'rb') as f:
        f.read(1)
attempt('server config canary', lambda: read(os.environ['TPR_CANARY']))
attempt('server config listing', lambda: os.listdir(os.environ['TPR_CONFIG_DIR']) and None)
attempt('other team job', lambda: read(os.environ['TPR_OTHER_JOB']))
attempt('other team plugin', lambda: read(os.environ['TPR_OTHER_PLUGIN']))
attempt('parent environ', lambda: read(f"/proc/{os.getppid()}/environ"))
attempt('write root', lambda: open(os.path.join(os.environ['MRSM_ROOT_DIR'], 'planted'), 'w').close())
import meerschaum as mrsm
from meerschaum._internal import team_scope, team_sandbox
results['report'] = team_sandbox.report()
results['hello'] = (team_scope.hello_report() or {}).get('sandbox')
def tmp():
    with tempfile.NamedTemporaryFile('w+') as f:
        f.write('x'); f.flush(); f.seek(0)
        assert f.read() == 'x'
        assert f.name.startswith(os.environ['TMPDIR'])
attempt('temporary file', tmp)
def threads():
    out = []
    ts = [threading.Thread(target=lambda: out.append(1)) for _ in range(4)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert len(out) == 4
attempt('threads', threads)
def processes():
    import multiprocessing as mp
    with mp.get_context('spawn').Pool(2) as pool:
        assert pool.map(abs, [-1, -2]) == [1, 2]
attempt('multiprocessing', processes)
attempt('dns localhost', lambda: socket.getaddrinfo('localhost', 80) and None)
attempt('tls context', lambda: ssl.create_default_context().cert_store_stats() and None)
def plugin():
    from meerschaum.plugins import import_plugins
    probe = import_plugins('tpr_probe')
    assert probe.PROBE == 1
attempt('own plugin', plugin)
def requirement():
    from meerschaum.utils.packages import attempt_import
    dep = attempt_import('tpr_sandbox_dep', venv='tpr_probe', install=False, lazy=False)
    assert dep.VALUE == 42
attempt('plugin requirement from its venv', requirement)
attempt('pandas', lambda: __import__('pandas') and None)
def memory_sync():
    import pandas as pd
    conn = mrsm.get_connector('sql', 'tpr_memory', flavor='sqlite', database=':memory:')
    pipe = mrsm.Pipe('tpr', 'memory', instance=conn)
    success, msg = pipe.sync(pd.DataFrame({'a': [1, 2]}))
    assert success, msg
    assert len(pipe.get_data()) == 2
attempt('in-memory sqlite sync', memory_sync)
print('JSON:' + json.dumps(results, default=str))
'''


def _venv_site_packages(venv_dir):
    lib = venv_dir / 'lib'
    return next(lib.glob('python*/site-packages'))


def test_a_team_worker_is_confined_to_its_sandbox(team_root, tmp_path):
    from meerschaum import _core
    status = json.loads(_core.team_sandbox_status())
    if not status['support'].get('Available'):
        pytest.skip(f"No Landlock on this system: {status['describe']}")
    root, plugins = team_root
    (root / 'config' / 'tpr_canary.txt').write_text('server secret')
    (plugins / 'tpr_probe').mkdir()
    (plugins / 'tpr_probe' / '__init__.py').write_text("PROBE = 1\nrequired = ['tpr_sandbox_dep']\n")
    (plugins / 'tpr_other').mkdir()
    other_plugin = plugins / 'tpr_other' / '__init__.py'
    other_plugin.write_text("OTHER = 1\n")
    other_job = root / 'jobs' / 'drylands-job' / 'properties.json'
    other_job.parent.mkdir(parents=True)
    other_job.write_text('{"team": "drylands"}')
    venv_dir = root / 'venvs' / 'tpr_probe'
    subprocess.run([sys.executable, '-m', 'venv', '--without-pip', str(venv_dir)], check=True)
    (_venv_site_packages(venv_dir) / 'tpr_sandbox_dep.py').write_text('VALUE = 42\n')
    launching = {
        'PATH': os.environ.get('PATH', ''),
        'HOME': os.environ.get('HOME', ''),
        'PYTHONPATH': str(REPO),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_DATAFRAME': 'pandas',
    }
    env = _core.access_team_process_env(
        str(root), 'wetlands', ['sql:wetlands'], launching, plugins=['tpr_probe'], sandbox=True,
    )
    assert 'MRSM_TEAM_SANDBOX' in env
    env.update({
        'TPR_CANARY': str(root / 'config' / 'tpr_canary.txt'),
        'TPR_CONFIG_DIR': str(root / 'config'),
        'TPR_OTHER_JOB': str(other_job),
        'TPR_OTHER_PLUGIN': str(other_plugin),
    })
    result = subprocess.run(
        [sys.executable, '-P', '-c', SANDBOX_PRELUDE + SANDBOX_PROBE],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        cwd=env['TMPDIR'],
    )
    assert result.returncode == 0, result.stderr[-6000:]
    line = [line for line in result.stdout.splitlines() if line.startswith('JSON:')][-1]
    out = json.loads(line[len('JSON:'):])
    report = out['report']
    assert report['enforced'], report
    assert report['canary'] == 'denied', report
    assert out['hello'] == report
    for label in (
        'server config canary', 'server config listing', 'other team job',
        'other team plugin', 'parent environ', 'write root',
    ):
        assert out[label] == 'denied', (label, json.dumps(out, indent=1))
    for label in (
        'temporary file', 'threads', 'multiprocessing', 'dns localhost', 'tls context',
        'own plugin', 'plugin requirement from its venv', 'pandas', 'in-memory sqlite sync',
    ):
        assert out[label] == 'ok', (label, json.dumps(out, indent=1))
    assert not (root / 'planted').exists()
