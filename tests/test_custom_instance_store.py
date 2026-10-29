"""Synthetic custom-instance storage survives primary work and telemetry in separate processes."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time


def test_fixture_primary_and_telemetry_writers_keep_every_row_and_valid_json(tmp_path):
    root = tmp_path / 'root'
    plugins = root / 'plugins'
    plugins.mkdir(parents=True)
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_')
        and key not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME')
    }
    env.update(
        MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins),
        MRSM_NO_AUTO_INSTALL='1', MRSM_NO_UPDATE_CHECK='1',
        PYTHONPATH=str(Path(__file__).resolve().parents[1]), POLARS_MAX_THREADS='1',
    )
    worker = r'''
import json, sys, time
from pathlib import Path
from types import SimpleNamespace
from tests.plugins import w15l_custom as fixture
from meerschaum.config import paths
root = Path(paths.ROOT_DIR_PATH)
role = sys.argv[1]
host = SimpleNamespace(label='concurrent')
pipe = SimpleNamespace(connector_keys='fixture' if role == 'primary' else 'mrsm',
    metric_key='primary' if role == 'primary' else 'engine_metrics', location_key=None,
    _attributes={'parameters': {}})
save = fixture._save
def slow_save(label, store):
    time.sleep(0.002)
    save(label, store)
fixture._save = slow_save
(root / (role + '.ready')).touch()
deadline = time.monotonic() + 20
while not (root / 'go').exists():
    assert time.monotonic() < deadline
    time.sleep(0.005)
assert fixture.W15lmemConnector.register_pipe(host, pipe)[0]
class Frame:
    def __init__(self, index): self.index = index
    def to_json(self, **kwargs):
        return json.dumps([{'id': self.index, 'source': role,
            'payload': 'x' * (3000 if self.index % 2 else 50)}])
for index in range(20):
    assert fixture.W15lmemConnector.sync_pipe(host, pipe, Frame(index))[0]
'''
    children = []
    try:
        for role in ('primary', 'telemetry'):
            children.append(subprocess.Popen(
                [sys.executable, '-c', worker, role], env=env, cwd=root,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        deadline = time.monotonic() + 30
        while not all((root / (role + '.ready')).exists() for role in ('primary', 'telemetry')):
            assert time.monotonic() < deadline, 'Fixture writer failed to initialize'
            assert all(child.poll() is None for child in children)
            time.sleep(0.005)
        (root / 'go').touch()
        store_path = root / 'w15lmem_concurrent.json'
        while any(child.poll() is None for child in children):
            assert time.monotonic() < deadline, 'Fixture writers did not finish'
            if store_path.exists():
                json.loads(store_path.read_text())
            time.sleep(0.002)
        for child in children:
            stdout, stderr = child.communicate(timeout=5)
            assert child.returncode == 0, stdout + stderr
        store = json.loads(store_path.read_text())
        assert len(store['pipes']) == 2
        assert len({record['pipe_id'] for record in store['pipes'].values()}) == 2
        for record in store['pipes'].values():
            assert [row['id'] for row in record['rows']] == list(range(20))
        assert not list(root.glob('*.tmp'))
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
