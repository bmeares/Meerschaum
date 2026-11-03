"""A bounded subprocess catches whole-frame JSON copies on sync and read."""

import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest


@pytest.mark.skipif(sys.platform != 'linux', reason='RSS sampling uses Linux /proc')
def test_large_json_sync_and_read_memory(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
        'MRSM_ROOT_DIR': str(tmp_path), 'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    code = r'''
import json, sys
from pathlib import Path
import pandas as pd
import meerschaum as mrsm
from meerschaum.connectors.sql import SQLConnector
conn = SQLConnector('jsonmemory', flavor='sqlite', database=str(Path(sys.argv[2]) / 'data.db'))
pipe = mrsm.Pipe('test:jsonmemory', 'json', instance=conn,
                 columns={'datetime': 'dt', 'id': 'id'}, dtypes={'payload': 'json'})
def payload(i):
    return {'n': i, 'tags': ['a', str(i % 7)], 'ok': bool(i % 2)}
if sys.argv[1] == 'sync':
    df = pd.DataFrame({'dt': pd.date_range('2024-01-01', periods=100000, freq='s', tz='UTC'),
                       'id': [i % 50 for i in range(100000)],
                       'payload': [payload(i) for i in range(100000)]})
    ok, message = pipe.sync(df)
    assert ok, message
else:
    df = pipe.get_data(as_pandas=True)
    assert len(df) == 100000
    for i in (0, 1, 49999, 99999):
        assert df.iloc[i]['payload'] == payload(i)
# Linux rusage may retain the parent's RSS peak across exec, including an xdist
# worker's earlier tests. VmHWM belongs to this interpreter's address space.
peak_kib = next(int(line.split()[1]) for line in Path('/proc/self/status').read_text().splitlines()
                if line.startswith('VmHWM:'))
print('RSS:' + json.dumps(peak_kib / 1024))
'''
    for phase in ('sync', 'read'):
        # A separate process for each phase, with a wall-clock and RSS watchdog. The output
        # goes to a file so a failure cannot fill a pipe while the parent samples memory.
        with (tmp_path / f'{phase}.log').open('w+') as log:
            child = subprocess.Popen([sys.executable, '-c', code, phase, str(tmp_path)],
                                     env=env, cwd=tmp_path, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 180
            try:
                while child.poll() is None:
                    assert time.monotonic() < deadline, f'{phase} exceeded 180 seconds'
                    try:
                        status = Path(f'/proc/{child.pid}/status').read_text()
                        rss = next((int(line.split()[1]) for line in status.splitlines()
                                    if line.startswith('VmRSS:')), 0)
                        assert rss < 768 * 1024, f'{phase} exceeded the 768 MiB watchdog'
                    except FileNotFoundError:
                        pass
                    time.sleep(0.03)
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=10)
            log.seek(0)
            output = log.read()
        assert child.returncode == 0, output[-6000:]
        peak = json.loads(next(line[4:] for line in output.splitlines() if line.startswith('RSS:')))
        # Allows build/interpreter variation; the old deep-copy path exceeds 600 MiB.
        # The release gate separately compares median phase peaks with v4 on both backends.
        assert peak <= 400, f'{phase}: {peak:.1f} MiB exceeds the 400 MiB regression bound'
