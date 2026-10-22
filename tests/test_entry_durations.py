"""The embedded CLI rejects durations its native timers cannot represent."""

import os
from pathlib import Path
import subprocess
import sys


def test_entry_rejects_invalid_timer_values_before_running_actions(tmp_path):
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (tmp_path / 'root').mkdir()
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(Path(__file__).resolve().parent.parent),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
    })
    script = '''
import meerschaum as mrsm
for key in ('min_seconds', 'timeout_seconds'):
    for value in (float('inf'), float('nan'), 1e300):
        success, message = mrsm.entry(['show', 'version'], _patch_args={key: value})
        assert not success, (key, value, message)
        assert '--' + key.replace('_', '-') in message, message
        assert 'finite' in message, message
'''
    result = subprocess.run(
        [sys.executable, '-c', script], env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
