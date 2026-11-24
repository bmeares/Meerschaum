"""The installed Windows API host keeps stdin available to DLLs and exits with its parent."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest


def watch_parent(root: Path):
    import _winapi
    import msvcrt
    from meerschaum.api.host._app import exit_with_parent

    handle = msvcrt.get_osfhandle(sys.stdin.fileno())
    assert exit_with_parent() is not None
    (root / 'watching').touch()
    while not (root / 'input-sent').exists() or _winapi.PeekNamedPipe(handle)[0]:
        time.sleep(0.01)

    # A pending CRT read on this descriptor would stall both duplication and DLL startup.
    os.close(os.dup(sys.stdin.fileno()))
    import numpy as np
    assert np.arange(3).sum() == 3
    (root / 'imported').touch()
    while True:
        time.sleep(1)


class TestWindowsHostParent(unittest.TestCase):
    @unittest.skipUnless(sys.platform == 'win32', 'Windows API host parent pipe')
    def test_native_import_and_parent_exit(self):
        python_flags = ['-I'] if sys.flags.isolated else []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plugins = root / 'plugins'
            plugins.mkdir()
            env = {key: value for key, value in os.environ.items() if not key.startswith('MRSM_')}
            env.update({
                'MRSM_API_HOST': '1', 'MRSM_API_HOST_PARENT_PIPE': '1',
                'MRSM_ROOT_DIR': directory, 'MRSM_PLUGINS_DIR': str(plugins),
                'MRSM_SQL_MAIN': json.dumps({'flavor': 'sqlite', 'database': str(root / 'main.db')}),
                'MRSM_NO_AUTO_INSTALL': '1',
            })
            proc = subprocess.Popen(
                [sys.executable, *python_flags, str(Path(__file__).resolve()), 'watch', directory],
                stdin=subprocess.PIPE, bufsize=0, env=env, cwd=root,
            )

            def wait_for(marker):
                deadline = time.monotonic() + 30
                while not (root / marker).exists():
                    self.assertIsNone(proc.poll(), f'Host exited before {marker}.')
                    self.assertLess(time.monotonic(), deadline, f'Host stalled before {marker}.')
                    time.sleep(0.05)

            try:
                wait_for('watching')
                proc.stdin.write(b'still alive\r\n\x1a\r')
                (root / 'input-sent').touch()
                wait_for('imported')
                self.assertIsNone(proc.poll())
                proc.stdin.write(b'last data before closure')
                proc.stdin.close()
                proc.wait(timeout=15)
            finally:
                if proc.poll() is None:
                    # A venv's python.exe may launch another interpreter. Reap both on timeout.
                    subprocess.run(['taskkill', '/PID', str(proc.pid), '/T', '/F'], timeout=15)
                proc.wait(timeout=15)
                proc.stdin.close()


if __name__ == '__main__':
    if sys.argv[1:2] == ['watch']:
        watch_parent(Path(sys.argv[2]))
    else:
        unittest.main()
