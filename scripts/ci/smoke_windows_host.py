"""Start the installed Windows API, request a plugin route and the Dash layout, then stop it."""

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import urlopen


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        plugins = root / 'plugins'
        plugins.mkdir()
        (plugins / 'smoke_host.py').write_text("""
from meerschaum.plugins import api_plugin

@api_plugin
def health(app):
    @app.get('/wheel-host-smoke')
    def smoke():
        return {'from_plugin': True}
""")
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        env = {**os.environ, 'MRSM_ROOT_DIR': str(root), 'MRSM_PLUGINS_DIR': str(plugins),
               'MRSM_PYTHON': sys.executable, 'MRSM_NO_AUTO_INSTALL': '1',
               'MRSM_SQL_SMOKE': json.dumps({'flavor': 'sqlite', 'database': str(root / 'smoke.db')}),
               'MRSM_NO_UPDATE_CHECK': '1'}
        with (root / 'server.log').open('w+') as log:
            api = subprocess.Popen([sys.executable, '-I', '-m', 'meerschaum', 'start', 'api',
                                    '-i', 'sql:smoke', '--host', '127.0.0.1', '--port', str(port),
                                    '--no-webterm', '--no-auth'], env=env, cwd=root,
                                   stdout=log, stderr=subprocess.STDOUT)
            try:
                base = f'http://127.0.0.1:{port}'
                deadline = time.monotonic() + 120
                while api.poll() is None and time.monotonic() < deadline:
                    try:
                        with urlopen(base + '/wheel-host-smoke', timeout=10) as response:
                            assert json.load(response) == {'from_plugin': True}
                        with urlopen(base + '/dash/_dash-layout', timeout=20) as response:
                            assert json.load(response)['props']
                        return
                    except (URLError, TimeoutError):
                        time.sleep(0.5)
                log.seek(0)
                raise RuntimeError(log.read()[-4000:])
            finally:
                if api.poll() is None:
                    subprocess.run(['taskkill', '/PID', str(api.pid), '/T', '/F'], check=True)
                api.wait(timeout=20)


if __name__ == '__main__':
    main()
