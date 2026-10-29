"""The SQL SDK shares the native interactive-client launcher, including ad hoc attributes."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def test_sdk_cli_launches_rainfrog_and_plain_clients_without_a_database_connection():
    scratch = Path.home() / '.cache' / 'mrsm-sql-cli-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as directory:
        root = Path(directory)
        (root / 'plugins').mkdir()
        (root / 'bin').mkdir()
        for name, body in (
            ('rainfrog', 'echo RAINFROG_ARGS: "$@"; case "$DATABASE_URL" in *sdk-secret*) echo SDK_CREDENTIAL_RECEIVED;; esac'),
            ('psql', 'echo PLAIN_ARGS: "$@"'),
        ):
            executable = root / 'bin' / name
            executable.write_text('#!/bin/sh\n' + body + '\n')
            executable.chmod(0o755)
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')}
        env.update({
            'PYTHONPATH': str(Path(__file__).resolve().parents[1]),
            'MRSM_ROOT_DIR': str(root),
            'MRSM_PLUGINS_DIR': str(root / 'plugins'),
            'MRSM_NO_AUTO_INSTALL': '1',
            'MRSM_NO_UPDATE_CHECK': '1',
            'PATH': str(root / 'bin'),
        })
        result = subprocess.run([sys.executable, '-c', '''
import meerschaum as mrsm
from meerschaum.config import get_config
assert 'adhoc_sdk' not in get_config('meerschaum', 'connectors', 'sql')
connector = mrsm.get_connector('sql', 'adhoc_sdk', flavor='postgresql',
    host='127.0.0.1', port=1, database='sdk', username='alice', password='sdk-secret')
assert connector.cli()[0]
assert connector.cli(nopretty=True)[0]
assert '_engine' not in connector.__dict__
assert 'adhoc_sdk' not in get_config('meerschaum', 'connectors', 'sql')
'''], env=env, cwd=root, text=True, capture_output=True, timeout=60)
        assert result.returncode == 0, result.stdout + result.stderr
        assert 'RAINFROG_ARGS: --driver postgres' in result.stdout
        assert 'SDK_CREDENTIAL_RECEIVED' in result.stdout
        assert 'PLAIN_ARGS: postgresql://alice@127.0.0.1:1/sdk' in result.stdout
        assert 'sdk-secret' not in result.stdout + result.stderr
