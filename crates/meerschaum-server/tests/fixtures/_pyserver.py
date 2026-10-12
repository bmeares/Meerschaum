#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Shared helper for the `meerschaum-server` fixture generators (`gen_openapi.py`,
`gen_contract.py`): start the Python v4 API server on a free port against a fresh sqlite
instance in a temporary root directory, and kill it (and its process group) on exit.

Two-phase start: `reexec_in_temp_root()` creates the temporary root and re-executes the generator
with `MRSM_ROOT_DIR`, an empty temporary `MRSM_PLUGINS_DIR` and the sqlite connector set *before*
`meerschaum` is imported, then removes the root when the child exits.

Under `scripts/parity/run.sh` (`MRSM_PARITY_REFERENCE` set), the child and the server run with the
reference interpreter in `-s` mode and without the repository on `PYTHONPATH`, so they import the
released 4.1.4 package. Otherwise the repository is put on `PYTHONPATH` (the `~/.local` gotcha).
The venvs directory is shared and persistent (the reference root's `venvs` under the harness,
the repository's `test_root/venvs` otherwise), so v4's on-demand installs happen once.

All data is synthetic. Never port 8989 (the test-API port).
"""

import os
import re
import sys
import time
import shutil
import signal
import socket
import atexit
import pathlib
import tempfile
import subprocess

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[3]
# Not `MRSM_`-prefixed: `MRSM_<TYPE>_<LABEL>` variables define connectors.
ROOT_ENV = 'SERVER_FIXTURE_ROOT'
REFERENCE_ENV = 'SERVER_FIXTURE_REFERENCE'
USERNAME = 'fixture_admin'
PASSWORD = 'fixture_pass'
INSTANCE = 'sql:fixture'


def free_port() -> int:
    """Return a free TCP port on localhost (never 8989)."""
    while True:
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        if port != 8989:
            return port


def python_cmd() -> list:
    """The interpreter command for children (`-s` under the parity harness)."""
    reference = os.environ.get('MRSM_PARITY_REFERENCE') or os.environ.get(REFERENCE_ENV)
    return [sys.executable, '-s'] if reference else [sys.executable]


def reexec_in_temp_root(script: str) -> None:
    """
    Phase 1: create the temporary root and re-execute `script` with the environment set before
    `meerschaum` imports. Exits the process with the child's return code.
    Returns immediately (phase 2) when the root is already set.
    """
    if ROOT_ENV in os.environ:
        return
    root = tempfile.mkdtemp(prefix='mrsm-server-fixture-')
    home = pathlib.Path(root) / 'home'
    home.mkdir(parents=True, exist_ok=True)
    # A minimal environment (`env -i` plus what the server needs), so nothing from this machine
    # (tokens, session variables, paths) can reach a recorded request or response.
    env = {
        'PATH': os.environ.get('PATH', '/usr/bin:/bin'),
        'HOME': str(home),
        'PYTHONHASHSEED': '0',
        ROOT_ENV: root,
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        'MRSM_VENVS_DIR': str(
            REPO / '.venv-ref' / 'mrsm_root' / 'venvs'
            if os.environ.get('MRSM_PARITY_REFERENCE')
            else REPO / 'test_root' / 'venvs'
        ),
        'MRSM_SQL_FIXTURE': f"sqlite:///{root}/fixture.db",
        'MRSM_NONINTERACTIVE': 'true',
    }
    if os.environ.get('MRSM_PARITY_REFERENCE'):
        env[REFERENCE_ENV] = os.environ['MRSM_PARITY_REFERENCE']
    else:
        env['PYTHONPATH'] = str(REPO)
    (pathlib.Path(root) / 'plugins').mkdir(parents=True, exist_ok=True)
    try:
        rc = subprocess.call(python_cmd() + [script] + sys.argv[1:], env=env, cwd=str(REPO))
    finally:
        if not os.environ.get('MRSM_SERVER_FIXTURE_KEEP'):
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"Kept {root}.")
    sys.exit(rc)


# Built from parts so this script itself passes the same check.
_HOME_DIR = '/' + 'home' + '/'
FORBIDDEN = re.compile(
    '|'.join(['CLAUDE' + '_CODE', _HOME_DIR, 'bme' + 'ares', 'MESSAGING' + '_TOKEN', 'XDG' + '_SESSION'])
)


def scrub(x, key=None):
    """
    Remove anything machine-specific from a recorded value: `env` / `environment` maps are
    emptied, `pid`s nulled, and the temporary root, the repository, the home directory, `/tmp`
    paths and the hostname are replaced by placeholders.
    """
    if isinstance(x, dict):
        return {
            k: ({} if k in ('env', 'environment') and isinstance(v, dict)
                else None if k == 'pid' else scrub(v, k))
            for k, v in x.items()
        }
    if isinstance(x, list):
        return [scrub(v) for v in x]
    if isinstance(x, str):
        root = os.environ.get(ROOT_ENV)
        if root:
            x = x.replace(root, '{TMP}')
        x = x.replace(str(REPO), '{REPO}')
        x = x.replace(os.path.expanduser('~'), '{HOME}')
        x = re.sub(_HOME_DIR + r'[^\s"\']*', '{HOME}', x)
        x = re.sub(r'/tmp/[^\s"\']*', '{TMP}', x)
        host = socket.gethostname()
        if host and len(host) > 3:
            x = x.replace(host, '{HOST}')
        return x
    return x


def write_fixture(path: pathlib.Path, data) -> None:
    """Scrub, write, and refuse to keep a fixture that still holds machine-specific text."""
    import json
    text = json.dumps(scrub(data), indent=2, sort_keys=False, ensure_ascii=False) + '\n'
    found = FORBIDDEN.search(text)
    if found:
        raise RuntimeError(f"Refusing to write {path.name}: found {found.group(0)!r}.")
    path.write_text(text, encoding='utf-8')


def root_dir() -> pathlib.Path:
    """The temporary root directory (phase 2)."""
    return pathlib.Path(os.environ[ROOT_ENV])


_server = {'proc': None}


def kill_server() -> None:
    """Terminate the server's process group."""
    proc = _server['proc']
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=15)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            pass


atexit.register(kill_server)
signal.signal(signal.SIGTERM, lambda *a: sys.exit(1))


def seed_admin() -> None:
    """Register the admin user directly on the sqlite instance."""
    import meerschaum as mrsm
    from meerschaum.core import User
    instance = mrsm.get_connector(INSTANCE)
    user = User(USERNAME, PASSWORD, type='admin', instance=instance)
    success, msg = instance.register_user(user)
    assert success, msg


def start_server(extra_args=None) -> int:
    """Start `mrsm start api` on a free port and wait for `/healthcheck`."""
    import requests
    port = free_port()
    root = root_dir()
    log = open(root / 'api.log', 'w', encoding='utf-8')
    _server['proc'] = subprocess.Popen(
        python_cmd() + [
            '-m', 'meerschaum', 'start', 'api',
            '-p', str(port), '-i', INSTANCE, '-w', '1',
            '--no-dash', '--no-webterm', '-y',
        ] + list(extra_args or []),
        env=os.environ,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        cwd=str(root),
    )
    deadline = time.time() + 240
    while time.time() < deadline:
        if _server['proc'].poll() is not None:
            raise RuntimeError("API server exited:\n" + (root / 'api.log').read_text())
        try:
            if requests.get(f"http://127.0.0.1:{port}/healthcheck", timeout=2).ok:
                return port
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("API server did not start:\n" + (root / 'api.log').read_text())
