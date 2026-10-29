#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Connectors this process built with attributes of its own win over the configured connector of the
same keys in the actions it runs (`mrsm.entry()`, `meerschaum.actions.actions[...]`), as in v4,
where those actions received the cached connector.

The regression: `scripts/test.sh` exports `MRSM_SQL_TEST_TIMESCALEDB_HA=postgresql+psycopg://...`
while `tests/connectors.py` builds `sql:test_timescaledb_ha` with `flavor='timescaledb-ha'`; the
native dispatcher rebuilt the connector from the URI alone, so `compress pipes` saw the flavor
`postgresql` ("Compression is not supported for flavor 'postgresql'").

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_adhoc_connectors.py -p no:cacheprovider
"""

import os
import pathlib
import subprocess
import sys

import pytest

import meerschaum as mrsm
from meerschaum import _core

pytestmark = pytest.mark.skipif(
    _core.load() is None,
    reason="The `_core` extension is not built (scripts/rust/build_py.sh).",
)

REPO = pathlib.Path(__file__).resolve().parent.parent


def _run_python(code: str, tmp_path: pathlib.Path, extra_env=None, timeout: int = 180):
    """Run `code` in a fresh interpreter on a throwaway root, with only `extra_env` connectors."""
    (tmp_path / 'root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') or key in ('MRSM_NO_AUTO_INSTALL', 'MRSM_DUCKDB_LIB')
    }
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'PYTHONPATH': str(REPO),
        **(extra_env or {}),
    })
    env.pop('TMUX', None)
    env.pop('TMUX_PANE', None)
    return subprocess.run(
        [sys.executable, '-c', code],
        env=env,
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_explicit_attributes_override_the_environment_connector(tmp_path):
    """
    `MRSM_SQL_W17CI1=sqlite:///<env.db>` defines `sql:w17ci1`, and the process builds
    `sql:w17ci1` with `uri=sqlite:///<own.db>`: native actions use the process's database.
    """
    env_db = tmp_path / 'env.db'
    own_db = tmp_path / 'own.db'
    code = f"""
import meerschaum as mrsm
conn = mrsm.get_connector('sql', 'w17ci1', uri='sqlite:///{own_db.as_posix()}')
pipe = mrsm.Pipe('w17ci1', 'adhoc', instance=conn, columns={{'datetime': 'dt'}})
success, message = pipe.sync({{'dt': ['2026-01-01', '2026-01-02'], 'v': [1, 2]}})
assert success, message
success, message = mrsm.entry(
    ['delete', 'pipes', '-i', 'sql:w17ci1', '-c', 'w17ci1', '-m', 'adhoc', '-y'],
)
assert success, message
assert not pipe.exists(), "the action deleted another database's pipe"
print('OK')
"""
    result = _run_python(code, tmp_path, {'MRSM_SQL_W17CI1': f'sqlite:///{env_db.as_posix()}'})
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    assert result.stdout.strip().splitlines()[-1] == 'OK'


def test_adhoc_context_keeps_attributes_the_configured_connector_lacks(tmp_path):
    """
    A flavor passed next to an environment URI of the same family (`timescaledb-ha` over
    `postgresql://`) reaches the native dispatcher; a connector built from the config alone
    (or with attributes equal to the configured ones) is left for the dispatcher to build.
    """
    code = """
import json
import meerschaum as mrsm
from meerschaum._internal.entry import adhoc_context
mrsm.get_connector(
    'sql', 'w17ci1_ha', flavor='timescaledb-ha', host='localhost', port=1,
    username='u', password='p', database='d',
)
mrsm.get_connector('sql', 'w17ci1_plain')
mrsm.get_connector('sql', 'w17ci1_same', uri='postgresql://u:p@localhost:1/d')
context = adhoc_context() or {'attributes': {}}
print('JSON:' + json.dumps({
    keys: attrs.get('flavor') for keys, attrs in context['attributes'].items()
}))
"""
    result = _run_python(code, tmp_path, {
        'MRSM_SQL_W17CI1_HA': 'postgresql://u:p@localhost:1/d',
        'MRSM_SQL_W17CI1_PLAIN': 'postgresql://u:p@localhost:1/d',
        'MRSM_SQL_W17CI1_SAME': 'postgresql://u:p@localhost:1/d',
    })
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    import json
    line = [ln for ln in result.stdout.splitlines() if ln.startswith('JSON:')][-1]
    flavors = json.loads(line[len('JSON:'):])
    assert flavors.get('sql:w17ci1_ha') == 'timescaledb-ha'
    assert 'sql:w17ci1_plain' not in flavors


def test_a_connector_rebuilt_with_new_attributes_is_the_one_actions_see(tmp_path):
    """
    `get_connector()` with different keyword arguments replaces the registered connector (v4's
    `refresh`); the context carries the new object's attributes, not the first one's.
    """
    code = """
import json
import meerschaum as mrsm
from meerschaum._internal.entry import adhoc_context
first = mrsm.get_connector(
    'sql', 'w17ci1_ha', flavor='timescaledb-ha', host='localhost', port=1,
    username='u', password='p', database='d',
)
second = mrsm.get_connector(
    'sql', 'w17ci1_ha', flavor='timescaledb-ha', host='localhost', port=2,
    username='u', password='p', database='d',
)
assert first is not second
attrs = (adhoc_context() or {'attributes': {}})['attributes'].get('sql:w17ci1_ha', {})
print('JSON:' + json.dumps([attrs.get('flavor'), attrs.get('port')]))
"""
    result = _run_python(code, tmp_path, {
        'MRSM_SQL_W17CI1_HA': 'postgresql://u:p@localhost:1/d',
    })
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    import json
    line = [ln for ln in result.stdout.splitlines() if ln.startswith('JSON:')][-1]
    assert json.loads(line[len('JSON:'):]) == ['timescaledb-ha', 2]


def test_an_unreachable_explicit_connector_does_not_slow_entry(tmp_path):
    """
    `adhoc_context()` hands over the Rust connector of every explicit connector without asking
    the database anything (the pool connects lazily, the default schema is asked for when a
    method runs): an unreachable one (no `schema`, so v5 would ask for the default) that the
    action never uses must not make each `mrsm.entry()` wait for the pool's acquire timeout.
    Also covers an explicit attribute with no single truth value (an array-like `==`).
    """
    code = """
import json, socket, time
import meerschaum as mrsm

### A port nothing listens on: bind, read it, close.
sock = socket.socket()
sock.bind(('127.0.0.1', 0))
port = sock.getsockname()[1]
sock.close()

class Ambiguous:
    def __eq__(self, other):
        raise ValueError('The truth value of an array with more than one element is ambiguous.')
    __hash__ = object.__hash__

conn = mrsm.get_connector(
    'sql', 'w17ci1_down', flavor='timescaledb', host='127.0.0.1', port=port,
    username='u', password='p', database='d',
)
### Compared with the configured `uri` (`str == Ambiguous` defers to `Ambiguous.__eq__`).
conn.__dict__['_explicit_attributes'] = {'uri': Ambiguous()}
durations = []
for _ in range(2):
    start = time.perf_counter()
    success, message = mrsm.entry(['show', 'version', '--nopretty'])
    durations.append(time.perf_counter() - start)
    assert success, message
print('JSON:' + json.dumps(durations))
"""
    result = _run_python(code, tmp_path, {
        'MRSM_SQL_W17CI1_DOWN': 'postgresql://u:p@127.0.0.1:1/d',
    })
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-4000:]
    import json
    line = [ln for ln in result.stdout.splitlines() if ln.startswith('JSON:')][-1]
    durations = json.loads(line[len('JSON:'):])
    assert max(durations) < 5, durations
