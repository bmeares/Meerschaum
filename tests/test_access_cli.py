"""
rc15 wave 2 (AC10, FX2 and the `copy pipes` rule of AC4): the access commands as a Python user
of the wheel runs them (`python -m meerschaum`), against SQLite instances in a temporary root.

The Rust integration tests (`crates/meerschaum-cli/tests/access_cli.rs`) drive the same commands
in process (with prompts and the editor) and through the standalone binary; these go through the
console script path of the wheel. The data is synthetic.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import sqlite3
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
ANSI = re.compile(r'\x1b\[[0-9;]*[A-Za-z]')


def _env(tmp_path: pathlib.Path) -> dict:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_SQL_ACC': f"sqlite:///{tmp_path / 'acc.db'}",
        'MRSM_SQL_ACCB': f"sqlite:///{tmp_path / 'accb.db'}",
        'MRSM_SQL_ACCSECRET': 'postgresql://analyst:Tr0ub4dor-pw@db.example.com:5432/analytics',
        'MRSM_CONFIG': json.dumps({
            'meerschaum': {
                'connectors': {
                    'webhook': {
                        'alerts': {
                            'url': 'https://hooks.example.com/services/T0/B1/XyZsecretpath',
                            'api_key': 'k3y-abc-123',
                        },
                    },
                },
            },
            'shell': {'updates': {'check': False}},
        }),
        'NO_COLOR': '1',
        'COLUMNS': '160',
    })
    if os.environ.get('MRSM_BIN'):
        env['MRSM_BIN'] = os.environ['MRSM_BIN']
    return env


def _mrsm(tmp_path: pathlib.Path, *args: str, stdin: str = '') -> subprocess.CompletedProcess:
    result = subprocess.run(
        [sys.executable, '-P', '-m', 'meerschaum', *args],
        input=stdin,
        capture_output=True,
        text=True,
        env=_env(tmp_path),
        timeout=180,
        cwd=str(tmp_path),
    )
    result.stdout = ANSI.sub('', result.stdout)
    result.stderr = ANSI.sub('', result.stderr)
    return result


def _ok(tmp_path: pathlib.Path, *args: str, stdin: str = '') -> str:
    result = _mrsm(tmp_path, *args, stdin=stdin)
    assert result.returncode == 0, f"{args}:\n{result.stdout}\n{result.stderr}"
    return result.stdout + result.stderr


def _parameters(db: pathlib.Path, connector_keys: str, metric_key: str) -> dict:
    """A pipe's registered parameters, read from the instance's `mrsm_pipes` table."""
    with sqlite3.connect(db) as connection:
        row = connection.execute(
            "SELECT parameters FROM mrsm_pipes WHERE connector_keys = ? AND metric_key = ?",
            (connector_keys, metric_key),
        ).fetchone()
    assert row is not None, f"{connector_keys}:{metric_key} is not registered in {db}"
    return json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})


@pytest.fixture
def home(tmp_path):
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    return tmp_path


def test_users_teams_access_locks_and_copies(home):
    acc = ('-i', 'sql:acc')

    # The first user of an instance without users becomes an admin (the default answer);
    # the second follows the instance's default role.
    _ok(
        home, 'register', 'users', 'alice', 'bob', *acc,
        stdin='\npassword123\npassword123\nalice@example.com\npassword123\npassword123\n\n',
    )
    rows = [json.loads(line) for line in _ok(home, 'show', 'users', '--nopretty', *acc).splitlines()
            if line.startswith('{')]
    assert [(r['username'], r['type'], r['role'], r['default_role']) for r in rows] == [
        ('alice', 'admin', 'admin', False),
        ('bob', 'regular', 'editor', True),
    ]
    assert rows[0]['email'] == 'alice@example.com'
    assert rows[1]['email'] is None

    # Teams.
    _ok(home, 'register', 'teams', 'analytics', '--members', 'bob', '--managers', 'alice', *acc)
    team = json.loads(
        _ok(home, 'show', 'teams', 'analytics', '--nopretty', *acc).strip().splitlines()[-1]
    )
    assert team == {'name': 'analytics', 'managers': ['alice'], 'members': ['alice', 'bob']}
    table = _ok(home, 'show', 'users', *acc)
    for header in ('User', 'Type', 'Role', 'Teams', 'Email'):
        assert header in table, table
    assert 'analytics (manager)' in next(line for line in table.splitlines() if ' alice ' in line)

    # A pipe's access.
    _ok(home, 'register', 'pipes', '-c', 'sql:acc', '-m', 'weather', *acc)
    _ok(
        home, 'edit', 'pipes', 'access', '-c', 'sql:acc', '-m', 'weather',
        '--owner', 'bob', '--team', 'Analytics', '--visibility', 'private', '--edit', 'owner', *acc,
    )
    access = _parameters(home / 'acc.db', 'sql:acc', 'weather')['access']
    assert access == {'owner': 'bob', 'team': 'analytics', 'visibility': 'private', 'edit': 'owner'}
    refused = _mrsm(home, 'edit', 'pipes', 'access', '-m', 'weather', '--team', 'nope', '--noask', *acc)
    assert refused.returncode != 0
    assert "Team 'nope' is not registered" in refused.stdout + refused.stderr

    # Locks refuse the operation, `-y` and `--force` included, and say how to unlock.
    _ok(home, 'lock', 'pipes', '-c', 'sql:acc', '-m', 'weather', '--locks', 'drop,delete', *acc)
    assert _parameters(home / 'acc.db', 'sql:acc', 'weather')['locks'] == ['drop', 'delete']
    for action in ('drop', 'delete'):
        refused = _mrsm(home, action, 'pipes', '-c', 'sql:acc', '-m', 'weather', '-y', '-f', *acc)
        text = refused.stdout + refused.stderr
        assert refused.returncode != 0, text
        ### The refusal names the locks that refuse it (deleting drops the table too).
        held = re.search(r'it is locked \(([^)]*)\)', text)
        assert held and action in held.group(1).split(', '), text
        assert 'mrsm unlock pipes' in text, text
    assert _parameters(home / 'acc.db', 'sql:acc', 'weather')['access']['owner'] == 'bob'

    # A copy keeps the locks and never the access.
    _ok(home, 'copy', 'pipes', 'sql:accb', '-c', 'sql:acc', '-m', 'weather', '-y', *acc)
    copied = _parameters(home / 'accb.db', 'sql:acc', 'weather')
    assert copied['locks'] == ['drop', 'delete']
    assert 'access' not in copied

    _ok(home, 'unlock', 'pipes', '-c', 'sql:acc', '-m', 'weather', *acc)
    assert 'locks' not in _parameters(home / 'acc.db', 'sql:acc', 'weather')

    # Deleting a team moves its pipes to Everyone; the pipes keep their owner.
    _ok(home, 'delete', 'teams', 'analytics', '-y', *acc)
    access = _parameters(home / 'acc.db', 'sql:acc', 'weather')['access']
    assert access['team'] is None and access['owner'] == 'bob'
    refused = _mrsm(home, 'delete', 'teams', 'Everyone', '-y', *acc)
    assert refused.returncode != 0


def test_the_actions_facade_takes_the_access_flags_as_keywords(home):
    """`meerschaum.actions.actions[...]` passes the flags' destinations as keywords."""
    code = '''
import json
from meerschaum.actions import actions
kw = {'mrsm_instance': 'sql:acc', 'connector_keys': ['sql:acc'], 'metric_keys': ['facade']}
results = [
    actions['register'](['pipes'], **kw),
    actions['register'](['teams', 'ops'], mrsm_instance='sql:acc'),
    actions['edit'](['pipes', 'access'], team='ops', visibility='everyone', edit_mode='owner',
                    noask=True, **kw),
    actions['lock'](['pipes'], locks='sync,drop', **kw),
    actions['unlock'](['pipes'], locks='sync', **kw),
]
print('RESULTS:' + json.dumps([list(result) for result in results]))
'''
    result = subprocess.run(
        [sys.executable, '-P', '-c', code],
        capture_output=True,
        text=True,
        env=_env(home),
        timeout=180,
        cwd=str(home),
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith('RESULTS:')]
    assert lines, result.stdout + result.stderr
    results = json.loads(lines[-1][len('RESULTS:'):])
    assert all(success for success, _ in results), results
    parameters = _parameters(home / 'acc.db', 'sql:acc', 'facade')
    assert parameters['access'] == {
        'owner': None, 'team': 'ops', 'visibility': 'everyone', 'edit': 'owner',
    }
    assert parameters['locks'] == ['drop']


def test_show_connectors_masks_every_secret(home):
    secrets = ('Tr0ub4dor-pw', 'k3y-abc-123', 'XyZsecretpath', 'T0/B1')
    for args in (
        ('show', 'connectors'),
        ('show', 'connectors', '--nopretty'),
        ('show', 'connectors', 'sql:accsecret'),
        ('show', 'connectors', 'webhook:alerts', '--nopretty'),
    ):
        text = _ok(home, *args)
        for secret in secrets:
            assert secret not in text, f"{args} printed {secret}:\n{text}"
        assert '******' in text, text
    text = _ok(home, 'show', 'connectors', 'sql:accsecret')
    assert 'postgresql://analyst:******@db.example.com:5432/analytics' in text, text
