#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Token edge cases found in v4.1.x (wave 17b):

- an instance which stores the `scopes` JSON column as text (SQLite) made `get_token_model()` raise
  `scopes: Input should be a valid list` and `get_token_scopes()` return the string `'["pipes:read"]'`;
- `get_token()` built the `Token` without its instance (the default instance served it later);
- a token of a non-default instance must authenticate against the API server's instance.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

import meerschaum as mrsm
from meerschaum.core import Token, User

from tests.connectors import conns, get_flavors

SQL_FLAVORS = [f for f in get_flavors() if f in conns and conns[f].type == 'sql']


def _user_and_token(conn, scopes):
    tag = uuid.uuid4().hex[:8]
    user = User(f'w17bt_{tag}', 'pw-' + tag * 2, instance=conn)
    success, msg = conn.register_user(user)
    assert success, msg
    token = Token(f'w17bt-{tag}', user=user, instance=conn, scopes=scopes)
    success, msg = token.register()
    assert success, msg
    return user, token


@pytest.mark.parametrize('flavor', SQL_FLAVORS)
def test_token_scopes_are_a_list_on_every_flavor(flavor):
    conn = conns[flavor]
    user, token = _user_and_token(conn, ['pipes:read'])
    try:
        model = conn.get_token_model(token.id)
        assert model.scopes == ['pipes:read']
        assert conn.get_token_scopes(token.id) == ['pipes:read']
        assert conn.get_token_scopes(token) == ['pipes:read']
        loaded = conn.get_token(token.id)
        assert loaded.scopes == ['pipes:read']
        assert str(loaded.instance_connector) == str(conn)
        assert token.get_scopes(refresh=True) == ['pipes:read']
        assert token.to_model(refresh=True).scopes == ['pipes:read']
        listed = conn.get_tokens(ids=[token.id])
        assert [t.scopes for t in listed] == [['pipes:read']]
        assert conn.get_token_model(token) is not None
    finally:
        token.delete()
        conn.delete_user(user)


def test_token_without_scopes_has_an_empty_list_when_edited_to_none():
    from meerschaum.connectors.instance._tokens import _scopes_list
    assert _scopes_list(None) == []
    assert _scopes_list('') == []
    assert _scopes_list('["a", "b"]') == ['a', 'b']
    assert _scopes_list(['a']) == ['a']
    assert _scopes_list(('a', 'b')) == ['a', 'b']
    assert _scopes_list('{"x": 1}') == []
    assert _scopes_list('*') == ['*']


@pytest.mark.parametrize('library', ['polars', 'pandas'])
def test_token_listing_does_not_depend_on_the_dataframe_library(monkeypatch, library):
    """The token store reads documents, including on fresh Polars roots."""
    from tests._worker_sqlite import worker_sqlite
    monkeypatch.setenv('MRSM_DATAFRAME', library)
    conn = worker_sqlite('token_frames_' + uuid.uuid4().hex[:8])
    user, token = _user_and_token(conn, ['pipes:read'])
    try:
        listed = conn.get_tokens(user=user, ids=[token.id])
        assert len(listed) == 1
        assert str(listed[0].id) == str(token.id)
        assert listed[0].scopes == ['pipes:read']
        assert listed[0].creation is not None
        assert listed[0].expiration is not None
    finally:
        token.delete()
        conn.delete_user(user)


# ---------------------------------------------------------------------------------------------
# HTTP: the server resolves the token on ITS instance, whatever the default instance is.


def _request(url, method='GET', data=None, headers=None):
    body = urllib.parse.urlencode(data).encode() if isinstance(data, dict) else data
    req = urllib.request.Request(url, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


@pytest.fixture(scope='module')
def server():
    from tests._binary import binary_or_skip
    binary_or_skip()
    from tests.console.conftest import Server
    ### The default instance is NOT the one the server serves (`-i`), like a deployment whose
    ### config names another instance than the API's.
    instance = Server(
        'w17bt-tokens',
        config={'meerschaum': {'instance': 'sql:w17botherdefault'}},
        args=['-i', 'sql:w7console'],
        native=False,
    )
    instance.env['MRSM_SQL_W17BOTHERDEFAULT'] = f"sqlite:///{instance.root / 'other.db'}"
    try:
        instance.start()
    except Exception as e:
        instance.stop()
        pytest.skip(f"cannot start a loopback API server: {e}")
    yield instance
    instance.stop()


@pytest.fixture(scope='module')
def creds(server):
    code = """
import json
import meerschaum as mrsm
from meerschaum.core import User, Token
conn = mrsm.get_connector('sql:w7console')
user = User('bob', 'bob-password-1', instance=conn, type='user')
success, msg = conn.register_user(user)
assert success, msg
token = Token('bobtok', user=user, instance=conn, scopes=['pipes:read'])
success, msg = token.register()
assert success, msg
print(json.dumps({'id': str(token.id), 'secret': token.secret, 'key': token.get_api_key()}))
"""
    result = server.python(code)
    assert result.returncode == 0, result.stderr[-3000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


def _claims(jwt):
    payload = jwt.split('.')[1]
    return json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))


def test_regular_users_token_logs_in_with_exactly_its_scopes(server, creds):
    status, text = _request(
        f"{server.base_url}/login", 'POST',
        {'grant_type': 'client_credentials', 'client_id': creds['id'],
         'client_secret': creds['secret']},
    )
    assert status == 200, text
    access = json.loads(text)['access_token']
    assert _claims(access)['scopes'] == ['pipes:read']
    bearer = {'Authorization': f'Bearer {access}'}
    assert _request(f"{server.base_url}/pipes", headers=bearer)[0] == 200
    status, text = _request(
        f"{server.base_url}/pipes/a/b/None/register", 'POST',
        json.dumps({}).encode(), {**bearer, 'Content-Type': 'application/json'},
    )
    assert status == 403, text
    assert 'pipes:register' in text


def test_api_key_of_a_regular_users_token_reads_pipes_on_the_servers_instance(server, creds):
    """
    v4.1.4 rebuilt the `Token` for a `mrsm-key:` request without its instance, so the scopes were
    read from the DEFAULT instance, found nothing and answered 403. v5 resolves it on the server's
    instance (PARITY W17T-001).
    """
    key = {'Authorization': 'Bearer ' + creds['key']}
    assert _request(f"{server.base_url}/pipes", headers=key)[0] == 200
    status, text = _request(
        f"{server.base_url}/pipes/a/b/None/register", 'POST',
        json.dumps({}).encode(), {**key, 'Content-Type': 'application/json'},
    )
    assert status == 403, text


def test_the_api_connector_instance_path(server, creds):
    """`get_token_model` through an `api:` instance."""
    from tests.console.conftest import USERNAME, PASSWORD
    conn = mrsm.get_connector(
        'api', 'w17bt_api', host='127.0.0.1', port=server.port, protocol='http',
        username=USERNAME, password=PASSWORD,
    )
    ### v4's `get_token_scopes()` returned `[]` for an existing token (PARITY W17T-004).
    assert conn.get_token_scopes(uuid.UUID(creds['id'])) == ['pipes:read']
    assert conn.get_token_model(uuid.UUID(creds['id'])).scopes == ['pipes:read']


# ---------------------------------------------------------------------------------------------
# CLI: `edit tokens` / `show tokens` act on the instance named by `-i` / `--instance`.


@pytest.mark.parametrize('flag', ['-i', '--instance'])
def test_edit_and_show_tokens_use_the_named_instance(tmp_path, flag):
    import os
    import subprocess
    import sys
    from tests._binary import binary_or_skip
    binary = binary_or_skip()
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(pathlib_repo()),
        'HOME': str(tmp_path),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_SQL_W17BTA': f"sqlite:///{tmp_path / 'a.db'}",
        'MRSM_SQL_W17BTB': f"sqlite:///{tmp_path / 'b.db'}",
        'MRSM_CONFIG': json.dumps({'meerschaum': {'instance': 'sql:w17btb'}}),
    })
    code = """
import meerschaum as mrsm
from meerschaum.core import User, Token
for label in ('a', 'b'):
    conn = mrsm.get_connector('sql:w17bt' + label)
    user = User('user' + label, 'password-' + label * 6, instance=conn)
    assert conn.register_user(user)[0]
    assert Token('tok' + label, user=user, instance=conn, scopes=['pipes:read']).register()[0]
"""
    result = subprocess.run(
        [sys.executable, '-c', code], env=env, cwd=str(root), capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr[-2000:]

    def mrsm_cli(*args, stdin=None):
        return subprocess.run(
            [binary, *args], env=env, cwd=str(root), capture_output=True, text=True,
            input=stdin, timeout=120,
        )

    shown = mrsm_cli('show', 'tokens', flag, 'sql:w17bta', '--nopretty')
    assert shown.returncode == 0, shown.stderr
    assert '"label": "toka"' in shown.stdout and 'tokb' not in shown.stdout
    default_shown = mrsm_cli('show', 'tokens', '--nopretty')
    assert '"label": "tokb"' in default_shown.stdout and 'toka' not in default_shown.stdout

    ### Typed text is appended to the prefilled label (no terminal); empty answers keep the rest.
    edited = mrsm_cli('edit', 'tokens', 'toka', flag, 'sql:w17bta', stdin='X\n\n\nn\n')
    assert 'Successfully edited 1 token' in edited.stdout + edited.stderr, edited.stdout
    after_a = mrsm_cli('show', 'tokens', flag, 'sql:w17bta', '--nopretty').stdout
    assert '"label": "tokaX"' in after_a and '"scopes": ["pipes:read"]' in after_a, after_a
    after_default = mrsm_cli('show', 'tokens', '--nopretty').stdout
    assert '"label": "tokb"' in after_default and 'tokaX' not in after_default, after_default


def pathlib_repo():
    import pathlib
    return pathlib.Path(__file__).resolve().parent.parent
