#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test how scopes are granted and checked, with a SQLite instance and no API server.

- A literal `*` grants every scope, including custom and future scopes, so it is stored as is.
- `*` satisfies the scopes which are necessary to execute actions and jobs.
- Stopping a job requires `jobs:stop` (the legacy misspelling `josb:stop` is still accepted).
"""

import json
import pathlib
import uuid
from types import SimpleNamespace

import pytest

import meerschaum as mrsm
from meerschaum import get_connector
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.core import User, Token


@pytest.fixture
def instance(tmp_path):
    """
    A SQLite instance with one registered user.
    """
    label = f"scopes_{uuid.uuid4().hex[:8]}"
    conn = get_connector(
        'sql', label,
        flavor='sqlite',
        database=str(tmp_path / 'scopes.db'),
    )
    user = User('scope_user', 'test1234', email='none@none.com', instance=conn)
    success, msg = conn.register_user(user)
    assert success, msg

    yield SimpleNamespace(conn=conn, keys=str(conn), user=user)
    conn.engine.dispose()


def _fake_prompts(monkeypatch, scopes_answer: str):
    """
    Answer the scope prompt, keep every other default, and decline every question.
    """
    import meerschaum.utils.prompt as prompt_module

    def fake_prompt(question, default_editable=None, **kw):
        if 'cope' in question:
            return scopes_answer
        return default_editable or ''

    monkeypatch.setattr(prompt_module, 'prompt', fake_prompt)
    monkeypatch.setattr(prompt_module, 'yes_no', lambda *a, **kw: False)
    monkeypatch.setattr(prompt_module, 'choose', lambda question, choices, **kw: choices[0])


class FakeTokenInstance:
    """
    An in-memory instance for tokens (a SQLite instance does not round-trip token scopes).
    """
    type = 'fake'

    def __init__(self):
        self.tokens = {}
        self.label = f"fake{uuid.uuid4().hex[:6]}"

    def __str__(self):
        return f"fake:{self.label}"

    def get_users(self, debug=False):
        return ['scope_user']

    def get_user_id(self, user, debug=False):
        return 1

    def register_token(self, token, debug=False):
        token.generate_credentials()
        self.tokens[token.id] = token.to_model(refresh=False)
        return True, "Success"

    def get_tokens(self, labels=None, ids=None, debug=False):
        return [
            Token(**dict(model), instance=str(self))
            for model in self.tokens.values()
            if (not labels or model.label in labels) and (not ids or model.id in ids)
        ]

    def get_token_model(self, token_id, debug=False):
        return self.tokens.get(token_id, None)

    def token_exists(self, token_id, debug=False):
        return token_id in self.tokens

    def edit_token(self, token, debug=False):
        self.tokens[token.id] = token.to_model(refresh=False)
        return True, "Success"

    def scopes(self, token_id):
        return list(self.tokens[token_id].scopes)


@pytest.fixture
def tokens_instance(monkeypatch):
    fake = FakeTokenInstance()
    import meerschaum.connectors.parse as parse_module
    monkeypatch.setattr(parse_module, 'parse_instance_keys', lambda *a, **kw: fake)
    return fake


def _register_token(fake, scopes=None) -> Token:
    token = Token(
        label=f"token-{uuid.uuid4().hex[:6]}",
        user=User('scope_user', user_id=1),
        scopes=scopes,
        instance=str(fake),
    )
    success, msg = token.register()
    assert success, msg
    return token


def test_edit_tokens_stores_a_star_literally(tokens_instance, monkeypatch):
    from meerschaum.actions.edit import _edit_tokens
    token = _register_token(tokens_instance, scopes=['pipes:read'])
    _fake_prompts(monkeypatch, '*')
    success, msg = _edit_tokens(action=[str(token.id)], mrsm_instance=str(tokens_instance))
    assert success, msg
    assert tokens_instance.scopes(token.id) == ['*']


def test_edit_tokens_keeps_other_scopes(tokens_instance, monkeypatch):
    from meerschaum.actions.edit import _edit_tokens
    token = _register_token(tokens_instance)
    _fake_prompts(monkeypatch, 'pipes:read custom:read')
    success, msg = _edit_tokens(action=[str(token.id)], mrsm_instance=str(tokens_instance))
    assert success, msg
    assert tokens_instance.scopes(token.id) == ['pipes:read', 'custom:read']


def test_edit_users_stores_a_star_literally(instance, monkeypatch):
    from meerschaum.actions.edit import _edit_users
    _fake_prompts(monkeypatch, '*')
    success, msg = _edit_users(
        action=['scope_user'],
        mrsm_instance=instance.keys,
        noask=True,
    )
    assert success, msg
    assert User('scope_user', instance=instance.conn).get_scopes(refresh=True) == ['*']


def test_register_tokens_keeps_a_star_literally(tokens_instance, monkeypatch):
    from meerschaum.actions.register import _register_tokens
    _fake_prompts(monkeypatch, '')
    success, msg = _register_tokens(
        mrsm_instance=str(tokens_instance),
        name='star-token',
        scopes=['*'],
    )
    assert success, msg
    (token_id,) = tokens_instance.tokens
    assert tokens_instance.scopes(token_id) == ['*']


def test_default_scopes_are_the_built_in_list(tokens_instance, monkeypatch):
    """
    A token created without scopes still gets every built-in scope (not `*`).
    """
    from meerschaum.actions.register import _register_tokens
    default_scopes = list(STATIC_CONFIG['tokens']['scopes'])
    assert Token(label='defaults').scopes == default_scopes
    _fake_prompts(monkeypatch, '')
    success, msg = _register_tokens(
        mrsm_instance=str(tokens_instance),
        name='default-token',
    )
    assert success, msg
    (token_id,) = tokens_instance.tokens
    assert tokens_instance.scopes(token_id) == default_scopes


def test_token_request_model_keeps_a_star():
    from meerschaum.api.models import RegisterTokenRequestModel
    assert RegisterTokenRequestModel(scopes=['*']).scopes == ['*']
    assert Token(scopes=['*']).scopes == ['*']


@pytest.fixture
def non_admin_allowed(monkeypatch):
    real_get_config = mrsm.get_config

    def get_config(*keys, **kw):
        if keys == ('system', 'api', 'permissions', 'actions', 'non_admin'):
            return True
        return real_get_config(*keys, **kw)

    monkeypatch.setattr(mrsm, 'get_config', get_config)


@pytest.mark.parametrize(
    'scopes,allowed',
    [
        (['*'], True),
        (['actions:execute', 'jobs:execute'], True),
        (['jobs:execute'], False),
        (['actions:execute'], False),
        (['pipes:read'], False),
        ([], False),
    ],
)
def test_star_satisfies_the_necessary_scopes(non_admin_allowed, scopes, allowed):
    from meerschaum.core.User import is_user_allowed_to_execute
    principal = SimpleNamespace(get_scopes=lambda refresh=False, debug=False: list(scopes))
    success, msg = is_user_allowed_to_execute(principal)
    assert success is allowed, msg
    if not allowed:
        assert 'Missing scope' in msg


def test_admins_and_no_auth_are_allowed(non_admin_allowed):
    from meerschaum.core.User import is_user_allowed_to_execute
    assert is_user_allowed_to_execute(None)[0]


@pytest.fixture
def api(monkeypatch):
    """
    The API app (which needs its optional dependencies), authenticated as whichever scopes
    the test sets.
    """
    pytest.importorskip('fastapi')
    pytest.importorskip('httpx')
    from fastapi.testclient import TestClient
    import meerschaum.api._oauth2 as oauth2
    from meerschaum.api import app

    granted = SimpleNamespace(scopes=[])

    async def load_user_or_token(request):
        return None

    monkeypatch.setattr(oauth2, 'load_user_or_token', load_user_or_token)
    monkeypatch.setattr(oauth2, 'get_granted_scopes', lambda request, principal, **kw: granted.scopes)
    client = TestClient(app, raise_server_exceptions=False)
    client.granted = granted
    return client


@pytest.fixture
def stopped_job():
    from meerschaum._internal.entry import entry
    from meerschaum.utils.daemon import Daemon
    name = f"scopetest-{uuid.uuid4().hex[:8]}"
    daemon = Daemon(
        target=entry,
        target_args=[['show', 'version']],
        target_kw={},
        daemon_id=name,
        label='show version',
        properties={'process': {'began': '2020-01-01T00:00:00', 'ended': '2020-01-01T00:00:01'}},
    )
    daemon.path.mkdir(parents=True, exist_ok=True)
    assert daemon.write_properties()[0]
    assert daemon.write_pickle()[0]
    yield name
    daemon.cleanup()


@pytest.mark.parametrize(
    'scopes,status_code',
    [
        (['*'], 200),
        (['jobs:execute', 'jobs:stop'], 200),
        (['jobs:execute', 'josb:stop'], 200),
        (['jobs:execute'], 403),
        (['jobs:stop'], 403),
        (['josb:stop'], 403),
        (['pipes:read'], 403),
    ],
)
def test_stop_route_needs_jobs_stop(api, stopped_job, scopes, status_code):
    api.granted.scopes = scopes
    response = api.post(f'/jobs/{stopped_job}/stop')
    assert response.status_code == status_code, response.text
    if status_code == 403:
        assert 'Missing required scope' in response.text
        assert "'jobs:stop'" in response.text or "'jobs:execute'" in response.text


def test_stop_route_names_the_current_scope(api, stopped_job):
    api.granted.scopes = ['jobs:execute']
    response = api.post(f'/jobs/{stopped_job}/stop')
    assert response.status_code == 403
    assert "Missing required scope: 'jobs:stop'" in response.text
    assert 'josb' not in response.text


def test_dash_token_form_grants_all_scopes_with_a_star():
    """
    The console's "Grant all scopes" switch registers a literal `*`.
    The callback is called in a fresh interpreter because importing it boots the Dash app.
    """
    import importlib.util
    import subprocess
    import sys
    if importlib.util.find_spec('dash_bootstrap_components') is None:
        pytest.skip("Dash is not installed.")

    code = (
        "import json\n"
        "import meerschaum.api.dash.callbacks.tokens as t\n"
        "captured = []\n"
        "class FakeToken:\n"
        "    def __init__(self, **kw):\n"
        "        captured.append(kw['scopes'])\n"
        "t.Token = FakeToken\n"
        "t.build_tokens_register_output_modal = lambda token: ['modal']\n"
        "t.get_user_from_session = lambda session_id: None\n"
        "t.register_token_click(1, 'all', ['pipes:read'], True, None, {})\n"
        "t.register_token_click(1, 'some', ['pipes:read'], False, None, {})\n"
        "print('SCOPES=' + json.dumps(captured))\n"
    )
    result = subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True,
        text=True,
        timeout=600,
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith('SCOPES=')]
    assert lines, result.stderr[-2000:]
    assert json.loads(lines[0][len('SCOPES='):]) == [['*'], ['pipes:read']]


def test_register_tokens_nopretty_prints_json(tokens_instance, monkeypatch, capsys):
    """
    `register tokens --nopretty` prints the token (with its UUID and datetimes) as JSON.
    """
    from meerschaum.actions.register import _register_tokens
    _fake_prompts(monkeypatch, '')
    success, msg = _register_tokens(
        mrsm_instance=str(tokens_instance),
        name='json-token',
        scopes=['*'],
        nopretty=True,
    )
    assert success, msg
    printed = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    (token_id,) = tokens_instance.tokens
    assert printed['id'] == str(token_id)
    assert printed['label'] == 'json-token'
    assert printed['scopes'] == ['*']
    assert printed['secret']


def test_dash_edit_token_form_keeps_a_star():
    """
    The console's edit form shows "Grant all scopes" on (and the checklist hidden) for a token
    which holds `*`, and saves `['*']` unless the switch is turned off.
    """
    import importlib.util
    import subprocess
    import sys
    if importlib.util.find_spec('dash_bootstrap_components') is None:
        pytest.skip("Dash is not installed.")

    code = '''
import json
from types import SimpleNamespace
import meerschaum.api.dash.callbacks.tokens as t
import meerschaum.api.dash.tokens as forms

def walk(component):
    yield component
    children = getattr(component, 'children', None)
    if isinstance(children, (list, tuple)):
        for child in children:
            yield from walk(child)
    elif children is not None and not isinstance(children, (str, int, float)):
        yield from walk(children)

def form_state(scopes):
    token = SimpleNamespace(
        id='11111111-1111-1111-1111-111111111111',
        label='label',
        expiration=None,
        is_valid=True,
        scopes=scopes,
    )
    components = list(walk(forms.build_edit_token_modal(token)))
    def find(kind):
        for component in components:
            component_id = getattr(component, 'id', None)
            if isinstance(component_id, dict) and component_id.get('type') == kind:
                return component
    switch = find('tokens-toggle-scopes-switch')
    div = find('tokens-scopes-checklist-div')
    checklist = find('tokens-scopes-checklist')
    return {
        'switch': switch.value,
        'hidden': (getattr(div, 'style', None) or {}).get('display') == 'none',
        'checked': list(checklist.value),
    }

saved = []
class FakeToken:
    def __init__(self, **kw):
        saved.append(kw['scopes'])
    def edit(self, debug=False):
        return True, 'Success'

t.Token = FakeToken
t.check_token_access = lambda token_id, session: (True, '')
t.get_api_connector = lambda: None
token_id = '11111111-1111-1111-1111-111111111111'
t.dash = SimpleNamespace(
    callback_context=SimpleNamespace(triggered=[{
        'value': 1,
        'prop_id': json.dumps({'index': token_id, 'type': 'tokens-edit-submit-button'}) + '.n_clicks',
    }]),
    no_update=None,
)
for grant_all in (True, False, None):
    t.edit_token_submit_button_click(1, None, ['pipes:read'], 'label', grant_all, {})

print('RESULT=' + json.dumps({
    'star': form_state(['*']),
    'some': form_state(['pipes:read']),
    'saved': saved,
}))
'''
    result = subprocess.run(
        [sys.executable, '-c', code],
        capture_output=True,
        text=True,
        timeout=600,
    )
    lines = [line for line in result.stdout.splitlines() if line.startswith('RESULT=')]
    assert lines, result.stderr[-3000:]
    data = json.loads(lines[0][len('RESULT='):])
    assert data['star']['switch'] is True
    assert data['star']['hidden'] is True
    assert data['some']['switch'] is False
    assert data['some']['hidden'] is False
    assert data['some']['checked'] == ['pipes:read']
    assert data['saved'] == [['*'], ['pipes:read'], ['pipes:read']]
