#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test how the Python API host and web console grant and check scopes (no API server).

- A literal `*` grants every scope, including custom and future scopes, so it is stored as is.
- `*` satisfies the scopes which are necessary to execute actions and jobs.
- The web console's token forms grant all scopes with a literal `*`.

The `edit` / `register` actions and the `/jobs/{name}/stop` route are native in Meerschaum 5
(`crates/meerschaum-cli/src/native/{tokens,users}.rs`, `crates/meerschaum-server/tests/routes.rs`).
"""

import json
from types import SimpleNamespace

import pytest

import meerschaum as mrsm
from meerschaum.core import Token


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
