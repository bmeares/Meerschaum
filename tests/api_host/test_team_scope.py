#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The lookup check for isolated teams in the processes every team shares (5.1): while the API host
serves a request of an isolated team's member, or a worker runs a call the server made for such a
team, `get_connector()` refuses and `get_config()` omits the connectors outside the team's set.
Plugin code cannot set or clear the team through the public API, and threads it starts keep it.
"""


from tests.api_host._helpers import SIGN_IN, run_python, write_plugin, extract_json


PROBE = '''
def probe():
    import os
    import meerschaum as mrsm
    from meerschaum.config import get_config
    out = {}
    for keys in ('sql:wetdb', 'sql:drydb', 'sql:main', 'sql:apihosttest'):
        try:
            conn = mrsm.get_connector(keys)
            out[keys] = 'built' if conn is not None else 'undefined'
        except PermissionError as e:
            out[keys] = str(e)
    out['sql_labels'] = sorted((get_config('meerschaum', 'connectors', 'sql') or {}).keys())
    out['whole'] = sorted(get_config()['meerschaum']['connectors']['sql'].keys())
    out['drydb_attrs'] = get_config('meerschaum', 'connectors', 'sql', 'drydb', warn=False)
    out['as_tuple'] = list(get_config('meerschaum', 'connectors', 'sql', 'main', as_tuple=True, warn=False))
    return out
'''


def test_the_lookup_check_in_a_shared_worker_and_its_limits(sandbox):
    root, plugins = sandbox
    out = extract_json(run_python(root, plugins, PROBE + '''
import json, threading
from concurrent.futures import ThreadPoolExecutor
import meerschaum as mrsm
from meerschaum._internal import team_scope
mrsm.get_connector('sql', 'wetdb', flavor='sqlite', database=':memory:')
mrsm.get_connector('sql', 'drydb', flavor='sqlite', database=':memory:')
team_scope.install_thread_propagation()
result = {'open': probe()}

### What the worker does for a call the host made for an isolated team (`team_scope` of the call).
token = team_scope._enter({
    'team': 'wetlands',
    'connectors': ['sql:wetdb'],
    'configured': ['sql:wetdb', 'sql:drydb', 'sql:main', 'sql:apihosttest'],
})
result['team'] = probe()

### Threads the team's code starts keep the team.
seen = {}
def in_thread():
    seen['thread'] = probe()['sql:main']
t = threading.Thread(target=in_thread)
t.start(); t.join()
with ThreadPoolExecutor(1) as pool:
    seen['pool'] = pool.submit(lambda: probe()['sql:main']).result()
result['threads'] = seen

### Plugin code cannot widen it through the public API.
widen = {}
try:
    mrsm.get_connector('sql', 'main', refresh=True)
    widen['refresh'] = 'built'
except PermissionError as e:
    widen['refresh'] = str(e)
try:
    mrsm.get_connector('sql', 'main', flavor='sqlite', database=':memory:')
    widen['attributes'] = 'built'
except PermissionError as e:
    widen['attributes'] = str(e)
try:
    mrsm.Pipe('a', 'b', instance='sql:main').instance_connector
    widen['pipe'] = 'built'
except PermissionError as e:
    widen['pipe'] = str(e)
### A value that is not the server's scope counts as a team with no connector at all.
inner = team_scope._SCOPE.set(None)
widen['junk_wetdb'] = probe()['sql:wetdb']
team_scope._SCOPE.reset(inner)
### A scope object cannot be changed.
try:
    team_scope.current()._connectors = frozenset({'sql:main'})
    widen['mutate'] = 'changed'
except AttributeError:
    widen['mutate'] = 'refused'
result['widen'] = widen

team_scope._exit(token)
result['after'] = probe()['sql:main']
print('JSON:' + json.dumps(result, default=str))
''', host_mode=False))
    refused = "The connector 'sql:main' is not available to your teams."
    assert out['open']['sql:main'] == 'built'
    assert out['open']['sql:drydb'] == 'built'
    team = out['team']
    assert team['sql:wetdb'] == 'built'
    assert team['sql:drydb'] == "The connector 'sql:drydb' is not available to your teams."
    assert team['sql:main'] == refused
    assert team['sql:apihosttest'] == "The connector 'sql:apihosttest' is not available to your teams."
    assert 'drydb' not in team['sql_labels'] and 'main' not in team['sql_labels']
    assert team['whole'] == team['sql_labels']
    assert team['drydb_attrs'] is None
    assert team['as_tuple'] == [False, None]
    assert out['threads'] == {'thread': refused, 'pool': refused}
    widen = out['widen']
    assert widen['refresh'] == refused
    assert widen['attributes'] == refused
    assert widen['pipe'] == refused
    assert widen['junk_wetdb'] == "The connector 'sql:wetdb' is not available to your teams."
    assert widen['mutate'] == 'refused'
    assert out['after'] == 'built'


def test_the_lookup_check_in_the_api_host(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'tpr_scope_probe', '''
        from meerschaum.plugins import api_plugin

        @api_plugin
        def init(app):
            @app.get('/tpr-scope-probe')
            def tpr_scope_probe():
''' + '\n'.join('                ' + line for line in PROBE.splitlines()) + '''
                return probe()
    ''')
    out = extract_json(run_python(root, plugins, SIGN_IN + '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.host import build_app
conn = get_api_connector()
mrsm.get_connector('sql', 'wetdb', flavor='sqlite', database=':memory:', teams=['wetlands'])
mrsm.get_connector('sql', 'drydb', flavor='sqlite', database=':memory:', teams=['drylands'])
for name, attrs in (('wetlands', {'isolated': True, 'instance': 'sql:wetdb'}),
                    ('drylands', {'isolated': True}), ('open', {})):
    team = mrsm.Team(name, instance=conn, attributes=attrs)
    if not team.exists():
        assert team.register()[0]
SESSIONS = {
    'ann': sign_in('tpr-ann', teams={'wetlands': 'member'}),
    'mixed': sign_in('tpr-mixed', teams={'wetlands': 'member', 'open': 'member'}),
    'admin': sign_in('tpr-admin', user_type='admin'),
}
app = build_app()
from starlette.testclient import TestClient
client = TestClient(app)
result = {}
for who, cookies in SESSIONS.items():
    client.cookies.clear()
    client.cookies.set('mrsm-session-id', cookies['session-id'])
    result[who] = client.get('/tpr-scope-probe').json()
### A client cannot name a team or a scope: nothing it sends is read.
client.cookies.clear()
client.cookies.set('mrsm-session-id', SESSIONS['ann']['session-id'])
result['ann_header'] = client.get(
    '/tpr-scope-probe',
    headers={'X-Mrsm-Team-Scope': json.dumps({'team': 'x', 'connectors': ['sql:main']})},
).json()
client.cookies.clear()
result['anonymous'] = client.get('/tpr-scope-probe').json()
print('JSON:' + json.dumps(result, default=str))
'''))
    ann = out['ann']
    assert ann['sql:wetdb'] == 'built'
    assert ann['sql:drydb'] == "The connector 'sql:drydb' is not available to your teams."
    assert ann['sql:main'] == "The connector 'sql:main' is not available to your teams."
    assert 'drydb' not in ann['sql_labels'] and 'apihosttest' not in ann['sql_labels']
    assert out['ann_header'] == ann
    ### The open side and administrators: unchanged.
    for who in ('mixed', 'admin', 'anonymous'):
        assert out[who]['sql:main'] == 'built', (who, out[who])
        assert out[who]['sql:drydb'] == 'built', (who, out[who])


def test_isolated_team_member_lists_and_registers_tokens_with_a_server_cache(sandbox):
    root, plugins = sandbox
    out = extract_json(run_python(root, plugins, SIGN_IN + '''
import json
import meerschaum as mrsm
from meerschaum.api import get_api_connector
from meerschaum.api.host import build_app
from meerschaum.core import Token, User
from starlette.testclient import TestClient

conn = get_api_connector()
team_conn = mrsm.get_connector(
    'sql', 'token_team', flavor='sqlite', database=':memory:', teams=['token-team']
)
assert mrsm.Team('token-team', instance=conn, attributes={
    'isolated': True,
    'instance': str(team_conn),
}).register()[0]
session = sign_in('team-token-user', role='viewer', teams={'token-team': 'member'})
owner = User('team-token-user', instance=conn)
assert Token('existing-token', instance=conn, user=owner, scopes=['pipes:read']).register()[0]
### Simulate the configured production cache without connecting to Valkey.
del conn._tokens_pipe
conn._cache_connector = 'valkey:main'
client = TestClient(build_app())
client.cookies.set('mrsm-session-id', session['session-id'])
dependencies = client.get('/dash/_dash-dependencies').json()

def click(button, state=()):
    output = next(
        dep['output'] for dep in dependencies
        if dep['inputs'] == [{'id': button, 'property': 'n_clicks'}]
    )
    return client.post('/dash/_dash-update-component', json={
        'output': output,
        'outputs': [
            {'id': component, 'property': prop.split('@')[0]}
            for component, prop in (
                part.split('.', 1) for part in output.strip('.').split('...')
            )
        ],
        'inputs': [{'id': button, 'property': 'n_clicks', 'value': 1}],
        'changedPropIds': [button + '.n_clicks'],
        'state': [*state, {'id': 'session-store', 'property': 'data', 'value': session}],
    })

listing = click('tokens-refresh-button')
response = click('tokens-register-button', [
    {'id': 'tokens-name-input', 'property': 'value', 'value': 'team-read'},
    {'id': 'tokens-scopes-checklist', 'property': 'value', 'value': ['pipes:read']},
    {'id': 'tokens-toggle-scopes-switch', 'property': 'value', 'value': False},
    {'id': 'tokens-expiration-datepickersingle', 'property': 'date', 'value': None},
])
del conn._cache_connector
del conn._tokens_pipe
tokens = conn.get_tokens(user=owner, labels=['team-read'])
print('JSON:' + json.dumps({
    'listing_status': listing.status_code,
    'existing_token_visible': 'existing-token' in listing.text,
    'registration_status': response.status_code,
    'credentials_shown': 'Client Secret' in response.text and 'mrsm-key:' in response.text,
    'scopes': [token.scopes for token in tokens],
    'owners': [token.user.username for token in tokens],
}))
''', extra_env={
        'MRSM_PATCH': '{"system":{"experimental":{"valkey_session_cache":true}}}',
        'MRSM_VALKEY_MAIN': '{"host":"127.0.0.1","port":1}',
    }))
    assert out == {
        'listing_status': 200,
        'existing_token_visible': True,
        'registration_status': 200,
        'credentials_shown': True,
        'scopes': [['pipes:read']],
        'owners': ['team-token-user'],
    }
