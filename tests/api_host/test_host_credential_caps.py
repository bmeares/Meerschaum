"""
A plugin route on the API host (`ScopedAuth`) decides like the Rust server: a credential holds
no more than its owner holds now. A deleted owner's API key is refused, a demoted
administrator's JWT keeps only the new role's permissions, and a JWT minted from an API token
keeps no more than the token holds now. The rule is one implementation
(`meerschaum.core.access.credential_permissions`, the core's `credential_permissions`).
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
import fastapi
from meerschaum.plugins import api_plugin

@api_plugin
def caps_routes(app):
    from meerschaum.api import ScopedAuth

    @app.get('/caps/read')
    def read(curr=fastapi.Depends(ScopedAuth(['pipes:read']))):
        return True

    @app.get('/caps/write')
    def write(curr=fastapi.Depends(ScopedAuth(['pipes:write']))):
        return True

    @app.get('/caps/who')
    def who(curr=fastapi.Depends(ScopedAuth(['pipes:read']))):
        return {'kind': type(curr).__name__, 'name': str(getattr(curr, 'username', None) or curr.id)}
'''

PRELUDE = """
import json
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api import get_api_connector, manager
from meerschaum.core import User, Token

client = TestClient(build_app())
instance = get_api_connector()

def status(path, credential):
    return client.get(path, headers={'Authorization': 'Bearer ' + credential}).status_code
"""


def _json(result) -> dict:
    lines = [line for line in result.stdout.splitlines() if line.startswith('JSON:')]
    assert lines, result.stdout + result.stderr
    return json.loads(lines[-1][len('JSON:'):])


def test_a_deleted_owners_api_key_is_refused(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'caps_plugin', PLUGIN)
    result = run_python(root, plugins, PRELUDE + """
assert instance.register_user(User('caps-admin', 'probe-password', type='admin', instance=instance))[0]
owner = User('caps-owner', 'probe-password', instance=instance)
assert instance.register_user(owner)[0]
token = Token(label='caps-key', user=owner, scopes=['pipes:read'], instance=instance)
assert token.register()[0]
key = token.get_api_key()
out = {'present': status('/caps/read', key)}
assert instance.delete_user(owner)[0]
out['deleted'] = status('/caps/read', key)
print('JSON:' + json.dumps(out))
""")
    assert _json(result) == {'present': 200, 'deleted': 401}


def test_a_demoted_administrators_jwt_keeps_only_the_new_role(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'caps_plugin', PLUGIN)
    result = run_python(root, plugins, PRELUDE + """
boss = User('caps-boss', 'probe-password', type='admin', instance=instance)
assert instance.register_user(boss)[0]
jwt = manager.create_access_token(data={'sub': boss.username}, scopes=['*'])
out = {'admin_write': status('/caps/write', jwt)}
demoted = User('caps-boss', '', type='regular', attributes={'role': 'viewer'}, instance=instance)
assert instance.edit_user(demoted)[0]
assert instance.get_user_type(User('caps-boss', instance=instance)) != 'admin'
out['viewer_write'] = status('/caps/write', jwt)
out['viewer_read'] = status('/caps/read', jwt)
print('JSON:' + json.dumps(out))
""")
    assert _json(result) == {'admin_write': 200, 'viewer_write': 403, 'viewer_read': 200}


def test_a_token_jwt_keeps_no_more_than_its_token(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'caps_plugin', PLUGIN)
    result = run_python(root, plugins, PRELUDE + """
holder = User('caps-holder', 'probe-password', type='admin', instance=instance)
assert instance.register_user(holder)[0]
token = Token(
    label='caps-narrowed', user=holder, scopes=['pipes:read', 'pipes:write'], instance=instance,
)
assert token.register()[0]
jwt = manager.create_access_token(
    data={'sub': str(token.id), 'mrsm_principal': 'token'}, scopes=['pipes:read', 'pipes:write'],
)
out = {'before': status('/caps/write', jwt)}
token.scopes = ['pipes:read']
assert token.edit()[0]
assert instance.get_token_scopes(token) == ['pipes:read']
out['after_write'] = status('/caps/write', jwt)
out['after_read'] = status('/caps/read', jwt)
print('JSON:' + json.dumps(out))
""")
    assert _json(result) == {'before': 200, 'after_write': 403, 'after_read': 200}


def test_credential_fingerprints_match_the_rust_server(sandbox):
    """The Rust server's `credential_fingerprint` test pins the same vector."""
    root, plugins = sandbox
    result = run_python(root, plugins, """
import json
import meerschaum.api._oauth2 as oauth2
oauth2.SECRET = b'0123456789abcdef'
print('JSON:' + json.dumps(oauth2.credential_fingerprint('$pbkdf2-sha256$1$c2FsdA$aGFzaA')))
""")
    assert _json(result) == 'yTngG9oGz5X69a6RX22jLw'


def test_user_jwts_end_with_a_password_change_or_a_reregistered_name(sandbox):
    """
    A user's JWT carries their credential: a password change or a deleted and re-registered
    username ends it, and a JWT without one (issued before 5.0 bound JWTs) is refused.
    """
    root, plugins = sandbox
    write_plugin(plugins, 'caps_plugin', PLUGIN)
    result = run_python(root, plugins, PRELUDE + """
import jwt as pyjwt
from datetime import datetime, timedelta, timezone
from meerschaum.api._oauth2 import SECRET

name = 'caps-renamed'
assert instance.register_user(User(name, 'first-password', instance=instance))[0]
token = manager.create_access_token(data={'sub': name}, scopes=['pipes:read'])
claimless = pyjwt.encode(
    {'sub': name, 'scopes': ['pipes:read'], 'exp': datetime.now(timezone.utc) + timedelta(hours=1)},
    SECRET, 'HS256',
)
out = {'bound': status('/caps/read', token), 'claimless': status('/caps/read', claimless)}

assert instance.edit_user(User(name, 'second-password', instance=instance))[0]
out['after_password_change'] = status('/caps/read', token)

fresh = manager.create_access_token(data={'sub': name}, scopes=['pipes:read'])
out['fresh'] = status('/caps/read', fresh)
assert instance.delete_user(User(name, instance=instance))[0]
assert instance.register_user(User(name, 'third-password', instance=instance))[0]
out['after_reregistration'] = status('/caps/read', fresh)
print('JSON:' + json.dumps(out))
""")
    assert _json(result) == {
        'bound': 200,
        'claimless': 401,
        'after_password_change': 401,
        'fresh': 200,
        'after_reregistration': 401,
    }


def test_console_sessions_are_bound_to_the_users_credential(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, """
import json, uuid
from meerschaum.api import get_api_connector
from meerschaum.api._oauth2 import user_credential
from meerschaum.api.dash.sessions import set_session, get_username_from_session
from meerschaum.core import User

instance = get_api_connector()
name = 'caps-console'
assert instance.register_user(User(name, 'first-password', instance=instance))[0]
bound, legacy = str(uuid.uuid4()), str(uuid.uuid4())
set_session(bound, {'username': name, 'credential': user_credential(name)})
set_session(legacy, {'username': name})
out = {'bound': get_username_from_session(bound), 'legacy': get_username_from_session(legacy)}
assert instance.delete_user(User(name, instance=instance))[0]
assert instance.register_user(User(name, 'second-password', instance=instance))[0]
from meerschaum.api.dash import sessions
sessions._credential_checks.clear()
out['after_reregistration'] = get_username_from_session(bound)
print('JSON:' + json.dumps(out))
""")
    assert _json(result) == {'bound': 'caps-console', 'legacy': None, 'after_reregistration': None}


def test_a_username_that_looks_like_a_token_id_stays_a_user(sandbox):
    """
    The issuer's `mrsm_principal` decides the principal: a user named after another user's token
    ID is that user, never the token, and a JWT that names no kind is refused.
    """
    root, plugins = sandbox
    write_plugin(plugins, 'caps_plugin', PLUGIN)
    result = run_python(root, plugins, PRELUDE + """
import uuid
import jwt as pyjwt
from datetime import datetime, timedelta, timezone
from meerschaum.api._oauth2 import SECRET

owner = User('caps-uuid-owner', 'probe-password', type='admin', instance=instance)
assert instance.register_user(owner)[0]
token = Token(label='caps-uuid-key', user=owner, scopes=['pipes:read'], instance=instance)
assert token.register()[0]
twin = User(str(token.id), 'twin-password', instance=instance)
assert instance.register_user(twin)[0]
loner = User(str(uuid.uuid4()), 'loner-password', instance=instance)
assert instance.register_user(loner)[0]

def who(credential):
    r = client.get('/caps/who', headers={'Authorization': 'Bearer ' + credential})
    return [r.status_code, r.json() if r.status_code == 200 else None]

twin_jwt = manager.create_access_token(data={'sub': twin.username}, scopes=['pipes:read'])
token_jwt = manager.create_access_token(
    data={'sub': str(token.id), 'mrsm_principal': 'token'}, scopes=['pipes:read'],
)
loner_jwt = manager.create_access_token(data={'sub': loner.username}, scopes=['pipes:read'])
kindless = pyjwt.encode(
    {'sub': str(token.id), 'scopes': ['pipes:read'],
     'exp': datetime.now(timezone.utc) + timedelta(hours=1)},
    SECRET, 'HS256',
)
out = {
    'twin': who(twin_jwt),
    'token': who(token_jwt),
    'loner': who(loner_jwt),
    'kindless': who(kindless)[0],
}
print('JSON:' + json.dumps(out))
""")
    out = _json(result)
    assert out['twin'][0] == 200 and out['twin'][1]['kind'] == 'User', out
    assert out['token'][0] == 200 and out['token'][1]['kind'] == 'Token', out
    assert out['loner'][0] == 200 and out['loner'][1]['kind'] == 'User', out
    assert out['kindless'] == 401
