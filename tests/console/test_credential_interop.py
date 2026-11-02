"""User and token credentials cross the real Rust / Python host boundary."""

import json
import urllib.error
import urllib.parse
import urllib.request

import pytest

from tests.console.conftest import PASSWORD, USERNAME, Server

pytestmark = pytest.mark.console

PLUGIN = '''
import fastapi
from meerschaum.plugins import api_plugin

@api_plugin
def routes(app):
    from meerschaum.api import ScopedAuth

    @app.get('/credential-interop/who')
    def who(principal=fastapi.Depends(ScopedAuth(['pipes:read']))):
        return {'kind': type(principal).__name__,
                'name': str(getattr(principal, 'username', None) or principal.id)}
'''


@pytest.fixture
def auth_server():
    server = Server('credential-interop', args=['--private'], native=False)
    plugin = server.plugins / 'credential_interop'
    plugin.mkdir()
    (plugin / '__init__.py').write_text(PLUGIN)
    try:
        server.start()
        yield server
    finally:
        server.stop()


def request(server, path, token=None, form=None, json_body=None):
    assert form is None or json_body is None
    headers = {'Authorization': 'Bearer ' + token} if token else {}
    data = urllib.parse.urlencode(form).encode() if form is not None else None
    if json_body is not None:
        data = json.dumps(json_body).encode()
        headers['Content-Type'] = 'application/json'
    req = urllib.request.Request(server.base_url + path, data=data, headers=headers)
    try:
        response = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.loads(response.read())


def python_json(server, code):
    result = server.python('''
import json, os
os.environ['MRSM_API_HOST'] = '1'
os.environ['MRSM_API_HOST_CONFIG'] = json.dumps({'mrsm_instance': 'sql:w7console'})
import meerschaum as mrsm
from meerschaum.api import manager
from meerschaum.core import User, Token
conn = mrsm.get_connector('sql:w7console')
''' + code)
    assert result.returncode == 0, result.stderr[-3000:]
    values = [line[8:] for line in result.stdout.splitlines() if line.startswith('INTEROP:')]
    assert len(values) == 1, result.stdout[-2000:]
    return json.loads(values[0])


def login(server, username=USERNAME, password=PASSWORD):
    status, body = request(server, '/login', form={'username': username, 'password': password})
    assert status == 200, body
    return body['access_token']


def assert_user(server, token, username):
    status, user = request(server, '/users/me', token)
    assert status == 200, user
    assert user['username'] == username
    assert request(server, '/credential-interop/who', token) == (
        200, {'kind': 'User', 'name': username},
    )


def test_user_jwts_from_either_issuer_end_when_the_password_changes(auth_server):
    server = auth_server
    rust_jwt = login(server)
    issued = python_json(server, '''
import jwt
from datetime import datetime, timedelta, timezone
from meerschaum.api._oauth2 import SECRET
token = manager.create_access_token(data={'sub': 'alice'}, scopes=['*'])
legacy = jwt.encode({'sub': 'alice', 'scopes': ['*'],
    'exp': datetime.now(timezone.utc) + timedelta(hours=1)}, SECRET, 'HS256')
print('INTEROP:' + json.dumps({'token': token, 'legacy': legacy}))
''')
    for token in (rust_jwt, issued['token']):
        assert_user(server, token, USERNAME)
    for path in ('/users/me', '/credential-interop/who'):
        assert request(server, path, issued['legacy'])[0] == 401

    python_json(server, '''
assert conn.edit_user(User('alice', 'replacement-password', instance=conn))[0]
print('INTEROP:true')
''')
    for token in (rust_jwt, issued['token']):
        for path in ('/users/me', '/credential-interop/who'):
            assert request(server, path, token)[0] == 401
    assert_user(server, login(server, password='replacement-password'), USERNAME)


def test_uuid_username_and_token_jwts_keep_their_principal_and_current_scopes(auth_server):
    server = auth_server
    issued = python_json(server, '''
owner = User('alice', instance=conn)
token = Token(label='interop-key', user=owner, scopes=['pipes:read'], instance=conn)
assert token.register()[0]
twin = User(str(token.id), 'twin-password', instance=conn)
assert conn.register_user(twin)[0]
print('INTEROP:' + json.dumps({
    'id': str(token.id), 'key': token.get_api_key(),
    'user_jwt': manager.create_access_token(data={'sub': twin.username}, scopes=['*']),
    'token_jwt': manager.create_access_token(
        data={'sub': str(token.id), 'mrsm_principal': 'token'}, scopes=['pipes:read']),
}))
''')
    assert_user(server, issued['user_jwt'], issued['id'])

    assert_user(server, login(server, issued['id'], 'twin-password'), issued['id'])
    empty = python_json(server, '''
token = Token(label='no-permissions', user=User('alice', instance=conn),
              scopes=[], instance=conn)
assert token.register()[0]
assert conn.get_token_scopes(token) == []
assert conn.get_token(token.id).scopes == []
print('INTEROP:' + json.dumps(token.get_api_key()))
''')
    for path in ('/pipes', '/credential-interop/who'):
        assert request(server, path, empty)[0] == 403
    status, result = request(server, '/login', token=issued['key'], form={})
    assert status == 200, result
    token_credentials = (issued['key'], issued['token_jwt'], result['access_token'])
    for token in token_credentials:
        assert request(server, '/pipes', token)[0] == 200
        assert request(server, '/credential-interop/who', token) == (
            200, {'kind': 'Token', 'name': issued['id']},
        )

    python_json(server, f'''
token = conn.get_token({issued['id']!r})
assert token is not None
token.scopes = []
assert token.edit()[0]
assert conn.get_token_scopes(token) == []
print('INTEROP:true')
''')
    for token in token_credentials:
        for path in ('/pipes', '/credential-interop/who'):
            assert request(server, path, token)[0] == 403
    python_json(server, f'''
token = conn.get_token({issued['id']!r})
assert token is not None and token.scopes == []
assert token.invalidate()[0]
print('INTEROP:true')
''')
    for token in token_credentials:
        for path in ('/pipes', '/credential-interop/who'):
            assert request(server, path, token)[0] == 401
    assert_user(server, issued['user_jwt'], issued['id'])


def test_http_empty_scopes_grant_nothing_and_omitted_edits_keep_the_current_scopes(auth_server):
    server = auth_server
    admin = login(server)
    python_json(server, '''
assert conn.register_user(User('limited-holder', 'holder-password', instance=conn))[0]
print('INTEROP:true')
''')
    holder = login(server, 'limited-holder', 'holder-password')
    for caller in (admin, holder):
        status, defaults = request(server, '/tokens/register', caller,
                                   json_body={'label': 'default-token'})
        assert status == 200, defaults
        assert request(server, '/pipes', defaults['api_key'])[0] == 200
        status, empty = request(server, '/tokens/register', caller,
                                json_body={'label': 'empty-token', 'scopes': []})
        assert status == 200, empty
        status, model = request(server, '/tokens/' + empty['id'], caller)
        assert status == 200 and model['scopes'] == [], model
        for path in ('/pipes', '/credential-interop/who'):
            assert request(server, path, empty['api_key'])[0] == 403

        status, key = request(server, '/tokens/register', caller,
                              json_body={'label': 'read-token', 'scopes': ['pipes:read']})
        assert status == 200, key
        assert request(server, '/pipes', key['api_key'])[0] == 200
        status, model = request(server, '/tokens/' + key['id'], caller)
        assert status == 200, model
        edit = {field: model[field] for field in ('creation', 'expiration', 'label')}
        edit_path = '/tokens/' + key['id'] + '/edit'
        status, result = request(server, edit_path, caller, json_body=edit)
        assert status == 200 and result[0] is True, result
        assert request(server, '/tokens/' + key['id'], caller)[1]['scopes'] == ['pipes:read']
        status, result = request(server, edit_path, caller, json_body={**edit, 'scopes': []})
        assert status == 200 and result[0] is True, result
        assert request(server, '/tokens/' + key['id'], caller)[1]['scopes'] == []
        for path in ('/pipes', '/credential-interop/who'):
            assert request(server, path, key['api_key'])[0] == 403
        status, result = request(server, edit_path, caller, json_body=edit)
        assert status == 200 and result[0] is True, result
        assert request(server, '/tokens/' + key['id'], caller)[1]['scopes'] == []
