#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`from meerschaum.api import session_user, optional_session_user, mount_static` in `@api_plugin`
routes (rc15): the console session's user as a FastAPI dependency, and static directories served
safely (no path outside the directory, no hidden files, web content types, `nosniff`), behind a
console session or an API token unless `auth=False`, with browsers sent to the login page.
"""

import json
import os

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
import pathlib
from meerschaum.plugins import api_plugin

HERE = pathlib.Path(__file__).parent

@api_plugin
def init(app):
    import fastapi
    from meerschaum.api import session_user, optional_session_user, mount_static

    @app.get('/helpers/me')
    def me(user=fastapi.Depends(session_user)):
        return {'username': user.username, 'type': user.type}

    @app.get('/helpers/maybe')
    def maybe(user=fastapi.Depends(optional_session_user)):
        return {'username': user.username if user is not None else None}

    mount_static(app, '/helpers/files', HERE / 'files')
    mount_static(app, '/helpers/public', HERE / 'public', auth=False, html=True)
'''


def _out(result):
    assert result.returncode == 0, result.stderr[-5000:]
    line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith('JSON:'))
    return json.loads(line[len('JSON:'):])


def _write_files(plugins):
    plugin = plugins / 'helpers'
    files = plugin / 'files'
    (files / 'sub' / '.git').mkdir(parents=True, exist_ok=True)
    (files / 'app.js').write_text('console.log("helpers");\n')
    (files / 'style.css').write_text('.helpers { color: red }\n')
    (files / 'data.json').write_text('{"ok": true}\n')
    (files / '.env').write_text('SECRET=canary-dotenv\n')
    (files / 'sub' / '.git' / 'config').write_text('canary-git\n')
    (plugin / 'secret.txt').write_text('canary-outside\n')
    for link, target in (('escape.txt', plugin / 'secret.txt'), ('inside.js', files / 'app.js')):
        if not (files / link).exists():
            os.symlink(target, files / link)
    (plugin / 'public').mkdir(exist_ok=True)
    (plugin / 'public' / 'index.html').write_text('<p>public index</p>')


def test_session_user_and_mount_static(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'helpers', PLUGIN)
    _write_files(plugins)
    out = _out(run_python(root, plugins, '''
import json
from datetime import timedelta
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api import get_api_connector, mount_static
from meerschaum.api._oauth2 import manager
from meerschaum.api.dash import sessions
from meerschaum.core import User, Token

app = build_app()
client = TestClient(app)
conn = get_api_connector()
user = User('helper-user', 'helper-password-1', type='admin', instance=conn)
assert conn.register_user(user)[0]
user = User('helper-user', instance=conn)
user._user_id = conn.get_user_id(user)
HELPER_SESSION = '00000000-0000-4000-8000-0000000be101'
GHOST_SESSION = '00000000-0000-4000-8000-0000000be102'
sessions.set_session(HELPER_SESSION, {'username': 'helper-user'})
sessions.set_session(GHOST_SESSION, {'username': 'deleted-user'})
token = Token(label='helper-token', user=user, instance=str(conn))
token.generate_credentials()
assert token.register()[0], 'token'
jwt_token = manager.create_access_token(data={'sub': 'helper-user'}, expires=timedelta(minutes=5))

def get(path, session=None, headers=None):
    client.cookies.clear()
    cookies = {'mrsm-session-id': session} if session else {}
    response = client.get(path, cookies=cookies, headers=headers or {}, follow_redirects=False)
    return {
        'status': response.status_code,
        'text': response.text,
        'type': response.headers.get('content-type'),
        'nosniff': response.headers.get('x-content-type-options'),
        'cache': response.headers.get('cache-control'),
        'location': response.headers.get('location'),
    }

out = {}
out['me_anonymous'] = get('/helpers/me')
out['me'] = get('/helpers/me', HELPER_SESSION)
out['me_ghost'] = get('/helpers/me', GHOST_SESSION)
out['maybe_anonymous'] = get('/helpers/maybe')
out['maybe'] = get('/helpers/maybe', HELPER_SESSION)
### The store's `params` filter reads `'_None'` as `IS NOT NULL`: such cookies are no session.
for value in ('_None', '_x', 'None'):
    out['me_wild ' + value] = get('/helpers/me', value)
    out['maybe_wild ' + value] = get('/helpers/maybe', value)
    out['file_wild ' + value] = get('/helpers/files/app.js', value, headers={'Accept': 'application/json'})

out['file_anonymous'] = get('/helpers/files/app.js', headers={'Accept': 'application/json'})
out['file_browser'] = get('/helpers/files/app.js?v=2', headers={'Accept': 'text/html,*/*'})
out['file_session'] = get('/helpers/files/app.js', HELPER_SESSION)
out['css'] = get('/helpers/files/style.css', HELPER_SESSION)
out['json'] = get('/helpers/files/data.json', HELPER_SESSION)
out['token'] = get('/helpers/files/app.js', headers={'Authorization': 'Bearer ' + token.get_api_key()})
out['jwt'] = get('/helpers/files/app.js', headers={'Authorization': 'Bearer ' + jwt_token})
out['bad_bearer'] = get('/helpers/files/app.js', headers={'Authorization': 'Bearer not-a-token'})
out['dotenv'] = get('/helpers/files/.env', HELPER_SESSION)
out['git'] = get('/helpers/files/sub/.git/config', HELPER_SESSION)
out['escape_link'] = get('/helpers/files/escape.txt', HELPER_SESSION)
out['inside_link'] = get('/helpers/files/inside.js', HELPER_SESSION)
out['public'] = get('/helpers/public/')
out['public_file'] = get('/helpers/public/index.html')

leaks = []
for attempt in ('../secret.txt', '%2e%2e/secret.txt', '..%2fsecret.txt', '%2e%2e%2fsecret.txt',
                '....//secret.txt', 'sub/../../secret.txt', '%252e%252e/secret.txt', '/etc/passwd'):
    result = get('/helpers/files/' + attempt, HELPER_SESSION)
    if 'canary' in result['text'] or 'root:x:0:0' in result['text']:
        leaks.append(attempt)
out['leaks'] = leaks

errors = {}
for args in (('/', root_dir := '.'), ('/dash/x', '.'), ('/static', '.'), ('/helpers/x', '/does/not/exist')):
    try:
        mount_static(app, args[0], args[1])
        errors[args[0]] = None
    except ValueError as e:
        errors[args[0]] = str(e)
out['errors'] = errors
print('JSON:' + json.dumps(out))
'''))
    assert out['me_anonymous']['status'] == 401
    assert json.loads(out['me']['text']) == {'username': 'helper-user', 'type': 'admin'}
    assert out['me_ghost']['status'] == 401, "a deleted user's session is no user"
    assert json.loads(out['maybe_anonymous']['text']) == {'username': None}
    assert json.loads(out['maybe']['text']) == {'username': 'helper-user'}
    for value in ('_None', '_x', 'None'):
        assert out['me_wild ' + value]['status'] == 401, (value, out['me_wild ' + value])
        assert json.loads(out['maybe_wild ' + value]['text']) == {'username': None}, value
        wild_file = out['file_wild ' + value]
        assert wild_file['status'] == 401 and 'canary' not in wild_file['text'], (value, wild_file)

    anonymous = out['file_anonymous']
    assert anonymous['status'] == 401 and 'canary' not in anonymous['text']
    assert json.loads(anonymous['text'])['success'] is False
    browser = out['file_browser']
    assert browser['status'] == 303
    assert browser['location'] == '/dash/login?next=%2Fhelpers%2Ffiles%2Fapp.js%3Fv%3D2'

    served = out['file_session']
    assert served['status'] == 200 and 'console.log("helpers")' in served['text']
    assert served['type'].startswith('text/javascript')
    assert served['nosniff'] == 'nosniff'
    assert served['cache'] == 'private, no-cache'
    assert out['css']['type'].startswith('text/css')
    assert out['json']['type'].startswith('application/json')
    assert out['token']['status'] == 200
    assert out['jwt']['status'] == 200
    assert out['bad_bearer']['status'] == 401

    for key in ('dotenv', 'git', 'escape_link'):
        assert out[key]['status'] == 404, (key, out[key])
        assert 'canary' not in out[key]['text']
    assert out['inside_link']['status'] == 200
    assert out['leaks'] == []

    assert out['public']['status'] == 200 and 'public index' in out['public']['text']
    assert out['public']['cache'] != 'private, no-cache'
    assert out['public_file']['status'] == 200

    errors = out['errors']
    assert all(errors[path] for path in ('/', '/dash/x', '/static', '/helpers/x')), errors
