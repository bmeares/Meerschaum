#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`meerschaum.api.manager` and the host's import surface (transition bug 4).

v4's `@api_plugin` routes wrote `curr_user = fastapi.Depends(manager)` against
`fastapi_login.LoginManager`; v5 keeps the name with a built-in manager (`meerschaum.api._login`)
so that the `api` extra (`fastapi`, `uvicorn`, `a2wsgi`, `python-multipart`, `PyJWT`) is all the
host imports: `fastapi-login`, `python-jose` and `aiofiles` are not installed by any extra.
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
import fastapi
from meerschaum.plugins import api_plugin

@api_plugin
def login_routes(app):
    from meerschaum.api import manager
    from meerschaum.api._oauth2 import optional_user, load_user_or_token

    @app.get('/lm/me')
    def me(curr_user=fastapi.Depends(manager)):
        return {'username': curr_user.username}

    @app.get('/lm/maybe')
    async def maybe(curr_user=fastapi.Depends(manager.optional)):
        return {'username': getattr(curr_user, 'username', None)}

    @app.get('/lm/optional-user')
    async def maybe_user(curr_user=fastapi.Depends(optional_user)):
        return {'username': getattr(curr_user, 'username', None)}

    @app.get('/lm/token-only')
    async def token_only(request: fastapi.Request):
        await load_user_or_token(request, users=False)
        return True
'''


def _json(result) -> dict:
    assert result.returncode == 0, result.stderr[-4000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    return json.loads(line[len('JSON:'):])


def test_depends_manager_resolves_the_user_of_a_bearer_jwt(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'lm_plugin', PLUGIN)
    result = run_python(root, plugins, """
        import json, time
        import jwt
        from starlette.testclient import TestClient
        from meerschaum.api.host import build_app
        from meerschaum.api._oauth2 import SECRET
        from meerschaum.api import get_api_connector
        from meerschaum.core import User

        client = TestClient(build_app())
        instance = get_api_connector()
        assert instance.register_user(User('bob', 'probe-password', instance=instance))[0]

        def token(secret=SECRET, **claims):
            payload = {'sub': 'bob', 'scopes': ['*'], 'exp': int(time.time()) + 60, **claims}
            return jwt.encode(payload, secret, algorithm='HS256')

        def get(path, tok=None):
            headers = {'Authorization': 'Bearer ' + tok} if tok else {}
            response = client.get(path, headers=headers)
            return [response.status_code, response.json()]

        out = {
            'valid': get('/lm/me', token()),
            'no_header': get('/lm/me'),
            'wrong_secret': get('/lm/me', token(secret=b'x' * 48)),
            'expired': get('/lm/me', token(exp=int(time.time()) - 600)),
            'garbage': get('/lm/me', 'not-a-jwt'),
            'no_sub': get('/lm/me', jwt.encode({'exp': int(time.time()) + 60}, SECRET, algorithm='HS256')),
            'optional_valid': get('/lm/maybe', token()),
            'optional_none': get('/lm/maybe'),
            'optional_user_valid': get('/lm/optional-user', token()),
            'optional_user_none': get('/lm/optional-user'),
            'optional_user_invalid': get('/lm/optional-user', 'not-a-jwt'),
            'user_disallowed': get('/lm/token-only', token()),
        }
        print('JSON:' + json.dumps(out))
    """)
    out = _json(result)
    assert out['valid'] == [200, {'username': 'bob'}]
    assert out['no_header'][0] == 401
    assert out['wrong_secret'] == [401, {'detail': 'Invalid credentials'}]
    assert out['expired'][0] == 401
    assert out['garbage'][0] == 401
    assert out['no_sub'][0] == 401
    assert out['optional_valid'] == [200, {'username': 'bob'}]
    assert out['optional_none'] == [200, {'username': None}]
    assert out['optional_user_valid'] == [200, {'username': 'bob'}]
    assert out['optional_user_none'] == [200, {'username': None}]
    assert out['optional_user_invalid'] == [200, {'username': None}]
    assert out['user_disallowed'][0] == 401


def test_manager_creates_tokens_the_host_accepts(sandbox):
    """`manager.create_access_token(...)` (v4 API) signs what `ScopedAuth`'s decoder validates."""
    root, plugins = sandbox
    result = run_python(root, plugins, """
        import json
        from datetime import timedelta
        from meerschaum.api import manager
        from meerschaum.api._oauth2 import decode_jwt, SECRET

        token = manager.create_access_token(
            data={'sub': 'alice'}, expires=timedelta(minutes=5), scopes=['pipes:read'],
        )
        claims = decode_jwt(token)
        out = {
            'sub': claims['sub'],
            'scopes': claims['scopes'],
            'secret_is_shared': manager.secret == SECRET,
            'has_exp': 'exp' in claims,
        }
        print('JSON:' + json.dumps(out))
    """)
    assert _json(result) == {
        'sub': 'alice', 'scopes': ['pipes:read'], 'secret_is_shared': True, 'has_exp': True,
    }


def test_token_jwts_follow_backing_token_revocation_and_expiry(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, """
        import json
        from datetime import datetime, timedelta, timezone
        import fastapi
        from starlette.testclient import TestClient
        from meerschaum.api import get_api_connector, manager, ScopedAuth
        from meerschaum.api.host import build_app
        from meerschaum.core import Token, User

        app = build_app()

        @app.get('/token-probe')
        def probe(principal=fastapi.Depends(ScopedAuth(['pipes:read']))):
            return {'id': str(principal.id)}

        instance = get_api_connector()
        user = User('token-probe-owner', 'probe-password', instance=instance)
        assert instance.register_user(user)[0]
        token = Token(label='token-probe', user=user, instance=instance, scopes=['pipes:read'])
        assert token.register()[0]
        access = manager.create_access_token(
            data={'sub': str(token.id)}, expires=timedelta(minutes=5), scopes=['pipes:read'],
        )
        client = TestClient(app)

        def status():
            return client.get('/token-probe', headers={'Authorization': 'Bearer ' + access}).status_code

        out = {'valid': status()}
        assert token.invalidate()[0]
        out['revoked'] = status()
        token.is_valid = True
        token.expiration = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert token.edit()[0]
        out['expired'] = status()
        assert token.delete()[0]
        out['deleted'] = status()
        print('JSON:' + json.dumps(out))
    """)
    assert _json(result) == {'valid': 200, 'revoked': 401, 'expired': 401, 'deleted': 401}


def test_user_jwt_is_rejected_after_its_user_is_deleted(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'lm_plugin', PLUGIN)
    result = run_python(root, plugins, """
        import json
        from starlette.testclient import TestClient
        from meerschaum.api.host import build_app
        from meerschaum.api import get_api_connector, manager
        from meerschaum.core import User

        client = TestClient(build_app())
        instance = get_api_connector()
        user = User('deleted-probe-owner', 'probe-password', instance=instance)
        assert instance.register_user(user)[0]
        access = manager.create_access_token(data={'sub': user.username}, scopes=['*'])

        def status():
            return client.get('/lm/me', headers={'Authorization': 'Bearer ' + access}).status_code

        out = {'valid': status(), 'repeated': status()}
        assert instance.delete_user(user)[0]
        out['deleted'] = status()
        print('JSON:' + json.dumps(out))
    """)
    assert _json(result) == {'valid': 200, 'repeated': 200, 'deleted': 401}


def test_the_host_imports_no_package_outside_the_api_extra(sandbox):
    """
    Building the host app (with an `@api_plugin` that uses `manager`) never imports
    `fastapi-login`, `python-jose` or `aiofiles`, even when they are installed.
    """
    root, plugins = sandbox
    write_plugin(plugins, 'lm_plugin', PLUGIN)
    result = run_python(root, plugins, """
        import importlib.abc, json, sys

        BLOCKED = ('jose', 'fastapi_login', 'aiofiles')

        class Blocker(importlib.abc.MetaPathFinder):
            def find_spec(self, name, path=None, target=None):
                if name.split('.')[0] in BLOCKED:
                    raise ImportError('blocked in this test: ' + name)
                return None

        sys.meta_path.insert(0, Blocker())

        from meerschaum.api.host import build_app
        app = build_app()
        paths = sorted(getattr(route, 'path', '') for route in app.routes)
        print('JSON:' + json.dumps({
            'routes': [p for p in paths if p.startswith('/lm/')],
            'imported': sorted(m for m in sys.modules if m.split('.')[0] in BLOCKED),
        }))
    """)
    out = _json(result)
    assert out['routes'] == ['/lm/maybe', '/lm/me', '/lm/optional-user', '/lm/token-only']
    assert out['imported'] == []
