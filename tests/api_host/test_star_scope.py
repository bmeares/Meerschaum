#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A literal `*` scope grants every scope, including custom (plugin-defined) ones, to a credential
whose owner holds `*` (an administrator): `ScopedAuth` in an `@api_plugin` route accepts such a
`*` JWT and `*` API token for a scope no built-in list contains, a credential holding another
scope is refused with v4's message, a non-administrator's `*` holds only their own permissions
(the server's rule), and `meerschaum.core.Token` keeps the `*` it is given instead of expanding it.
"""

from tests.api_host._helpers import run_python, sandbox  # noqa: F401


def test_star_grants_custom_scopes_in_plugin_routes(sandbox):
    root, plugins = sandbox
    result = run_python(root, plugins, """
import asyncio, time
from types import SimpleNamespace
import meerschaum.api._oauth2 as oauth2
from fastapi import HTTPException
from meerschaum.api import ScopedAuth
from meerschaum.api._oauth2 import SECRET
from meerschaum.api import get_api_connector
from meerschaum.core import Token, User

instance = get_api_connector()
assert instance.register_user(User('bob', 'bob-password-1', type='admin', instance=instance))[0]
assert instance.register_user(User('carol', 'carol-password-1', instance=instance))[0]
bob = User('bob', instance=instance)
carol = User('carol', instance=instance)
bob_id = instance.get_user_id(bob)

def bearer(scopes):
    token = oauth2.jwt.encode(
        {'sub': 'bob', 'scopes': scopes, 'exp': int(time.time()) + 60}, SECRET, algorithm='HS256',
    )
    return SimpleNamespace(headers={'authorization': 'Bearer ' + token})

### An API token of bob's holding `scopes`.
class KeyPrincipal(Token):
    def __init__(self, scopes):
        super().__init__(label='k', scopes=scopes, user_id=bob_id, instance=instance)
    def get_scopes(self, refresh=False, debug=False):
        return list(self.scopes)

def check(request, principal, scope):
    async def load(_request, **_kw):
        return principal
    oauth2.load_user_or_token = load
    try:
        asyncio.run(ScopedAuth([scope])(request))
        return 'ok'
    except HTTPException as exc:
        return (exc.status_code, exc.detail)

custom = 'myplugin:read'
out = {}
### JWTs carry their own scopes.
out['jwt_star'] = check(bearer(['*']), bob, custom)
out['jwt_other'] = check(bearer(['pipes:read']), bob, custom)
out['jwt_custom'] = check(bearer([custom]), bob, custom)
out['jwt_star_non_admin'] = check(bearer(['*']), carol, custom)
### API tokens are resolved against the instance on every request.
key = SimpleNamespace(headers={'authorization': 'Bearer mrsm-key:id:secret'})
out['key_star'] = check(key, KeyPrincipal(['*']), custom)
out['key_other'] = check(key, KeyPrincipal(['pipes:read']), custom)
out['token_keeps_star'] = Token(label='x', scopes=['*']).scopes
out['token_keeps_custom'] = Token(label='y', scopes=[custom, 'pipes:read']).scopes

assert out == {
    'jwt_star': 'ok',
    'jwt_other': (403, "Missing required scope: 'myplugin:read'"),
    'jwt_custom': 'ok',
    'jwt_star_non_admin': (403, "Missing required scope: 'myplugin:read'"),
    'key_star': 'ok',
    'key_other': (403, "Missing required scope: 'myplugin:read'"),
    'token_keeps_star': ['*'],
    'token_keeps_custom': [custom, 'pipes:read'],
}, out
print('ok')
""")
    assert result.returncode == 0, result.stderr[-3000:] + result.stdout[-1000:]
    assert result.stdout.strip().splitlines()[-1] == 'ok'
