"""
A bearer token whose header nests JSON deeper than the interpreter's recursion limit is an
invalid token (401), not a server error: PyJWT before 2.14.0 parses the header before checking
the signature and lets the `RecursionError` escape (GHSA-8wjv-2p76-3863).
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
import fastapi
from meerschaum.plugins import api_plugin

@api_plugin
def depth_routes(app):
    from meerschaum.api import ScopedAuth, manager

    @app.get('/depth/manager')
    def by_manager(curr=fastapi.Depends(manager)):
        return True

    @app.get('/depth/scoped')
    def by_scope(curr=fastapi.Depends(ScopedAuth(['pipes:read']))):
        return True
'''


def test_a_deeply_nested_token_header_is_unauthorized(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'depth_plugin', PLUGIN)
    result = run_python(root, plugins, """
import base64, json
import jwt
from starlette.testclient import TestClient
from meerschaum.api.host import build_app
from meerschaum.api._oauth2 import decode_jwt

def segment(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

token = '.'.join([segment(b'[' * 100_000), segment(b'{}'), segment(b'x')])
client = TestClient(build_app(), raise_server_exceptions=False)
out = {
    path: client.get(path, headers={'Authorization': 'Bearer ' + token}).status_code
    for path in ('/depth/manager', '/depth/scoped')
}
try:
    decode_jwt(token)
    out['decode'] = 'decoded'
except jwt.PyJWTError:
    out['decode'] = 'invalid'
except RecursionError:
    out['decode'] = 'recursion'
print('JSON:' + json.dumps(out))
""")
    lines = [line for line in result.stdout.splitlines() if line.startswith('JSON:')]
    assert lines, result.stdout + result.stderr
    assert json.loads(lines[-1][len('JSON:'):]) == {
        '/depth/manager': 401,
        '/depth/scoped': 401,
        'decode': 'invalid',
    }
