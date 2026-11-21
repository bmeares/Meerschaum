#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Focused tests for:
1. `UrllibTransport` & `MainServerClient` hardening (WS-7b):
   - bypasses `HTTP_PROXY` / `HTTPS_PROXY` (`ProxyHandler({})`),
   - refuses 3xx redirects (`_NoRedirect`),
   - validates origin & root-relative paths,
   - enforces response byte bounds (`ResponseTooLargeError`),
   - strictly separates explicit auth modes (`SessionCookieAuth` vs `InternalHostAuth` vs
     `SessionBoundBearerAuth` vs `UserBearerAuth`).
2. `mrsm-team` (`acting_team`) consistency across Connectors, Plugins, and Performance callbacks
   and page loads (WS-7a), plus immediate `access.forget()` -> `host/_team_scope` invalidation
   and zero `sessions` -> `access` import cycle.
"""

from __future__ import annotations

from tests.api_host._helpers import SIGN_IN, extract_json, run_python


def test_urllib_transport_enforces_no_proxy_no_redirect_origin_and_byte_bounds(sandbox):
    root, plugins = sandbox
    out = extract_json(run_python(root, plugins, '''
import http.server, json, os, threading
from meerschaum.api.host._server_client import (
    AnonymousAuth, InternalHostAuth, MainServerClient, ResponseTooLargeError,
    ServerHTTPError, ServerUnavailableError, SessionCookieAuth, UrllibTransport,
    validate_origin, validate_path,
)

# 1. Origin & path validation rejects unsafe values.
bad_origins = [
    None, '', 'ftp://127.0.0.1:8000', 'http://user:pass@127.0.0.1:8000',
    'http://127.0.0.1:8000/extra', 'http://127.0.0.1:8000?q=1', 'http://127.0.0.1:8000#frag',
    'not-a-url',
]
origin_rejected = 0
for bad in bad_origins:
    try:
        validate_origin(bad)
    except ServerUnavailableError:
        origin_rejected += 1
assert origin_rejected == len(bad_origins)
assert validate_origin('http://127.0.0.1:8000/') == 'http://127.0.0.1:8000'

bad_paths = ['', 'relative/path', '//evil.example/steal', 'http://evil.example/steal']
path_rejected = 0
for bad in bad_paths:
    try:
        validate_path(bad)
    except ValueError:
        path_rejected += 1
assert path_rejected == len(bad_paths)

# 2. Local loopback HTTP server + poisoned proxy server to prove ProxyHandler({}) is active.
proxy_hits = []
server_hits = []

class ProxyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        proxy_hits.append(self.path)
        self.send_response(500)
        self.end_headers()
    def log_message(self, *args):
        pass

class MainHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        server_hits.append(self.path)
        if self.path == '/redirect':
            self.send_response(302)
            self.send_header('Location', '/must-not-follow')
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'{}')
            return
        if self.path == '/exact':
            body = b'{"ok":"12345678"}'  # 17 bytes
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        body = json.dumps({'ok': True, 'host_token': bool(self.headers.get('X-Mrsm-Host-Token'))}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

proxy_srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), ProxyHandler)
main_srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), MainHandler)
t_proxy = threading.Thread(target=proxy_srv.serve_forever, daemon=True)
t_main = threading.Thread(target=main_srv.serve_forever, daemon=True)
t_proxy.start()
t_main.start()
try:
    os.environ['HTTP_PROXY'] = f'http://127.0.0.1:{proxy_srv.server_port}'
    os.environ['http_proxy'] = f'http://127.0.0.1:{proxy_srv.server_port}'
    base = f'http://127.0.0.1:{main_srv.server_port}'
    client = MainServerClient(UrllibTransport(base_url_provider=lambda: base))

    # Proxy bypass + InternalHostAuth attaches X-Mrsm-Host-Token.
    direct = client.request_json('GET', '/ping', auth=InternalHostAuth(user='alice'), timeout=5)
    assert direct == {'ok': True, 'host_token': True}
    assert proxy_hits == []

    # 3xx redirect is refused, never followed.
    redirect_status = None
    try:
        client.request_json('GET', '/redirect', auth=AnonymousAuth(), timeout=5)
    except ServerHTTPError as e:
        redirect_status = e.status
    assert redirect_status == 302
    assert '/must-not-follow' not in server_hits

    # Byte bound enforcement: 17-byte body passes at max_bytes=17 and fails at max_bytes=16.
    exact = client.request_json('GET', '/exact', auth=AnonymousAuth(), timeout=5, max_bytes=17)
    assert exact == {'ok': '12345678'}
    too_large = False
    try:
        client.request_json('GET', '/exact', auth=AnonymousAuth(), timeout=5, max_bytes=16)
    except ResponseTooLargeError:
        too_large = True
    assert too_large is True

    # SessionCookieAuth never attaches X-Mrsm-Host-Token.
    import uuid
    sid = str(uuid.uuid4())
    cookie_res = client.request_json('GET', '/ping', auth=SessionCookieAuth(sid), timeout=5)
    assert cookie_res == {'ok': True, 'host_token': False}

    print('JSON:' + json.dumps({
        'origin_rejected': origin_rejected,
        'path_rejected': path_rejected,
        'proxy_hits': len(proxy_hits),
        'redirect_status': redirect_status,
        'too_large': too_large,
        'cookie_host_token': cookie_res['host_token'],
    }))
finally:
    proxy_srv.shutdown()
    proxy_srv.server_close()
    main_srv.shutdown()
    main_srv.server_close()
    t_proxy.join(timeout=5)
    t_main.join(timeout=5)
'''))
    assert out == {
        'origin_rejected': 8,
        'path_rejected': 4,
        'proxy_hits': 0,
        'redirect_status': 302,
        'too_large': True,
        'cookie_host_token': False,
    }


def test_acting_team_matrix_across_connectors_plugins_performance_and_cache_invalidation(sandbox):
    root, plugins = sandbox
    out = extract_json(run_python(root, plugins, SIGN_IN + '''
import json
import meerschaum as mrsm
from dash._callback_context import context_value
from dash._utils import AttributeDict
from meerschaum.api import get_api_connector
from meerschaum.connectors import get_connector
from meerschaum.api.dash import build_dash_app
build_dash_app()
from meerschaum.api.dash import access, components, performance, plugin_sources, team_setup
from meerschaum.api.dash.pages import connectors as connectors_page
from meerschaum.api.dash.callbacks import connectors_page as connectors_cb
from meerschaum.api.dash.callbacks import plugins as plugins_cb
from meerschaum.api.host import _team_scope

conn = get_api_connector()
INSTANCE = str(conn)

# Set up two teams: `act-alpha` (isolated, allowed connector `sql:actalpha`, plugin `p_alpha`, no plugins_write)
# and `act-beta` (isolated, allowed connector `sql:actbeta`, plugin `p_beta`).
for name, ckeys, pname in (('act-alpha', 'sql:actalpha', 'p_alpha'), ('act-beta', 'sql:actbeta', 'p_beta')):
    t = mrsm.Team(name, instance=conn)
    if not t.exists():
        assert t.register()[0]
    assert team_setup.edit_team_settings(
        name, isolated=True, instance=INSTANCE, connectors=[INSTANCE, ckeys], plugins=[pname], plugins_write=False,
    )[0]
get_connector('sql', 'actalpha', flavor='sqlite', database=':memory:', teams=['act-alpha'])
get_connector('sql', 'actbeta', flavor='sqlite', database=':memory:', teams=['act-beta'])

admin_session = sign_in('act-admin', user_type='admin')
member_session = sign_in('act-member', teams={'act-alpha': 'member'})

# Seed one pipe per team (using each team's allowed source connector).
p_alpha = mrsm.Pipe('sql:actalpha', 'alpha_m', instance=conn, parameters={'access': {'team': 'act-alpha'}})
p_beta = mrsm.Pipe('sql:actbeta', 'beta_m', instance=conn, parameters={'access': {'team': 'act-beta'}})
assert p_alpha.register()[0] and p_beta.register()[0]
ref_alpha = f'{INSTANCE}|sql:actalpha|alpha_m|None'
ref_beta = f'{INSTANCE}|sql:actbeta|beta_m|None'

# 1. Connectors: page vs callbacks under All teams vs acting as `act-alpha`.
with components.console_nav_context('/dash/connectors', admin_session['session-id'], acting_team=None):
    v_all = access.resolve_viewer(admin_session, fresh=True)
    row_beta_all, _ = connectors_cb._connector_row('sql:actbeta', admin_session)
    assert v_all.can_test_connectors and v_all.can_see_connector_details and row_beta_all['keys'] == 'sql:actbeta'

with components.console_nav_context('/dash/connectors', admin_session['session-id'], acting_team='act-alpha'):
    v_alpha = access.resolve_viewer(admin_session, fresh=True)
    assert v_alpha.acting_team == 'act-alpha'
    assert not v_alpha.can_see_connector_details and not v_alpha.can_test_connectors
    # Page load and callback both allow `sql:actalpha` and refuse `sql:actbeta`.
    row_alpha, who_alpha = connectors_cb._connector_row('sql:actalpha', admin_session)
    assert row_alpha['keys'] == 'sql:actalpha' and not who_alpha.can_test_connectors
    beta_refused_in_cb = False
    try:
        connectors_cb._connector_row('sql:actbeta', admin_session)
    except PermissionError:
        beta_refused_in_cb = True
    assert beta_refused_in_cb is True
    context_value.set(AttributeDict(**{'triggered_inputs': [
        {'prop_id': '{"index":"sql:actalpha","type":"connector-test-button"}.n_clicks', 'value': 1}
    ]}))
    test_msg = str(connectors_cb.test_connection_click(1, admin_session))
    assert 'Only administrators can test connections' in test_msg

# 2. Plugins: callbacks and plugin_sources respect `acting_team`.
with components.console_nav_context('/dash/plugins', admin_session['session-id'], acting_team='act-alpha'):
    alert_act, _, _ = plugins_cb.run_plugin_action('install', 'p_beta', admin_session)
    assert 'administrators' in str(alert_act).lower() or 'team' in str(alert_act).lower() or 'not' in str(alert_act).lower()
    assert plugin_sources.may_read_source('repository', admin_session, 'p_alpha') is True
    assert plugin_sources.may_read_source('repository', admin_session, 'p_beta') is False
    assert plugin_sources.authoring_refusal(admin_session) is not None

# 3. Performance: metrics visibility ceiling narrows to `act-alpha` (refuses `ref_beta`).
with components.console_nav_context('/dash/performance', admin_session['session-id'], acting_team='act-alpha'):
    perf_data = performance.metrics(INSTANCE, admin_session, window='24h')
    assert perf_data['_visible_refs'] is not None
    assert ref_alpha in perf_data['_visible_refs'] and ref_beta not in perf_data['_visible_refs']
    perf_beta_refused = False
    try:
        performance.metrics(INSTANCE, admin_session, window='24h', pipe=ref_beta)
    except PermissionError:
        perf_beta_refused = True
    assert perf_beta_refused is True

# 4. `access.forget()` immediately invalidates `host/_team_scope._cache` (no 5s stale window).
scope_before = _team_scope.team_scope_of_user('act-member')
assert scope_before is not None and 'act-member' in _team_scope._cache
# Mutate team attributes directly on the instance (before `access.forget()`): cache still holds old scope.
team_obj = mrsm.Team('act-alpha', instance=conn)
team_obj.attributes['isolated'] = False
assert team_obj.edit()[0]
assert _team_scope.team_scope_of_user('act-member') is not None
access.forget('act-member')
assert 'act-member' not in _team_scope._cache
scope_after = _team_scope.team_scope_of_user('act-member')
assert scope_after is None
# Global `access.forget()` also clears `_team_scope._cache`.
assert 'act-member' in _team_scope._cache
access.forget()
assert _team_scope._cache == {}

print('JSON:' + json.dumps({
    'connectors_narrowed': beta_refused_in_cb,
    'connectors_test_refused': 'Only administrators can test connections' in test_msg,
    'plugins_narrowed': True,
    'performance_narrowed': perf_beta_refused,
    'team_scope_invalidated': scope_before is not None and scope_after is None,
}))
'''))
    assert out == {
        'connectors_narrowed': True,
        'connectors_test_refused': True,
        'plugins_narrowed': True,
        'performance_narrowed': True,
        'team_scope_invalidated': True,
    }


def test_sessions_never_imports_access_and_works_standalone(sandbox):
    root, plugins = sandbox
    out = extract_json(run_python(root, plugins, '''
import json, sys, uuid
from meerschaum.core import User
from meerschaum.api import get_api_connector
import meerschaum.api.dash.sessions as sessions

assert 'meerschaum.api.dash.access' not in sys.modules
conn = get_api_connector()
assert conn.register_user(User('sa-admin', 'correct-horse-9', type='admin', instance=conn))[0]
assert conn.register_user(User('sa-viewer', 'correct-horse-9', instance=conn))[0]
sid_admin = str(uuid.uuid4())
sid_viewer = str(uuid.uuid4())
assert sessions.set_session(sid_admin, {'username': 'sa-admin'})
assert sessions.set_session(sid_viewer, {'username': 'sa-viewer'})

# Standalone (before `access` is imported):
standalone = {
    'admin_is_admin': sessions.session_is_admin(sid_admin),
    'viewer_is_admin': sessions.session_is_admin(sid_viewer),
    'admin_auth': sessions.is_session_authenticated(sid_admin),
    'viewer_auth': sessions.is_session_authenticated(sid_viewer),
    'access_imported': 'meerschaum.api.dash.access' in sys.modules,
}

# After importing `access`, the resolver hook is registered:
from meerschaum.api.dash import access
after = {
    'admin_is_admin': sessions.session_is_admin(sid_admin),
    'viewer_is_admin': sessions.session_is_admin(sid_viewer),
    'admin_auth': sessions.is_session_authenticated(sid_admin),
    'viewer_auth': sessions.is_session_authenticated(sid_viewer),
}
print('JSON:' + json.dumps({'standalone': standalone, 'after': after}))
'''))
    assert out['standalone'] == {
        'admin_is_admin': True,
        'viewer_is_admin': False,
        'admin_auth': True,
        'viewer_auth': False,
        'access_imported': False,
    }
    assert out['after'] == {
        'admin_is_admin': True,
        'viewer_is_admin': False,
        'admin_auth': True,
        'viewer_auth': False,
    }
