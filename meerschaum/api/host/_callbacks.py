#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Console callbacks need a console session.

Every Dash callback runs through `POST /dash/_dash-update-component` of the one Dash app, which
FastAPI's authentication never sees, so the host guards it with a Flask `before_request` hook:
a callback request without a valid console session gets `401` (a JSON body and the header
`X-Mrsm-Login`, which `assets/session_guard.js` answers by sending the browser to the login page),
unless the callback is public:

- the console's page router (it shows the login page to anonymous visitors and applies each
  page's `login_required` itself) and the login and registration callbacks;
- the callbacks a `@dash_plugin(public=True)` function registers, and
  `@web_callback(..., public=True)`.

A callback is identified by the `output` of the request, the key Dash itself looks the callback up
by (`callback_map[output]`), so a request cannot name a public callback and run another.
One of the console's own callbacks that the renderer fires by itself (nothing changed, or only
inputs that are still unset, like the `n_clicks` of a Sign out button that was just rendered on a
public plugin page) gets `204` (no update) instead: it does not run, and there is nothing to sign
in for. What the visitor does (a click, a value, an interval tick) and every plugin callback get
the `401`.
A session is the `mrsm-session-id` cookie the login sets, or the `session-store` value the request
carries (which is then copied into the cookie). The same rule covers the callback API routes of
Dash (`@callback(api_endpoint=...)`); Dash's own MCP server (`api:dash:mcp:enabled`) also accepts
an API token (`Authorization: Bearer ...`).

`--no-auth` and `api:dash:callbacks_require_session: false` turn the guard off.
"""

from __future__ import annotations

import json

from meerschaum.utils.typing import Any, Dict, List, Optional

CALLBACK_ROUTE: str = '_dash-update-component'
LOGIN_HEADER: str = 'X-Mrsm-Login'
SESSION_STORE_ID: str = 'session-store'
#: The console's page router writes the page into this component.
ROUTER_OUTPUT: str = 'page-layout-div.children'
_GUARD_ATTR: str = '_mrsm_callback_guard'


def callbacks_require_session() -> bool:
    """
    Return whether console callbacks need a session: not with `--no-auth`, and not when
    `api:dash:callbacks_require_session` is `false` (it defaults to `true`).
    """
    from meerschaum.api import no_auth
    if no_auth:
        return False
    from meerschaum.config import get_config
    value = get_config('api', 'dash', 'callbacks_require_session', warn=False)
    return value is not False


def is_public_callback(dash_app: Any, output: Any) -> bool:
    """
    Return whether the callback Dash runs for a request's `output` is public.
    """
    from meerschaum.plugins import _is_public_callback
    if not isinstance(output, str):
        return False
    callback_map = getattr(dash_app, 'callback_map', None) or {}
    spec = callback_map.get(output, None)
    if not isinstance(spec, dict):
        return False
    return _is_public_callback(spec.get('callback', None))


def _blueprint_outputs(callback: Any) -> List[str]:
    """
    Return the `id.property` strings of a `dash_extensions` callback blueprint's outputs.
    """
    outputs = []
    for output in list(getattr(callback, 'outputs', None) or []):
        component_id = getattr(output, 'component_id', None)
        component_property = getattr(output, 'component_property', None)
        if isinstance(component_id, str) and component_property:
            outputs.append(f"{component_id}.{component_property}")
    return outputs


def _is_console_function(func: Any) -> bool:
    module = str(getattr(func, '__module__', '') or '')
    return module == 'meerschaum.api.dash' or module.startswith('meerschaum.api.dash.')


def mark_console_router_public(dash_app: Any) -> int:
    """
    Mark the console's page router public: it renders the login page for visitors without a
    session (and each page's `login_required` is its own check). Only the console's own callbacks
    count, not a plugin's callback writing into the same component.

    Returns
    -------
    The number of callbacks marked.
    """
    from meerschaum.plugins import _mark_public_callback
    marked = 0
    blueprint = getattr(dash_app, 'blueprint', None)
    for callback in list(getattr(blueprint, 'callbacks', None) or []):
        func = getattr(callback, 'f', None)
        if callable(func) and _is_console_function(func) and ROUTER_OUTPUT in _blueprint_outputs(
            callback
        ):
            _mark_public_callback(func)
            marked += 1
    for callback_id, spec in list((getattr(dash_app, 'callback_map', None) or {}).items()):
        func = (spec or {}).get('callback', None)
        if callable(func) and _is_console_function(func) and ROUTER_OUTPUT in callback_id:
            _mark_public_callback(func)
            marked += 1
    if not marked:
        from meerschaum.utils.warnings import warn
        warn(
            "The console's page router was not found: visitors without a session may not see "
            "the login page.",
            stack=False,
        )
    return marked


def _store_session_id(body: Any) -> Optional[str]:
    """
    Return the `session-id` of the `session-store` value a callback request carries, if it is a
    session ID (a UUID: anything else, such as `'_None'`, counts as no session).
    """
    if not isinstance(body, dict):
        return None
    from meerschaum.api.dash.sessions import valid_session_id
    for key in ('inputs', 'state'):
        for item in body.get(key, None) or []:
            if not isinstance(item, dict) or item.get('id', None) != SESSION_STORE_ID:
                continue
            value = item.get('value', None)
            if isinstance(value, dict) and value.get('session-id', None):
                return valid_session_id(value['session-id'])
    return None


def _session_active(session_id: Optional[str]) -> bool:
    from meerschaum.api.dash.sessions import is_session_active, valid_session_id
    if valid_session_id(session_id) is None:
        return False
    try:
        return bool(is_session_active(session_id))
    except Exception:
        return False


def _prop_key(component_id: Any, prop: Any) -> str:
    """`id.property` as the renderer writes `changedPropIds` (a dict ID as compact sorted JSON)."""
    if isinstance(component_id, dict):
        component_id = json.dumps(component_id, sort_keys=True, separators=(',', ':'))
    return f"{component_id}.{prop}"


def _untriggered(body: Any) -> bool:
    """
    Whether a callback request was fired by the renderer rather than by the visitor: nothing
    changed, or every changed input is still unset (`None`, e.g. the `n_clicks` of a button that
    was just rendered and never clicked).
    """
    if not isinstance(body, dict):
        return False
    changed = body.get('changedPropIds', None) or []
    if not changed:
        return True
    values: Dict[str, Any] = {}
    for item in body.get('inputs', None) or []:
        for entry in (item if isinstance(item, list) else [item]):
            if isinstance(entry, dict):
                values[_prop_key(entry.get('id', None), entry.get('property', None))] = entry.get(
                    'value', None
                )
    return all(values.get(str(prop_id), None) is None for prop_id in changed)


def _is_console_callback(dash_app: Any, output: Any) -> bool:
    """Whether the callback Dash runs for `output` is one of the console's own."""
    if not isinstance(output, str):
        return False
    spec = (getattr(dash_app, 'callback_map', None) or {}).get(output, None)
    func = spec.get('callback', None) if isinstance(spec, dict) else None
    for _ in range(32):
        if func is None:
            return False
        if _is_console_function(func):
            return True
        func = getattr(func, '__wrapped__', None)
    return False


def _no_update_response():
    """`204`: Dash's "no update" (the callback did not run)."""
    from flask import Response
    response = Response(status=204)
    response.headers['Cache-Control'] = 'no-store'
    return response


def _login_path() -> str:
    from meerschaum.api import endpoints
    return endpoints['dash'].rstrip('/') + '/login'


def _login_required_response():
    """
    The `401` answering a callback without a session (the Q13 JSON shape).
    """
    from flask import Response
    login_path = _login_path()
    body = json.dumps({
        'success': False,
        'message': "Sign in to the web console to continue.",
        'login': login_path,
    })
    response = Response(body, status=401, mimetype='application/json')
    response.headers[LOGIN_HEADER] = login_path
    response.headers['Cache-Control'] = 'no-store'
    return response


def _api_route_handler(dash_app: Any, path: str) -> Any:
    """
    Return the function behind a Dash callback API route (`@callback(api_endpoint=...)`) at
    `path`, if there is one.
    """
    for route, handler in list((getattr(dash_app, 'callback_api_paths', None) or {}).items()):
        route = route if str(route).startswith('/') else '/' + str(route)
        if route == path:
            return handler
    return None


def _mcp_route(dash_app: Any) -> Optional[str]:
    """
    Return the path of Dash's MCP server in the Flask app, if it is enabled.
    """
    if not getattr(dash_app, '_enable_mcp', False):
        return None
    mcp_path = getattr(dash_app, '_mcp_path', None)
    if not isinstance(mcp_path, str):
        return None
    prefix = getattr(getattr(dash_app, 'config', None), 'routes_pathname_prefix', '/') or '/'
    return prefix + mcp_path.lstrip('/')


def install_callback_guard(dash_app: Any) -> bool:
    """
    Make the console's callbacks need a session (see the module's docstring); installed once.

    Returns
    -------
    Whether the guard is active (`False` with `--no-auth` or
    `api:dash:callbacks_require_session: false`).
    """
    if getattr(dash_app, _GUARD_ATTR, None) is not None:
        return bool(getattr(dash_app, _GUARD_ATTR))
    enforced = callbacks_require_session()
    setattr(dash_app, _GUARD_ATTR, enforced)
    if not enforced:
        return False

    server = dash_app.server

    @server.before_request
    def _mrsm_require_console_session():
        from flask import request, g
        from meerschaum.api.dash.sessions import get_session_id_from_request
        path = request.path or ''

        if request.method == 'POST' and path.rstrip('/').endswith('/' + CALLBACK_ROUTE):
            body = request.get_json(silent=True)
            output = body.get('output', None) if isinstance(body, dict) else None
            cookie_session = get_session_id_from_request(request)
            store_session = _store_session_id(body)
            cookie_active = _session_active(cookie_session)
            store_active = (
                store_session is not None
                and store_session != cookie_session
                and _session_active(store_session)
            )
            ### The cookie can be gone (expired before the session, or cleared) while the
            ### console's store still names a live session: copy it back into the cookie.
            if store_active and not cookie_active:
                g._mrsm_mirror_session = store_session
            if cookie_active or store_active or is_public_callback(dash_app, output):
                return None
            ### The console's own controls (Sign out on a public plugin page's bar, ...) fire as they
            ### render: without a session there is nothing for them to do, and nowhere to send
            ### the visitor. Anything the visitor does, and every plugin callback, needs a session.
            if _untriggered(body) and _is_console_callback(dash_app, output):
                return _no_update_response()
            return _login_required_response()

        if request.method == 'POST':
            handler = _api_route_handler(dash_app, path)
            if handler is not None:
                from meerschaum.plugins import _is_public_callback
                if _is_public_callback(handler) or _session_active(
                    get_session_id_from_request(request)
                ):
                    return None
                return _login_required_response()

        mcp_route = _mcp_route(dash_app)
        if mcp_route is not None and path.rstrip('/') == mcp_route.rstrip('/'):
            from meerschaum.api.host._auth import principal_from_authorization
            if _session_active(get_session_id_from_request(request)):
                return None
            if principal_from_authorization(request.headers.get('Authorization', None)):
                return None
            return _login_required_response()
        return None

    @server.after_request
    def _mrsm_mirror_session_cookie(response):
        from flask import request, g
        session_id = g.pop('_mrsm_mirror_session', None)
        if not session_id:
            return response
        from meerschaum.api.dash.sessions import SESSION_COOKIE_NAME, set_session_cookie
        ### The callback may have set (login) or cleared (sign out) the cookie itself.
        already = any(
            str(value).startswith(SESSION_COOKIE_NAME + '=')
            for value in response.headers.getlist('Set-Cookie')
        )
        if already or not _session_active(session_id):
            return response
        set_session_cookie(response, session_id, secure=request.is_secure)
        return response

    return True


def secure_console(dash_app: Any) -> Dict[str, Any]:
    """
    Apply the console's callback rules to the built Dash app: mark the page router public and
    install the guard. Return `{'router': n_marked, 'guard': bool}`.
    """
    return {
        'router': mark_console_router_public(dash_app),
        'guard': install_callback_guard(dash_app),
    }
