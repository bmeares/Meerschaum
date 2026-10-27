#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Who may do what on the Pipes pages (`/dash/pipes`, a pipe's page and New pipe).

- Reading (the table, a pipe's tabs) needs a console session (or `--no-auth`).
- Running actions (Sync, Verify, Clear, Drop, Delete) and editing need an authenticated session:
  `is_session_authenticated()` (an administrator when `api:permissions:actions:non_admin` is off,
  as `start api --secure` sets it), as the rest of the console. The server authorizes the actions
  again when they run (`POST /execute`).
- Writing a pipe's registration directly (New pipe, Copy, editing parameters) also needs the
  `pipes:write` scope; registering needs `api:permissions:registration:pipes`, as
  `POST /pipes/{connector}/{metric}/{location}/register` does.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, Dict, Optional, Tuple


def session_data_from_context(session_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    The `session-store` data of the current request: the given data, else the router callback's
    `session-store` input, else the session cookie.
    """
    if isinstance(session_data, dict) and session_data.get('session-id'):
        return session_data
    try:
        import dash
        inputs = dash.callback_context.inputs or {}
        data = inputs.get('session-store.data', None)
        if isinstance(data, dict) and data.get('session-id'):
            return data
    except Exception:
        pass
    try:
        import flask
        if flask.has_request_context():
            from meerschaum.api.dash.sessions import SESSION_COOKIE_NAME
            session_id = flask.request.cookies.get(SESSION_COOKIE_NAME, None)
            if session_id:
                return {'session-id': session_id}
    except Exception:
        pass
    return session_data if isinstance(session_data, dict) else {}


def session_id(session_data: Optional[Dict[str, Any]]) -> Optional[str]:
    """The session's ID, if any."""
    if not isinstance(session_data, dict):
        return None
    value = session_data.get('session-id', None)
    return str(value) if value else None


def can_read(session_data: Optional[Dict[str, Any]]) -> bool:
    """Whether the session may view pipes (any signed-in user, or `--no-auth`)."""
    from meerschaum.api import no_auth
    if no_auth:
        return True
    from meerschaum.api.dash.sessions import is_session_active
    sid = session_id(session_data)
    return bool(sid) and is_session_active(sid)


def can_act(session_data: Optional[Dict[str, Any]]) -> bool:
    """Whether the session may run actions and edit pipes."""
    from meerschaum.api.dash.sessions import is_state_authenticated
    return is_state_authenticated(session_data)


def username(session_data: Optional[Dict[str, Any]]) -> Optional[str]:
    """The session's username (`None` with `--no-auth`)."""
    from meerschaum.api.dash.sessions import get_username_from_session
    sid = session_id(session_data)
    return get_username_from_session(sid) if sid else None


def has_scope(session_data: Optional[Dict[str, Any]], scope: str) -> bool:
    """
    Whether the session's user holds `scope` (administrators and `--no-auth` hold every scope;
    a user without a `scopes` attribute holds every scope, as on the API).
    """
    from meerschaum.api import no_auth, get_api_connector
    if no_auth:
        return True
    from meerschaum.api.dash.sessions import session_is_admin
    sid = session_id(session_data)
    if not sid:
        return False
    if session_is_admin(sid):
        return True
    name = username(session_data)
    if not name:
        return False
    from meerschaum.core import User
    try:
        scopes = User(name, instance=get_api_connector()).get_scopes(refresh=True) or []
    except Exception:
        return False
    return '*' in scopes or scope in scopes


def can_write_pipes(session_data: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """`(allowed, reason)`: whether the session may edit or copy pipes' registrations."""
    if not can_act(session_data):
        return False, "You are not allowed to edit pipes."
    if not has_scope(session_data, 'pipes:write'):
        return False, "Your account does not have the `pipes:write` scope."
    return True, ''


def can_register(session_data: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """`(allowed, reason)`: whether the session may register new pipes."""
    from meerschaum.config import get_config
    if not get_config('api', 'permissions', 'registration', 'pipes', warn=False):
        return False, (
            "This server does not allow registering pipes "
            "(`api:permissions:registration:pipes`)."
        )
    return can_write_pipes(session_data)
