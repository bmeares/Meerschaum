#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Who may do what on the Pipes pages (`/dash/pipes`, a pipe's page and New pipe), over the
console's access model (`meerschaum.api.dash.access`):

- Reading (the table, a pipe's tabs) needs a console session (or `--no-auth`) and `pipes:read`;
  a pipe the viewer may not see (its team's, a private one) is never listed or shown.
- Each action needs its permission (Sync and Verify `pipes:write`, Clear and Delete
  `pipes:delete`, Drop `pipes:drop`, Copy `pipes:register`), edit rights on the pipe, and no
  lock against it. The server authorizes the actions again when they run (`POST /execute`).
- Registering needs `api:permissions:registration:pipes` and `pipes:register`, as
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


def viewer(session_data: Optional[Dict[str, Any]] = None):
    """The viewer of the session (`meerschaum.api.dash.access.Viewer`)."""
    from meerschaum.api.dash.access import get_viewer
    return get_viewer(session_data_from_context(session_data))


def can_read(session_data: Optional[Dict[str, Any]]) -> bool:
    """Whether the session may view pipes (a signed-in user with `pipes:read`, or `--no-auth`)."""
    who = viewer(session_data)
    return bool(who.no_auth or (who.signed_in and who.can('pipes:read')))


def can_act(session_data: Optional[Dict[str, Any]]) -> bool:
    """Whether the session may run actions (`actions:execute`)."""
    who = viewer(session_data)
    return bool(who.no_auth or (who.signed_in and who.can('actions:execute')))


def username(session_data: Optional[Dict[str, Any]]) -> Optional[str]:
    """The session's username (`None` with `--no-auth`)."""
    from meerschaum.api.dash.sessions import get_username_from_session
    sid = session_id(session_data_from_context(session_data))
    return get_username_from_session(sid) if sid else None


def has_scope(session_data: Optional[Dict[str, Any]], scope: str) -> bool:
    """Whether the session's user holds `scope` (administrators and `--no-auth` hold every one)."""
    who = viewer(session_data)
    return bool(who.no_auth or (who.signed_in and who.can(scope)))


def can_write_pipes(session_data: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """`(allowed, reason)`: whether the session may edit pipes' registrations (`pipes:write`)."""
    who = viewer(session_data)
    if not (who.no_auth or who.signed_in):
        return False, "Sign in to edit pipes."
    if not who.can('pipes:write'):
        return False, "Your account may not edit pipes."
    return True, ''


def can_register(session_data: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """`(allowed, reason)`: whether the session may register new pipes."""
    from meerschaum.config import get_config
    if not get_config('api', 'permissions', 'registration', 'pipes', warn=False):
        return False, (
            "This server does not allow registering pipes "
            "(`api:permissions:registration:pipes`)."
        )
    who = viewer(session_data)
    if not (who.no_auth or who.signed_in):
        return False, "Sign in to register pipes."
    if not who.can('pipes:register'):
        return False, "Your account may not register pipes."
    return True, ''


def can_define_sql(session_data: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """`(allowed, reason)`: whether the session may set or change SQL definitions."""
    who = viewer(session_data)
    if not who.can('pipes:define_sql'):
        return False, "Your account may not set SQL definitions."
    return True, ''
