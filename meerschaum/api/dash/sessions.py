#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Console sessions for the console and for plugins: the session store
(`meerschaum.api.dash.session_store`) and whether a session's user may act, asked of the one viewer
computation (`meerschaum.api.dash.viewer`), the same as every console page.
"""

from meerschaum.utils.typing import Dict, Any, Optional
from meerschaum.api import debug, no_auth
from meerschaum.utils.warnings import dprint
from meerschaum.api.dash.session_store import (
    CREDENTIAL_CHECK_SECONDS,
    EXPIRES_SECONDS,
    INSTANCE_SESSIONS_TARGET,
    SESSION_CACHE_SECONDS,
    SESSION_COOKIE_NAME,
    SESSION_KEY_TEMPLATE,
    SessionStoreUnavailable,
    clear_session_cookie,
    delete_session,
    get_instance_sessions_pipe,
    get_session_data,
    get_session_id_from_request,
    get_session_key,
    get_user_from_session,
    get_username_from_session,
    is_session_active,
    set_session,
    set_session_cookie,
    update_session,
    user_session_ids,
    valid_session_id,
)
import meerschaum.api.dash.viewer as viewer_model

__all__ = (
    'CREDENTIAL_CHECK_SECONDS',
    'EXPIRES_SECONDS',
    'INSTANCE_SESSIONS_TARGET',
    'SESSION_CACHE_SECONDS',
    'SESSION_COOKIE_NAME',
    'SESSION_KEY_TEMPLATE',
    'SessionStoreUnavailable',
    'clear_session_cookie',
    'delete_session',
    'get_instance_sessions_pipe',
    'get_session_data',
    'get_session_id_from_request',
    'get_session_key',
    'get_user_from_session',
    'get_username_from_session',
    'is_session_active',
    'is_session_authenticated',
    'is_state_authenticated',
    'public_callback',
    'session_is_admin',
    'set_session',
    'set_session_cookie',
    'update_session',
    'user_session_ids',
    'valid_session_id',
)


def is_session_authenticated(session_id: Optional[str]) -> bool:
    """
    Return whether a session's user may run actions: the `actions:execute` permission of their
    effective permissions (administrators; users whose role or permissions include it, such as
    the Operator role; nobody but administrators under `--secure`; never a member of an isolated
    team unless the server's native execution allows it, and never while acting as a team).
    `--no-auth` allows everyone.

    Reading pages needs only a session (`is_session_active`); what a page shows and which of
    its buttons appear follow the viewer's permissions (`meerschaum.api.dash.access`).

    Parameters
    ----------
    session_id: str
        The session UUID.

    Returns
    -------
    A bool whether the session is authenticated to perform actions.
    """
    if debug:
        dprint(f"Checking authentication for {session_id=}")
    if no_auth:
        return True
    session_id = valid_session_id(session_id)
    if session_id is None:
        return False
    try:
        viewer = viewer_model.resolve_viewer(session_id=session_id)
    except Exception:
        return False
    return bool(viewer.signed_in and viewer.can('actions:execute'))


def public_callback(func):
    """
    Mark a console callback public: the API host runs it for visitors without a session.
    Only the login and registration callbacks are; plugins use `@web_callback(..., public=True)`
    or `@dash_plugin(public=True)`. Place it above `@dash_app.callback(...)`.
    """
    from meerschaum.plugins import _mark_public_callback
    return _mark_public_callback(func)


def is_state_authenticated(session_store_data: Optional[Dict[str, Any]]) -> bool:
    """
    Return whether a callback's `session-store` state belongs to an
    authenticated session.

    The API host answers a callback request (`POST /dash/_dash-update-component`) only for a
    valid console session unless the callback is public (`meerschaum.api.host._callbacks`), but
    `api:dash:callbacks_require_session: false` turns that off, and a session is not an
    authorization: a callback that runs actions takes `State('session-store', 'data')` and checks
    it through this function (the `actions:execute` permission) before doing any work. A callback
    that reads or writes pipes, users or jobs checks the viewer's permissions and the pipe's
    access instead (`meerschaum.api.dash.access.resolve_viewer(session_store_data)`).

    Parameters
    ----------
    session_store_data: Optional[Dict[str, Any]]
        The `session-store` component's `data` dict, as passed into the callback.

    Returns
    -------
    A `bool` indicating whether the session may perform actions.
    """
    if not isinstance(session_store_data, dict):
        return False

    session_id = session_store_data.get('session-id', None)
    if not session_id:
        return is_session_authenticated(None)

    return is_session_authenticated(str(session_id))


def session_is_admin(session_id: Optional[str]) -> bool:
    """
    Check whether a session ID corresponds to an admin user (read again every few seconds, so a
    demotion applies at once; an administrator acting as a team is not one while acting).
    """
    if debug:
        dprint(f"Check admin for {session_id=}")
    if no_auth:
        return True
    session_id = valid_session_id(session_id)
    if session_id is None:
        return False
    try:
        viewer = viewer_model.resolve_viewer(session_id=session_id)
    except Exception:
        return False
    return bool(viewer.signed_in and viewer.is_admin)
