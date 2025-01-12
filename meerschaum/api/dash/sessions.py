#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define caching functions for session management.
"""

import json

from meerschaum.utils.typing import Union, Dict, Any, Optional
from meerschaum.api import debug, no_auth
from meerschaum.api import get_cache_connector, get_api_connector
from meerschaum.core import User
from meerschaum.config import get_config
from meerschaum.utils.warnings import dprint

SESSION_KEY_TEMPLATE: str = 'mrsm_session_id:{session_id}'
EXPIRES_SECONDS: int = get_config('system', 'api', 'cache', 'session_expires_minutes') * 60
_active_sessions: Dict[str, Dict[str, Any]] = {}


def get_session_key(session_id: str) -> str:
    """
    Return the session key for the cache connector.
    """
    return SESSION_KEY_TEMPLATE.format(session_id=session_id)


def set_session(session_id: str, session_data: Dict[str, Any]):
    """
    Set a `session_id` to a dictionary.
    """
    conn = get_cache_connector()
    if conn is None:
        _active_sessions[session_id] = session_data
        if debug:
            dprint(f"Setting in-memory data for {session_id}:\n{session_data}")
        return

    session_key = get_session_key(session_id)
    session_data_str = json.dumps(session_data, separators=(',', ':'))
    if debug:
        dprint(f"Setting production data for {session_id=}:\n{session_data_str}")

    conn.set(session_key, session_data_str, ex=EXPIRES_SECONDS)


def update_session(session_id: Optional[str], session_data: Dict[str, Any]):
    """
    Update the session's data dictionary.
    """
    existing_session_data = get_session_data(session_id) or {}
    existing_session_data.update(session_data)
    set_session(str(session_id), existing_session_data)


def get_session_data(session_id: Optional[str]) -> Union[Dict[str, Any], None]:
    """
    Return the session data dictionary.
    """
    if debug:
        dprint(f"Getting session data for {session_id=}")
    conn = get_cache_connector()
    if conn is None:
        return _active_sessions.get(str(session_id), None)

    session_key = get_session_key(str(session_id))
    session_data_str = conn.get(session_key)
    if not session_data_str:
        return None

    return json.loads(session_data_str)


def get_username_from_session(session_id: Optional[str]) -> Union[str, None]:
    """
    If a `session_id` has been set, return the username.
    Otherwise return `None`.
    """
    if debug:
        dprint(f"Getting username for {session_id=}")
    session_data = get_session_data(session_id)
    if session_data is None:
        return None

    return session_data.get('username', None)


def is_session_active(session_id: Union[str, None]) -> bool:
    """
    Return whether a given `session_id` has been set.
    """
    return get_username_from_session(str(session_id)) is not None


def delete_session(session_id: str):
    """
    Delete a session if it's been set.
    """
    ### TODO: Delete webterm sessions.
    if debug:
        dprint(f"Deleting {session_id=}")
    conn = get_cache_connector()
    if conn is None:
        _ = _active_sessions.pop(session_id, None)
        return

    session_key = get_session_key(session_id)
    conn.client.delete(session_key)


def is_session_authenticated(session_id: Optional[str]) -> bool:
    """
    Check is a session ID is active.
    If running in secure mode, check whether a session ID corresponds to an admin.

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

    if session_id is None:
        return False

    session_data = get_session_data(session_id)
    if session_data is None:
        return False
    username = session_data.get('username', None)
    if username is None:
        return False

    cached_auth = session_data.get('authenticated', None)
    if cached_auth is not None:
        return cached_auth

    permissions = get_config('system', 'api', 'permissions')
    allow_non_admin = permissions.get('actions', {}).get('non_admin', False)

    is_auth = True if allow_non_admin else session_is_admin(session_id)
    update_session(session_id, {'authenticated': is_auth})
    return is_auth


def session_is_admin(session_id: str) -> bool:
    """
    Check whether a session ID corresponds to an admin user.
    """
    if debug:
        dprint(f"Check admin for {session_id=}")
    session_data = get_session_data(session_id)
    if session_data is None:
        return False

    username = session_data.get('username', None)
    if username is None:
        return False

    cached_admin = session_data.get('admin', None)
    if cached_admin is not None:
        return cached_admin

    conn = get_api_connector()
    user = User(username, instance=conn)
    user_type = conn.get_user_type(user, debug=debug)
    is_admin = user_type == 'admin'
    update_session(session_id, {'admin': is_admin})
    return is_admin
