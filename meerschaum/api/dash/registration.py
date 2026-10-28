#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Who may create an account from the console, and the accounts it creates:

- **First run.** While the API's user store has no users, the login page offers
  **Create admin account**: the first account is an administrator (`create_first_admin`).
- **Self-registration** (`/dash/register`) is open only when `api:permissions:registration:users`
  is `true` (off by default) and the server is not under the public preset. A self-registered
  user gets the default role (`api:permissions:default_role`) and is in the team Everyone, like
  every user: nothing about either is stored with the account.

The checks run in the callbacks themselves (the visitor has no session to trust). A hosted console
registers through the Rust server, sharing its first-user lock with HTTP clients. In-process hosts
without a server URL use a local lock.
"""

from __future__ import annotations

import threading

from meerschaum.utils.typing import Any, Dict, Optional, SuccessTuple, Tuple

_first_user_lock = threading.Lock()


def _users(conn: Any = None) -> list:
    from meerschaum.api import get_api_connector
    conn = conn if conn is not None else get_api_connector()
    return list(conn.get_users() or [])


def first_run(conn: Any = None) -> bool:
    """
    Whether the API's user store has no users yet (and accounts may be created from the console:
    not with `--no-auth`, nor under the public preset). A store that cannot be read is not a
    first run: the login form shows instead.
    """
    from meerschaum.api import no_auth
    if no_auth or _public_preset():
        return False
    try:
        return not _users(conn)
    except Exception:
        return False


def _public_preset() -> bool:
    try:
        from meerschaum.api import public_preset
        return bool(public_preset)
    except Exception:
        return False


def self_registration_open() -> bool:
    """
    Whether visitors may register their own accounts: `api:permissions:registration:users` is
    `true` (it ships `false`) and the server is not under the public preset.
    """
    from meerschaum.config import get_config
    if _public_preset():
        return False
    ### The server's own answer (it passes `registration_users` to this host), else the config.
    try:
        from meerschaum.api.host import get_host_config
        resolved = get_host_config().get('registration_users', None)
    except Exception:
        resolved = None
    if isinstance(resolved, bool):
        return resolved
    return get_config('api', 'permissions', 'registration', 'users', warn=False) is True


def registration_allowed(conn: Any = None) -> bool:
    """Whether an account may be created now: on a first run, or with self-registration open."""
    return first_run(conn) or self_registration_open()


def validate_account(
    username: Optional[str],
    password: Optional[str],
    confirm: Optional[str] = None,
    email: Optional[str] = None,
) -> SuccessTuple:
    """A valid username, a long enough password typed twice the same, and a valid email if given."""
    from meerschaum.api.dash.users import validate_new_user
    return validate_new_user(username, password, confirm, email)


def _register_on_server(username: str, password: str, email: Optional[str]) -> Optional[SuccessTuple]:
    """Use the server's single registration lock; `None` only for an in-process host."""
    import json
    import urllib.error
    import urllib.parse
    import urllib.request
    from meerschaum.api.dash.execute import server_url, _error_message
    base_url = server_url()
    if not base_url:
        return None
    form = {'username': username, 'password': password}
    if email:
        form['email'] = str(email).strip()
    request = urllib.request.Request(
        base_url + '/users/register',
        data=urllib.parse.urlencode(form).encode('utf-8'),
        headers={'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        return False, _error_message(e)
    except Exception as e:
        return False, f"Could not register the account with the server: {e}"
    if isinstance(payload, list) and len(payload) == 2 and isinstance(payload[0], bool):
        return payload[0], str(payload[1])
    if isinstance(payload, dict) and isinstance(payload.get('success'), bool):
        return payload['success'], str(payload.get('message') or '')
    return False, 'The server returned an invalid registration response.'


def create_first_admin(
    username: Optional[str],
    password: Optional[str],
    confirm: Optional[str] = None,
    email: Optional[str] = None,
    conn: Any = None,
) -> SuccessTuple:
    """
    Register the instance's first account as an administrator, only while it has no users.
    """
    from meerschaum.api import get_api_connector, debug, no_auth
    from meerschaum.core import User
    if no_auth or _public_preset():
        return False, "Accounts cannot be created here."
    valid, message = validate_account(username, password, confirm, email)
    if not valid:
        return False, message
    conn = conn if conn is not None else get_api_connector()
    username = str(username).strip()
    with _first_user_lock:
        try:
            if _users(conn):
                return False, "This instance already has users: sign in."
        except Exception as e:
            return False, f"Could not read the users: {e}"
        result = _register_on_server(username, password, email)
        if result is not None:
            return result
        user = User(
            username,
            password,
            type='admin',
            email=(str(email).strip() or None) if email else None,
            instance=conn,
        )
        try:
            return conn.register_user(user, debug=debug)
        except Exception as e:
            return False, f"Failed to create '{username}': {e}"


def register_account(
    username: Optional[str],
    password: Optional[str],
    email: Optional[str] = None,
    confirm: Optional[str] = None,
    conn: Any = None,
) -> Tuple[SuccessTuple, bool]:
    """
    Register a visitor's own account (`/dash/register`). Returns `((success, message), admin)`:
    on a first run the account is an administrator; otherwise self-registration must be open and
    the account gets the default role.
    """
    from meerschaum.api import get_api_connector, debug
    from meerschaum.core import User
    conn = conn if conn is not None else get_api_connector()
    if first_run(conn):
        result = create_first_admin(username, password, confirm, email, conn=conn)
        return result, bool(result[0])
    if not self_registration_open():
        return (False, "Registration is closed."), False
    valid, message = validate_account(username, password, confirm, email)
    if not valid:
        return (False, message), False
    username = str(username).strip()
    result = _register_on_server(username, password, email)
    if result is not None:
        return result, False
    user = User(
        username,
        password,
        email=(str(email).strip() or None) if email else None,
        instance=conn,
    )
    try:
        if conn.get_user_id(user, debug=debug) is not None:
            return (False, f"'{username}' is taken."), False
        return conn.register_user(user, debug=debug), False
    except Exception as e:
        return (False, f"Failed to register '{username}': {e}"), False


def sign_in(username: str, response: Any = None, secure: bool = False) -> Dict[str, Any]:
    """
    Start a console session for `username` (after its password was checked) and return the
    `session-store` data; with `response`, also set the session cookie on it.
    """
    import uuid
    from meerschaum.api.dash.sessions import set_session, set_session_cookie
    session_id = str(uuid.uuid4())
    set_session(session_id, {'username': username})
    if response is not None:
        set_session_cookie(response, session_id, secure=secure)
    return {'session-id': session_id}
