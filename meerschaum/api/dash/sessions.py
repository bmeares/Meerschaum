#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define caching functions for session management.
"""

import json
import time
import threading

from meerschaum.utils.typing import Union, Dict, Any, Optional
from meerschaum.api import debug, no_auth
from meerschaum.api import get_cache_connector, get_api_connector
from meerschaum.core import User
from meerschaum.config import get_config
from meerschaum.utils.warnings import dprint

SESSION_KEY_TEMPLATE: str = 'mrsm_session_id:{session_id}'
SESSION_COOKIE_NAME: str = 'mrsm-session-id'
EXPIRES_SECONDS: int = get_config('api', 'cache', 'session_expires_minutes') * 60
_active_sessions: Dict[str, Dict[str, Any]] = {}

### The API host always keeps sessions in the instance, including when Valkey is configured,
### where the Rust server's terminal reads them too (`crates/meerschaum-server/src/webterm.rs`): rows
### of `session_id` (primary key), `data` (compact JSON string) and `expires` (Unix seconds)
### in the temporary pipe `mrsm_sessions`.
INSTANCE_SESSIONS_TARGET: str = 'mrsm_sessions'
_instance_pipe: Any = None
_instance_write_lock = threading.Lock()

### Every Dash callback and every plugin route looks its session up; in this process a result is
### reused for `SESSION_CACHE_SECONDS` (dropped when this process changes or deletes the session,
### so a logout is immediate here and at most this stale in another process).
SESSION_CACHE_SECONDS: float = 5.0
_session_cache: Dict[str, Any] = {}
_session_cache_lock = threading.Lock()


def _cache_get(session_id: str):
    with _session_cache_lock:
        entry = _session_cache.get(session_id, None)
    if entry is None:
        return None
    expires_at, data = entry
    if time.monotonic() >= expires_at:
        with _session_cache_lock:
            _session_cache.pop(session_id, None)
        return None
    return data


def _cache_set(session_id: str, data: Any) -> None:
    now = time.monotonic()
    with _session_cache_lock:
        if len(_session_cache) > 1024:
            for key in [k for k, (until, _) in _session_cache.items() if until <= now]:
                _session_cache.pop(key, None)
        _session_cache[session_id] = (now + SESSION_CACHE_SECONDS, data)


def _cache_drop(session_id: Any) -> None:
    with _session_cache_lock:
        _session_cache.pop(str(session_id), None)


def _use_instance_store() -> bool:
    """
    Return whether sessions live in the instance (API host, shared with the Rust terminal).
    """
    import meerschaum.api as _api
    return bool(getattr(_api, '_host_mode', False))


def get_instance_sessions_pipe():
    """
    Return the temporary pipe holding sessions, with the parameters the Rust server's session store (`meerschaum-web`) expects.
    """
    global _instance_pipe
    if _instance_pipe is None:
        from meerschaum import Pipe
        _instance_pipe = Pipe(
            'mrsm', 'sessions',
            instance=get_api_connector(),
            temporary=True,
            parameters={
                'target': INSTANCE_SESSIONS_TARGET,
                'columns': {'primary': 'session_id'},
                'dtypes': {'session_id': 'str', 'data': 'str', 'expires': 'int'},
                'upsert': True,
                'hypertable': False,
            },
        )
    return _instance_pipe


def _instance_get(session_id: str) -> Union[Dict[str, Any], None]:
    pipe = get_instance_sessions_pipe()
    if not pipe.exists():
        return None
    doc = pipe.get_doc(params={'session_id': session_id})
    if not doc:
        return None
    try:
        expires = int(doc.get('expires', 0) or 0)
    except (TypeError, ValueError):
        expires = 0
    if expires <= int(time.time()):
        return None
    try:
        return json.loads(doc.get('data', None))
    except Exception:
        return None


def _instance_set(session_id: str, data_str: str) -> None:
    pipe = get_instance_sessions_pipe()
    doc = {
        'session_id': session_id,
        'data': data_str,
        'expires': int(time.time()) + EXPIRES_SECONDS,
    }
    with _instance_write_lock:
        success, message = pipe.sync([doc], check_existing=False, debug=debug)
    if not success:
        raise RuntimeError(f"Failed to write session: {message}")


def _instance_delete(session_id: str) -> None:
    pipe = get_instance_sessions_pipe()
    if not pipe.exists():
        return
    with _instance_write_lock:
        pipe.clear(params={'session_id': session_id}, debug=debug)


def set_session_cookie(response, session_id: str, secure: bool = False) -> None:
    """
    Attach the session ID to a response as an `HttpOnly` cookie.

    Works with both Flask responses (Dash callbacks, via
    `dash.callback_context.response`) and Starlette/FastAPI responses.
    The cookie is scoped to `path='/'` so plain HTTP routes outside the
    `/dash` WSGI mount (e.g. `/webterm`) can read the session.

    Parameters
    ----------
    response: Any
        A response object exposing `set_cookie()` (Flask or Starlette).

    session_id: str
        The session UUID to store in the cookie.

    secure: bool, default False
        If `True`, mark the cookie as `Secure` (HTTPS-only).
    """
    response.set_cookie(
        SESSION_COOKIE_NAME,
        str(session_id),
        max_age=EXPIRES_SECONDS,
        path='/',
        httponly=True,
        samesite='Lax',
        secure=secure,
    )


def clear_session_cookie(response) -> None:
    """
    Remove the session cookie from a response (e.g. on logout).
    """
    ### Dash's response adapter (`dash.callback_context.response`) has `set_cookie` only.
    if hasattr(response, 'delete_cookie'):
        response.delete_cookie(SESSION_COOKIE_NAME, path='/')
        return
    response.set_cookie(
        SESSION_COOKIE_NAME,
        '',
        max_age=0,
        expires=0,
        path='/',
        httponly=True,
        samesite='Lax',
    )


def get_session_id_from_request(request) -> Union[str, None]:
    """
    Return the session ID stored in a request's session cookie, if any.

    Works with both Flask and Starlette/FastAPI requests (both expose
    a `cookies` mapping). Validate the returned ID with
    `is_session_authenticated()` before performing any actions.
    """
    cookies = getattr(request, 'cookies', None)
    if cookies is None:
        return None
    return cookies.get(SESSION_COOKIE_NAME, None) or None


def get_session_key(session_id: str) -> str:
    """
    Return the session key for the cache connector.
    """
    return SESSION_KEY_TEMPLATE.format(session_id=session_id)


def set_session(session_id: str, session_data: Dict[str, Any]):
    """
    Set a `session_id` to a dictionary.
    """
    _cache_drop(session_id)
    conn = None if _use_instance_store() else get_cache_connector()
    if conn is None and _use_instance_store():
        _instance_set(str(session_id), json.dumps(session_data, separators=(',', ':')))
        return
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
    existing_session_data = get_session_data(session_id, use_cache=False) or {}
    existing_session_data.update(session_data)
    set_session(str(session_id), existing_session_data)


def get_session_data(
    session_id: Optional[str],
    use_cache: bool = True,
) -> Union[Dict[str, Any], None]:
    """
    Return the session data dictionary (looked up at most once per `SESSION_CACHE_SECONDS`
    per process unless `use_cache` is `False`).
    """
    key = str(session_id)
    if use_cache:
        cached = _cache_get(key)
        if cached is not None:
            return dict(cached) if isinstance(cached, dict) else cached
    data = _get_session_data(session_id)
    if use_cache and data is not None:
        _cache_set(key, dict(data))
    return data


def _get_session_data(session_id: Optional[str]) -> Union[Dict[str, Any], None]:
    """
    Return the session data dictionary from the store.
    """
    if debug:
        dprint(f"Getting session data for {session_id=}")
    conn = None if _use_instance_store() else get_cache_connector()
    if conn is None and _use_instance_store():
        return _instance_get(str(session_id))
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


def get_user_from_session(session_id: Optional[str]) -> Union[User, None]:
    """
    Return a `User` from the current session.
    """
    username = get_username_from_session(session_id)
    if username is None:
        return None
    
    conn = get_api_connector()
    return User(username, instance=conn)


def is_session_active(session_id: Union[str, None]) -> bool:
    """
    Return whether a given `session_id` has been set.
    """
    return get_username_from_session(str(session_id)) is not None


def delete_session(session_id: str):
    """
    Delete a session if it's been set.
    """
    if debug:
        dprint(f"Deleting {session_id=}")
    _cache_drop(session_id)
    conn = None if _use_instance_store() else get_cache_connector()
    if conn is None and _use_instance_store():
        _instance_delete(str(session_id))
        return
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

    permissions = get_config('api', 'permissions')
    allow_non_admin = permissions.get('actions', {}).get('non_admin', False)

    is_auth = True if allow_non_admin else session_is_admin(session_id)
    update_session(session_id, {'authenticated': is_auth})
    return is_auth


def is_state_authenticated(session_store_data: Optional[Dict[str, Any]]) -> bool:
    """
    Return whether a callback's `session-store` state belongs to an
    authenticated session.

    Dash callbacks are reachable by anyone who can POST to
    `/dash/_dash-update-component` — the WSGI-mounted Dash app sits outside
    FastAPI's auth, and the session ID arrives as callback state (the login
    callback also mirrors it into the `mrsm-session-id` cookie for plain HTTP
    routes, but callbacks read the store). So any callback which reads or
    writes pipes, users, tokens, or job logs must take
    `State('session-store', 'data')` and check it through this function
    before doing any work.

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
