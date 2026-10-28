#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define caching functions for session management.
"""

import json
import time
import uuid
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


def valid_session_id(session_id: Any) -> Union[str, None]:
    """
    Return `session_id` if it is a console session ID (the lowercase hyphenated UUID the login
    mints, `str(uuid.uuid4())`), else `None`.

    A session ID comes from the browser (the `mrsm-session-id` cookie, or the `session-store`
    value a callback carries), and the instance store looks it up with a `params` filter, where
    strings such as `'_None'` (`IS NOT NULL`), `'_x'` (`!= 'x'`) or `'None'` are not literal
    values: `'_None'` would name the first live session. Every function here therefore treats any
    other value as no session, like the Rust server's session readers (`meerschaum-web`).
    """
    if isinstance(session_id, uuid.UUID):
        return str(session_id)
    if not isinstance(session_id, str) or len(session_id) != 36:
        return None
    try:
        canonical = str(uuid.UUID(session_id))
    except ValueError:
        return None
    return session_id if canonical == session_id else None

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
    ### `params` is a filter, not a literal match: only a UUID may reach it (`valid_session_id`).
    session_id = valid_session_id(session_id)
    if session_id is None:
        return None
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
    if valid_session_id(session_id) is None:
        raise ValueError("A console session ID is a UUID.")
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
    ### `'_None'` here would clear every session (`WHERE session_id IS NOT NULL`).
    session_id = valid_session_id(session_id)
    if session_id is None:
        return
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
    Return the session ID stored in a request's session cookie, if any (`None` when the cookie
    is not a session ID, see `valid_session_id()`).

    Works with both Flask and Starlette/FastAPI requests (both expose
    a `cookies` mapping). Validate the returned ID with
    `is_session_authenticated()` before performing any actions.
    """
    cookies = getattr(request, 'cookies', None)
    if cookies is None:
        return None
    return valid_session_id(cookies.get(SESSION_COOKIE_NAME, None))


def get_session_key(session_id: str) -> str:
    """
    Return the session key for the cache connector.
    """
    return SESSION_KEY_TEMPLATE.format(session_id=session_id)


def set_session(session_id: str, session_data: Dict[str, Any]) -> bool:
    """
    Set a `session_id` to a dictionary.

    Returns
    -------
    Whether the session was written: `False` (nothing written) when `session_id` is not a
    session ID (`valid_session_id()`), since no lookup would ever find it.
    """
    session_id = valid_session_id(session_id)
    if session_id is None:
        if debug:
            dprint("Not setting a session: its ID is not a UUID.")
        return False
    _cache_drop(session_id)
    conn = None if _use_instance_store() else get_cache_connector()
    if conn is None and _use_instance_store():
        _instance_set(session_id, json.dumps(session_data, separators=(',', ':')))
        return True
    if conn is None:
        _active_sessions[session_id] = session_data
        if debug:
            dprint(f"Setting in-memory data for {session_id}:\n{session_data}")
        return True

    session_key = get_session_key(session_id)
    session_data_str = json.dumps(session_data, separators=(',', ':'))
    if debug:
        dprint(f"Setting production data for {session_id=}:\n{session_data_str}")

    conn.set(session_key, session_data_str, ex=EXPIRES_SECONDS)
    return True


def update_session(session_id: Optional[str], session_data: Dict[str, Any]) -> bool:
    """
    Update the session's data dictionary.
    Return whether it was written (`False` for an ID that is not a session ID).
    """
    session_id = valid_session_id(session_id)
    if session_id is None:
        return False
    existing_session_data = get_session_data(session_id, use_cache=False) or {}
    existing_session_data.update(session_data)
    return set_session(session_id, existing_session_data)


def get_session_data(
    session_id: Optional[str],
    use_cache: bool = True,
) -> Union[Dict[str, Any], None]:
    """
    Return the session data dictionary (looked up at most once per `SESSION_CACHE_SECONDS`
    per process unless `use_cache` is `False`); `None` for an ID that is not a session ID.
    """
    key = valid_session_id(session_id)
    if key is None:
        return None
    if use_cache:
        cached = _cache_get(key)
        if cached is not None:
            return dict(cached) if isinstance(cached, dict) else cached
    data = _get_session_data(key)
    if use_cache and data is not None:
        _cache_set(key, dict(data))
    return data


def _get_session_data(session_id: Optional[str]) -> Union[Dict[str, Any], None]:
    """
    Return the session data dictionary from the store.
    """
    session_id = valid_session_id(session_id)
    if session_id is None:
        return None
    if debug:
        dprint(f"Getting session data for {session_id=}")
    conn = None if _use_instance_store() else get_cache_connector()
    if conn is None and _use_instance_store():
        return _instance_get(session_id)
    if conn is None:
        return _active_sessions.get(session_id, None)

    session_key = get_session_key(session_id)
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
    session_id = valid_session_id(session_id)
    if session_id is None:
        return False
    return get_username_from_session(session_id) is not None


def delete_session(session_id: str):
    """
    Delete a session if it's been set (nothing happens for an ID that is not a session ID).
    """
    session_id = valid_session_id(session_id)
    if session_id is None:
        return
    if debug:
        dprint(f"Deleting {session_id=}")
    _cache_drop(session_id)
    conn = None if _use_instance_store() else get_cache_connector()
    if conn is None and _use_instance_store():
        _instance_delete(session_id)
        return
    if conn is None:
        _ = _active_sessions.pop(session_id, None)
        return

    session_key = get_session_key(session_id)
    conn.client.delete(session_key)


def is_session_authenticated(session_id: Optional[str]) -> bool:
    """
    Return whether a session's user may run actions: the `actions:execute` permission of their
    effective permissions (administrators; users whose role or permissions include it, such as
    the Operator role; nobody but administrators under `--secure`). `--no-auth` allows everyone.

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

    from meerschaum.api.dash.access import get_viewer
    try:
        viewer = get_viewer(session_id=session_id)
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
    access instead (`meerschaum.api.dash.access.get_viewer(session_store_data)`).

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
    Check whether a session ID corresponds to an admin user (read again every few seconds, so a
    demotion applies at once: `meerschaum.api.dash.access.get_viewer`).
    """
    if debug:
        dprint(f"Check admin for {session_id=}")
    if no_auth:
        return True
    session_id = valid_session_id(session_id)
    if session_id is None:
        return False
    from meerschaum.api.dash.access import get_viewer
    try:
        viewer = get_viewer(session_id=session_id)
    except Exception:
        return False
    return bool(viewer.signed_in and viewer.is_admin)
