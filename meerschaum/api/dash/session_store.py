#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The console's session store: session IDs, the `mrsm-session-id` cookie, and the session data of
each signed-in browser (the instance's sessions pipe, Valkey, or this process), with a short
per-process cache. It decides nothing about rights: `meerschaum.api.dash.viewer` turns a session's
user into a viewer, and `meerschaum.api.dash.sessions` is the plugin-facing module over both.
"""

import json
import time
import uuid
import threading

from meerschaum.utils.typing import Union, Dict, Any, List, Optional
from meerschaum.api import debug
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

### A store that cannot be read (a busy instance: a pool or statement timeout, a dropped
### connection) is not a missing session: such a read raises `SessionStoreUnavailable` (the session
### is unknown, never assumed), and the API host answers `503` instead of sending the visitor to
### sign in. Only the `SESSION_CACHE_SECONDS` cache above is ever reused.
class SessionStoreUnavailable(RuntimeError):
    """The session store could not be read: whether the session exists is unknown."""


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
    ### `get_data` raises when the instance cannot be read (`get_doc` would answer `None`, the same
    ### as no session) and returns `None` while the table does not exist yet.
    try:
        docs = pipe.get_data(params={'session_id': session_id}, limit=1, as_docs=True, debug=debug)
    except Exception as e:
        raise SessionStoreUnavailable(f"Could not read the console's sessions: {e}") from e
    if not docs:
        return None
    doc = docs[0]
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
    ### A new session for a user is bound to the user's current credential (a user that does not
    ### exist gets none, so the session names nobody). `update_session` keeps the bound one.
    username = session_data.get('username', None) if isinstance(session_data, dict) else None
    if username is not None and 'credential' not in session_data:
        from meerschaum.api._oauth2 import user_credential
        credential = user_credential(str(username))
        if credential:
            session_data = {**session_data, 'credential': credential}
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
    ### Raises `SessionStoreUnavailable` when the store cannot be read: callers never mistake it
    ### for a missing session, and nothing older than the cache above is reused.
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
    try:
        session_data_str = conn.get(session_key)
    except Exception as e:
        raise SessionStoreUnavailable(f"Could not read the console's sessions: {e}") from e
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

    username = session_data.get('username', None)
    if username is None:
        return None
    ### A session is its user's only while it carries their current credential (one made before
    ### the username was deleted and registered again, before a password change, or before 5.0
    ### bound sessions to their user, is signed out).
    if not _credential_current(str(username), session_data.get('credential', None)):
        return None
    return username


#: How long a checked `(username, credential)` stays checked (the Rust server's identity cache).
CREDENTIAL_CHECK_SECONDS: float = 5.0
_credential_checks: Dict[Any, float] = {}


def _credential_current(username: str, credential: Any) -> bool:
    """
    Whether `credential` is `username`'s current one (`meerschaum.api._oauth2.credential_matches`),
    remembered for `CREDENTIAL_CHECK_SECONDS`. A read that fails raises `SessionStoreUnavailable`.
    """
    if not credential or not isinstance(credential, str):
        return False
    key = (username, credential)
    now = time.monotonic()
    checked_at = _credential_checks.get(key, None)
    if checked_at is not None and now - checked_at < CREDENTIAL_CHECK_SECONDS:
        return True
    from meerschaum.api._oauth2 import credential_matches
    try:
        matches = credential_matches(username, credential)
    except Exception as e:
        from meerschaum.utils.warnings import warn
        warn(f"Could not read the console user's credential: {e}", stack=False)
        raise SessionStoreUnavailable("Could not read the console user's credential.") from e
    if not matches:
        _credential_checks.pop(key, None)
        return False
    if len(_credential_checks) > 4096:
        _credential_checks.clear()
    _credential_checks[key] = now
    return True


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


def user_session_ids(username: str) -> List[str]:
    """
    The IDs of every session of `username` in the store (the instance's sessions pipe, Valkey, or
    this process), or `[]` when the store cannot be read.
    """
    session_ids: List[str] = []
    try:
        if _use_instance_store():
            pipe = get_instance_sessions_pipe()
            if pipe.exists():
                for doc in pipe.get_docs(select_columns=['session_id', 'data']) or []:
                    try:
                        data = json.loads(doc.get('data') or '{}')
                    except Exception:
                        continue
                    if isinstance(data, dict) and data.get('username') == username:
                        session_ids.append(str(doc.get('session_id')))
            return session_ids
        cache = get_cache_connector()
        if cache is None:
            return [
                str(session_id)
                for session_id, data in list(_active_sessions.items())
                if isinstance(data, dict) and data.get('username') == username
            ]
        prefix = SESSION_KEY_TEMPLATE.format(session_id='')
        for key in cache.client.scan_iter(match=prefix + '*'):
            key = key.decode('utf-8') if isinstance(key, bytes) else str(key)
            try:
                data = json.loads(cache.get(key) or '{}')
            except Exception:
                continue
            if isinstance(data, dict) and data.get('username') == username:
                session_ids.append(key[len(prefix):])
    except Exception:
        return []
    return session_ids
