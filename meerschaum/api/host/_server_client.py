#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Hardened Python-to-Rust backchannel client (`MainServerClient`) and transport seam.

Every HTTP call from the Python API host / Dash console back to the supervising Rust server
(`server_url` in `MRSM_API_HOST_CONFIG`) goes through `MainServerClient`:

- **Origin validation** (`validate_origin`): only plain `http`/`https` host origins with no
  userinfo, path, query, or fragment are accepted, and request paths must be root-relative
  (`/` without `//`).
- **Hardened production transport** (`UrllibTransport`): explicitly disables environment proxies
  (`ProxyHandler({})`) so local auth tokens never leak to `HTTP_PROXY`/`HTTPS_PROXY`, refuses
  all 3xx redirects (`_NoRedirect`), enforces timeouts, and bounds response reads (`max_bytes + 1`).
- **In-memory test transport** (`InMemoryTransport`): records requests and returns canned
  responses in-process with zero sockets.
- **Explicit auth modes**: strongly separated (`AnonymousAuth`, `InternalHostAuth`,
  `SessionCookieAuth`, `SessionBoundBearerAuth`, `UserBearerAuth`) so callers cannot
  accidentally upgrade a cookie-ceiling request to `X-Mrsm-Host-Token`.
- **Unified JWT minting**: consolidates internal host tokens, user bearers, scoped read tokens,
  and session-bound (`CREDENTIAL_CLAIM`) bearers in one place.
"""

from __future__ import annotations

import contextlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Iterator, Protocol, Sequence, Union

from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, Tuple


# ------------------------------------------------------------------------------------------------
# Exceptions & raw wire types
# ------------------------------------------------------------------------------------------------

class ServerClientError(RuntimeError):
    """Base error for Python-to-Rust server backchannel requests."""


class ServerUnavailableError(ServerClientError):
    """No trusted server URL is configured or the server could not be reached."""


class ResponseTooLargeError(ServerClientError):
    """The server response exceeded the caller's byte bound."""


class ServerHTTPError(ServerClientError):
    """The server answered with a non-2xx HTTP status (including refused 3xx redirects)."""

    def __init__(self, status: int, body: bytes = b'', reason: str = ''):
        self.status = int(status)
        self.code = self.status
        self.body = bytes(body or b'')
        self.reason = str(reason or '')
        self.message = extract_error_message(self.body, fallback=(self.reason or f"HTTP {self.status}"))
        super().__init__(self.message)


@dataclass(frozen=True)
class RawResponse:
    """Raw HTTP response returned by a `Transport`."""

    status: int
    body: bytes = b''
    headers: Dict[str, str] = field(default_factory=dict)
    reason: str = ''


@dataclass(frozen=True)
class RecordedRequest:
    """One request captured by `InMemoryTransport`."""

    method: str
    path: str
    headers: Dict[str, str]
    body: Optional[bytes] = None
    timeout: float = 30.0
    max_bytes: Optional[int] = None

    def json(self) -> Any:
        return json.loads((self.body or b'').decode('utf-8'))


# ------------------------------------------------------------------------------------------------
# Origin & path validation
# ------------------------------------------------------------------------------------------------

def server_url() -> Optional[str]:
    """
    Return the URL of the Rust server that runs this host (`server_url` in the host
    configuration), if it gave one.
    """
    from meerschaum.api.host import get_host_config
    url = get_host_config().get('server_url', None)
    return str(url).rstrip('/') if url else None


def _active_server_url() -> Optional[str]:
    """
    Resolve the current server URL, honoring test patches on `meerschaum.api.dash.execute.server_url`
    when `execute` is already loaded.
    """
    execute_mod = sys.modules.get('meerschaum.api.dash.execute')
    if execute_mod is not None:
        fn = getattr(execute_mod, 'server_url', None)
        if callable(fn) and fn is not server_url:
            val = fn()
            return str(val).rstrip('/') if val else None
    return server_url()


def validate_origin(base_url: Any) -> str:
    """
    Validate that `base_url` is a clean `http`/`https` origin (no userinfo, path, query, or
    fragment) and return it without a trailing slash.
    """
    if not isinstance(base_url, str) or not base_url.strip():
        raise ServerUnavailableError('Trusted Main origin is unavailable.')
    cleaned = base_url.strip()
    parsed = urllib.parse.urlsplit(cleaned)
    if (
        parsed.scheme not in ('http', 'https')
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ('', '/')
        or parsed.query
        or parsed.fragment
    ):
        raise ServerUnavailableError('Invalid trusted Main origin.')
    return cleaned.rstrip('/')


def validate_path(path: Any) -> str:
    """Ensure `path` is a root-relative path on the configured origin (never scheme/host-relative)."""
    if not isinstance(path, str) or not path.startswith('/') or path.startswith('//'):
        raise ValueError('Server request path must start with a single "/".')
    return path


# ------------------------------------------------------------------------------------------------
# Unified JSON error extraction & v5 envelope decoding
# ------------------------------------------------------------------------------------------------

def extract_error_message(body: bytes, fallback: str = '') -> str:
    """
    Extract the human-readable error message from a v5 `{"success": false, "message": ...}` or
    FastAPI `{"detail": ...}` body, falling back to `fallback`.
    """
    try:
        payload = json.loads(body.decode('utf-8'))
    except Exception:
        return fallback
    if isinstance(payload, dict):
        for key in ('message', 'detail'):
            if payload.get(key):
                return str(payload[key])
    return fallback


def decode_envelope(body: Any) -> Any:
    """
    Read a v5 route's `{"success", "message", ...}` the way console callers expect: its single
    data field (`/execute/runs` -> list of runs), or `[success, message]` when it has none
    (`/execute/validate`, `.../retain`). Other bodies are returned unchanged.
    """
    if (
        isinstance(body, dict)
        and isinstance(body.get('success', None), bool)
        and isinstance(body.get('message', None), str)
    ):
        data = {key: value for key, value in body.items() if key not in ('success', 'message')}
        if len(data) == 1:
            return next(iter(data.values()))
        if not data:
            return [body['success'], body['message']]
    return body


# ------------------------------------------------------------------------------------------------
# Unified JWT minting
# ------------------------------------------------------------------------------------------------

def mint_internal_token(minutes: int = 5) -> str:
    """
    Return a token the Rust server accepts as the host's (`X-Mrsm-Host-Token`): an HS256 JWT
    signed with the API secret whose `mrsm_host` claim is `mcp`, valid for `minutes`.
    """
    from meerschaum.api.host import TOKEN_CLAIM
    from meerschaum.api._oauth2 import manager
    return manager.create_access_token(
        data={TOKEN_CLAIM: 'mcp'},
        expires=timedelta(minutes=minutes),
        scopes=['*'],
    )


def mint_user_bearer(
    username: Optional[str],
    *,
    scopes: Optional[Iterable[str]] = None,
    minutes: int = 5,
    respect_no_auth: bool = True,
) -> Optional[str]:
    """
    Return a short-lived bearer token for `username`, signed with the shared API secret.

    - When `scopes` is `None`: uses `username`'s current effective permissions from `viewer_for`
      (returns `None` if `--no-auth` when `respect_no_auth=True`, or if the user is not signed in).
    - When `scopes` is provided (e.g. `['plugins:read']`): mints a token narrowed to those scopes.
    """
    if not username:
        return None
    from meerschaum.api import no_auth, get_api_connector, debug
    from meerschaum.api._oauth2 import manager
    if respect_no_auth and no_auth:
        return None
    if scopes is not None:
        from meerschaum.core import User
        conn = get_api_connector()
        user_type = conn.get_user_type(User(username, instance=conn), debug=debug)
        return manager.create_access_token(
            data={'sub': username, 'type': user_type},
            expires=timedelta(minutes=minutes),
            scopes=list(scopes),
        )
    from meerschaum.api.dash.access import viewer_for
    viewer = viewer_for(username)
    if not viewer.signed_in:
        return None
    resolved_scopes = ['*'] if viewer.is_admin else sorted(viewer.permissions)
    return manager.create_access_token(
        data={'sub': username, 'type': ('admin' if viewer.is_admin else None)},
        expires=timedelta(minutes=minutes),
        scopes=resolved_scopes,
    )


def mint_session_bound_bearer(
    session_id: Optional[str],
    *,
    username: Optional[str] = None,
    scopes: Optional[Iterable[str]] = None,
    seconds: int = 60,
) -> Optional[str]:
    """
    Mint a short-lived user bearer bound to the session's exact stored `credential`
    (`CREDENTIAL_CLAIM`), verified freshly against the session store (never adopting a
    deleted/re-registered user during a wait).
    """
    from meerschaum.api.dash.sessions import get_session_data, valid_session_id
    from meerschaum.api._oauth2 import manager, CREDENTIAL_CLAIM, PRINCIPAL_CLAIM
    sid = valid_session_id(session_id)
    if not sid:
        return None
    current = get_session_data(sid, use_cache=False)
    if not isinstance(current, dict):
        return None
    session_user = current.get('username')
    credential = current.get('credential')
    if (
        not isinstance(session_user, str)
        or not session_user
        or not isinstance(credential, str)
        or not credential
    ):
        return None
    if username is not None and session_user != username:
        return None
    if scopes is None:
        from meerschaum.api.dash.access import viewer_for
        viewer = viewer_for(session_user)
        if not viewer.signed_in:
            return None
        resolved_scopes = sorted(viewer.permissions)
    else:
        resolved_scopes = list(scopes)
    return manager.create_access_token(
        data={
            'sub': session_user,
            PRINCIPAL_CLAIM: 'user',
            CREDENTIAL_CLAIM: credential,
        },
        expires=timedelta(seconds=seconds),
        scopes=resolved_scopes,
    )


# ------------------------------------------------------------------------------------------------
# Explicit, strongly separated auth modes
# ------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class AnonymousAuth:
    """No credentials attached."""


@dataclass(frozen=True)
class InternalHostAuth:
    """Host-to-server internal auth (`X-Mrsm-Host-Token`, optional `X-Mrsm-User`)."""

    user: Optional[str] = None
    minutes: int = 5


@dataclass(frozen=True)
class SessionCookieAuth:
    """
    Console session cookie auth (`mrsm-session-id=<uuid>`), optionally preserving any
    explicitly presented `Authorization` bearer ceiling from the active Flask request.
    Never mints a JWT and never attaches `X-Mrsm-Host-Token`.
    """

    session_id: str
    forward_auth_ceiling: bool = True


@dataclass(frozen=True)
class SessionBoundBearerAuth:
    """Short-lived bearer JWT bound to a live session's `CREDENTIAL_CLAIM`."""

    session_id: str
    username: Optional[str] = None
    scopes: Optional[Tuple[str, ...]] = None
    seconds: int = 60


@dataclass(frozen=True)
class UserBearerAuth:
    """Short-lived bearer JWT minted for `username` (no-op under `--no-auth` or `None`)."""

    username: Optional[str] = None
    scopes: Optional[Tuple[str, ...]] = None
    minutes: int = 5


AuthMode = Union[
    AnonymousAuth,
    InternalHostAuth,
    SessionCookieAuth,
    SessionBoundBearerAuth,
    UserBearerAuth,
]


def _auth_headers(auth: AuthMode) -> Dict[str, str]:
    """Build strictly scoped auth headers for `auth`."""
    if isinstance(auth, AnonymousAuth):
        return {}
    if isinstance(auth, InternalHostAuth):
        from meerschaum.api.host import TOKEN_HEADER
        headers = {TOKEN_HEADER: mint_internal_token(minutes=auth.minutes)}
        if auth.user:
            headers['X-Mrsm-User'] = str(auth.user)
        return headers
    if isinstance(auth, SessionCookieAuth):
        from meerschaum.api.dash.sessions import SESSION_COOKIE_NAME, valid_session_id
        sid = valid_session_id(auth.session_id)
        if not sid:
            raise ValueError('A valid console session ID is required.')
        headers = {'cookie': f'{SESSION_COOKIE_NAME}={sid}'}
        if auth.forward_auth_ceiling:
            try:
                from flask import has_request_context, request as browser_request
                if has_request_context() and 'Authorization' in browser_request.headers:
                    headers['authorization'] = browser_request.headers['Authorization']
            except Exception:
                pass
        return headers
    if isinstance(auth, SessionBoundBearerAuth):
        token = mint_session_bound_bearer(
            auth.session_id,
            username=auth.username,
            scopes=auth.scopes,
            seconds=auth.seconds,
        )
        if not token:
            raise PermissionError('A current console session is required.')
        return {'authorization': f'Bearer {token}'}
    if isinstance(auth, UserBearerAuth):
        execute_mod = sys.modules.get('meerschaum.api.dash.execute')
        if (
            auth.scopes is None
            and execute_mod is not None
            and callable(getattr(execute_mod, 'mint_bearer', None))
        ):
            token = execute_mod.mint_bearer(auth.username, minutes=auth.minutes)
        else:
            token = mint_user_bearer(
                auth.username, scopes=auth.scopes, minutes=auth.minutes,
            )
        return {'Authorization': f'Bearer {token}'} if token else {}
    raise TypeError(f'Unsupported auth mode: {type(auth).__name__}')


# ------------------------------------------------------------------------------------------------
# Transport port & adapters
# ------------------------------------------------------------------------------------------------

class Transport(Protocol):
    """Narrow wire transport port for Python-to-Rust backchannel calls."""

    def send(
        self,
        method: str,
        path: str,
        *,
        headers: Dict[str, str],
        body: Optional[bytes] = None,
        timeout: float = 30.0,
        max_bytes: Optional[int] = None,
    ) -> RawResponse:
        ...


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse all 3xx redirects so local auth headers are never forwarded off-origin."""

    def redirect_request(self, *args, **kwargs):
        return None


class UrllibTransport:
    """
    Production transport over `urllib.request`:
    - validates the configured origin via `validate_origin()`
    - bypasses all environment proxies (`ProxyHandler({})`)
    - refuses 3xx redirects (`_NoRedirect`)
    - enforces per-request timeouts and response byte bounds (`max_bytes + 1`)
    """

    DEFAULT_MAX_BYTES: int = 16 * 1024 * 1024
    DEFAULT_ERROR_BYTES: int = 65536

    def __init__(self, base_url_provider: Optional[Callable[[], Optional[str]]] = None):
        self._base_url_provider = base_url_provider or _active_server_url
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _NoRedirect(),
        )

    def send(
        self,
        method: str,
        path: str,
        *,
        headers: Dict[str, str],
        body: Optional[bytes] = None,
        timeout: float = 30.0,
        max_bytes: Optional[int] = None,
    ) -> RawResponse:
        origin = validate_origin(self._base_url_provider())
        clean_path = validate_path(path)
        limit = self.DEFAULT_MAX_BYTES if max_bytes is None else int(max_bytes)
        req = urllib.request.Request(
            origin + clean_path,
            data=body,
            headers=dict(headers),
            method=str(method).upper(),
        )
        try:
            with self._opener.open(req, timeout=timeout) as response:
                data = response.read(limit + 1)
                if len(data) > limit:
                    raise ResponseTooLargeError(f'Server response exceeded {limit} bytes.')
                resp_headers = {k.lower(): v for k, v in response.headers.items()}
                return RawResponse(
                    status=int(response.status),
                    body=data,
                    headers=resp_headers,
                    reason=str(getattr(response, 'reason', '') or ''),
                )
        except urllib.error.HTTPError as e:
            err_limit = min(limit, self.DEFAULT_ERROR_BYTES)
            try:
                err_body = e.read(err_limit)
            except Exception:
                err_body = b''
            resp_headers = {k.lower(): v for k, v in (getattr(e, 'headers', None) or {}).items()}
            return RawResponse(
                status=int(e.code),
                body=err_body,
                headers=resp_headers,
                reason=str(getattr(e, 'reason', '') or ''),
            )
        except ResponseTooLargeError:
            raise
        except Exception as e:
            raise ServerUnavailableError(str(getattr(e, 'reason', None) or e)) from e


class InMemoryTransport:
    """
    In-memory test transport with zero sockets.

    Pass either:
    - `handler`: callable `(RecordedRequest) -> RawResponse | (status, body) | dict | bytes | Exception`
    - `responses`: iterable of canned responses returned in FIFO order
    """

    def __init__(
        self,
        handler: Optional[Callable[[RecordedRequest], Any]] = None,
        responses: Optional[Sequence[Any]] = None,
    ):
        self.handler = handler
        self._queue: List[Any] = list(responses or [])
        self.requests: List[RecordedRequest] = []

    def send(
        self,
        method: str,
        path: str,
        *,
        headers: Dict[str, str],
        body: Optional[bytes] = None,
        timeout: float = 30.0,
        max_bytes: Optional[int] = None,
    ) -> RawResponse:
        clean_path = validate_path(path)
        recorded = RecordedRequest(
            method=str(method).upper(),
            path=clean_path,
            headers=dict(headers),
            body=body,
            timeout=timeout,
            max_bytes=max_bytes,
        )
        self.requests.append(recorded)
        if self.handler is not None:
            raw = self.handler(recorded)
        elif self._queue:
            raw = self._queue.pop(0)
        else:
            raise ServerUnavailableError(f'No canned response for {recorded.method} {recorded.path}.')
        if isinstance(raw, BaseException):
            raise raw
        response = self._coerce(raw)
        if max_bytes is not None and len(response.body) > int(max_bytes):
            raise ResponseTooLargeError(f'Server response exceeded {int(max_bytes)} bytes.')
        return response

    @staticmethod
    def _coerce(raw: Any) -> RawResponse:
        if isinstance(raw, RawResponse):
            return raw
        if isinstance(raw, tuple) and len(raw) in (2, 3):
            status = int(raw[0])
            payload = raw[1]
            hdrs = dict(raw[2]) if len(raw) == 3 else {}
            if isinstance(payload, (dict, list)):
                data = json.dumps(payload).encode('utf-8')
            elif isinstance(payload, str):
                data = payload.encode('utf-8')
            else:
                data = bytes(payload or b'')
            return RawResponse(status=status, body=data, headers=hdrs)
        if isinstance(raw, (dict, list)):
            return RawResponse(status=200, body=json.dumps(raw).encode('utf-8'))
        if isinstance(raw, str):
            return RawResponse(status=200, body=raw.encode('utf-8'))
        if isinstance(raw, (bytes, bytearray)):
            return RawResponse(status=200, body=bytes(raw))
        raise TypeError(f'Unsupported canned response type: {type(raw).__name__}')


# ------------------------------------------------------------------------------------------------
# MainServerClient
# ------------------------------------------------------------------------------------------------

class MainServerClient:
    """
    Canonical Python-to-Rust backchannel client sitting over `Transport`.
    """

    def __init__(self, transport: Optional[Transport] = None):
        self.transport: Transport = transport if transport is not None else UrllibTransport()

    @property
    def available(self) -> bool:
        """Whether requests can be dispatched (an in-memory transport or a configured `server_url`)."""
        if not isinstance(self.transport, UrllibTransport):
            return True
        return _active_server_url() is not None

    def request(
        self,
        method: str,
        path: str,
        *,
        auth: AuthMode = AnonymousAuth(),
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        json_body: Any = None,
        form_body: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
        max_bytes: Optional[int] = None,
        max_request_bytes: Optional[int] = None,
    ) -> RawResponse:
        merged_headers: Dict[str, str] = {}
        if headers:
            merged_headers.update(headers)
        merged_headers.update(_auth_headers(auth))

        payload = body
        if json_body is not None:
            payload = json.dumps(json_body, separators=(',', ':')).encode('utf-8')
            merged_headers.setdefault('Content-Type', 'application/json')
        elif form_body is not None:
            payload = urllib.parse.urlencode(form_body).encode('utf-8')
            merged_headers.setdefault('Content-Type', 'application/x-www-form-urlencoded')

        if payload is not None and max_request_bytes is not None and len(payload) > max_request_bytes:
            raise ValueError(f'Server request exceeds its {max_request_bytes}-byte bound.')

        return self.transport.send(
            method,
            path,
            headers=merged_headers,
            body=payload,
            timeout=timeout,
            max_bytes=max_bytes,
        )

    def request_json(
        self,
        method: str,
        path: str,
        *,
        auth: AuthMode = AnonymousAuth(),
        headers: Optional[Dict[str, str]] = None,
        json_body: Any = None,
        form_body: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
        max_bytes: Optional[int] = None,
        max_request_bytes: Optional[int] = None,
        unwrap_envelope: bool = False,
    ) -> Any:
        merged = {'Accept': 'application/json'}
        if headers:
            merged.update(headers)
        response = self.request(
            method,
            path,
            auth=auth,
            headers=merged,
            json_body=json_body,
            form_body=form_body,
            timeout=timeout,
            max_bytes=max_bytes,
            max_request_bytes=max_request_bytes,
        )
        if response.status < 200 or response.status >= 300:
            raise ServerHTTPError(response.status, response.body, response.reason)
        parsed = json.loads(response.body.decode('utf-8'))
        return decode_envelope(parsed) if unwrap_envelope else parsed


_default_client: MainServerClient = MainServerClient()


def get_client() -> MainServerClient:
    """Return the active module-level `MainServerClient`."""
    return _default_client


def set_client(client: Optional[MainServerClient]) -> MainServerClient:
    """Replace the module-level `MainServerClient` (`None` restores the default `UrllibTransport`)."""
    global _default_client
    _default_client = client if client is not None else MainServerClient()
    return _default_client


@contextlib.contextmanager
def use_transport(transport: Transport) -> Iterator[MainServerClient]:
    """Context manager installing a temporary `MainServerClient(transport)` for tests."""
    previous = _default_client
    installed = MainServerClient(transport)
    set_client(installed)
    try:
        yield installed
    finally:
        set_client(previous)
