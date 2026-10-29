#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Helpers for `@api_plugin` routes: the web console's signed-in user (`session_user`) and static
directories (`mount_static`). Import them from `meerschaum.api`:

```python
from meerschaum.plugins import api_plugin

@api_plugin
def init(app):
    import fastapi
    from meerschaum.api import session_user, mount_static

    @app.get('/my-plugin/me')
    def me(user=fastapi.Depends(session_user)):
        return {'username': user.username}

    mount_static(app, '/my-plugin/files', '/path/to/files')
```
"""

from __future__ import annotations

import os
import pathlib
from urllib.parse import quote

from fastapi import HTTPException, Request

from meerschaum.utils.typing import Any, Optional, Union

#: The types `mount_static` serves that some platforms' `mimetypes` get wrong or miss.
WEB_CONTENT_TYPES = {
    '.js': 'text/javascript',
    '.mjs': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.map': 'application/json',
    '.svg': 'image/svg+xml',
    '.wasm': 'application/wasm',
    '.webmanifest': 'application/manifest+json',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.webp': 'image/webp',
    '.avif': 'image/avif',
    '.geojson': 'application/geo+json',
    '.parquet': 'application/vnd.apache.parquet',
    '.arrow': 'application/vnd.apache.arrow.file',
    '.csv': 'text/csv',
    '.md': 'text/markdown',
}

#: Paths `mount_static` may not take (the console, the host's internal routes, the page routes and
#: the API's own static files).
RESERVED_PREFIXES = ('/dash', '/_mrsm', '/_pages', '/static', '/webterm', '/docs', '/redoc')


def _session_username(request: Any) -> Optional[str]:
    """
    Return the username of the request's console session (the `mrsm-session-id` cookie), if the
    session is active and its user still exists.
    """
    from meerschaum.api.dash.sessions import (
        get_session_id_from_request,
        get_username_from_session,
    )
    session_id = get_session_id_from_request(request)
    if not session_id:
        return None
    try:
        return get_username_from_session(session_id)
    except Exception:
        return None


def _session_user_sync(request: Any):
    from meerschaum.api import get_api_connector
    from meerschaum.core import User
    username = _session_username(request)
    if not username:
        return None
    conn = get_api_connector()
    user = User(username, instance=conn)
    try:
        user_id = conn.get_user_id(user)
    except Exception:
        user_id = None
    if user_id is None:
        return None
    user._user_id = user_id
    try:
        user.type = conn.get_user_type(user)
    except Exception:
        user.type = None
    return user


async def optional_session_user(request: Request):
    """
    A FastAPI dependency: the `User` of the request's web console session, or `None` without one.

    Examples
    --------
    >>> @app.get('/my-plugin/greeting')
    ... def greeting(user=fastapi.Depends(optional_session_user)):
    ...     return {'greeting': f"Hello, {user.username if user else 'guest'}!"}
    """
    from starlette.concurrency import run_in_threadpool
    return await run_in_threadpool(_session_user_sync, request)


async def session_user(request: Request):
    """
    A FastAPI dependency: the `User` signed in to the web console (the browser's session
    cookie), answering `401` without one. Routes the console's pages call (`fetch`, HTMX) send
    the cookie by themselves. With `--no-auth` it never refuses (and is `None` without a session).

    Examples
    --------
    >>> @app.get('/my-plugin/me')
    ... def me(user=fastapi.Depends(session_user)):
    ...     return {'username': user.username, 'type': user.type}
    """
    from meerschaum.api import no_auth
    user = await optional_session_user(request)
    if user is not None or no_auth:
        return user
    raise HTTPException(
        status_code=401,
        detail="Sign in to the web console first.",
        headers={'WWW-Authenticate': 'Bearer'},
    )


def _request_is_authenticated(request: Any) -> bool:
    """
    Whether a request has a console session or a valid API token (`Authorization` header).
    """
    if _session_username(request):
        return True
    from meerschaum.api.host._auth import principal_from_authorization
    return principal_from_authorization(request.headers.get('authorization', None)) is not None


def _console_mounted() -> bool:
    try:
        from meerschaum.api.host.dash import console_status
        return bool(console_status().get('mounted', False))
    except Exception:
        return False


def _wants_html(request: Any) -> bool:
    return request.method in ('GET', 'HEAD') and 'text/html' in request.headers.get('accept', '')


def _sign_in_response(request: Any):
    """
    `303` to the console's login page (back here afterwards) for a browser, else `401`.
    """
    from starlette.responses import JSONResponse, RedirectResponse
    from meerschaum.api import endpoints
    if _wants_html(request) and _console_mounted():
        target = request.url.path + (('?' + request.url.query) if request.url.query else '')
        login = endpoints['dash'].rstrip('/') + '/login?next=' + quote(target, safe='')
        return RedirectResponse(login, status_code=303, headers={'Cache-Control': 'no-store'})
    return JSONResponse(
        status_code=401,
        content={'success': False, 'message': "Sign in to the web console or use an API token."},
        headers={'WWW-Authenticate': 'Bearer', 'Cache-Control': 'no-store'},
    )


def _route_path(scope: dict) -> str:
    """
    The path of a request below its mount point (what `StaticFiles` looks up).
    """
    try:
        from starlette._utils import get_route_path
        return get_route_path(scope)
    except ImportError:
        path = scope.get('path', '') or ''
        root_path = scope.get('root_path', '') or ''
        return path[len(root_path):] if root_path and path.startswith(root_path) else path


class StaticDirectory:
    """
    The ASGI app `mount_static` mounts: Starlette's `StaticFiles` (which never serves a path
    outside the directory, symlinks resolved), no hidden files (`.env`, `.git/...`), the content
    types browsers expect and `X-Content-Type-Options: nosniff`, and, with `auth`, a console
    session or an API token first.
    """

    def __init__(self, directory: Union[str, pathlib.Path], auth: bool = True, html: bool = False):
        from starlette.staticfiles import StaticFiles
        self.directory = str(pathlib.Path(directory).resolve())
        self.auth = auth
        self.files = StaticFiles(directory=self.directory, html=html, follow_symlink=False)

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.files(scope, receive, send)
            return
        from starlette.requests import Request as StarletteRequest
        from starlette.responses import PlainTextResponse
        from starlette.concurrency import run_in_threadpool
        from meerschaum.api import no_auth

        request = StarletteRequest(scope, receive)
        if self.auth and not no_auth:
            if not await run_in_threadpool(_request_is_authenticated, request):
                await _sign_in_response(request)(scope, receive, send)
                return

        route_path = _route_path(scope).replace('\\', '/')
        if any(part.startswith('.') for part in route_path.split('/') if part):
            await PlainTextResponse('Not Found', status_code=404)(scope, receive, send)
            return

        auth = self.auth and not no_auth

        async def _send(message):
            if message['type'] == 'http.response.start':
                headers = [
                    (name, value) for name, value in message.get('headers', [])
                    if not (auth and name.lower() == b'cache-control')
                ]
                headers.append((b'x-content-type-options', b'nosniff'))
                if auth:
                    headers.append((b'cache-control', b'private, no-cache'))
                message = {**message, 'headers': headers}
            await send(message)

        await self.files(scope, receive, _send)


def mount_static(
    app: Any,
    path: str,
    directory: Union[str, pathlib.Path],
    auth: bool = True,
    html: bool = False,
    name: Optional[str] = None,
) -> StaticDirectory:
    """
    Serve the files of `directory` at `path` from an `@api_plugin` function.

    Parameters
    ----------
    app: fastapi.FastAPI
        The app your `@api_plugin` function receives.

    path: str
        Where to serve the files, e.g. `'/my-plugin/files'` (`/my-plugin/files/report.pdf`).

    directory: Union[str, pathlib.Path]
        The directory to serve. Nothing outside it is ever served (including through symlinks),
        nor hidden files and directories (`.env`, `.git`).

    auth: bool, default True
        Require a web console session or an API token (`Authorization: Bearer ...`). A browser
        without one is sent to the console's login page and back. `auth=False` serves anyone.

    html: bool, default False
        Serve `index.html` for a directory (and `404.html` when a file is missing).

    name: Optional[str], default None
        The route's name (defaults to `static:<path>`).

    Returns
    -------
    The mounted ASGI app.

    Examples
    --------
    >>> from meerschaum.plugins import api_plugin
    >>>
    >>> @api_plugin
    ... def init(app):
    ...     import pathlib
    ...     from meerschaum.api import mount_static
    ...     mount_static(app, '/my-plugin/static', pathlib.Path(__file__).parent / 'static')
    """
    import mimetypes
    for extension, content_type in WEB_CONTENT_TYPES.items():
        mimetypes.add_type(content_type, extension)

    route = '/' + str(path).strip().strip('/')
    if route == '/':
        raise ValueError("`mount_static()` needs a path such as '/my-plugin/files', not '/'.")
    for prefix in RESERVED_PREFIXES:
        if route == prefix or route.startswith(prefix + '/'):
            raise ValueError(f"`mount_static()` cannot serve at '{route}': '{prefix}' is the API's.")
    directory_path = pathlib.Path(os.path.expanduser(str(directory))).resolve()
    if not directory_path.is_dir():
        raise ValueError(f"`mount_static()`: '{directory_path}' is not a directory.")

    served = StaticDirectory(directory_path, auth=auth, html=html)
    app.mount(route, served, name=(name or f"static:{route}"))
    return served
