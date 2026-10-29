#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The web console in the API host (design D21).

The host serves the Plotly Dash console (`meerschaum.api.dash`, v4's console on the v5 façade):
ONE Dash app, mounted at `/dash` with `a2wsgi`, which the Rust server proxies every `/dash/*`
request to. Plugins' `@dash_plugin` functions and `@web_page` layouts register into that app at
their v4 paths (`/dash/<path>`); the per-plugin Dash apps of 5.0.0rc1 (`/dash/<plugin>/`) are
gone, and their paths redirect to the v4 paths.

A `@web_page` that returns an HTML string (the page contract: figures are Plotly JSON in a
`data-mrsm-figure` attribute) is served at `/_pages/<path>` as the bare fragment, and as a themed
document at `/_pages/<path>?frame=1`, which the console embeds in an iframe so its navbar and theme
stay around it.

`console_status()` tells the Rust server (through `/_mrsm/host/status` and the registry) whether
the console mounted; if `dash` cannot be imported the host logs one warning, mounts nothing and
`/dash` shows the pointer page.
"""

from __future__ import annotations

import os
import sys
import traceback

from meerschaum.utils.typing import Any, Dict, List, Optional

DASH_PREFIX: str = '/dash'
PAGE_ROUTE_PREFIX: str = '/_pages'
ASSETS_PREFIX: str = PAGE_ROUTE_PREFIX + '/_assets'

#: The state of the console: `mounted`, why not, and the Dash version.
_status: Dict[str, Any] = {'mounted': False, 'reason': 'not built', 'dash_version': None}
_status_warned: Dict[str, bool] = {'warned': False}


def console_status() -> Dict[str, Any]:
    """
    Return `{'mounted': bool, 'reason': str | None, 'dash_version': str | None}`: whether this
    host serves the console at `/dash`, and if not why not.
    """
    return dict(_status)


def _set_status(mounted: bool, reason: Optional[str] = None) -> None:
    dash_version = None
    if mounted:
        try:
            dash = sys.modules.get('dash')
            dash_version = getattr(dash, '__version__', None)
        except Exception:
            dash_version = None
    _status.update({'mounted': mounted, 'reason': reason, 'dash_version': dash_version})


def plugin_path(plugin_name: str, page_str: str) -> str:
    """
    The path 5.0.0rc1 served a plugin's page at (`/dash/<plugin>/<rest>`; a path already under the
    plugin's name kept it once). `redirect_target()` maps these old paths to the v4 ones.
    """
    rest = page_str.lstrip('/')
    if rest == 'dash' or rest.startswith('dash/'):
        rest = rest[len('dash'):].lstrip('/')
    if rest == plugin_name or rest.startswith(plugin_name + '/'):
        rest = rest[len(plugin_name):].lstrip('/')
    return f"{DASH_PREFIX}/{plugin_name}/{rest}".rstrip('/')


def page_route(page_str: str) -> str:
    """
    Return the host route serving a page-contract `@web_page`: `/dash/foo/bar` is served at
    `/_pages/foo/bar`. The prefix avoids `/dash` (the console) and `/_mrsm` (internal).
    """
    rest = page_str.lstrip('/')
    if rest == 'dash' or rest.startswith('dash/'):
        rest = rest[len('dash'):].lstrip('/')
    return f"{PAGE_ROUTE_PREFIX}/{rest}".rstrip('/') or PAGE_ROUTE_PREFIX


def _normalize_page_result(result: Any):
    """
    Return `(html, status)` for a page-contract result (`str` or `(html, status)`).
    """
    if isinstance(result, tuple):
        html = result[0] if result else ''
        status = int(result[1]) if len(result) > 1 and result[1] is not None else 200
        return ('' if html is None else str(html)), status
    return str(result), 200


_contract_pages: Dict[str, Dict[str, Any]] = {}


def get_page_contract_pages(debug: bool = False, refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Return the page-contract pages found when the routes were mounted (computed once).
    """
    if refresh:
        _contract_pages.clear()
    if not _contract_pages:
        _contract_pages.update(collect_page_contract_pages(debug=debug))
    return _contract_pages


def collect_page_contract_pages(debug: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Return the page-contract `@web_page` registrations by page path.
    A function is in page-contract mode when it returns a `str` or a `(html, status)` tuple; as
    in v4, each function is called once at startup to find out, and again on every request.
    """
    from meerschaum.plugins import _plugin_endpoints_to_pages, _get_parent_plugin
    out: Dict[str, Dict[str, Any]] = {}
    for page_group, pages_dicts in list(_plugin_endpoints_to_pages.items()):
        for page_str, page_dict in list(pages_dicts.items()):
            function = page_dict['function']
            try:
                result = function()
            except Exception:
                continue
            if not isinstance(result, (str, tuple)):
                continue
            out[page_str] = {
                **{k: v for k, v in page_dict.items() if k != 'function'},
                'function': function,
                'page_group': page_group,
                'plugin': _get_parent_plugin(function=function) or page_group,
                'route': page_route(page_str),
            }
    return out


def _plotly_bundle() -> Optional[str]:
    """
    The path of the Plotly.js bundle shipped by the `plotly` package (a dependency of Dash).
    """
    try:
        import importlib.util
        spec = importlib.util.find_spec('plotly')
        if spec is None or not spec.submodule_search_locations:
            return None
        path = os.path.join(list(spec.submodule_search_locations)[0], 'package_data', 'plotly.min.js')
        return path if os.path.isfile(path) else None
    except Exception:
        return None


#: Where a page document finds htmx, the figure/map script and MapLibre (the API host's `/static`).
CONTRACT_STATIC: str = '/static/contract'


def page_document(
    fragment: str,
    dark_theme: bool = True,
    title: str = '',
    theme: Optional[str] = None,
    head: Optional[List[str]] = None,
) -> str:
    """
    Wrap a page-contract fragment in a document with the console's theme, htmx and the script that
    draws `data-mrsm-figure` and `data-mrsm-map` elements.

    `theme` is the page's `@web_page(theme=...)` (`'dark'`, `'light'` or a stylesheet URL; one
    theme stylesheet is loaded), `head` its `head=[...]` tags (HTML), after those of `add_head()`.
    """
    import html as html_lib
    from meerschaum.plugins import get_console_head
    if theme in (None, 'dark', 'light'):
        dark = dark_theme if theme is None else theme == 'dark'
        theme_href = '/static/css/' + ('bootstrap.min.css' if dark else 'bootstrap_light.min.css')
    else:
        theme_href = str(theme)
    extra_head = ''.join(list(get_console_head()) + list(head or []))
    return (
        '<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<title>{html_lib.escape(title)}</title>'
        f'<link rel="stylesheet" href="{html_lib.escape(theme_href, quote=True)}">'
        + ('<link rel="stylesheet" href="/static/css/dbc_dark.css">' if dark_theme else '')
        + f'<link rel="stylesheet" href="{CONTRACT_STATIC}/contract.css">'
        + extra_head
        + f'</head><body class="{"dbc_dark" if dark_theme else ""} mrsm-contract-page">'
        f'<main>{fragment}</main>'
        f'<script src="{CONTRACT_STATIC}/htmx.min.js"></script>'
        f'<script src="{ASSETS_PREFIX}/plotly.min.js"></script>'
        f'<script src="{CONTRACT_STATIC}/contract.js"></script>'
        '</body></html>'
    )


def mount_page_contract_routes(fastapi_app, debug: bool = False) -> List[str]:
    """
    Serve every page-contract `@web_page` at its `/_pages/...` route as a themed document (with
    Plotly.js from the `plotly` package at `/_pages/_assets/plotly.min.js`). Requests to pages with `login_required`
    need an active `mrsm-session-id` session (the browser sends the cookie).

    Returns
    -------
    The page paths served.
    """
    from starlette.responses import HTMLResponse, Response, FileResponse
    from starlette.concurrency import run_in_threadpool
    from meerschaum.api import no_auth

    async def _plotly(request):
        path = _plotly_bundle()
        if path is None:
            return Response(status_code=404)
        return FileResponse(
            path,
            media_type='application/javascript',
            headers={'Cache-Control': 'public, max-age=3600'},
        )

    fastapi_app.add_route(
        ASSETS_PREFIX + '/plotly.min.js', _plotly, methods=['GET'], include_in_schema=False,
    )

    # Dash initialization functions may register pages after an earlier registry lookup.
    # Mount the final startup registrations, rather than an incomplete cached snapshot.
    pages = get_page_contract_pages(debug=debug, refresh=True)
    served = []
    for page_str, page in pages.items():
        function = page['function']
        login_required = bool(page.get('login_required', True))
        dark_theme = bool(page.get('dark_theme', True))
        title = str(page.get('page_key', page_str))
        theme = page.get('theme', None)
        head = list(page.get('head', None) or [])

        def _make(
            function=function,
            login_required=login_required,
            dark_theme=dark_theme,
            title=title,
            theme=theme,
            head=head,
        ):
            async def _serve(request):
                if login_required and not no_auth:
                    from meerschaum.api.dash.sessions import (
                        get_session_id_from_request, is_session_active,
                    )
                    session_id = get_session_id_from_request(request)
                    if not session_id or not await run_in_threadpool(is_session_active, session_id):
                        return Response(status_code=401)
                try:
                    result = await run_in_threadpool(function)
                except Exception:
                    traceback.print_exc()
                    return Response(status_code=500)
                html, status = _normalize_page_result(result)
                ### The console embeds the page as a document (`?frame=1`); the bare route is the
                ### fragment itself, the page contract.
                if request.query_params.get('frame'):
                    html = page_document(html, dark_theme, title, theme=theme, head=head)
                return HTMLResponse(html, status_code=status)
            return _serve

        fastapi_app.add_route(page['route'], _make(), methods=['GET'], include_in_schema=False)
        served.append(page_str)
    return served


NATIVE_ROUTE_PREFIX: str = PAGE_ROUTE_PREFIX + '/native'

#: The pages of native (Rust) plugins by console path, as the server lists them.
_native_pages: Dict[str, Dict[str, Any]] = {}


def _server_get(path: str, user: Optional[str] = None, timeout: float = 15.0):
    """
    `GET` a route of the server that runs this host, as the host (`X-Mrsm-Host-Token`), on behalf
    of `user` if given. Return `(status, body_bytes)`; `(0, b'')` when there is no server.
    """
    import urllib.error
    import urllib.request
    from meerschaum.api.dash.execute import server_url
    from meerschaum.api.host._auth import mint_internal_token
    from meerschaum.api.host import TOKEN_HEADER
    base_url = server_url()
    if base_url is None:
        return 0, b''
    headers = {TOKEN_HEADER: mint_internal_token()}
    if user:
        headers['X-Mrsm-User'] = user
    request = urllib.request.Request(base_url + path, headers=headers, method='GET')
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception:
        return 0, b''


def get_native_pages(refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Return the pages native (Rust) plugins declare, by their console path (`/dash/<path>`), from the
    server's `GET /_mrsm/native-pages`. Empty without a server or when no native plugin has pages.
    """
    import json
    if refresh:
        _native_pages.clear()
    if _native_pages:
        return _native_pages
    status, body = _server_get('/_mrsm/native-pages')
    if status != 200:
        return _native_pages
    try:
        pages = json.loads(body.decode('utf-8'))
    except ValueError:
        return _native_pages
    for page in pages if isinstance(pages, list) else []:
        raw_path = str(page.get('path', '')).strip('/')
        if not raw_path:
            continue
        console = raw_path if raw_path.startswith('dash') else f"dash/{raw_path}"
        key = '/' + console
        _native_pages[key] = {
            **page,
            'console_path': key,
            'route': f"{NATIVE_ROUTE_PREFIX}/{raw_path.removeprefix('dash/')}",
            'page_key': page.get('page_key') or ' '.join(
                word.capitalize()
                for word in raw_path.removeprefix('dash/').replace('-', ' ').replace('_', ' ').split()
            ),
        }
    return _native_pages


def mount_native_page_routes(fastapi_app) -> List[str]:
    """
    Serve the pages of native plugins at `/_pages/native/<path>` (the bare fragment; `?frame=1` is
    the themed document the console's iframe loads): the host fetches each from the server with the
    session's user, so the page's own login rule applies.
    """
    from starlette.responses import HTMLResponse, Response
    from starlette.concurrency import run_in_threadpool
    from meerschaum.api import no_auth

    pages = get_native_pages()
    if not pages:
        return []

    async def _serve(request):
        from meerschaum.api.dash.sessions import (
            get_session_id_from_request, is_session_active, get_username_from_session,
        )
        from urllib.parse import quote
        rest = request.path_params['path']
        page = next((p for p in pages.values() if p['route'] == f"{NATIVE_ROUTE_PREFIX}/{rest}"), None)
        if page is None:
            return Response(status_code=404)
        user = None
        session_id = get_session_id_from_request(request)
        if session_id and await run_in_threadpool(is_session_active, session_id):
            user = await run_in_threadpool(get_username_from_session, session_id)
        if page.get('login_required', True) and not no_auth and user is None:
            return Response(status_code=401)
        query = '&'.join(
            f"{quote(k)}={quote(v)}"
            for k, v in request.query_params.multi_items()
            if k != 'frame'
        )
        raw_path = str(page['path']).strip('/')
        status, body = await run_in_threadpool(
            _server_get,
            f"/_mrsm/native-pages/{quote(raw_path, safe='/')}" + (('?' + query) if query else ''),
            user,
        )
        if status == 0:
            return Response(status_code=502)
        html = body.decode('utf-8', errors='replace')
        if status == 200 and request.query_params.get('frame'):
            html = page_document(
                html, bool(page.get('dark_theme', True)), str(page.get('page_key', '')),
            )
        return HTMLResponse(html, status_code=status)

    fastapi_app.add_route(
        NATIVE_ROUTE_PREFIX + '/{path:path}', _serve, methods=['GET'], include_in_schema=False,
    )
    return list(pages)


def _plugin_names() -> List[str]:
    """
    The plugins with Dash functions or pages: the owners of rc1's `/dash/<plugin>/` paths.
    """
    from meerschaum.plugins import _dash_plugins, _plugin_endpoints_to_pages, _get_parent_plugin
    from meerschaum.api.host._app import plugin_name_of
    names = []
    for module_name in list(_dash_plugins):
        name = plugin_name_of(module_name)
        if name and name not in names:
            names.append(name)
    for page_group, pages in list(_plugin_endpoints_to_pages.items()):
        for page in pages.values():
            name = _get_parent_plugin(function=page.get('function')) or page_group
            if name and name not in names:
                names.append(name)
    return names


def redirect_target(path: str, query: str = '') -> Optional[str]:
    """
    Return where a 5.0.0rc1 plugin path (`/dash/<plugin>/<rest>`) lives now (`/dash/<rest>`), or
    `None` if `path` is not such a path (or already is a page of the console).
    """
    from meerschaum.api.dash.callbacks.dashboard import _paths
    parts = path.rstrip('/').split('/')
    if len(parts) < 3 or parts[1] != DASH_PREFIX.lstrip('/'):
        return None
    plugin = parts[2]
    if plugin not in _plugin_names():
        return None
    ### A page that really is under the plugin's name keeps its path.
    if path.rstrip('/') in _paths:
        return None
    rest = '/'.join(parts[3:])
    target = f"{DASH_PREFIX}/{rest}".rstrip('/') or DASH_PREFIX
    if target not in _paths and not (rest and f"{DASH_PREFIX}/{parts[3]}" in _paths):
        return None
    return target + (('?' + query) if query else '')


class ConsoleApp:
    """
    The ASGI app mounted at `/dash`: redirects 5.0.0rc1 plugin paths to their v4 paths, and serves
    the Dash console's WSGI app for everything else.
    """

    def __init__(self, wsgi_app: Any):
        self.wsgi_app = wsgi_app

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http':
            path = scope.get('path', '')
            root_path = scope.get('root_path', '')
            ### Starlette keeps the full path in `path` for a mounted app (older versions strip it).
            full_path = path if path.startswith(DASH_PREFIX) else root_path + path
            if not full_path.startswith(DASH_PREFIX):
                full_path = DASH_PREFIX + path
            query = (scope.get('query_string') or b'').decode('latin-1')
            target = redirect_target(full_path, query)
            if target is not None:
                from starlette.responses import RedirectResponse
                response = RedirectResponse(target, status_code=307)
                await response(scope, receive, send)
                return
        await self.wsgi_app(scope, receive, send)


def mount_console(fastapi_app, debug: bool = False) -> bool:
    """
    Build the Dash console and mount it at `/dash` (with the plugins' functions and pages).

    If the console's packages cannot be imported, log one warning, mount nothing and report
    `console_status()['mounted']` as `False` (`/dash` then shows the pointer page).

    Returns
    -------
    Whether the console is mounted.
    """
    from meerschaum.utils.warnings import warn
    try:
        ### The host never installs the console's packages by itself: `pip install meerschaum[dash]`.
        from meerschaum.api.dash import console_available
        if not console_available():
            raise ImportError("dash, dash-bootstrap-components, dash-ace and a2wsgi are required")
        from meerschaum.utils.packages import attempt_import
        a2wsgi = attempt_import('a2wsgi', lazy=False, check_update=False)
        if a2wsgi is None:
            raise ImportError("a2wsgi")
        from meerschaum.api.dash import build_dash_app
        dash_app = build_dash_app(debug=debug)
    except ImportError as e:
        _set_status(False, f"The web console's packages are not installed ({e}).")
        if not _status_warned['warned']:
            _status_warned['warned'] = True
            warn(
                "The web console is not available: install it with "
                "`pip install \"meerschaum[dash]\"`. "
                f"/dash shows a pointer page instead ({e}).",
                stack=False,
            )
        return False
    except Exception as e:
        _set_status(False, f"The web console failed to build: {e}")
        warn(
            "The web console failed to build; /dash shows a pointer page instead:\n"
            + traceback.format_exc(),
            stack=False,
        )
        return False

    ### Callbacks need a console session unless public (`_callbacks`); plugin pages' themes and
    ### head tags (`_page_heads`). Both before the first request, when Dash registers the callbacks.
    from meerschaum.api.host._callbacks import secure_console
    from meerschaum.api.host._page_heads import install_page_heads
    secure_console(dash_app)
    install_page_heads(dash_app)

    fastapi_app.mount(
        DASH_PREFIX,
        ConsoleApp(a2wsgi.WSGIMiddleware(dash_app.server)),
        name='dash',
    )
    _set_status(True)
    return True
