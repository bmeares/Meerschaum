#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Plugin-owned Dash apps in the API host (v5 design §8.4).

Meerschaum runs no Dash app of its own. Each plugin with `@dash_plugin` functions or legacy
`@web_page` Dash layouts gets its own app, created by `make_dash_app()` with
`requests_pathname_prefix='/dash/<plugin>/'` and mounted inside the host's FastAPI app with
`a2wsgi`. The shim pre-populates the app's layout with the core component IDs plugin callbacks
read and write:

- `mrsm-location` (`dcc.Location`),
- `session-store` (`dcc.Store`, `{'session-id': ...}`), filled on page load from the shared
  `mrsm-session-id` cookie through `meerschaum.api.dash.sessions`,
- `dbc-dark-store` and the `mrsm-theme-dark` / `mrsm-theme-light` stylesheets.

`index_string`, `external_stylesheets` and `assets/` belong to the plugin's app; a plugin's
`assets/` directory is the app's assets folder.

In the host, `meerschaum.api.dash` is a light package (`install_dash_surface()`): its submodules
(`sessions`, ...) import as before, but importing it no longer builds the v4 console.
"""

from __future__ import annotations

import os
import sys
import types
import traceback
import importlib.machinery

from meerschaum.utils.typing import Any, Callable, Dict, List, Optional

DASH_PREFIX: str = '/dash'
SESSION_STORE_ID: str = 'session-store'
LOCATION_ID: str = 'mrsm-location'
DARK_STORE_ID: str = 'dbc-dark-store'
PAGE_DIV_ID: str = 'page-layout-div'

CONSOLE_LINKS = (
    ('Home', '/dash'),
    ('Pipes', '/dash/pipes'),
    ('Jobs', '/dash/jobs'),
    ('Plugins', '/dash/plugins'),
)

INDEX_STRING: str = """<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        <link rel="stylesheet" href="/static/css/bootstrap.min.css" id="mrsm-theme-dark">
        <link rel="stylesheet" href="/static/css/bootstrap_light.min.css" id="mrsm-theme-light">
        <script>document.getElementById('mrsm-theme-light').disabled = true;</script>
        {%css%}
    </head>
    <body class="dbc_dark">
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>"""

_apps: Dict[str, Any] = {}


def install_dash_surface() -> types.ModuleType:
    """
    Make `meerschaum.api.dash` a light package in this process: submodules import from the real
    directory, `make_dash_app` is the shim, and the v4 console is never built.
    """
    existing = sys.modules.get('meerschaum.api.dash')
    if existing is not None and getattr(existing, '_mrsm_host_surface', False):
        return existing
    import meerschaum.api as api_pkg
    real_dir = os.path.join(os.path.dirname(api_pkg.__file__), 'dash')
    module = types.ModuleType(
        'meerschaum.api.dash',
        "The `meerschaum.api.dash` surface in the API host (plugin-owned Dash apps, §8.4).",
    )
    spec = importlib.machinery.ModuleSpec('meerschaum.api.dash', None, is_package=True)
    spec.submodule_search_locations = [real_dir]
    module.__spec__ = spec
    module.__path__ = [real_dir]
    module.__package__ = 'meerschaum.api.dash'
    module.__file__ = os.path.join(real_dir, '__init__.py')
    module._mrsm_host_surface = True
    module.make_dash_app = make_dash_app
    module.get_dash_app = get_dash_app
    module.dash_app = None
    sys.modules['meerschaum.api.dash'] = module
    setattr(api_pkg, 'dash', module)
    return module


def _dash_classes():
    """
    Return `(app_class, transforms, uses_multiplexer)`: `DashProxy` with v4's transforms when
    `dash_extensions` is installed (plugins written for v4 use `Trigger` and duplicate outputs),
    else plain `dash.Dash`.
    """
    from meerschaum.utils.packages import attempt_import
    dash = attempt_import('dash', lazy=False, check_update=False)
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            enrich = attempt_import('dash_extensions.enrich', lazy=False, check_update=False)
        return (
            enrich.DashProxy,
            [enrich.TriggerTransform(), enrich.MultiplexerTransform()],
            True,
        )
    except Exception:
        return dash.Dash, None, False


def _plugin_assets_folder(plugin_name: str) -> Optional[str]:
    import meerschaum as mrsm
    try:
        module = mrsm.Plugin(plugin_name).module
        plugin_file = getattr(module, '__file__', None)
    except Exception:
        plugin_file = None
    if not plugin_file:
        return None
    assets = os.path.join(os.path.dirname(plugin_file), 'assets')
    return assets if os.path.isdir(assets) else None


def plugin_path(plugin_name: str, page_str: str) -> str:
    """
    Map a v4 page path (`/dash/foo/bar`) to its path in the plugin's app
    (`/dash/<plugin>/foo/bar`). A path already under the plugin's name keeps it once
    (`/dash/<plugin>/bar` stays `/dash/<plugin>/bar`), so those v4 URLs need no redirect.
    """
    rest = page_str.lstrip('/')
    if rest == 'dash' or rest.startswith('dash/'):
        rest = rest[len('dash'):].lstrip('/')
    if rest == plugin_name or rest.startswith(plugin_name + '/'):
        rest = rest[len(plugin_name):].lstrip('/')
    return f"{DASH_PREFIX}/{plugin_name}/{rest}".rstrip('/')


def _core_layout(html, dcc, dark_theme: bool = True):
    return html.Div([
        dcc.Location(id=LOCATION_ID, refresh=False),
        dcc.Store(id=SESSION_STORE_ID, storage_type='local', data={}),
        dcc.Store(id=DARK_STORE_ID, data=dark_theme),
        html.Div([], id=PAGE_DIV_ID),
    ])


def _navbar(html, plugin_name: str, pages: Dict[str, Dict[str, Any]]):
    links = [html.A(label, href=href, style={'marginRight': '1em'}) for label, href in CONSOLE_LINKS]
    links += [
        html.A(page['page_key'], href=path, style={'marginRight': '1em'})
        for path, page in pages.items()
    ]
    return html.Nav(links, id=f'mrsm-navbar-{plugin_name}', className='mrsm-plugin-navbar')


def session_store_data(request) -> Dict[str, Any]:
    """
    The `session-store` data for a request: `{'session-id': ...}` when its `mrsm-session-id`
    cookie names an active session, else `{}`.
    """
    from meerschaum.api.dash.sessions import get_session_id_from_request, is_session_active
    session_id = get_session_id_from_request(request)
    if session_id and is_session_active(session_id):
        return {'session-id': session_id}
    return {}


def make_dash_app(plugin_name: str, pages: Optional[Dict[str, Dict[str, Any]]] = None, **kwargs: Any):
    """
    Create (once) the Dash app owned by `plugin_name`, mounted at `/dash/<plugin_name>/`, with
    the core stores and the page router.

    Parameters
    ----------
    plugin_name: str
        The plugin owning the app.

    pages: Optional[Dict[str, Dict[str, Any]]], default None
        Layouts by path in the app (`/dash/<plugin>/...`), each a dict with `layout`,
        `login_required`, `skip_navbar`, `page_key` and `dark_theme`.

    kwargs: Any
        Extra `dash.Dash` arguments.
    """
    if plugin_name in _apps:
        return _apps[plugin_name]

    from meerschaum.utils.packages import attempt_import
    from meerschaum.api import no_auth
    dash = attempt_import('dash', lazy=False, check_update=False)
    html, dcc = dash.html, dash.dcc
    app_class, transforms, multiplexer = _dash_classes()

    app_kwargs: Dict[str, Any] = {
        'title': kwargs.pop('title', f'{plugin_name} | Meerschaum'),
        'requests_pathname_prefix': f'{DASH_PREFIX}/{plugin_name}/',
        'suppress_callback_exceptions': True,
        'update_title': None,
    }
    assets = _plugin_assets_folder(plugin_name)
    if assets:
        app_kwargs['assets_folder'] = assets
    if transforms is not None:
        app_kwargs['transforms'] = transforms
    app_kwargs.update(kwargs)
    dash_app = app_class(f'mrsm_plugin_{plugin_name}', **app_kwargs)
    dash_app.index_string = INDEX_STRING
    pages = dict(pages or {})
    dash_app.layout = _core_layout(html, dcc)
    dash_app._mrsm_plugin = plugin_name
    dash_app._mrsm_pages = pages

    Output, Input = dash.Output, dash.Input
    store_output = (
        Output(SESSION_STORE_ID, 'data')
        if multiplexer
        else Output(SESSION_STORE_ID, 'data', allow_duplicate=True)
    )
    dark_output = (
        Output(DARK_STORE_ID, 'data')
        if multiplexer
        else Output(DARK_STORE_ID, 'data', allow_duplicate=True)
    )
    callback_kwargs = {} if multiplexer else {'prevent_initial_call': 'initial_duplicate'}

    @dash_app.callback(
        Output(PAGE_DIV_ID, 'children'),
        store_output,
        dark_output,
        Input(LOCATION_ID, 'pathname'),
        **callback_kwargs
    )
    def _route_page(pathname):
        from flask import request
        store = session_store_data(request)
        path = (pathname or '').rstrip('/')
        page = dash_app._mrsm_pages.get(path)
        if page is None and dash_app._mrsm_pages and path == f'{DASH_PREFIX}/{plugin_name}':
            page = next(iter(dash_app._mrsm_pages.values()))
        if page is None:
            layout = [
                _navbar(html, plugin_name, dash_app._mrsm_pages),
                html.H3('Page not found'),
                html.P(f"No page of plugin '{plugin_name}' at {pathname}."),
            ]
            return html.Div(layout, key=f'page-content::{path}'), store, True
        if page.get('login_required', True) and not no_auth and not store:
            layout = [
                html.H3('Login required'),
                html.A('Log in', href='/dash/login'),
            ]
            return html.Div(layout, key=f'page-content::{path}'), store, True
        layout = page['layout']
        if not page.get('skip_navbar', False):
            layout = (
                [_navbar(html, plugin_name, dash_app._mrsm_pages)]
                + (layout if isinstance(layout, list) else [layout])
            )
        return (
            html.Div(layout, key=f'page-content::{path}'),
            store,
            bool(page.get('dark_theme', True)),
        )

    _apps[plugin_name] = dash_app
    return dash_app


def get_dash_app(plugin_name: str):
    """
    Return the Dash app owned by a plugin, if one was created.
    """
    return _apps.get(plugin_name, None)


def collect_plugin_pages(debug: bool = False) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Return the legacy `@web_page` Dash layouts by plugin, keyed by their path in the plugin's app.
    Layouts are built once at startup, as in v4. Pages returning HTML strings belong to the page
    contract (§8.3) and are not mounted as Dash layouts.
    """
    from meerschaum.plugins import _plugin_endpoints_to_pages, _get_parent_plugin
    from meerschaum.utils.warnings import warn
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for page_group, pages_dicts in list(_plugin_endpoints_to_pages.items()):
        for page_str, page_dict in list(pages_dicts.items()):
            function = page_dict['function']
            plugin_name = _get_parent_plugin(function=function) or page_group
            try:
                layout = function()
            except Exception:
                warn(
                    f"Failed to build the page '{page_str}' of plugin '{plugin_name}':\n"
                    + traceback.format_exc(),
                    stack=False,
                )
                continue
            if isinstance(layout, (str, tuple)):
                continue
            out.setdefault(plugin_name, {})[plugin_path(plugin_name, page_str)] = {
                'layout': layout,
                'login_required': page_dict.get('login_required', True),
                'skip_navbar': page_dict.get('skip_navbar', False),
                'page_key': page_dict.get('page_key', page_str),
                'dark_theme': page_dict.get('dark_theme', True),
                'page_group': page_group,
            }
    return out


PAGE_ROUTE_PREFIX: str = '/_pages'


def page_route(page_str: str) -> str:
    """
    Return the host route serving a page-contract `@web_page` (§8.3): `/dash/foo/bar` is served at
    `/_pages/foo/bar`. The prefix avoids `/dash` and `/_mrsm`, which the console refuses to proxy.
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


def get_page_contract_pages(debug: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Return the page-contract pages found when the routes were mounted (computed once).
    """
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


def mount_page_contract_routes(fastapi_app, debug: bool = False) -> List[str]:
    """
    Serve every page-contract `@web_page` at its `/_pages/...` route, returning an HTML fragment
    that the Rust console wraps in its chrome. Requests to pages with `login_required` need an
    active `mrsm-session-id` session (the console forwards the cookie).

    Returns
    -------
    The page paths served.
    """
    from starlette.responses import HTMLResponse, Response
    from starlette.concurrency import run_in_threadpool
    from meerschaum.api import no_auth

    pages = get_page_contract_pages(debug=debug)
    served = []
    for page_str, page in pages.items():
        function = page['function']
        login_required = bool(page.get('login_required', True))

        def _make(function=function, login_required=login_required):
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
                return HTMLResponse(html, status_code=status)
            return _serve

        fastapi_app.add_route(page['route'], _make(), methods=['GET'], include_in_schema=False)
        served.append(page_str)
    return served


def mount_dash_apps(fastapi_app, debug: bool = False) -> List[str]:
    """
    Create, initialize and mount each plugin's Dash app at `/dash/<plugin>/`.

    Returns
    -------
    The names of the plugins with a mounted Dash app.
    """
    from meerschaum.plugins import _dash_plugins
    from meerschaum.api.host._app import plugin_name_of
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import attempt_import

    dash_functions: Dict[str, List[Callable[[Any], Any]]] = {}
    for module_name, functions in list(_dash_plugins.items()):
        dash_functions.setdefault(plugin_name_of(module_name), []).extend(functions)

    pages_by_plugin = collect_plugin_pages(debug=debug)
    plugin_names = [name for name in dash_functions if name]
    plugin_names += [name for name in pages_by_plugin if name and name not in plugin_names]
    if not plugin_names:
        return []

    a2wsgi = attempt_import('a2wsgi', lazy=False, check_update=False)
    mounted = []
    for plugin_name in plugin_names:
        dash_app = make_dash_app(plugin_name, pages=pages_by_plugin.get(plugin_name, {}))
        for function in dash_functions.get(plugin_name, []):
            try:
                function(dash_app)
            except Exception:
                warn(
                    f"Failed to load function '{function.__name__}' "
                    + f"from plugin '{plugin_name}':\n"
                    + traceback.format_exc(),
                    stack=False,
                )
        fastapi_app.mount(
            f'{DASH_PREFIX}/{plugin_name}',
            a2wsgi.WSGIMiddleware(dash_app.server),
        )
        mounted.append(plugin_name)
    return mounted
