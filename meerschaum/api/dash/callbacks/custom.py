#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register what plugins add to the console: `@dash_plugin` functions (callbacks on the core app) and
`@web_page` pages, at their v4 paths (`/dash/<path>`).

A `@web_page` function returns either a Dash layout (the v4 contract) or an HTML string /
`(html, status)` tuple (the v5 page contract: a fragment whose charts are `data-mrsm-figure`
elements). A fragment is served by the host at `/_pages/<path>` (as a themed document at
`/_pages/<path>?frame=1`), and its console page is an iframe of that document, so the console's
navbar and theme stay around it.
"""

import traceback

from meerschaum.api.dash import dash_app, DASH_PREFIX
from meerschaum.plugins import _dash_plugins, _plugin_endpoints_to_pages
from meerschaum.utils.warnings import warn, dprint
from meerschaum.utils.typing import Any, Dict, List, Optional
from meerschaum.api.dash.callbacks.dashboard import (
    _paths,
    _required_login,
    _pages,
    _paths_without_dbc_dark,
)
from meerschaum.api.dash.components import build_pages_navbar, build_plugin_navbar

#: Page-contract pages by console path: `{'route': ..., 'plugin': ...}`.
contract_pages: Dict[str, Dict[str, Any]] = {}


def page_navbar(page: Dict[str, Any]) -> Any:
    """
    The bar above a plugin's page: none with `skip_navbar`, the console's top navigation with
    `navbar=True`, else the plain bar (the logo and Sign out), so the page keeps its own layout.
    """
    if page.get('skip_navbar', False):
        return None
    if page.get('navbar', False):
        return build_pages_navbar()
    return build_plugin_navbar()


def with_navbar(layout: Any, page: Dict[str, Any]) -> Any:
    """A plugin page's layout under its bar (`page_navbar`)."""
    navbar = page_navbar(page)
    if navbar is None:
        return layout
    return [navbar] + (list(layout) if isinstance(layout, list) else [layout])


def _activate_plugin_venv(function: Any, debug: bool = False) -> None:
    """
    Activate the venv of the plugin that defined `function`, as the API host does for
    `@api_plugin` functions, so its `required` packages import.
    """
    try:
        from meerschaum.plugins import _get_parent_plugin
        import meerschaum as mrsm
        plugin_name = _get_parent_plugin(function=function)
        if plugin_name:
            mrsm.Plugin(plugin_name).activate_venv(debug=debug)
    except Exception:
        pass


def init_dash_plugins(debug: bool = False):
    """
    Fire the initial callbacks for Dash plugins.
    """
    for _module_name, _functions in list(_dash_plugins.items()):
        for _function in list(_functions):
            _activate_plugin_venv(_function, debug=debug)
            try:
                _function(dash_app)
            except Exception:
                warn(
                    f"Failed to load function '{_function.__name__}' "
                    + f"from plugin '{_module_name}':\n"
                    + traceback.format_exc(),
                    stack=False,
                )


def console_path(endpoint: str) -> str:
    """
    Return the console path of a page: `/dash/<endpoint>` unless it already starts with `dash`.
    """
    if endpoint.lstrip('/').startswith('dash'):
        return '/' + endpoint.lstrip('/')
    return DASH_PREFIX + '/' + endpoint.lstrip('/')


def build_contract_layout(route: str, page_key: str) -> List[Any]:
    """
    Return the Dash content of a page-contract page: an iframe of the host's `/_pages/...` route.
    """
    from meerschaum.utils.packages import import_html
    html = import_html(check_update=False)
    return [
        html.Iframe(
            src=route + '?frame=1',
            id={'type': 'mrsm-contract-frame', 'index': page_key},
            className='mrsm-contract-frame',
            title=page_key,
            style={
                'width': '100%',
                'height': 'calc(100vh - 80px)',
                'border': '0',
                'display': 'block',
            },
        ),
    ]


def add_plugin_pages(debug: bool = False):
    """
    Allow users to add pages via the `@web_page` decorator.
    """
    from meerschaum.api.host.dash import page_route, _normalize_page_result
    for page_group, pages_dicts in list(_plugin_endpoints_to_pages.items()):
        if debug:
            dprint(f"Adding pages for group '{page_group}'...")
        for _endpoint, _page_dict in list(pages_dicts.items()):
            function = _page_dict['function']
            _activate_plugin_venv(function, debug=debug)
            try:
                page_layout = function()
            except Exception:
                warn(
                    f"Failed to build the page '{_endpoint}' of '{page_group}':\n"
                    + traceback.format_exc(),
                    stack=False,
                )
                continue

            is_contract = isinstance(page_layout, (str, tuple))
            path = console_path(_endpoint)
            if is_contract:
                route = page_route(_endpoint)
                contract_pages[path] = {
                    'route': route,
                    'function': function,
                    'page_group': page_group,
                    'page_key': _page_dict['page_key'],
                    'login_required': bool(_page_dict['login_required']),
                    'path': path,
                }
                page_layout = build_contract_layout(route, _page_dict['page_key'])

            page_layout = with_navbar(page_layout, _page_dict)
            _pages[_page_dict['page_key']] = path
            _paths[path] = page_layout
            if _page_dict['login_required']:
                _required_login.add(path)
            if not _page_dict.get('dark_theme', True):
                _paths_without_dbc_dark.add(path)


def add_native_pages() -> None:
    """
    Add the pages native (Rust) plugins declare: each is a page-contract page, shown in an iframe
    of the host's `/_pages/native/<path>` document.
    """
    from meerschaum.api.host.dash import get_native_pages
    for path, page in get_native_pages().items():
        contract_pages[path] = {**page, 'login_required': bool(page.get('login_required', True))}
        layout = with_navbar(build_contract_layout(page['route'], page['page_key']), page)
        _pages[page['page_key']] = path
        _paths[path] = layout
        if page.get('login_required', True):
            _required_login.add(path)
        if not page.get('dark_theme', True):
            _paths_without_dbc_dark.add(path)


def register_layout_pages(pages: Dict[str, Dict[str, Any]]) -> None:
    """
    Register Dash layouts by console path (what `make_dash_app(plugin_name, pages)` of the
    5.0.0 release candidate took): each page is a dict with `layout` and optionally
    `login_required`, `skip_navbar`, `navbar`, `page_key` and `dark_theme`.
    """
    for endpoint, page in pages.items():
        path = console_path(endpoint)
        layout = with_navbar(page['layout'], page)
        _paths[path] = layout
        _pages[page.get('page_key', endpoint)] = path
        if page.get('login_required', True):
            _required_login.add(path)
        if not page.get('dark_theme', True):
            _paths_without_dbc_dark.add(path)


def add_plugin_assets() -> Dict[str, str]:
    """
    Serve each plugin's `assets/` directory at `/dash/_plugin-assets/<plugin>/` and load its
    stylesheets and scripts in every page (5.0.0rc1 gave each plugin's Dash app its own assets
    folder; Dash has one assets folder, the console's, so a plugin's are served beside it).

    Returns
    -------
    The assets directories by plugin name.
    """
    import os
    import meerschaum as mrsm
    from meerschaum.api.host._app import plugin_name_of
    from meerschaum.plugins import _get_parent_plugin
    names = []
    for module_name in list(_dash_plugins):
        names.append(plugin_name_of(module_name))
    for page_group, pages_dicts in list(_plugin_endpoints_to_pages.items()):
        for page_dict in pages_dicts.values():
            names.append(_get_parent_plugin(function=page_dict['function']) or page_group)

    directories: Dict[str, str] = {}
    for name in dict.fromkeys(name for name in names if name):
        try:
            plugin_file = getattr(mrsm.Plugin(name).module, '__file__', None)
        except Exception:
            plugin_file = None
        if not plugin_file:
            continue
        assets = os.path.join(os.path.dirname(plugin_file), 'assets')
        if os.path.isdir(assets):
            directories[name] = assets
    if not directories:
        return directories

    from flask import abort, send_from_directory

    def _serve(plugin: str, filename: str):
        directory = directories.get(plugin, None)
        if directory is None:
            abort(404)
        return send_from_directory(directory, filename)

    dash_app.server.add_url_rule(
        '/_plugin-assets/<plugin>/<path:filename>',
        endpoint='mrsm_plugin_assets',
        view_func=_serve,
    )
    for name, directory in directories.items():
        for filename in sorted(os.listdir(directory)):
            url = f"{DASH_PREFIX}/_plugin-assets/{name}/{filename}"
            if filename.endswith('.css'):
                dash_app.config.external_stylesheets.append(url)
            elif filename.endswith('.js'):
                dash_app.config.external_scripts.append(url)
    return directories


def register_plugins(debug: bool = False) -> None:
    """
    Run the plugins' Dash functions and add their pages (`init_dash_plugins()` then `add_plugin_pages()`).
    """
    init_dash_plugins(debug=debug)
    add_plugin_pages(debug=debug)
    try:
        add_plugin_assets()
    except Exception:
        warn(f"Could not serve the plugins' assets:\n{traceback.format_exc()}", stack=False)
    try:
        add_native_pages()
    except Exception:
        warn(f"Could not add the pages of native plugins:\n{traceback.format_exc()}", stack=False)
