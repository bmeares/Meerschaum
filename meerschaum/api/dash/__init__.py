#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Plotly Dash web console (v4's console, restored in v5 on the Python façade; design D21).

The standalone binary serves the API, `/docs`, `/engine/*` and MCP; the console comes with the
Python package and is served by the API host (`meerschaum.api.host`), which the Rust server
proxies `/dash/*` to. There is ONE Dash app, mounted at `/dash` (`requests_pathname_prefix`
`/dash/`): the core pages (the Overview at `/dash`, pipes, pipelines, jobs, connectors, plugins,
the Terminal (v4's dashboard), users, tokens, login, register, settings), each under the
console's top navigation, and the pages and callbacks of plugins (`@dash_plugin`, `@web_page`),
at their v4 paths.

The app is built on first use (`build_dash_app()`, or the first access of `meerschaum.api.dash.dash_app`),
so importing helpers such as `meerschaum.api.dash.sessions` from a plugin builds nothing.
"""

from __future__ import annotations

import sys
import threading

from meerschaum.utils.typing import Any, List, Optional
from meerschaum.api import (
    debug,
    _get_pipes,
    get_pipe as get_api_pipe,
    pipes as api_pipes,
    get_api_connector,
    endpoints,
    dash_mcp_enabled,
)

DASH_PREFIX: str = endpoints['dash']

### The dark (Darkly) and light (Flatly) Bootstrap themes are loaded in `index_string`
### with ids so exactly one can be enabled per route (see the dbc-dark-store callback).
### `dbc_dark.css`/`dash.css` load after them via {%css%}.
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

_build_lock = threading.RLock()
_state: dict = {'app': None, 'building': False, 'error': None}


def console_available() -> bool:
    """
    Return whether the console's packages are importable (`dash`, `dash_bootstrap_components`,
    `a2wsgi`, ...) without importing them: the check the API host uses to decide whether to
    start and serve the console (design decision 32).
    """
    import importlib.util
    try:
        return all(
            importlib.util.find_spec(name) is not None
            for name in ('dash', 'dash_bootstrap_components', 'dash_ace', 'a2wsgi')
        )
    except (ImportError, ValueError):
        return False


def _dash_classes():
    """
    Return `(app_class, transforms)`: `dash_extensions`' `DashProxy` with `Trigger` and
    `Multiplexer` (v4's app, and what plugins written for it expect) when `dash_extensions` is
    installed, else a plain `dash.Dash`.
    """
    import warnings
    from meerschaum.utils.packages import attempt_import
    dash = attempt_import('dash', lazy=False, check_update=False)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _ = attempt_import('dataclass_wizard', lazy=False, check_update=False, warn=False)
            enrich = attempt_import('dash_extensions.enrich', lazy=False, check_update=False)
        if enrich is None:
            raise ImportError("dash_extensions")
        return enrich.DashProxy, [enrich.TriggerTransform(), enrich.MultiplexerTransform()]
    except Exception:
        return dash.Dash, None


def build_dash_app(debug: bool = False):
    """
    Build the console's Dash app once: the layout, the pages, the callbacks and the plugins'
    functions and pages. Importing the console's packages (`dash`, `dash_bootstrap_components`,
    `dash_ace`, ...) happens here; a missing one raises an `ImportError`.

    Returns
    -------
    The `dash.Dash` app (`meerschaum.api.dash.dash_app`). Mount `dash_app.server` (a Flask app)
    with `a2wsgi.WSGIMiddleware` at `/dash` (`meerschaum.api.host.dash.mount_console()` does).
    """
    if _state['app'] is not None:
        return _state['app']
    with _build_lock:
        if _state['app'] is not None:
            return _state['app']
        if _state['error'] is not None:
            raise _state['error']
        try:
            app = _build(debug=debug)
        except BaseException as e:
            _state['error'] = e
            raise
        _state['app'] = app
        return app


def _build(debug: bool = False):
    from meerschaum.utils.packages import attempt_import, import_html, import_dcc
    dash, dbc = attempt_import(
        'dash', 'dash_bootstrap_components',
        lazy=False, check_update=False, warn=False,
    )
    if dash is None or dbc is None:
        raise ImportError(
            "The web console needs the `dash` extra: `pip install \"meerschaum[dash]\"`."
        )
    ### The editors in the pipe cards (`dash-ace`); fail here, not halfway through a page.
    ace = attempt_import('dash_ace', lazy=False, check_update=False, warn=False)
    if ace is None:
        raise ImportError(
            "The web console needs `dash-ace`: `pip install \"meerschaum[dash]\"`."
        )
    html, dcc = import_html(check_update=False), import_dcc(check_update=False)

    from meerschaum.api.dash.components import location

    stylesheets = [
        '/static/css/dbc_dark.css',
        '/static/css/dash.css',
        dbc.icons.FONT_AWESOME,
    ]

    ### Dash's own MCP server (`enable_mcp`) exposes the app's layout, components and callbacks.
    ### It is opt-in (`api:dash:mcp:enabled`): it lives inside this WSGI app, where the API's
    ### token auth never runs, and it only exists from Dash 4.3.0.
    _dash_mcp_kwargs = {}
    if dash_mcp_enabled:
        _dash_version = getattr(dash, '__version__', '0')
        packaging_version = attempt_import('packaging.version', lazy=False, check_update=False)
        if packaging_version.parse(_dash_version) >= packaging_version.parse('4.3.0'):
            _dash_mcp_kwargs['enable_mcp'] = True
        else:
            from meerschaum.utils.warnings import warn
            warn(
                f"The Dash MCP server requires `dash>=4.3.0` (found {_dash_version}). "
                "Upgrade Dash or unset `api:dash:mcp:enabled`.",
                stack=False,
            )

    app_class, transforms = _dash_classes()
    extra_kwargs = dict(_dash_mcp_kwargs)
    if transforms is not None:
        extra_kwargs['transforms'] = transforms
    dash_app = app_class(
        __name__,
        title='Meerschaum Web',
        requests_pathname_prefix=DASH_PREFIX + '/',
        external_stylesheets=stylesheets,
        update_title=None,
        suppress_callback_exceptions=True,
        **extra_kwargs
    )

    ### Limit Dash's MCP server to reading the layout: a callback must opt in explicitly.
    if _dash_mcp_kwargs:
        from dash.mcp import configure_mcp_server
        configure_mcp_server(
            include_callbacks=False,
            include_clientside_callbacks=False,
            include_pages=False,
        )

    ### The console is dark by default: the Darkly theme is enabled, the light (Flatly) theme is
    ### disabled, and <body> carries `dbc_dark` (dbc_dark.css scopes its overrides under that class).
    ### A page registered with `@web_page(dark_theme=False)` flips this per route (the
    ### dbc-dark-store callback). The inline script disables the light sheet before first paint.
    dash_app.index_string = INDEX_STRING

    dash_app.layout = html.Div([
        location,
        dcc.Store(id='session-store', storage_type='local', data={}),
        ### Drives the per-route `dbc_dark` body class (see update_page_layout_div).
        dcc.Store(id='dbc-dark-store', data=True),
        html.Div(id='dbc-dark-dummy', style={'display': 'none'}),
        ### Each page, with its navbar (`components.build_pages_navbar`), rendered by the router.
        html.Div([], id='page-layout-div'),
    ])

    @dash_app.server.before_request
    def _skip_sourcemap_requests():
        """Return 404 for browser-requested sourcemaps to avoid noisy tracebacks."""
        from flask import request
        if request.path.endswith('.map') and '/_dash-component-suites/' in request.path:
            return ('', 404)
        return None

    @dash_app.server.after_request
    def _conditional_component_assets(response):
        """Revalidate unversioned bundles using HTTP's weak ETag comparison."""
        from flask import request
        if (request.method in ('GET', 'HEAD')
            and request.path.startswith(dash_app.config.routes_pathname_prefix + '_dash-component-suites/')
            and response.status_code == 200 and response.get_etag()[0]):
            response.cache_control.no_cache = True
            response.make_conditional(request)
        return response

    ### The callbacks and pages import `dash_app` from this module: publish it first.
    module = sys.modules[__name__]
    module.dash_app = dash_app

    import meerschaum.api.dash.pages as pages
    import meerschaum.api.dash.callbacks as callbacks
    module.pages = pages
    module.callbacks = callbacks
    callbacks.register_plugins(debug=debug)
    return dash_app


def get_dash_app(plugin_name: Optional[str] = None):
    """
    Return the console's Dash app (building it on first use). `plugin_name` is accepted for
    plugins written against the 5.0.0 release candidate, which owned an app each: there is one app.
    """
    return build_dash_app()


def make_dash_app(plugin_name: Optional[str] = None, pages: Optional[dict] = None, **kwargs: Any):
    """
    Return the console's Dash app. Kept for plugins written against the 5.0.0 release candidate
    (which created one app per plugin): every plugin shares the one core app now, so `pages` are
    registered in it at their v4 paths and `kwargs` are ignored.
    """
    app = build_dash_app()
    if pages:
        from meerschaum.api.dash.callbacks.custom import register_layout_pages
        register_layout_pages(pages)
    return app


def __getattr__(name: str):
    """
    `meerschaum.api.dash.dash_app` builds the app on first access, as importing the package did in v4.
    """
    if name == 'dash_app':
        app = build_dash_app()
        return app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
