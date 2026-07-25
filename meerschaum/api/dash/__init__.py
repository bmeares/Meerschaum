#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the Dash app to be hooked into FastAPI.
"""

from __future__ import annotations

from meerschaum.utils.packages import (
    attempt_import,
    import_dcc,
    import_html,
)
flask_compress = attempt_import('flask_compress', lazy=False)
dash, dbc = attempt_import('dash', 'dash_bootstrap_components', lazy=False)

from meerschaum.utils.typing import List, Optional
from meerschaum.api import (
    app as fastapi_app,
    debug,
    _get_pipes,
    get_pipe as get_api_pipe,
    pipes as api_pipes,
    get_api_connector,
    endpoints,
    dash_mcp_enabled,
)

from meerschaum.connectors.parse import parse_instance_keys
import warnings
### Suppress the depreciation warnings from importing enrich.
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    _ = attempt_import('dataclass_wizard', lazy=False)
    enrich = attempt_import('dash_extensions.enrich', lazy=False)
html, dcc = import_html(), import_dcc()
from meerschaum.api.dash.components import location, pages_offcanvas

### The dark (Darkly) and light (Flatly) Bootstrap themes are loaded in index_string
### with ids so exactly one can be enabled per route (see the dbc-dark-store callback).
### `dbc_dark.css`/`dash.css` load after them via {%css%}.
stylesheets = [
    '/static/css/dbc_dark.css',
    '/static/css/dash.css',
    dbc.icons.FONT_AWESOME,
]
scripts = ['/static/js/node_modules/xterm/lib/xterm.js']

### Dash's own MCP server (`enable_mcp`) exposes the app's layout, components,
### and callbacks so an agent can understand the page it is looking at. It is
### distinct from Meerschaum's MCP server at `/mcp`, and opt-in via
### `api:dash:mcp:enabled`, for two reasons:
###
###   1. It lives inside this WSGI app, so FastAPI's token auth never runs for
###      it. Every callback it advertises — core and plugin — becomes an
###      unauthenticated, self-describing tool.
###   2. It only reached Dash in 4.3.0, while Meerschaum still supports
###      `dash>=4.1.0`. Passing the kwarg to an older Dash raises a `TypeError`
###      and takes the whole app down with it.
_dash_mcp_kwargs = {}
if dash_mcp_enabled:
    _dash_version = getattr(dash, '__version__', '0')
    packaging_version = attempt_import('packaging.version', lazy=False)
    if packaging_version.parse(_dash_version) >= packaging_version.parse('4.3.0'):
        _dash_mcp_kwargs['enable_mcp'] = True
    else:
        from meerschaum.utils.warnings import warn
        warn(
            f"The Dash MCP server requires `dash>=4.3.0` (found {_dash_version}). "
            "Upgrade Dash or unset `api:dash:mcp:enabled`.",
            stack=False,
        )

dash_app = enrich.DashProxy(
    __name__,
    title='Meerschaum Web',
    requests_pathname_prefix=endpoints['dash'] + '/',
    external_stylesheets=stylesheets,
    update_title=None,
    suppress_callback_exceptions=True,
    transforms=[
        enrich.TriggerTransform(),
        enrich.MultiplexerTransform(),
    ],
    **_dash_mcp_kwargs
)

### Dash exposes every callback as an invocable MCP tool by default. Meerschaum's
### callbacks read and write pipes, users, and tokens, so limit the Dash MCP
### server to what it is actually for — reading the layout — and require a
### callback to opt in explicitly (`@callback(..., mcp_enabled=True)`).
if _dash_mcp_kwargs:
    from dash.mcp import configure_mcp_server
    configure_mcp_server(
        include_callbacks=False,
        include_clientside_callbacks=False,
        include_pages=False,
    )

### The console is dark by default: the Darkly theme is enabled, the light (Flatly)
### theme is disabled, and <body> carries `dbc_dark` (dbc_dark.css scopes its overrides
### under that class). A plugin page registered with `@web_page(dark_theme=False)` flips
### this per route — the dbc-dark-store callback disables Darkly, enables Flatly, and
### removes the `dbc_dark` class — so the page renders with the light theme. The inline
### script disables the light sheet before first paint to avoid a flash.
dash_app.index_string = """<!DOCTYPE html>
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

dash_app.layout = html.Div([
    location,
    dcc.Store(id='session-store', storage_type='local', data={}),
    ### Drives the per-route `dbc_dark` body class (see update_page_layout_div).
    dcc.Store(id='dbc-dark-store', data=True),
    html.Div(id='dbc-dark-dummy', style={'display': 'none'}),
    ### Persistent across navigation so its accordion isn't destroyed/recreated
    ### (which crashed dbc's accordion). Toggled by the logo in the page navbars.
    pages_offcanvas,
    html.Div([], id='page-layout-div'),
])


@dash_app.server.before_request
def _skip_sourcemap_requests():
    """Return 404 for browser-requested sourcemaps to avoid noisy tracebacks."""
    from flask import request
    path = request.path
    if path.endswith('.map') and '/_dash-component-suites/' in path:
        return ('', 404)
    return None

import meerschaum.api.dash.pages as pages
import meerschaum.api.dash.callbacks as callbacks

a2wsgi = attempt_import('a2wsgi', lazy=False)
fastapi_app.mount(
    endpoints['dash'], a2wsgi.WSGIMiddleware(dash_app.server)
)
