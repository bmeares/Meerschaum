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
