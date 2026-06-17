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
from meerschaum.api.dash.components import location

stylesheets = [
    '/static/css/bootstrap.min.css',
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

### The `dbc_dark` theme is opt-in: dbc_dark.css scopes every rule under `.dbc_dark`.
### Set it on <body> by default so the console (and its body-portaled dropdown menus)
### stays dark; a plugin page can remove the class from <body> to opt out cleanly.
dash_app.index_string = """<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
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

fastapi_middleware_wsgi = attempt_import('fastapi.middleware.wsgi')
fastapi_app.mount(
    endpoints['dash'], fastapi_middleware_wsgi.WSGIMiddleware(dash_app.server)
)
