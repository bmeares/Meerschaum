#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the Dash app to be hooked into FastAPI.
"""

from __future__ import annotations
import uuid
from meerschaum.utils.typing import List, Optional
from meerschaum.config.static import _static_config
from meerschaum.api import (
    app as fastapi_app,
    debug,
    _get_pipes,
    get_pipe as get_api_pipe,
    pipes as api_pipes,
    get_api_connector,
    endpoints,
)
from meerschaum.utils.packages import attempt_import
from meerschaum.connectors.parse import parse_instance_keys
dash = attempt_import('dash', lazy=False)
enrich = attempt_import('dash_extensions.enrich', lazy=False)
#  multipage = attempt_import('dash_extensions.multipage', lazy=False)
html = attempt_import('dash_html_components', warn=False)
dcc = attempt_import('dash_core_components', warn=False)
from meerschaum.api.dash.components import location

### I know it's a bad idea to manipulate the static config, but it's necessary to read input.
#  _static_config()['system']['prompt']['web'] = True

active_sessions = {}
running_jobs = {}
running_monitors = {}
stopped_jobs = {}
stopped_monitors = {}

stylesheets = [
    '/static/css/darkly.min.css', '/static/css/dbc_dark.css', '/static/css/dash.css',
]
dash_app = enrich.DashProxy(
    __name__,
    title = 'Meerschaum Web',
    requests_pathname_prefix = endpoints['dash'] + '/',
    external_stylesheets = stylesheets,
    update_title = None,
    #  prevent_initial_callbacks = True,
    suppress_callback_exceptions = True,
    transforms = [
        enrich.TriggerTransform(),
        enrich.MultiplexerTransform(),
        enrich.ServersideOutputTransform(),
        #  enrich.NoOutputTransform(),
    ],
)

dash_app.layout = html.Div([
    location,
    dcc.Store(id='session-store', storage_type='session', data={}),
    html.Div([], id='page-layout-div')
])

import meerschaum.api.dash.pages as pages
import meerschaum.api.dash.callbacks as callbacks

fastapi_middleware_wsgi = attempt_import('fastapi.middleware.wsgi')
fastapi_app.mount(endpoints['dash'], fastapi_middleware_wsgi.WSGIMiddleware(dash_app.server))
