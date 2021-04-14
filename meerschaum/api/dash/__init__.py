#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the Dash app to be hooked into FastAPI.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Optional
from meerschaum.config import __doc__ as doc
from meerschaum.api import app, debug, _get_pipes, get_pipe, pipes, get_connector
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import get_connector_labels
from meerschaum.config import get_config
from meerschaum.connectors.parse import parse_instance_keys
dash = attempt_import('dash', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
html = attempt_import('dash_html_components', warn=False)
px = attempt_import('plotly.express', warn=False)
daq = attempt_import('dash_daq', warn=False)

stylesheets = ['/static/css/darkly.min.css', '/static/css/dbc_dark.css', '/static/css/dash.css']
dash_app = dash.Dash(
    __name__,
    title = 'Meerschaum Web',
    requests_pathname_prefix = '/dash/',
    external_stylesheets = stylesheets,
)

from meerschaum.api.dash.keys import keys_lists_content, text_tab_content, dropdown_tab_content

web_instance_keys : Optional[str] = str(get_connector())
possible_instances = get_connector_labels('sql', 'api')

dash_app.layout = html.Div(
    id = 'main-div',
    children = [
        keys_lists_content,
        dbc.Navbar(
            #  style = {'width' : 'auto'},
            children = [
                html.A(
                    dbc.Row(
                        [
                            dbc.Col(html.Img(
                                src = "assets/logo_48x48.png",
                                style = {'padding' : '0.5em'}
                            )),
                            dbc.Col(dbc.NavbarBrand("Meerschaum Web Interface", className="m1-2")),
                        ],
                        align='center',
                        no_gutters=True
                    ),
                    href='#',
                ),
                dbc.NavbarToggler(id="navbar-toggler"),
                dbc.Row(
                    [
                        dbc.Col(html.Div(className='dbc_dark', children=[dbc.Select(
                            id = 'instance-select',
                            bs_size = 'sm',
                            options = [],
                            className = 'dbc_dark',
                        )])),
                        dbc.Col(html.Pre(html.A(doc, href='/docs'))),
                    ],
                    className='navbar-nav ml-auto'
                ),
            ],
            color = 'dark',
            dark = True
        ),
        dbc.Row(
            dbc.Col(
                dbc.Tabs(
                    [
                        dbc.Tab(dropdown_tab_content, label='Filter'),
                        dbc.Tab(text_tab_content, label='Text'),
                    ]
                ),
                width = {'size' : 6},
            ),
            style = {'max-width' : '100%', 'padding' : '15px'},
        ),
        #  dcc.DatePickerRange(
            #  id='begin_end',
        #  ),
    ],
)

import meerschaum.api.dash.callbacks
def get_web_connector():
    """
    Parse the web instance keys into a connector.
    """
    return parse_instance_keys(web_instance_keys, debug=debug)


fastapi_middleware_wsgi = attempt_import('fastapi.middleware.wsgi')
app.mount('/dash', fastapi_middleware_wsgi.WSGIMiddleware(dash_app.server))
