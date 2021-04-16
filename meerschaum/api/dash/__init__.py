#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Build the Dash app to be hooked into FastAPI.
"""

from __future__ import annotations
from meerschaum.connectors import get_connector
from meerschaum.utils.typing import List, Optional
from meerschaum.config import __doc__ as doc, get_config
from meerschaum.config.static import _static_config
from meerschaum.api import (
    app as fastapi_app,
    debug,
    _get_pipes,
    get_pipe as get_api_pipe,
    pipes as api_pipes,
    get_connector as get_api_connector,
)
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import get_connector_labels
from meerschaum.config import get_config
from meerschaum.connectors.parse import parse_instance_keys
dash = attempt_import('dash', lazy=False)
#  dash = attempt_import('dash_devices', lazy=False, warn=False)
dex = attempt_import('dash_extensions', lazy=False)
enrich = attempt_import('dash_extensions.enrich', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
html = attempt_import('dash_html_components', warn=False)
px = attempt_import('plotly.express', warn=False)
daq = attempt_import('dash_daq', warn=False)

### I know it's a bad idea to manipulate the static config, but it's necessary to read input.
_static_config()['system']['prompt']['web'] = True

stylesheets = ['/static/css/darkly.min.css', '/static/css/dbc_dark.css', '/static/css/dash.css']
dash_app = enrich.DashProxy(
    __name__,
    title = 'Meerschaum Web',
    requests_pathname_prefix = '/dash/',
    external_stylesheets = stylesheets,
    update_title = None,
    #  prevent_initial_callbacks = True,
    transforms = [
        enrich.TriggerTransform(),
        enrich.MultiplexerTransform(),
        enrich.ServersideOutputTransform(),
        enrich.NoOutputTransform(),
    ],
)

from meerschaum.api.dash.keys import (
    keys_lists_content, text_tab_content, dropdown_tab_content
)
from meerschaum.api.dash.components import (
    go_button, show_pipes_button, search_parameters_editor, keyboard, websocket, location,
    test_button,
)

dash_app.layout = html.Div(
    id = 'main-div',
    children = [
        location,
        websocket,
        keys_lists_content,
        keyboard,
        dcc.Interval(
            id = 'check-input-interval',
            interval = 1 * 1000,
            n_intervals = 0,
            disabled = True,
        ),
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
                        align = 'center',
                        no_gutters = True
                    ),
                    href = '#',
                ),
                dbc.NavbarToggler(id="navbar-toggler"),
                dbc.Collapse(
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Div(
                                    className = 'dbc_dark',
                                    children = [
                                        dbc.Select(
                                            id = 'instance-select',
                                            bs_size = 'sm',
                                            options = [
                                                {'label' : i, 'value' : i}
                                                for i in get_connector_labels('sql', 'api')
                                            ],
                                            className = 'dbc_dark',
                                        )
                                    ]
                                ),
                                #  width = {'size' : 2},
                            ), ### end of instance column
                            dbc.Col(
                                html.Pre(html.A(doc, href='/docs')),
                                #  width = {'order' : 'last'},
                                style = {'padding-left' : '15px', 'margin-top' : '15px'},
                            ),
                        ],
                        no_gutters = True,
                        className = "ml-auto flex-nowrap mt-3 mt-md-0",
                        align = 'center',
                        #  className='navbar-nav ml-auto'
                    ),
                    id = 'navbar-collapse',
                    navbar = True,
                ), ### end of navbar-collapse
            ],
            color = 'dark',
            dark = True
        ), ### end nav-bar
        dbc.Row(
            id = 'content-row',
            children = [
                dbc.Col(
                    children = [
                        dbc.Tabs(
                            id = 'pipes-filter-tabs',
                            children = [
                                dbc.Tab(
                                    dropdown_tab_content, label='Filter',
                                    id='pipes-filter-dropdown-tab',
                                    tab_id='dropdown',
                                ),
                                dbc.Tab(
                                    text_tab_content, label='Text',
                                    id='pipes-filter-input-tab',
                                    tab_id='input',
                                ),
                            ]
                        ),
                        #  html.Br(),
                        #  action_row,
                        test_button,
                        go_button,
                        show_pipes_button,
                        html.Div(id='ws-div'),
                        #  search_parameters_editor,
                    ],
                    id = 'content-col-left',
                    width = {'size' : 6},
                ),
                dbc.Col(
                    children = [
                        dbc.Col([
                            ### Place alert divs here.
                                html.Div(id='success-alert-div'),
                                html.Div(id='instance-alert-div')
                            ],
                            width={'size' : 8, 'offset' : 2}
                        ),
                        html.Div(
                            id = 'content-div-right',
                            children = [],
                        )
                    ],
                    width = {'size' : 6},
                    id = 'content-col-right',
                ),
            ],
            style = {'max-width' : '100%', 'padding' : '15px'},
        ),
        #  dcc.DatePickerRange(
            #  id='begin_end',
        #  ),
    ],
)

import meerschaum.api.dash.callbacks

fastapi_middleware_wsgi = attempt_import('fastapi.middleware.wsgi')
fastapi_app.mount('/dash', fastapi_middleware_wsgi.WSGIMiddleware(dash_app.server))
