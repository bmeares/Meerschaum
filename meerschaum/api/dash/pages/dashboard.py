#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The main dashboard layout.
"""

import uuid
from meerschaum.config import __doc__ as doc, get_config
from meerschaum.utils.misc import get_connector_labels
from meerschaum.utils.packages import attempt_import
from meerschaum.api import endpoints
dex = attempt_import('dash_extensions', lazy=False)
enrich = attempt_import('dash_extensions.enrich', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
html = attempt_import('dash_html_components', warn=False)
px = attempt_import('plotly.express', warn=False)
daq = attempt_import('dash_daq', warn=False)

from meerschaum.api.dash.components import (
    go_button, show_pipes_button, search_parameters_editor, keyboard, websocket, test_button,
)
from meerschaum.api.dash.keys import (
    keys_lists_content, text_tab_content, dropdown_tab_content
)

layout = html.Div(
    id = 'main-div',
    children = [
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
            children = [
                html.A(
                    dbc.Row(
                        [
                            dbc.Col(html.Img(
                                src = endpoints['dash'] + "/assets/logo_48x48.png",
                                style = {'padding' : '0.5em'}
                            )),
                            dbc.Col(dbc.NavbarBrand(
                                "Meerschaum Web Interface",
                                className = "m1-2"
                            )),
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
