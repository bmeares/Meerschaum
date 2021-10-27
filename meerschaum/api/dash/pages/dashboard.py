#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The main dashboard layout.
"""

import uuid
from meerschaum.config import __doc__ as doc, get_config
from meerschaum.utils.misc import get_connector_labels
from meerschaum.utils.packages import attempt_import, import_html, import_dcc, import_pandas
from meerschaum.api import endpoints
dex = attempt_import('dash_extensions', lazy=False)
#  enrich = attempt_import('dash_extensions.enrich', lazy=False)
dbc = attempt_import('dash_bootstrap_components', lazy=False)
html, dcc = import_html(), import_dcc()
pd = import_pandas()
px = attempt_import('plotly.express', warn=False)
daq = attempt_import('dash_daq', warn=False)

from meerschaum.api.dash.components import (
    go_button, search_parameters_editor, keyboard, websocket, test_button,
    get_items_menu, bottom_buttons_content, console_div, download_dataframe, navbar,
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
        download_dataframe,
        dcc.Interval(
            id = 'check-input-interval',
            interval = 1 * 1000,
            n_intervals = 0,
            disabled = True,
        ),
        navbar,
        html.Div(
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
                                        text_tab_content,
                                        label='Text',
                                        id='pipes-filter-input-tab',
                                        tab_id='input',
                                        tab_style={"display": "none"},
                                    ),
                                ]
                            ),
                            html.Br(),
                            bottom_buttons_content,
                            #  action_row,
                            test_button,
                            #  go_button,
                            #  show_pipes_button,
                            #  get_items_menu,
                            html.Div(id='ws-div'),
                            #  search_parameters_editor,
                        ],
                        id = 'content-col-left',
                        md=12, lg=6,
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
                                children = [console_div],
                            ),
                            html.Div(id='terminal'),
                        ],
                        md=12, lg=6,
                        id = 'content-col-right',
                    ),
                ],
                style = {'max-width' : '100%', 'padding' : '15px'},
            ), ### end of Row
            className = 'container-fluid',
        ), ### end of Div
        html.P('', id='line-buffer', style = {'display': 'none'}),
    ],
)
