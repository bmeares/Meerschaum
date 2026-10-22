#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The main dashboard layout.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

from meerschaum.api.dash.components import (
    go_button,
    search_parameters_editor,
    test_button,
    get_items_menu,
    bottom_buttons_content,
    console_div,
    download_dataframe,
    navbar,
    download_logs,
    refresh_jobs_interval,
)
from meerschaum.api.dash.keys import (
    keys_lists_content,
    dropdown_tab_content,
)

layout = html.Div(
    id='main-div',
    children=[
        keys_lists_content,
        download_dataframe,
        download_logs,
        refresh_jobs_interval,
        navbar,
        html.Div(
            dbc.Row(
                id='content-row',
                children=[
                    dbc.Col(
                        children=[
                            dropdown_tab_content,
                            html.Br(),
                            bottom_buttons_content,
                            test_button,
                            html.Div(id='ws-div'),
                        ],
                        id='content-col-left',
                        md=12,
                        lg=6,
                    ),
                    dbc.Col(
                        children=[
                            dbc.Col([
                                    html.Div(id='success-alert-div'),
                                    html.Div(id='instance-alert-div')
                                ],
                                width={'size': 8, 'offset': 2}
                            ),
                            html.Div(id='terminal-exec-status', role='status'),
                            html.Div(id='webterm-div'),
                            ### The output of `Exec` and `Follow logs` (`callbacks/execute.py`).
                            html.Div(
                                [
                                    html.Div(id='exec-alert-div'),
                                    html.Div(id='exec-header', className='exec-header'),
                                    html.Div(id='exec-view'),
                                    html.Div(id='exec-dummy-2', style={'display': 'none'}),
                                    html.Div(id='exec-dummy-3', style={'display': 'none'}),
                                    dcc.Store(id='exec-store', data={}),
                                    dcc.Store(id='exec-hide', data=0),
                                    dcc.Store(id='exec-status-store', data=None),
                                ],
                                id='exec-div',
                                style={'display': 'none'},
                            ),
                            html.Div(
                                id='content-div-right',
                                children=[console_div],
                            ),
                            html.Div(id='terminal'),
                        ],
                        md=12,
                        lg=6,
                        id='content-col-right',
                    ),
                ],
                style={'max-width': '100%', 'padding': '15px'},
            ), ### end of Row
            className='container-fluid',
        ), ### end of Div
        ### Confirms a pipe card's destructive action (delete, drop, clear) before it runs.
        dbc.Modal(
            [
                dbc.ModalHeader(html.H4("Confirm")),
                dbc.ModalBody(id='manage-confirm-body'),
                dbc.ModalFooter([
                    dbc.Button("Cancel", color='secondary', id='manage-cancel-button'),
                    dbc.Button("Confirm", color='danger', id='manage-confirm-button'),
                ]),
            ],
            id='manage-confirm-modal',
            is_open=False,
        ),
        dcc.Store(id='manage-confirm-store', data=None),
        html.P('', id='line-buffer', style={'display': 'none'}),
    ],
)
