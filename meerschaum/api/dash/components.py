#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Custom components are defined here.
"""

from __future__ import annotations
from dash.dependencies import Input, Output, State
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.typing import SuccessTuple
from meerschaum.config.static import _static_config
from meerschaum.utils.misc import remove_ansi
from meerschaum.actions import get_shell
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
dex = attempt_import('dash_extensions', lazy=False)
dash_ace = attempt_import('dash_ace', lazy=False)
html = attempt_import('dash_html_components', warn=False)

component_ids = {

}

keyboard = dex.Keyboard(
    id = 'keyboard',
    captureKeys = ['Enter'],
)
go_button = dbc.Button('Execute', id='go-button', color='primary', style={'width': '100%'})
test_button = dbc.Button('Test', id='test-button', color='danger', style={'display' : 'none'})
get_items_menu = dbc.DropdownMenu(
    label='Get Items', id='get-items-menu', children=[
        dbc.DropdownMenuItem("Pipes", id='get-pipes-button'),
        dbc.DropdownMenuItem("Jobs", id='get-jobs-button'),
    ],
    style={'width': '100%'}
)
cancel_button = dbc.Button('Cancel', id='cancel-button', color='danger', style={'width': '100%'})
bottom_buttons_content = dbc.Card(
    dbc.CardBody(
        dbc.Row([
            dbc.Col(go_button, width='2'),
            dbc.Col(cancel_button, width='2'),
            dbc.Col(get_items_menu, width='2'),
        ], no_gutters=False)
    )
)
console_div = html.Div(id='console-div', children=[html.Pre(get_shell().intro, id='console-pre')])

location = dcc.Location(id='location', refresh=False)
websocket = dex.WebSocket(id='ws', url="")

search_parameters_editor = dash_ace.DashAceEditor(
    id = 'search-parameters-editor',
    theme = 'monokai',
    mode = 'json',
    tabSize = 2,
    placeholder = (
        'Additional search parameters. ' +
        'Simple dictionary format or JSON accepted.'
    ),
    style = {'height' : 100},
)

def alert_from_success_tuple(success : SuccessTuple) -> dbc.Alert:
    """
    Return an Alert from a SuccessTuple.
    """
    return dbc.Alert('', is_open=False) if not isinstance(success, tuple) else (
        dbc.Alert(
            remove_ansi(success[1]),
            id = 'success-alert',
            dismissable = True,
            fade = True,
            is_open = not (success[1] in _static_config()['system']['success']['ignore']),
            color = 'success' if success[0] else 'danger',
        )
    )

