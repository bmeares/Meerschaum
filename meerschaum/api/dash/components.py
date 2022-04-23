#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Custom components are defined here.
"""

from __future__ import annotations
from dash.dependencies import Input, Output, State
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.utils.typing import SuccessTuple, List
from meerschaum.config.static import _static_config
from meerschaum.utils.misc import remove_ansi
from meerschaum.actions import get_shell
from meerschaum.api import endpoints
from meerschaum.utils.misc import get_connector_labels
from meerschaum.config import __doc__ as doc
dbc = attempt_import('dash_bootstrap_components', lazy=False)
html, dcc = import_html(), import_dcc()
dex = attempt_import('dash_extensions', lazy=False, check_update=True)
dash_ace = attempt_import('dash_ace', lazy=False)

component_ids = {

}

keyboard = dex.Keyboard(
    id = 'keyboard',
    captureKeys = ['Enter'],
)
go_button = dbc.Button('Execute', id='go-button', color='primary', style={'width': '100%'})
test_button = dbc.Button('Test', id='test-button', color='danger', style={'display' : 'none'})
get_items_menu = dbc.DropdownMenu(
    label='More', id='get-items-menu', children=[
        #  dbc.DropdownMenuItem("Pipes", id='get-pipes-button'),
        dbc.DropdownMenuItem("Graphs", id='get-graphs-button'),
        dbc.DropdownMenuItem("Jobs", id='get-jobs-button'),
        dbc.DropdownMenuItem("Plugins", id='get-plugins-button'),
        dbc.DropdownMenuItem("Users", id='get-users-button'),
    ],
    style={'width': '100%', 'font-size': '0.5em'},
    menu_variant='dark',
    toggle_style={'width': '100%'},
    color='secondary',
    size='sm',
)
show_pipes_button = dbc.Button('Show Pipes', id='get-pipes-button', color='info', style={'width': '100%'})
cancel_button = dbc.Button('Cancel', id='cancel-button', color='danger', style={'width': '100%'})
bottom_buttons_content = dbc.Card(
    dbc.CardBody(
        dbc.Row([
            dbc.Col(go_button, lg=3, md=3),
            dbc.Col(cancel_button, lg=3, md=3),
            dbc.Col(show_pipes_button, lg=3, md=3),
            dbc.Col(lg=True, md=False),
            dbc.Col(get_items_menu, lg=2, md=2),
        ],
        #  no_gutters=False
        )
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

sidebar = dbc.Offcanvas(
    children=[
    
    ],
    title='Pages',
)

download_dataframe = dcc.Download(id='download-dataframe-csv')

instance_select = dbc.Select(
    id = 'instance-select',
    size = 'sm',
    options = [
        {'label': i, 'value': i}
        for i in get_connector_labels('sql', 'api')
    ],
    class_name = 'dbc_dark custom-select custom-select-sm',
)


navbar = dbc.Navbar(
    [
        dbc.Row(
            [
                dbc.Col(html.Img(src=endpoints['dash'] + "/assets/logo_48x48.png", style = {'padding': '0.5em', 'padding-left': '2em'}), width='auto', align='start'),
                dbc.Col(dbc.NavbarBrand("Meerschaum Web Console", class_name='ms-2', style={'margin-top': '10px', 'display': 'inline-block'}), align='start', width=2),
                dbc.Col(md=True, lg=True, sm=False),
                dbc.Col(html.Center(instance_select), sm=2, md=2, lg=1, align='end', class_name='d-flex justify-content-center text-center'),
                dbc.Col(html.Pre(html.A(doc, href='/docs')), width='auto', align='end'),
            ],
            #  align = 'center',
            style = {'width': '100%'},
            justify = 'around',
        ),
    ],
    color = 'dark', dark=True
)



def alert_from_success_tuple(success: SuccessTuple) -> dbc.Alert:
    """
    Return a `dbc.Alert` from a `SuccessTuple`.
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

def build_cards_grid(cards: List[dbc.Card], num_columns: int = 3) -> html.Div:
    """
    Because `CardColumns` were removed in Bootstrap 5, this function recreates a similar effect.
    """
    rows_childrens = []
    for i, card in enumerate(cards):
        if i % num_columns == 0:
            rows_childrens.append([])
        rows_childrens[-1].append(dbc.Col(card, sm=12, lg=int(12/num_columns)))
    ### Append mising columns to keep the grid shape.
    if rows_childrens and len(rows_childrens[-1]) != num_columns:
        for i in range(num_columns - len(rows_childrens[-1])):
            rows_childrens[-1].append(dbc.Col(sm=12, lg=int(12/num_columns)))
    _rows = [dbc.Row(children) for children in rows_childrens]
    rows = []
    for r in _rows:
        rows.append(r)
        rows.append(html.Br())
    return html.Div(rows)

