#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the tokens page layout.
"""

import dash_bootstrap_components as dbc
import dash.html as html
import dash.dcc as dcc
from meerschaum.plugins import web_page
from meerschaum._internal.static import STATIC_CONFIG


@web_page('tokens', login_required=True, page_group='Settings')
def page_layout():
    """
    Return the layout for the tokens page.
    """
    return dbc.Container([
        html.Br(), 
        html.H3('Tokens'),
        html.Div(id="tokens-alert-div"),
        dbc.Modal(
            [
                dbc.ModalHeader(html.H4("Register Token")),
                dbc.ModalBody([
                    dbc.Form([
                        dbc.Row(
                            [
                                dbc.Label("Label", width='auto'),
                                dbc.Col(
                                    [
                                        dbc.Input(placeholder="Enter optional label"),
                                    ],
                                    className="me-3",
                                ),
                            ],
                            className='g-2',
                        ),
                        html.Br(),
                        dbc.Row([
                            dbc.Switch(
                                id="tokens-toggle-scopes-switch",
                                label="Grant all scopes",
                                value=True,
                            ),
                        ]),
                        html.Div([
                            dbc.Button(
                                "Deselect all",
                                size='sm',
                                id="tokens-deselect-scopes-button",
                            ),
                            dbc.Row([
                                dbc.Label("Scopes", width='auto'),
                                dbc.Col([
                                    dbc.Checklist(
                                        options=[
                                            {"label": scope, "value": scope}
                                            for scope in STATIC_CONFIG['tokens']['scopes']
                                        ],
                                        value=[
                                            scope
                                            for scope in STATIC_CONFIG['tokens']['scopes']
                                        ],
                                        id="tokens-scopes-checklist",
                                        style={'columnCount': 3},
                                    ),
                                ]),
                            ]),
                        ], id='tokens-scopes-checklist-div', style={'display': 'none'}),
                    ]),
                ]),
                dbc.ModalFooter([
                    dbc.Button('Register', id='tokens-register-button'),
                ]),
            ],
            id='tokens-create-modal',
            size='lg',
            is_open=False,
        ),
        html.Div(
            [
                dbc.Button(
                    "⟳",
                    color='black',
                    size='sm',
                    id='tokens-refresh-button',
                ),
                dbc.Button(
                    html.B('+'),
                    color='black',
                    size='sm',
                    id='tokens-create-button',
                ),
            ],
            id='tokens-controls-div',
            style={'text-align': 'right'},
        ),
        html.Div(id='tokens-output-div'),
    ])
