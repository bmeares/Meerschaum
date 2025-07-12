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
            id="tokens-register-input-modal",
            size='lg',
            is_open=False,
        ),
        dbc.Modal(
            id="tokens-register-output-modal",
            size='lg',
            is_open=False,
            backdrop='static',
        ),
        html.Div(id='tokens-register-output-modal-div'),
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
