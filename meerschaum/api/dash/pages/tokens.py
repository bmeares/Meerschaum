#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the tokens page layout.
"""

import dash_bootstrap_components as dbc
import dash.html as html
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.api.dash.components import build_pages_navbar, build_icon


layout = [
    build_pages_navbar(),
    dbc.Container([
        html.Div(
            [
                html.H3('Tokens'),
                html.Div(
                    [
                        dbc.Button(
                            build_icon('refresh'),
                            color='black',
                            size='sm',
                            id='tokens-refresh-button',
                            title='Refresh tokens',
                        ),
                        dbc.Button(
                            'New token',
                            color='primary',
                            id='tokens-create-button',
                        ),
                    ],
                    id='tokens-controls-div',
                    className='console-page-actions',
                ),
            ],
            className='console-page-header',
        ),
        html.Div(id="tokens-alert-div"),
        dbc.Modal(
            id="tokens-register-input-modal",
            size='lg',
            fullscreen='sm-down',
            scrollable=True,
            is_open=False,
            class_name='tokens-modal',
        ),
        dbc.Modal(
            id="tokens-register-output-modal",
            size='lg',
            fullscreen='sm-down',
            scrollable=True,
            is_open=False,
            backdrop='static',
            class_name='tokens-modal',
        ),
        html.Div(id='tokens-register-output-modal-div'),
        html.Div(id='tokens-output-div'),
    ], class_name='console-page tokens-page'),
]
