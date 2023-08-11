#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create new user accounts.
"""

from meerschaum.api import CHECK_UPDATE
import dash_bootstrap_components as dbc
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

layout = dbc.Container([
    html.Div([
        html.Br(),
        html.H3("Register your account"),
        dbc.Row(
            [
                dbc.Col([
                        dbc.Label("Username", html_for='register-username-input'),
                        dbc.Input(
                            id="register-username-input",
                            type="text",
                            placeholder="Enter username"
                        ),
                        dbc.FormFeedback("Username is available.", type="valid"),
                        dbc.FormFeedback("Username is unavailable.", type="invalid"),
                    ],
                    width = 6,
                ),
                dbc.Col([
                    #  dbc.FormGroup([
                        dbc.Label("Password", html_for='register-password-input'),
                        dbc.Input(
                            id="register-password-input", type="password", value="",
                            placeholder='Enter password'
                        ),
                        dbc.FormFeedback("Password is acceptable.", type="valid"),
                        dbc.FormFeedback("Password is too short.", type="invalid"),
                    ],
                    width = 6,
                ),
            ],
        ),
        html.Br(),
        dbc.Row(
            dbc.Col([
                    dbc.Label("Email", html_for="register-email-input"),
                    dbc.Input(id="register-email-input", type="email", placeholder="Optional"),
                    dbc.FormFeedback("", type="valid"),
                    dbc.FormFeedback("", type="invalid"),
                ],
            ),
        ),
        html.Br(),
        dbc.Row([
            dbc.Col([
                html.Button(
                    children = 'Sign Up',
                    type = 'submit',
                    id = 'register-button',
                    className = 'btn btn-primary btn-lg'
                ),
            ]),
        ]),

    ])
])
