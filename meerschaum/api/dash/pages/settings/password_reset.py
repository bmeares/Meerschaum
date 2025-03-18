#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the password reset page layout.
"""

import dash_bootstrap_components as dbc
import dash.html as html
import dash.dcc as dcc
from meerschaum.plugins import web_page


@web_page('password-reset', login_required=True, page_group='Settings')
def page_layout():
    """
    Return the layout for this page.
    """
    return dbc.Container([
        html.Br(),
        html.H3('Password Reset'),
        html.Br(),
        html.Div(id="password-reset-alert-div"),
        dbc.Form([
            dbc.Row(
                [
                    dbc.Label("New Password", html_for="password-reset-input", width=2),
                    dbc.Col(
                        dbc.Input(
                            type="password",
                            id="password-reset-input",
                            placeholder="Enter new password",
                        ),
                        width=10,
                    ),
                ],
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Label("Confirm Password", html_for="password-reset-confirm-input", width=2),
                    dbc.Col(
                        dbc.Input(
                            type="password",
                            id="password-reset-confirm-input",
                            placeholder="Confirm new password",
                        ),
                        width=10,
                    ),
                ],
                className="mb-3",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Button("Submit", id="password-reset-button", disabled=True),
                    ),
                ],
                justify="end",
                className="mb-3",
            ),
        ]),
    ])
