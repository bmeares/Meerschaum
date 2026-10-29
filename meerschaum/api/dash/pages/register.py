#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create an account (`/dash/register`), built per request: the form while accounts may be created
(self-registration is on, or the instance has no users yet: the first account is an
administrator), else **Registration is closed** with Sign in.
"""

from meerschaum.api import CHECK_UPDATE
import dash_bootstrap_components as dbc
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)


def build_form(first_admin: bool = False):
    """The registration form (`first_admin`: the instance's first account, an administrator)."""
    return html.Div([
        html.Br(),
        html.H3("Create admin account" if first_admin else "Create your account",
                id='register-title'),
        dbc.Row(
            [
                dbc.Col([
                        dbc.Label("Username", html_for='register-username-input'),
                        dbc.Input(
                            id="register-username-input",
                            type="text",
                            placeholder="Enter username",
                            autoComplete='username',
                        ),
                        dbc.FormFeedback("Username is available.", type="valid"),
                        dbc.FormFeedback("Username is unavailable.", type="invalid"),
                    ],
                    md=6,
                ),
                dbc.Col([
                        dbc.Label("Password", html_for='register-password-input'),
                        dbc.Input(
                            id="register-password-input", type="password", value="",
                            placeholder='Enter password', autoComplete='new-password',
                        ),
                        dbc.FormFeedback("Password is acceptable.", type="valid"),
                        dbc.FormFeedback("Password is too short.", type="invalid"),
                    ],
                    md=6,
                ),
            ],
        ),
        html.Br(),
        dbc.Row(
            dbc.Col([
                    dbc.Label("Email", html_for="register-email-input"),
                    dbc.Input(id="register-email-input", type="email", placeholder="Optional",
                              autoComplete='email'),
                    dbc.FormFeedback("", type="valid"),
                    dbc.FormFeedback("", type="invalid"),
                ],
            ),
        ),
        html.Br(),
        html.Div(id='register-alert-div'),
        dbc.Row([
            dbc.Col([
                html.Button(
                    children='Sign Up',
                    type='submit',
                    id='register-button',
                    className='btn btn-primary btn-lg'
                ),
            ]),
        ]),
    ])


def build_closed():
    """Registration is closed: a short title and Sign in."""
    return html.Div(
        [
            html.H3('Registration is closed', id='register-closed'),
            dbc.Button('Sign in', href='/dash/login', color='primary'),
        ],
        className='console-empty',
    )


def layout(pathname=None, href=None):
    """The form, or Registration is closed."""
    from meerschaum.api.dash.registration import first_run, self_registration_open
    first_admin = first_run()
    return dbc.Container(
        build_form(first_admin=first_admin)
        if (first_admin or self_registration_open())
        else build_closed()
    )
