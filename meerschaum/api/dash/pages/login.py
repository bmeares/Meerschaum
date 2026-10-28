#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's login page, built per request (`layout(pathname, href)`):

- while the API's user store has no users, **Create admin account** (the first account is an
  administrator: `meerschaum.api.dash.registration.create_first_admin`);
- otherwise the login form, with **Create account** when self-registration is on
  (`api:permissions:registration:users`, off by default).
"""

from __future__ import annotations

from meerschaum.api import endpoints, CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.utils.typing import Any, Optional
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

dash_endpoint = endpoints['dash']


def _banner() -> Any:
    return dbc.Container(
        html.Img(
            src=endpoints['dash'] + "/assets/banner_1920x320.png",
            width='100%',
            className='center',
            alt='Meerschaum',
            id='login-banner',
        ),
    )


def build_login_form(registration: bool = False) -> Any:
    """The username and password, Login, and Create account when visitors may register."""
    return dbc.Container(id='login-container', children=[
        dbc.Row([
            dbc.Col(dbc.Input(
                placeholder='Username',
                type='text',
                id='username-input',
                class_name='form-control',
                autoComplete='username',
                n_submit=0,
            )),
            dbc.Col(dbc.Input(
                placeholder='Password',
                type='password',
                id='password-input',
                class_name='form-control',
                autoComplete='current-password',
                n_submit=0,
            )),
        ]),
        html.Br(),
        dbc.Row([
            dbc.Col([
                html.Button(
                    children='Login',
                    n_clicks=0,
                    type='submit',
                    id='login-button',
                    className='btn btn-primary btn-lg',
                ),
            ]),
        ]),
        html.Br(),
        html.Div(
            (
                [dcc.Link('Create account', href=(dash_endpoint + '/register'), refresh=False,
                          id='login-register-link')]
                if registration else []
            ),
            id='registration-div',
        ),
    ], className='form-group')


def build_first_admin_form() -> Any:
    """The first account of an instance without users: an administrator."""
    field = lambda label, control, field_id: html.Div(
        [dbc.Label(label, html_for=field_id), control],
        className='new-job-field',
    )
    return dbc.Container(
        [
            html.H3('Create admin account', id='first-admin-title'),
            html.Div(id='first-admin-alert'),
            field('Username', dbc.Input(id='first-admin-username', autoComplete='username', n_submit=0),
                  'first-admin-username'),
            field('Password', dbc.Input(id='first-admin-password', type='password',
                                        autoComplete='new-password', n_submit=0),
                  'first-admin-password'),
            field('Confirm password', dbc.Input(id='first-admin-confirm', type='password',
                                                autoComplete='new-password', n_submit=0),
                  'first-admin-confirm'),
            field('Email', dbc.Input(id='first-admin-email', type='email', placeholder='Optional',
                                     autoComplete='email'),
                  'first-admin-email'),
            html.Br(),
            html.Button(
                'Create admin account',
                id='first-admin-button',
                n_clicks=0,
                type='submit',
                className='btn btn-primary btn-lg',
            ),
        ],
        id='first-admin-container',
        className='form-group first-admin',
    )


def layout(pathname: Optional[str] = None, href: Optional[str] = None) -> Any:
    """The login page: Create admin account on an instance without users, else the login form."""
    from meerschaum.api.dash.registration import first_run, self_registration_open
    body = build_first_admin_form() if first_run() else build_login_form(self_registration_open())
    return dbc.Container([
        html.Br(),
        dbc.Container([
            dcc.Location(id='location-login', refresh=True),
            html.Div(id="login-alert-div"),
            html.Div([
                _banner(),
                html.Br(),
                body,
            ]),
        ], className='jumbotron'),
    ])
