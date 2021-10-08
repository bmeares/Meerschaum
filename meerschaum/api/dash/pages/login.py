#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Login page for the Web interface.
"""

from __future__ import annotations
import json
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(), import_dcc()
import dash_bootstrap_components as dbc
from meerschaum.api import permissions_config, endpoints
from meerschaum.config import get_config

dash_endpoint = endpoints['dash']
allow_user_registration = permissions_config['registration']['users']

registration_div = html.Div(
    id = 'registration-div',
    style = {'height' : '100%'},
    children = (
        [
            dcc.Link(
                'No account? Create one here.',
                href = (dash_endpoint + '/register'),
                refresh = False
            ),
        ] if allow_user_registration
        else [
            dcc.Markdown("""
                #### **Web registration is disabled for security.**
                You can still register users on this instance with a SQL connector.
            """),
            dbc.Button(
                'More information.',
                id = 'show-user-registration-disabled-button',
                color = 'link',
                size = 'sm',
            ),
            dbc.Collapse(
                id = 'user-registration-disabled-collapse',
                children = [
                    dcc.Markdown(
                        "For example, to register user `newuser` on instance `sql:main`):"
                    ),
                    html.Pre(
                        html.Code('mrsm register user newuser -i sql:main', className='codeblock'),
                    ),
                    dcc.Markdown("""
                        To enable online registration, open the `system` configuration file and""" +
                    """ set the permissions to `true`:"""
                    ),
                    html.Pre(
                        html.Code('mrsm edit config system', className='codeblock'),
                    ),
                    html.Br(),
                    dcc.Markdown('The settings file should look something like this:'),
                    html.Pre(
                        html.Code(
                            json.dumps({
                                'api' : {
                                    'permissions' : {
                                        'registration' : {
                                            'users' : True,
                                        },
                                    }
                                }
                            }, indent=2),
                        className='codeblock'),
                    ),
                ]
            ),
        ]
    )
)

layout = dbc.Container([
    html.Br(),
    dbc.Container([
        dcc.Location(id='location-login', refresh=True),
        html.Div([
            dbc.Container(
                html.Img(
                    src = endpoints['dash'] + "/assets/banner_1920x320.png",
                    width = '100%',
                    className = 'center'
                ),
            ),
            html.Br(),
            dbc.Container(id='login-container', children=[
                dbc.Row([
                    dbc.Col(
                    dcc.Input(
                        placeholder = 'Username',
                        type = 'text',
                        id = 'username-input',
                        className = 'form-control',
                        n_submit = 0,
                    )),
                    dbc.Col(dcc.Input(
                        placeholder = 'Password',
                        type = 'password',
                        id = 'password-input',
                        className = 'form-control',
                        n_submit = 0,
                    )),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        html.Button(
                            children = 'Login',
                            n_clicks = 0,
                            type = 'submit',
                            id = 'login-button',
                            className = 'btn btn-primary btn-lg'
                        ),
                    ]),
                ]),
                html.Br(),
                dbc.Row([
                    dbc.Col([
                        registration_div,
                    ]),
                ]),
            ], className='form-group'),
        ]),
    ], className='jumbotron')
])

