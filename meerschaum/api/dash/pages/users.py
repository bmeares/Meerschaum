#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The users of this API instance (`/dash/users`), and one user's page (`/dash/users/{name}`).
"""

from __future__ import annotations
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.users import build_users_modals

search_box = dbc.Input(
    id="search-users-input",
    placeholder="Search users",
    type="search",
    debounce=True,
    autoComplete='off',
)

layout = [
    build_pages_navbar(),
    dcc.Location(id='users-location'),
    dbc.Container([
        html.Div(
            [
                html.H3('Users'),
                html.Div(
                    dbc.Button('Add user', id='users-add-button', color='primary'),
                    id='users-add-button-div',
                    hidden=True,
                    className='console-page-actions',
                ),
            ],
            id='users-header',
            className='console-page-header',
        ),
        html.Div(search_box, id='users-search-wrapper', className='console-search'),
        html.Div(id='users-alert-div'),
        dbc.Pagination(
            id='users-pagination',
            max_value=1,
            active_page=1,
            first_last=True,
            previous_next=True,
            fully_expanded=False,
            style={'justify-content': 'center', 'display': 'none'},
        ),
        html.Div([], id='users-content-div'),
        dcc.Store(id='users-refresh', data=0),
        *build_users_modals(),
    ], class_name='console-page users-page'),
]
