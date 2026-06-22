#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
View the users registered on this API instance.
"""

from __future__ import annotations
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import build_pages_navbar

search_box = dbc.Input(
    id="search-users-input",
    placeholder="Search for users...",
    type="text",
    debounce=True,
)

layout = [
    build_pages_navbar(),
    dcc.Location(id='users-location'),
    dbc.Container([
        html.Div([
            html.Br(),
            html.Div(search_box, id='users-search-wrapper'),
            html.Br(),
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
        ])
    ])
]
