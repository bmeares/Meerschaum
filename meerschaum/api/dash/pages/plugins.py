#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
View the available plugins hosted by this API instance.
"""

from __future__ import annotations
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin
from meerschaum.utils.typing import Optional
from meerschaum.api.dash.components import build_pages_navbar

search_box = dbc.Input(
    id="search-plugins-input",
    placeholder="Search for plugins...",
    type="text",
    debounce=True,
)

layout = [
    build_pages_navbar(),
    dcc.Location(id='plugins-location'),
    dbc.Container([
        html.Div([
            html.Br(),
            html.Div(search_box, id='plugins-search-wrapper'),
            html.Br(),
            dbc.Pagination(
                id='plugins-pagination',
                max_value=1,
                active_page=1,
                first_last=True,
                previous_next=True,
                fully_expanded=False,
                style={'justify-content': 'center', 'display': 'none'},
            ),
            html.Div([], id='plugins-content-div'),
        ])
    ])
]
