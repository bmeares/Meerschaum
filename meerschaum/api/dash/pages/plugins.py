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
from meerschaum.api.dash.components import pages_navbar

search_box = dbc.Input(
    id = "search-plugins-input",
    placeholder = "Search for plugins...",
    type = "text",
)

layout = [
    pages_navbar,
    dbc.Container([
        html.Div([
            html.Br(),
            html.Div(
                dbc.Container([
                    html.H3('Plugins'),
                    html.P([
                        (
                            'Plugins extend the functionality of Meerschaum.'
                            ' To find out more, check out the '
                        ),
                        html.A(
                            'plugins documentation',
                            href='https://meerschaum.io/reference/plugins/',
                            rel="noreferrer noopener",
                            target="_blank",
                        ),
                        '.',
                    ]),
                ]),
                className='page-header',
                style={'background-color': 'var(--dark)', 'padding': '1em'},
            ),
            html.Br(),
            search_box,
            html.Br(),
            html.Div([], id='plugins-cards-div'),
        ])
    ])
]
