#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The terminal page (`/dash/webterm`): the Rust server's terminal in an iframe
(`meerschaum.api.dash.webterm`).
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import build_pages_navbar
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

layout = [
    build_pages_navbar(),
    dbc.Container(
        [html.Div(id='webterm-page-div')],
        fluid=True,
        style={'max-width': '96%'},
    ),
]
