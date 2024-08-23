#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Display pipes via a shareable URL.
"""

from meerschaum.api import get_api_connector, CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

layout = dbc.Container([
    dcc.Location('pipes-location'),
    html.Div(id='pipe-output-div'),
])
