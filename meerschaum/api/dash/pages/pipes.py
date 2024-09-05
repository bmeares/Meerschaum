#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Display pipes via a shareable URL.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import download_dataframe

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

layout = dbc.Container([
    dcc.Location('pipes-location'),
    download_dataframe,
    html.Div(id='pipe-output-div'),
])
