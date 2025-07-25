#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Display pipes via a shareable URL.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
from meerschaum.api.dash.components import (
    download_dataframe,
    pages_navbar_with_instance_select,
)

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

layout = [
    pages_navbar_with_instance_select,
    dcc.Location('pipes-location'),
    download_dataframe,
    dbc.Container([
        dcc.Loading(
            html.Div(id='pipe-output-div'),
            id='pipes-loading',
            type='circle',
            delay_hide=1000,
            delay_show=1000,
        ),
    ])
]
