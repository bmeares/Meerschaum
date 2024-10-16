#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Display pipes via a shareable URL.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import download_logs, refresh_jobs_interval

layout = dbc.Container([
    dcc.Location('job-location'),
    html.Div(id='job-output-div'),
    download_logs,
    refresh_jobs_interval,
])
