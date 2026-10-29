#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Jobs page (`/dash/jobs`): persistent jobs and the console's runs, one tab each
(`/dash/runs` opens the Runs tab), and one job's card at `/dash/jobs/{name}`. The listing (its
tabs, New job form, filter and list) is built per URL by `callbacks/jobs.py`.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import download_logs, refresh_jobs_interval, build_pages_navbar
from meerschaum.api.dash.jobs import job_logs_modal

layout = [
    build_pages_navbar(),
    dbc.Container([
        dcc.Location('job-location'),
        html.Div(id='job-output-div'),
        download_logs,
        job_logs_modal,
        refresh_jobs_interval,
    ], class_name='console-page jobs-page'),
]
