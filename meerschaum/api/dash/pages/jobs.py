#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The Jobs page (`/dash/jobs`): persistent jobs and the console's runs, one tab each
(`/dash/runs` opens the Runs tab), and one job's card at `/dash/jobs/{name}`. The listing (its
tabs, New job form, filter and table) is built per URL by `callbacks/jobs.py`.
"""

from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc

html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc

from meerschaum.api.dash.components import download_logs, build_pages_navbar
from meerschaum.api.dash.jobs import job_logs_modal, JOBS_REFRESH_MS

layout = [
    build_pages_navbar(),
    dbc.Container([
        dcc.Location('job-location'),
        html.Div(id='job-output-div'),
        download_logs,
        job_logs_modal,
        ### The job cards' refresh (the Runs tab, one job's page); the Jobs tab's table has its
        ### own (`jobs-list-interval`) and turns this one off.
        dcc.Interval(id='refresh-jobs-interval', interval=JOBS_REFRESH_MS, n_intervals=0),
    ], class_name='console-page jobs-page', fluid=True),
]
