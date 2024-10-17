#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for jobs' cards.
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone

from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.sessions import get_username_from_session
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.jobs import (
    build_job_card,
    build_manage_job_buttons_div_children,
    build_status_children,
    build_process_timestamps_children,
)
from meerschaum.api.routes._jobs import _get_job
from meerschaum.api.dash.sessions import is_session_authenticated
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
import dash_bootstrap_components as dbc
from dash import no_update


@dash_app.callback(
    Output("download-logs", "data"),
    Input({'type': 'job-download-logs-button', 'index': ALL}, 'n_clicks'),
    prevent_initial_call=True,
)
def download_job_logs(n_clicks):
    """
    When the download logs button is clicked, download the logs as one text file.

    It would have been more efficient to return the existing files on disk,
    but because the rotating log will keep file sizes down, this should not grow too large.
    """
    if not n_clicks:
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate

    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    job_name = component_dict['index']
    try:
        job = _get_job(job_name)
    except Exception:
        job = None
    if job is None or not job.exists():
        raise PreventUpdate

    now = datetime.now(timezone.utc)
    filename = job_name + '_' + str(int(now.timestamp())) + '.log'
    return {
        'content': job.get_logs(),
        'filename': filename,
    }


@dash_app.callback(
    Output({'type': 'manage-job-alert-div', 'index': MATCH}, 'children'),
    Output({'type': 'manage-job-buttons-div', 'index': MATCH}, 'children'),
    Output({'type': 'manage-job-status-div', 'index': MATCH}, 'children'),
    Output({'type': 'process-timestamps-div', 'index': MATCH}, 'children'),
    Input({'type': 'manage-job-button', 'action': ALL, 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'),
    State({'type': 'job-label-p', 'index': MATCH}, 'children'),
    prevent_initial_call=True,
)
def manage_job_button_click(
    n_clicks: Optional[int] = None,
    session_data: Optional[Dict[str, Any]] = None,
    job_label: Optional[str] = None,
):
    """
    Start, stop, pause, or delete the given job.
    """
    if not n_clicks:
        raise PreventUpdate

    session_id = session_data.get('session-id', None)
    username = get_username_from_session(session_id)

    if not is_session_authenticated(session_id):
        success, msg = False, f"User '{username}' is not authenticated to manage jobs."
        return (
            alert_from_success_tuple((success, msg)),
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate

    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    job_name = component_dict['index']
    manage_job_action = component_dict['action']
    try:
        job = _get_job(job_name, job_label.replace('\n', ' ') if job_label else None)
    except Exception:
        job = None
    if job is None or not job.exists():
        raise PreventUpdate

    manage_functions = {
        'start': job.start,
        'stop': job.stop,
        'pause': job.pause,
        'delete': job.delete,
    }
    if manage_job_action not in manage_functions:
        return (
            alert_from_success_tuple((False, f"Invalid action '{manage_job_action}'.")),
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    old_status = job.status
    try:
        success, msg = manage_functions[manage_job_action]()
    except Exception as e:
        success, msg = False, traceback.format_exc()

    ### Wait for a status change before building the elements.
    timeout_seconds = 1.0
    check_interval_seconds = 0.01
    begin = time.perf_counter()
    while (time.perf_counter() - begin) < timeout_seconds:
        if job.status != old_status:
            break
        time.sleep(check_interval_seconds)

    return (
        alert_from_success_tuple((success, msg)),
        build_manage_job_buttons_div_children(job),
        build_status_children(job),
        build_process_timestamps_children(job),
    )

dash_app.clientside_callback(
    """
    function(n_clicks_arr, url){
        display_block = {"display": "block"};

        var clicked = false;
        for (var i = 0; i < n_clicks_arr.length; i++){
            if (n_clicks_arr[i]){
                clicked = true;
                break;
            }
        }

        if (!clicked){
            return dash_clientside.no_update;
        }

        const triggered_id = dash_clientside.callback_context.triggered_id;
        const job_name = triggered_id["index"];

        iframe = document.getElementById('webterm-iframe');
        if (!iframe){ return dash_clientside.no_update; }

        iframe.contentWindow.postMessage(
            {
               action: "show",
               subaction: "logs",
               subaction_text: job_name,
            },
            url
        );
        dash_clientside.set_props("webterm-div", {"style": display_block});
        return [];
    }
    """,
    Output('content-div-right', 'children'),
    Input({'type': 'follow-logs-button', 'index': ALL}, 'n_clicks'),
    State('mrsm-location', 'href'),
)


@dash_app.callback(
    Output({'type': 'manage-job-buttons-div', 'index': ALL}, 'children'),
    Output({'type': 'manage-job-status-div', 'index': ALL}, 'children'),
    Output({'type': 'process-timestamps-div', 'index': ALL}, 'children'),
    Input('refresh-jobs-interval', 'n_intervals'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def refresh_jobs_on_interval(
    n_intervals: Optional[int] = None,
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    When the jobs refresh interval fires, rebuild the jobs' onscreen components.
    """
    session_id = session_data.get('session-id', None)
    is_authenticated = is_session_authenticated(session_id)

    job_names = [
        component_dict['id']['index']
        for component_dict in dash.callback_context.outputs_grouping[0]
    ]

    ### NOTE: The job may have been deleted, but the card may still exist.
    jobs = [_get_job(name) for name in job_names]

    return (
        [
            (
                build_manage_job_buttons_div_children(job)
                if is_authenticated
                else []
            )
            for job in jobs
        ],
        [
            build_status_children(job)
            for job in jobs
        ],
        [
            build_process_timestamps_children(job)
            for job in jobs
        ],
    )


@dash_app.callback(
    Output('job-output-div', 'children'),
    Input('job-location', 'pathname'),
    State('session-store', 'data'),
)
def render_job_page_from_url(
    pathname: str,
    session_data: Optional[Dict[str, Any]],
):
    """
    Load the `/job/{name}` page.
    """
    if not str(pathname).startswith('/dash/job'):
        return no_update

    session_id = (session_data or {}).get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))

    job_name = pathname.replace('/dash/job', '').lstrip('/').rstrip('/')
    if not job_name:
        return no_update

    job = _get_job(job_name)
    if not job.exists():
        return [
            html.Br(),
            html.H2("404: Job does not exist."),
        ]

    return [
        html.Br(),
        build_job_card(job, authenticated=authenticated, include_follow=False),
        html.Br(),
    ]
