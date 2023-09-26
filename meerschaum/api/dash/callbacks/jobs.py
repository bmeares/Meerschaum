#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for jobs' cards.
"""

from __future__ import annotations
import json
import functools
import time
import traceback
import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
from meerschaum.api.dash import dash_app, debug, active_sessions
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash import Patch
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.utils.daemon import Daemon
from dash.exceptions import PreventUpdate
from meerschaum.api.dash.jobs import (
    build_manage_job_buttons_div_children,
    build_status_children,
    build_process_timestamps_children,
)
from meerschaum.api.dash.users import is_session_authenticated

@dash_app.callback(
    Output("download-logs", "data"),
    Input({'type': 'job-download-logs-button', 'index': ALL}, 'n_clicks'),
    prevent_initial_call = True,
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
    daemon_id = component_dict['index']
    daemon = Daemon(daemon_id=daemon_id)
    return {
        'content': daemon.log_text,
        'filename': daemon.rotating_log.file_path.name,
    }


@dash_app.callback(
    Output({'type': 'manage-job-alert-div', 'index': MATCH}, 'children'),
    Output({'type': 'manage-job-buttons-div', 'index': MATCH}, 'children'),
    Output({'type': 'manage-job-status-div', 'index': MATCH}, 'children'),
    Output({'type': 'process-timestamps-div', 'index': MATCH}, 'children'),
    Input({'type': 'manage-job-button', 'action': ALL, 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call = True,
)
def manage_job_button_click(
        n_clicks: Optional[int] = None,
        session_data: Optional[Dict[str, Any]] = None,
    ):
    """
    Start, stop, or pause the given job.
    """
    if not n_clicks:
        raise PreventUpdate

    session_id = session_data.get('session-id', None)
    username = active_sessions.get(session_id, {}).get('username', None)

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
    daemon_id = component_dict['index']
    manage_job_action = component_dict['action']
    daemon = Daemon(daemon_id=daemon_id)

    manage_functions = {
        'start': functools.partial(daemon.run, allow_dirty_run=True),
        'stop': daemon.quit,
        'pause': daemon.pause,
    }
    if manage_job_action not in manage_functions:
        return (
            alert_from_success_tuple((False, f"Invalid action '{manage_job_action}'.")),
            dash.no_update,
            dash.no_update,
            dash.no_update,
        )

    old_status = daemon.status
    try:
        success, msg = manage_functions[manage_job_action]()
    except Exception as e:
        success, msg = False, traceback.format_exc()

    ### Wait for a status change before building the elements.
    timeout_seconds = 1.0
    check_interval_seconds = 0.01
    begin = time.perf_counter()
    while (time.perf_counter() - begin) < timeout_seconds:
        if daemon.status != old_status:
            break
        time.sleep(check_interval_seconds)

    return (
        alert_from_success_tuple((success, msg)),
        build_manage_job_buttons_div_children(daemon),
        build_status_children(daemon),
        build_process_timestamps_children(daemon),
    )


@dash_app.callback(
    Output({'type': 'manage-job-buttons-div', 'index': ALL}, 'children'),
    Output({'type': 'manage-job-status-div', 'index': ALL}, 'children'),
    Output({'type': 'process-timestamps-div', 'index': ALL}, 'children'),
    Input('refresh-jobs-interval', 'n_intervals'),
    State('session-store', 'data'),
    prevent_initial_call = True,
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

    daemon_ids = [
        component_dict['id']['index']
        for component_dict in dash.callback_context.outputs_grouping[0]
    ]

    ### NOTE: The daemon may have been deleted, but the card may still exist.
    daemons = []
    for daemon_id in daemon_ids:
        try:
            daemon = Daemon(daemon_id=daemon_id)
        except Exception as e:
            daemon = None
        daemons.append(daemon)

    return (
        [
            (
                build_manage_job_buttons_div_children(daemon)
                if is_authenticated
                else []
            )
            for daemon in daemons
        ],
        [
            build_status_children(daemon)
            for daemon in daemons
        ],
        [
            build_process_timestamps_children(daemon)
            for daemon in daemons
        ],
    )
