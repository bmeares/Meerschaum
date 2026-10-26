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

import meerschaum as mrsm
from meerschaum.jobs import get_jobs
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.sessions import get_username_from_session
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.jobs import (
    STATUS_EMOJI,
    build_job_card,
    build_archived_run_card,
    is_automatic_job,
    job_visible_to_session,
    build_manage_job_buttons_div_children,
    build_status_children,
    build_process_timestamps_children,
)
from meerschaum.api.dash._support import get_job as _get_job
from meerschaum.api.dash.sessions import is_session_authenticated, is_state_authenticated
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
import dash_bootstrap_components as dbc
from dash import no_update


@dash_app.callback(
    Output("download-logs", "data"),
    Input({'type': 'job-download-logs-button', 'index': ALL}, 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def download_job_logs(n_clicks, session_store_data):
    """
    When the download logs button is clicked, download the logs as one text file.

    It would have been more efficient to return the existing files on disk,
    but because the rotating log will keep file sizes down, this should not grow too large.
    """
    if not n_clicks:
        raise PreventUpdate

    ### Job logs may contain connector URIs and query text.
    if not is_state_authenticated(session_store_data):
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
    if job is None or not job.exists() or not job_visible_to_session(job, (session_store_data or {}).get('session-id')):
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
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        raise PreventUpdate

    manage_functions = {
        'start': job.start,
        'stop': job.stop,
        'pause': job.pause,
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
    except Exception:
        success, msg = False, traceback.format_exc()

    ### Wait for a status change before building the elements.
    timeout_seconds = 1.0
    check_interval_seconds = mrsm.get_config('system', 'cli', 'refresh_seconds')
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

@dash_app.callback(
    Output({'type': 'job-delete-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'job-delete-alert-div', 'index': MATCH}, 'children'),
    Input({'type': 'job-delete-button', 'index': MATCH}, 'n_clicks'),
    Input({'type': 'job-delete-cancel-button', 'index': MATCH}, 'n_clicks'),
    Input({'type': 'job-delete-confirm-button', 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'),
    State({'type': 'job-label-p', 'index': MATCH}, 'children'),
    prevent_initial_call=True,
)
def job_delete_modal(
    open_clicks: Optional[int] = None,
    cancel_clicks: Optional[int] = None,
    confirm_clicks: Optional[int] = None,
    session_data: Optional[Dict[str, Any]] = None,
    job_label: Optional[str] = None,
):
    """
    Open the delete-job confirmation modal, cancel it, or perform the deletion on confirm.
    """
    ctx = dash.callback_context.triggered
    if not ctx or ctx[0]['value'] is None:
        raise PreventUpdate

    prop_id = ctx[0]['prop_id']
    if 'job-delete-button' in prop_id:
        return True, no_update
    if 'job-delete-cancel-button' in prop_id:
        return False, no_update

    ### Confirm button: actually delete the job.
    session_id = (session_data or {}).get('session-id', None)
    if not is_session_authenticated(session_id):
        username = get_username_from_session(session_id)
        return True, alert_from_success_tuple(
            (False, f"User '{username}' is not authenticated to manage jobs.")
        )

    component_dict = json.loads(prop_id.split('.' + 'n_clicks')[0])
    job_name = component_dict['index']
    try:
        job = _get_job(job_name, job_label.replace('\n', ' ') if job_label else None)
    except Exception:
        job = None
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        return False, no_update

    try:
        success, msg = job.delete()
    except Exception:
        success, msg = False, traceback.format_exc()

    if not success:
        return True, alert_from_success_tuple((success, msg))

    ### Card is removed on the next refresh-jobs-interval tick.
    return False, no_update


### "Follow logs" is handled by `meerschaum.api.dash.callbacks.execute` (the output pane).


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
    jobs = [job if job_visible_to_session(job, session_id) else None for job in jobs]

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


#: The tabs of the Jobs page (`/dash/runs` opens `runs`).
JOBS_PAGE_TABS = ('jobs', 'runs')


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
    Load the Jobs page (`/dash/jobs`, or `/dash/runs` for its Runs tab) or one job's card
    (`/dash/jobs/{name}`).
    """
    is_runs = str(pathname).startswith('/dash/runs')
    if not (str(pathname).startswith('/dash/jobs') or is_runs):
        return no_update

    session_id = (session_data or {}).get('session-id', None)
    authenticated = is_session_authenticated(str(session_id))

    job_name = pathname.replace('/dash/runs' if is_runs else '/dash/jobs', '').lstrip('/').rstrip('/')
    if not job_name:
        return [
            html.Br(),
            dbc.Tabs(
                [dbc.Tab(label='Jobs', tab_id='jobs'), dbc.Tab(label='Runs', tab_id='runs')],
                id='jobs-page-tabs',
                active_tab=('runs' if is_runs else 'jobs'),
            ),
            html.Br(),
            html.Div(id='jobs-page-tab-content'),
            html.Br(),
        ]

    job = _get_job(job_name)
    if not job.exists() or not job_visible_to_session(job, session_id):
        return [
            html.Br(),
            html.H2("404: Job does not exist."),
        ]

    return [
        html.Br(),
        build_job_card(job, authenticated=authenticated, include_follow=False),
        html.Br(),
    ]


@dash_app.callback(
    Output('jobs-page-tab-content', 'children'),
    Input('jobs-page-tabs', 'active_tab'),
    State('session-store', 'data'),
)
def render_jobs_page_tab(active_tab: Optional[str], session_data: Optional[Dict[str, Any]]):
    """
    Build the cards of the Jobs page's active tab.
    """
    if active_tab not in JOBS_PAGE_TABS:
        raise PreventUpdate
    return build_jobs_tab(active_tab == 'runs', (session_data or {}).get('session-id', None))


def build_jobs_tab(is_runs: bool, session_id: Optional[str]):
    """
    The cards of persistent jobs, or of the console's runs (automatic execution workers and the
    summaries retained after their output expired).
    """
    authenticated = is_session_authenticated(str(session_id))
    jobs = get_jobs(executor_keys='local', combine_local_and_systemd=True)
    cards = [
        build_job_card(job, authenticated=authenticated, include_follow=False)
        for job in jobs.values()
        if is_automatic_job(job) == is_runs and job_visible_to_session(job, session_id)
    ]
    if is_runs and authenticated:
        from meerschaum.api.dash.execute import execution_request
        try:
            records = execution_request('/execute/runs', get_username_from_session(session_id)) or []
            cards.extend(build_archived_run_card(record) for record in records)
        except Exception as exc:
            cards.append(alert_from_success_tuple((False, f'Unable to load retained summaries: {exc}')))
    if not cards:
        return html.P('No runs.' if is_runs else 'No jobs.', className='text-muted')
    return build_cards_grid(cards, 3)


def _read_job_logs(job_name: str, session_id: Optional[str]) -> Optional[Dict[str, Any]]:
    """The logs and status of a job the session may read, or `None`."""
    try:
        job = _get_job(job_name)
    except Exception:
        return None
    if job is None or not job.exists() or not job_visible_to_session(job, session_id):
        return None
    text = job.get_logs() or ''
    return {'job': job_name, 'text': text, 'status': job.status, 'digest': _digest(text)}


def _digest(text: str) -> str:
    """Tell whether the logs changed (a rotated log may keep its length)."""
    import hashlib
    return hashlib.sha1(text.encode('utf-8', 'replace')).hexdigest()


@dash_app.callback(
    Output('job-logs-modal', 'is_open'),
    Output('job-logs-modal-title', 'children'),
    Output('job-logs-meta', 'data'),
    Output('job-logs-text', 'data'),
    Output('job-logs-interval', 'disabled'),
    Input({'type': 'job-view-logs-button', 'index': ALL}, 'n_clicks'),
    Input('job-logs-interval', 'n_intervals'),
    Input('job-logs-modal', 'is_open'),
    State('job-logs-meta', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def view_job_logs(view_clicks, n_intervals, is_open, meta, session_store_data):
    """
    "Logs" > "View": open the logs modal on the job's logs, and re-read them while the job runs
    (only what changed is sent; the browser keeps its scroll position unless it follows the end).
    """
    ctx = dash.callback_context
    return job_logs_outputs(
        ctx.triggered_id,
        ctx.triggered[0]['value'] if ctx.triggered else None,
        is_open,
        meta,
        session_store_data,
    )


def job_logs_outputs(trigger_id, triggered_value, is_open, meta, session_store_data):
    """
    The outputs of `view_job_logs`: the modal's state, title, the meta and text stores, and
    whether the refresh interval is disabled.
    """
    if trigger_id == 'job-logs-modal':
        if is_open:
            raise PreventUpdate
        return no_update, no_update, None, None, True

    ### Job logs may contain connector URIs and query text.
    if not is_state_authenticated(session_store_data):
        raise PreventUpdate
    session_id = (session_store_data or {}).get('session-id')

    if isinstance(trigger_id, dict):
        ### A card appeared (no click).
        if not triggered_value:
            raise PreventUpdate
        logs = _read_job_logs(trigger_id['index'], session_id)
        if logs is None:
            raise PreventUpdate
        status = logs['status']
        return (
            True,
            build_logs_modal_title(logs['job'], status),
            {'job': logs['job'], 'status': status, 'digest': logs['digest']},
            {'job': logs['job'], 'text': logs['text'], 'opened': time.time()},
            status not in ('running', 'paused'),
        )

    if not is_open or not meta:
        return no_update, no_update, no_update, no_update, True
    logs = _read_job_logs(meta['job'], session_id)
    if logs is None:
        return no_update, no_update, no_update, no_update, True
    status, digest = logs['status'], logs['digest']
    if status == meta.get('status') and digest == meta.get('digest'):
        raise PreventUpdate
    return (
        no_update,
        build_logs_modal_title(logs['job'], status) if status != meta.get('status') else no_update,
        {'job': logs['job'], 'status': status, 'digest': digest},
        {'job': logs['job'], 'text': logs['text']} if digest != meta.get('digest') else no_update,
        status not in ('running', 'paused'),
    )


def build_logs_modal_title(job_name: str, status: str):
    """The logs modal's title: the job's status emoji and name."""
    emoji = STATUS_EMOJI.get(status, STATUS_EMOJI['stopped'])
    return [html.Span(emoji, title=str(status).capitalize()), ' ', html.B(job_name)]


### Render the logs with their ANSI colors through the Pipelines page's output renderer
### (`assets/pipeline_output.js`): it opens at the end and follows new lines only while the
### view is scrolled to the bottom, so scrolling up to read history is never interrupted.
dash_app.clientside_callback(
    """
    function(data){
        if (!data) return dash_clientside.no_update;
        function render(attempt){
            const view = document.getElementById('job-logs-view');
            // The modal mounts its body as it opens.
            if (!view){
                if (attempt < 40) setTimeout(() => render(attempt + 1), 25);
                return;
            }
            if (data.opened && view._mrsmOpened !== data.opened){
                view._mrsmOpened = data.opened;
                view._mrsmText = null;
                view._mrsmFollow = true;
            }
            if (window.mrsmRenderPipelineOutput) window.mrsmRenderPipelineOutput(view, data.text);
            else { view.textContent = data.text; view.scrollTop = view.scrollHeight; }
        }
        render(0);
        return dash_clientside.no_update;
    }
    """,
    Output('job-logs-dummy', 'children'),
    Input('job-logs-text', 'data'),
)


@dash_app.callback(
    Output({'type': 'manage-job-alert-div', 'index': MATCH}, 'children', allow_duplicate=True),
    Input({'type': 'retain-run-button', 'index': MATCH}, 'n_clicks'),
    State('session-store', 'data'), prevent_initial_call=True,
)
def retain_automatic_run(clicks, session):
    if not clicks or not is_state_authenticated(session):
        raise PreventUpdate
    from meerschaum.api.dash.execute import execution_request
    from urllib.parse import quote
    name = dash.callback_context.triggered_id['index']
    try:
        result = execution_request('/execute/runs/' + quote(name, safe='') + '/retain',
            get_username_from_session((session or {}).get('session-id')), method='POST')
        if result and result[0]:
            return [alert_from_success_tuple((True, 'Kept as a persistent Job.')),
                    html.A('Open Jobs', href='/dash/jobs')]
        return alert_from_success_tuple(tuple(result))
    except Exception as exc:
        return alert_from_success_tuple((False, str(exc)))
