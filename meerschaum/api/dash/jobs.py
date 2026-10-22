#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with jobs via the web interface.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Dict, WebState
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.api.dash.sessions import is_session_authenticated
from meerschaum.api import CHECK_UPDATE
from meerschaum.jobs import (
    get_jobs,
    get_executor_keys_from_context,
    Job,
)
from meerschaum.config import get_config
from meerschaum.utils.misc import sorted_dict
from dash.dependencies import Input, Output, State
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(), import_dcc()

STATUS_EMOJI: Dict[str, str] = {
    'running': get_config('formatting', 'emoji', 'running'),
    'paused': get_config('formatting', 'emoji', 'paused'),
    'stopped': get_config('formatting', 'emoji', 'stopped'),
    'dne': get_config('formatting', 'emoji', 'failure')
}

EXECUTOR_KEYS: str = get_executor_keys_from_context()


def is_automatic_job(job):
    """Explicit worker metadata takes precedence; support pre-rc6 execution workers."""
    try:
        properties = (job.daemon.properties if hasattr(job, 'daemon')
                      else job.get_daemon_properties())
        metadata = properties.get('mrsm_execution', {})
    except Exception:
        metadata = {}
    metadata = metadata if isinstance(metadata, dict) else {}
    kind = metadata.get('kind')
    return kind == 'temporary_execution' if kind else getattr(job, 'name', '').startswith('execute-')



def job_visible_to_session(job, session_id):
    """Automatic workers belong to their creator and administrators, including local reads."""
    if not is_automatic_job(job):
        return True
    from meerschaum.api.dash.sessions import session_is_admin, get_username_from_session
    from meerschaum.api import no_auth
    if no_auth or session_is_admin(session_id):
        return True
    properties = job.daemon.properties
    owner = properties.get('mrsm_owner')
    return bool(owner and owner == get_username_from_session(session_id))

def get_jobs_cards(state: WebState):
    """
    Build cards and alerts lists for jobs.
    """
    jobs = get_jobs(executor_keys=EXECUTOR_KEYS, include_hidden=False)
    session_id = state['session-store.data'].get('session-id', None)
    is_authenticated = is_session_authenticated(session_id)

    cards = []

    for name, job in jobs.items():
        if is_automatic_job(job):
            continue
        cards.append(build_job_card(job, authenticated=is_authenticated))

    return cards, []



def build_archived_run_card(record):
    """Show retained execution identity after its output expires."""
    import json
    from urllib.parse import urlencode
    metadata = record.get('metadata', {})
    execution = metadata.get('daemon', {}).get('properties', {}).get('mrsm_execution', {})
    execution = execution if isinstance(execution, dict) else {}
    pipeline = execution.get('pipeline') or {}
    pipeline_link = (html.A(pipeline.get('name', 'Pipeline'),
        href='/dash/pipelines?' + urlencode({'pipeline': pipeline.get('name', ''),
                                            'instance': pipeline.get('registry', '')}))
                     if isinstance(pipeline, dict) and pipeline.get('name') else None)
    return dbc.Card(dbc.CardBody([
        html.H5(record['name']), pipeline_link,
        html.P('Run: ' + str(execution.get('run_id', 'legacy'))),
        html.P('Completed: ' + str(record.get('ended', ''))),
        html.P('Output retention expired; execution summary retained.'),
        html.Details([html.Summary('Execution details'),
            html.Pre(json.dumps({'result': metadata.get('result'),
                                 'sysargs': metadata.get('sysargs'),
                                 'pipeline': pipeline}, indent=2))]),
    ]))

def build_job_card(
    job: Job,
    authenticated: bool = False,
    include_follow: bool = True,
):
    """
    Return a card for a given job.
    """
    footer_children = html.Div(
        build_process_timestamps_children(job),
        id={'type': 'process-timestamps-div', 'index': job.name},
    )
    follow_logs_button = dbc.DropdownMenuItem(
        "Follow logs",
        id={'type': 'follow-logs-button', 'index': job.name},
    )
    download_logs_button = dbc.DropdownMenuItem(
        "Download logs",
        id={'type': 'job-download-logs-button', 'index': job.name},
    )
    logs_menu_children = (
        ([follow_logs_button] if include_follow else []) + [download_logs_button]
        if authenticated
        else []
    )
    header_children = [
        html.Div(
            build_status_children(job),
            id={'type': 'manage-job-status-div', 'index': job.name},
            style={'float': 'left'},
        ),
    ] + ([
        html.Div(
            dbc.DropdownMenu(
                logs_menu_children,
                label="Logs",
                size="sm",
                align_end=True,
                color="secondary",
                menu_variant='dark',
            ),
            style={'float': 'right'},
        ),
    ] if authenticated else [])

    body_children = [
        html.H4(
            html.B(
                html.A(
                    "🔗 " + job.name,
                    href=f"/dash/jobs/{job.name}",
                    target="_blank",
                    style={
                        'color': 'white',
                        'text-decoration': 'none',
                    },
                )
            ),
            className="card-title",
        ),
        html.Div(
            html.P(
                job.label,
                className="card-text job-card-text",
                style={"word-wrap": "break-word"},
                id={'type': 'job-label-p', 'index': job.name},
            ),
            style={"white-space": "pre-wrap"},
        ),
        html.Div(
            (
                build_manage_job_buttons_div_children(job)
                if authenticated
                else []
            ),
            id={'type': 'manage-job-buttons-div', 'index': job.name},
        ),
        html.Div(id={'type': 'manage-job-alert-div', 'index': job.name}),
        build_delete_job_modal(job),
    ]

    return dbc.Card([
        dbc.CardHeader(header_children),
        dbc.CardBody(body_children),
        dbc.CardFooter(footer_children),
    ])


def build_manage_job_buttons_div_children(job: Job):
    """
    Return the children for the manage job buttons div.
    """
    buttons = build_manage_job_buttons(job)
    if not buttons:
        return []
    return [
        html.Br(),
        dbc.Row([
            dbc.Col(button, width=6)
            for button in buttons
        ])
    ]


def build_manage_job_buttons(job: Job):
    """
    Return the currently available job management buttons for a given Job.
    """
    if job is None:
        return []

    start_button = dbc.Button(
        'Start',
        size='sm',
        color='success',
        style={'width': '100%'},
        id={
            'type': 'manage-job-button',
            'action': 'start',
            'index': job.name,
        },
    )
    pause_button = dbc.Button(
        'Pause',
        size='sm',
        color='warning',
        style={'width': '100%'},
        id={
            'type': 'manage-job-button',
            'action': 'pause',
            'index': job.name,
        },
    )
    stop_button = dbc.Button(
        'Stop',
        size='sm',
        color='danger',
        style={'width': '100%'},
        id={
            'type': 'manage-job-button',
            'action': 'stop',
            'index': job.name,
        },
    )
    delete_button = dbc.Button(
        'Delete',
        size='sm',
        color='danger',
        style={'width': '100%'},
        id={
            'type': 'job-delete-button',
            'index': job.name,
        },
    )
    buttons = []
    if is_automatic_job(job) and job.status == 'stopped':
        buttons.append(dbc.Button('Keep as Job', size='sm', color='info',
            id={'type': 'retain-run-button', 'index': job.name}))
    status = job.status
    if status == 'paused' or (status == 'stopped' and not is_automatic_job(job)):
        buttons.append(start_button)
    if status == 'stopped':
        buttons.append(delete_button)
    if status in ('running',):
        buttons.append(pause_button)
    if status in ('running', 'paused'):
        buttons.append(stop_button)

    return buttons


def build_delete_job_modal(job: Job) -> dbc.Modal:
    """
    Return the confirmation modal for deleting a job.
    """
    return dbc.Modal(
        [
            dbc.ModalHeader(html.H4(["Delete job ", html.B(job.name), "?"])),
            dbc.ModalBody([
                html.Div(id={'type': 'job-delete-alert-div', 'index': job.name}),
                html.P(["Are you sure you want to delete job ", html.B(job.name), "?"]),
                html.P(html.B("This action cannot be undone!")),
            ]),
            dbc.ModalFooter([
                dbc.Button(
                    "Cancel",
                    color='secondary',
                    id={'type': 'job-delete-cancel-button', 'index': job.name},
                ),
                dbc.Button(
                    "Delete",
                    color='danger',
                    id={'type': 'job-delete-confirm-button', 'index': job.name},
                ),
            ]),
        ],
        is_open=False,
        id={'type': 'job-delete-modal', 'index': job.name},
    )


def build_status_children(job: Job) -> List[html.P]:
    """
    Return the status HTML component for this Job.
    """
    if job is None:
        return STATUS_EMOJI['dne']

    status_str = (
        STATUS_EMOJI.get(job.status, STATUS_EMOJI['stopped'])
        + ' '
        + job.status.capitalize()
    )
    return html.P(
        html.B(status_str),
        className=f"{job.status}-job",
    )


def build_process_timestamps_children(job: Job) -> List[dbc.Row]:
    """
    Return the children to the process timestamps in the footer of the job card.
    """
    if job is None:
        return []

    children = []
    for timestamp_key, timestamp in sorted_dict(
        {
            'began': job.began,
            'paused': job.paused,
            'ended': job.ended,
        }
    ).items():
        if timestamp is None:
            continue

        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M UTC')
        children.append(
            dbc.Row(
                [
                    dbc.Col(
                        html.P(
                            timestamp_key.capitalize(),
                            style={'font-size': 'small'},
                            className='text-muted mb-0',
                        ),
                        width=4,
                    ),
                    dbc.Col(
                        html.P(
                            timestamp_str,
                            style={'font-size': 'small', 'text-align': 'right'},
                            className='text-muted mb-0',
                        ),
                        width=8,
                    ),
                ],
                justify='between',
            )
        )
    return children


def get_run_cards(state):
    """Automatic execution workers and retained summaries in the Web Console."""
    session_id = (state.get('session-store.data') or {}).get('session-id')
    authenticated = is_session_authenticated(session_id)
    jobs = get_jobs(executor_keys='local', combine_local_and_systemd=True)
    cards = [build_job_card(job, authenticated=authenticated, include_follow=False)
             for job in jobs.values()
             if is_automatic_job(job) and job_visible_to_session(job, session_id)]
    if authenticated:
        from meerschaum.api.dash.execute import execution_request
        from meerschaum.api.dash.sessions import get_username_from_session
        records = execution_request('/execute/runs', get_username_from_session(session_id)) or []
        cards.extend(build_archived_run_card(record) for record in records)
    return cards, []
