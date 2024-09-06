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

def get_jobs_cards(state: WebState):
    """
    Build cards and alerts lists for jobs.
    """
    jobs = get_jobs(executor_keys=EXECUTOR_KEYS, include_hidden=False)
    session_id = state['session-store.data'].get('session-id', None)
    is_authenticated = is_session_authenticated(session_id)

    cards = []

    for name, job in jobs.items():
        footer_children = html.Div(
            build_process_timestamps_children(job),
            id = {'type': 'process-timestamps-div', 'index': name},
        )
        follow_logs_button = dbc.DropdownMenuItem(
            "Follow logs",
            id = {'type': 'follow-logs-button', 'index': name},
        )
        download_logs_button = dbc.DropdownMenuItem(
            "Download logs",
            id = {'type': 'job-download-logs-button', 'index': name},
        )
        logs_menu_children = (
            ([follow_logs_button] if is_authenticated else []) + [download_logs_button]
        )
        header_children = [
            html.Div(
                build_status_children(job),
                id={'type': 'manage-job-status-div', 'index': name},
                style={'float': 'left'},
            ),
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
        ]

        body_children = [
            html.H4(html.B(name), className="card-title"),
            html.Div(
                html.P(
                    job.label,
                    className="card-text job-card-text",
                    style={"word-wrap": "break-word"},
                    id={'type': 'job-label-p', 'index': name},
                ),
                style={"white-space": "pre-wrap"},
            ),
            html.Div(
                (
                    build_manage_job_buttons_div_children(job)
                    if is_authenticated
                    else []
                ),
                id={'type': 'manage-job-buttons-div', 'index': name},
            ),
            html.Div(id={'type': 'manage-job-alert-div', 'index': name}),
        ]

        cards.append(
            dbc.Card([
                dbc.CardHeader(header_children),
                dbc.CardBody(body_children),
                dbc.CardFooter(footer_children),
            ])
        )

    return cards, []


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
            'type': 'manage-job-button',
            'action': 'delete',
            'index': job.name,
        },
    )
    buttons = []
    status = job.status
    if status in ('stopped', 'paused'):
        buttons.append(start_button)
    if status == 'stopped':
        buttons.append(delete_button)
    if status in ('running',):
        buttons.append(pause_button)
    if status in ('running', 'paused'):
        buttons.append(stop_button)

    return buttons


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
