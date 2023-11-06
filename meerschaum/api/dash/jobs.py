#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with jobs via the web interface.
"""

from __future__ import annotations
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional, Dict, Any, Tuple, Union, WebState
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.users import is_session_authenticated
from meerschaum.api import CHECK_UPDATE
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(), import_dcc()
dateutil_parser = attempt_import('dateutil.parser', check_update=CHECK_UPDATE)
from meerschaum.utils.daemon import (
    get_daemons,
    get_running_daemons,
    get_paused_daemons,
    get_stopped_daemons,
    Daemon,
)
from meerschaum.config import get_config
from meerschaum.utils.misc import sorted_dict

STATUS_EMOJI: Dict[str, str] = {
    'running': get_config('formatting', 'emoji', 'running'),
    'paused': get_config('formatting', 'emoji', 'paused'),
    'stopped': get_config('formatting', 'emoji', 'stopped'),
    'dne': get_config('formatting', 'emoji', 'failure')
}

def get_jobs_cards(state: WebState):
    """
    Build cards and alerts lists for jobs.
    """
    daemons = get_daemons()
    session_id = state['session-store.data'].get('session-id', None)
    is_authenticated = is_session_authenticated(session_id)

    alert = alert_from_success_tuple(daemons)
    cards = []

    for d in daemons:
        footer_children = html.Div(
            build_process_timestamps_children(d),
            id = {'type': 'process-timestamps-div', 'index': d.daemon_id},
        )
        header_children = [
            html.Div(
                build_status_children(d),
                id = {'type': 'manage-job-status-div', 'index': d.daemon_id},
                style = {'float': 'left'},
            ),
            html.Div(
                dbc.Button(
                    'Download logs',
                    size = 'sm',
                    color = 'link',
                    id = {'type': 'job-download-logs-button', 'index': d.daemon_id},
                ),
                style = {'float': 'right'},
            ),
        ]

        body_children = [
            html.H4(html.B(d.daemon_id), className="card-title"),
            html.Div(
                html.P(
                    d.label,
                    className = "card-text job-card-text", 
                    style = {"word-wrap": "break-word"}
                ),
                style={"white-space": "pre-wrap"},
            ),
            html.Div(
                (
                    build_manage_job_buttons_div_children(d)
                    if is_authenticated
                    else []
                ),
                id={'type': 'manage-job-buttons-div', 'index': d.daemon_id}
            ),
            html.Div(id={'type': 'manage-job-alert-div', 'index': d.daemon_id}),
        ]

        cards.append(
            dbc.Card([
                dbc.CardHeader(header_children),
                dbc.CardBody(body_children),
                dbc.CardFooter(footer_children),
            ])
        )

    return cards, []


def build_manage_job_buttons_div_children(daemon: Daemon):
    """
    Return the children for the manage job buttons div.
    """
    buttons = build_manage_job_buttons(daemon)
    if not buttons:
        return []
    return [
        html.Br(),
        dbc.Row([
            dbc.Col(button, width=4)
            for button in buttons
        ])
    ]


def build_manage_job_buttons(daemon: Daemon):
    """
    Return the currently available job management buttons for a given Daemon.
    """
    if daemon is None:
        return []
    start_button = dbc.Button(
        'Start',
        size = 'sm',
        color = 'success',
        style = {'width': '100%'},
        id = {
            'type': 'manage-job-button',
            'action': 'start',
            'index': daemon.daemon_id,
        },
    )
    pause_button = dbc.Button(
        'Pause',
        size = 'sm',
        color = 'warning',
        style = {'width': '100%'},
        id = {
            'type': 'manage-job-button',
            'action': 'pause',
            'index': daemon.daemon_id,
        },
    )
    stop_button = dbc.Button(
        'Stop',
        size = 'sm',
        color = 'danger',
        style = {'width': '100%'},
        id = {
            'type': 'manage-job-button',
            'action': 'stop',
            'index': daemon.daemon_id,
        },
    )
    buttons = []
    if daemon.status in ('stopped', 'paused'):
        buttons.append(start_button)
    if daemon.status in ('running',):
        buttons.append(pause_button)
    if daemon.status in ('running', 'paused'):
        buttons.append(stop_button)

    return buttons


def build_status_children(daemon: Daemon) -> List[html.P]:
    """
    Return the status HTML component for this daemon.
    """
    if daemon is None:
        return STATUS_EMOJI['dne']

    status_str = (
        STATUS_EMOJI.get(daemon.status, STATUS_EMOJI['stopped'])
        + ' '
        + daemon.status.capitalize()
    )
    return html.P(
        html.B(status_str),
        className = f"{daemon.status}-job",
    )


def build_process_timestamps_children(daemon: Daemon) -> List[dbc.Row]:
    """
    Return the children to the process timestamps in the footer of the job card.
    """
    if daemon is None:
        return []
    children = []
    for timestamp_key, timestamp_val in sorted_dict(
        daemon.properties.get('process', {})
    ).items():
        timestamp = dateutil_parser.parse(timestamp_val)
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M UTC')
        children.append(
            dbc.Row(
                [
                    dbc.Col(
                        html.P(
                            timestamp_key.capitalize(),
                            style = {'font-size': 'small'},
                            className = 'text-muted mb-0',
                        ),
                        width = 4,
                    ),
                    dbc.Col(
                        html.P(
                            timestamp_str,
                            style = {'font-size': 'small', 'text-align': 'right'},
                            className = 'text-muted mb-0',
                        ),
                        width = 8,
                    ),
                ],
                justify = 'between', 
            )
        )
    return children
