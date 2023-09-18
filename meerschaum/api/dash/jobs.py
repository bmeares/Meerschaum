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
from meerschaum.api import CHECK_UPDATE
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(), import_dcc()
dateutil_parser = attempt_import('dateutil.parser', check_update=CHECK_UPDATE)
from meerschaum.utils.daemon import (
    get_daemons,
    get_running_daemons,
    get_paused_daemons,
    get_stopped_daemons,
)

def get_jobs_cards(state: WebState):
    """
    Build cards and alerts lists for jobs.
    """
    daemons = get_daemons()
    running_daemons = get_running_daemons(daemons)
    paused_daemons = get_paused_daemons(daemons)
    stopped_daemons = get_stopped_daemons(daemons)
    alert = alert_from_success_tuple(daemons)
    cards = []

    for d in running_daemons:
        _footer = (
            html.P(
                "Started at " + dateutil_parser.parse(
                    d.properties['process']['began']).strftime('%-H:%M on %b %-d')
                + " (UTC)"
            ) if (d.pid_path.exists() and d.properties.get('process', {}).get('began', None))
            else html.P("No information available.")
        )
        cards.append(
            dbc.Card([
                dbc.CardHeader(html.P("Running", className="running-job")),
                dbc.CardBody(
                    [
                        html.H4(d.daemon_id, className="card-title"),
                        html.Div(
                            html.P(
                                d.label,
                                className = "card-text job-card-text", 
                                style = {"word-wrap": "break-word"}
                            ),
                            style={"white-space": "pre-wrap"},
                        ),
                    ], style={"max-width": "100%", "overflow": "auto", "height": "auto"}
                ),
                dbc.CardFooter(
                    _footer
                ),
            ])
        )

    for d in paused_daemons:
        _footer = (
            html.P(
                "Started at " + dateutil_parser.parse(
                    d.properties['process']['began']).strftime('%-H:%M on %b %-d')
                + " (UTC)"
            ) if (d.pid_path.exists() and d.properties.get('process', {}).get('began', None))
            else html.P("No information available.")
        )
        cards.append(
            dbc.Card([
                dbc.CardHeader(html.P("Paused", className="paused-job")),
                dbc.CardBody(
                    [
                        html.H4(d.daemon_id, className="card-title"),
                        html.Div(
                            html.P(
                                d.label,
                                className = "card-text job-card-text",
                                style = {"word-wrap": "break-word"}
                            ),
                            style={"white-space": "pre-wrap"},
                        ),
                    ], style={"max-width": "100%", "overflow": "auto", "height": "auto"}
                ),
                dbc.CardFooter(
                    _footer
                ),
            ])
        )


    for d in stopped_daemons:
        _header = dbc.CardHeader(
            dbc.Row([
                dbc.Col(html.P("Stopped", className='stopped-job')),
            ])
        )
        _footer = (
            html.P(
                "Finished at " + dateutil_parser.parse(
                    d.properties['process']['ended']).strftime('%-H:%M on %b %-d')
                + " (UTC)"
            ) if (not d.pid_path.exists() and d.properties.get('process', {}).get('ended', None))
            else html.P("No information available.")
        )
        cards.append(
            dbc.Card([
                _header,
                dbc.CardBody(
                    [
                        html.H4(d.daemon_id, className="card-title"),
                        html.Div(
                            html.P(
                                d.label,
                                className="card-text job-card-text",
                                style={"word-wrap": "break-word"}
                            ),
                            style={"white-space": "pre-wrap"},
                        ),
                    ], style={"max-width": "100%", "overflow": "auto", "height": "auto"}
                ),
                dbc.CardFooter(
                    _footer
                ),
            ])
        )

    return cards, []

