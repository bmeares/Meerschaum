#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with jobs via the web interface.
"""

from __future__ import annotations
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional, Dict, Any, Tuple, Union
from meerschaum.utils.packages import attempt_import
from meerschaum.api.dash.components import alert_from_success_tuple
dbc = attempt_import('dash_bootstrap_components', lazy=False)
html = attempt_import('dash_html_components', warn=False)
dateutil_parser = attempt_import('dateutil.parser')
from meerschaum.utils.daemon import get_daemons, get_running_daemons, get_stopped_daemons

def get_jobs_cards(state):
    """
    Build cards for jobs.
    """
    daemons = get_daemons()
    running_daemons = get_running_daemons(daemons)
    stopped_daemons = get_stopped_daemons(daemons, running_daemons)
    alert = alert_from_success_tuple(daemons)
    return (
        dbc.CardColumns([
            dbc.Card([
                dbc.CardHeader(
                    html.P(
                        ("Running" if d.pid_path.exists() else "Stopped"),
                        className=("running-job" if d.pid_path.exists() else "stopped-job"),
                    ),
                ),
                dbc.CardBody(
                    [
                        html.H4(d.daemon_id, className="card-title"),
                        html.Div(
                            html.P(d.label, className="card-text", style={"word-wrap": "break-word"}),
                            style={"white-space": "pre-wrap"},
                        ),
                    ], style={"max-width": "100%", "overflow": "auto", "height": "auto"}
                ),
                dbc.CardFooter(
                    html.P(
                        (
                            ("Started at "
                                + dateutil_parser.parse(
                                    d.properties['process']['began']
                                ).strftime('%-H:%M on %b %-d') + " (UTC)") if d.pid_path.exists()
                            else ("Finished at "
                                + dateutil_parser.parse(
                                    d.properties['process']['ended']
                                ).strftime('%-H:%M on %b %-d') + " (UTC)"
                            )
                        )
                    ),
                ),
            ]) for d in running_daemons + stopped_daemons
        ]),
        [alert]
    )
