#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the Webterm via the dashboard.
"""

import time
from meerschaum.config import get_config
from meerschaum.api import debug, CHECK_UPDATE, get_api_connector
from meerschaum.api.dash import active_sessions
from meerschaum.api.dash.components import alert_from_success_tuple, console_div
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple, Optional
from meerschaum.utils.packages import attempt_import, import_html, import_dcc, run_python_package
from meerschaum._internal.term.tools import is_webterm_running
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.core import User
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

MAX_WEBTERM_ATTEMPTS: int = 10

def get_webterm(state: WebState) -> Tuple[List[dbc.Card], List[SuccessTuple]]:
    """
    Start the webterm and return its iframe.
    """
    permissions = get_config('system', 'api', 'permissions')
    allow_non_admin = permissions.get('actions', {}).get('non_admin', False)
    if not allow_non_admin:
        session_id = state['session-store.data'].get('session-id', None)
        username = active_sessions.get(session_id, {}).get('username', None)
        user = User(username, instance=get_api_connector())
        user_type = get_api_connector().get_user_type(user, debug=debug)
        if user_type != 'admin':
            msg = f"User '{username}' is not authorized to access the webterm."
            return (
                html.Div(
                    html.Pre(msg, id='console-pre'),
                    id = "console-div",
                ),
                [alert_from_success_tuple((
                    False,
                    "This Meerschaum instance only allows administrators to access the Webterm."
                ))]
            )


    protocol, host, port = 'http', '0.0.0.0', 8765
    if not is_webterm_running(host, port, protocol):
        run_python_package(
            'meerschaum', [
                'start', 'webterm', '-d', '--noask',
                '--name', STATIC_CONFIG['api']['webterm_job_name'],
            ],
            foreground = False,
            venv = None,
        )

    webterm_iframe = html.Iframe(
        src = f'{protocol}://{host}:{port}',
        id = "webterm-iframe",
    )
    for i in range(MAX_WEBTERM_ATTEMPTS):
        if is_webterm_running(host, port, protocol):
            return webterm_iframe, []
        time.sleep(1)
    return console_div, [alert_from_success_tuple((False, "Could not start the webterm server."))]
