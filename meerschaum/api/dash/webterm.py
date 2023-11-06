#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the Webterm via the dashboard.
"""

import time
from urllib.parse import urlparse
from meerschaum.config import get_config
from meerschaum.api import debug, CHECK_UPDATE, get_api_connector, no_auth
from meerschaum.api.dash import active_sessions
from meerschaum.api.dash.users import is_session_authenticated
from meerschaum.api.dash.components import alert_from_success_tuple, console_div
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple, Optional, Any
from meerschaum.utils.packages import attempt_import, import_html, import_dcc, run_python_package
from meerschaum._internal.term.tools import is_webterm_running
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.threading import Thread, RLock
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

MAX_WEBTERM_ATTEMPTS: int = 10

_locks = {'webterm_thread': RLock()}

def get_webterm(state: WebState) -> Tuple[Any, Any]:
    """
    Start the webterm and return its iframe.
    """
    session_id = state['session-store.data'].get('session-id', None)
    username = active_sessions.get(session_id, {}).get('username', None)
    if not is_session_authenticated(session_id):
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

    for i in range(MAX_WEBTERM_ATTEMPTS):
        if is_webterm_running('localhost', 8765):
            return (
                html.Iframe(
                    src = f"/webterm?s={session_id}",
                    id = "webterm-iframe",
                ),
                []
            )
        time.sleep(1)
    return console_div, [alert_from_success_tuple((False, "Could not start the webterm server."))]



webterm_procs = {}
def start_webterm() -> None:
    """
    Start the webterm thread.
    """
    from meerschaum._internal.entry import entry
    from meerschaum.utils.packages import run_python_package

    def run():
        _ = run_python_package(
            'meerschaum',
            ['start', 'webterm'],
            capture_output = True,
            as_proc = True,
            store_proc_dict = webterm_procs,
            store_proc_key = 'process',
            venv = None,
        )

    with _locks['webterm_thread']:
        if webterm_procs.get('thread', None) is None:
            webterm_thread = Thread(target=run)
            webterm_procs['thread'] = webterm_thread
            webterm_thread.start()


def stop_webterm() -> None:
    """
    Stop the webterm thread.
    """
    webterm_thread = webterm_procs.get('thread', None)
    webterm_proc = webterm_procs.get('process', None)
    with _locks['webterm_thread']:
        if webterm_proc is not None:
            webterm_proc.terminate()
        if webterm_thread is not None:
            webterm_thread.join()
