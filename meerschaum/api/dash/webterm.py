#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the Webterm via the dashboard.
"""

import time

import meerschaum as mrsm
from meerschaum.api import CHECK_UPDATE, get_api_connector
from meerschaum.api.dash.sessions import is_session_authenticated, get_username_from_session
from meerschaum.api.dash.components import alert_from_success_tuple, console_div
from meerschaum.utils.typing import WebState, Tuple, Any
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum._internal.term.tools import is_webterm_running
from meerschaum.utils.threading import Thread, RLock
from meerschaum.utils.misc import is_tmux_available
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

MAX_WEBTERM_ATTEMPTS: int = 10
TMUX_IS_ENABLED: bool = (
    is_tmux_available() and mrsm.get_config('system', 'webterm', 'tmux', 'enabled')
)

_locks = {'webterm_thread': RLock()}

def get_webterm(state: WebState) -> Tuple[Any, Any]:
    """
    Start the webterm and return its iframe.
    """
    session_id = state['session-store.data'].get('session-id', None)
    username = get_username_from_session(session_id)
    if not is_session_authenticated(session_id):
        msg = f"User '{username}' is not authorized to access the webterm."
        return (
            html.Div(
                html.Pre(msg, id='console-pre'),
                id="console-div",
            ),
            [alert_from_success_tuple((
                False,
                "This Meerschaum instance only allows administrators to access the Webterm."
            ))]
        )

    for i in range(MAX_WEBTERM_ATTEMPTS):
        if is_webterm_running('localhost', 8765, session_id=(username or session_id)):
            return (
                [
                    html.Div(
                        [
                            dbc.Button(
                                "⟳",
                                color='black',
                                size='sm',
                                id='webterm-refresh-button',
                            ),
                            dbc.Button(
                                '⛶',
                                color='black',
                                size='sm',
                                id='webterm-fullscreen-button',
                            ),
                        ] + [
                            dbc.Button(
                                html.B('+'),
                                color='black',
                                size='sm',
                                id='webterm-new-tab-button',
                            ),
                        ] if TMUX_IS_ENABLED else [],
                        id='webterm-controls-div',
                        style={'text-align': 'right'},
                    ),
                    html.Iframe(
                        src=f"/webterm/{session_id}",
                        id="webterm-iframe",
                    ),
                ],
                []
            )
        time.sleep(1)
    return console_div, [alert_from_success_tuple((False, "Could not start the webterm server."))]


webterm_procs = {}
def start_webterm() -> None:
    """
    Start the webterm thread.
    """
    from meerschaum.utils.packages import run_python_package

    def run():
        conn = get_api_connector()
        _ = run_python_package(
            'meerschaum',
            ['start', 'webterm', '-i', str(conn)],
            capture_output=True,
            as_proc=True,
            store_proc_dict=webterm_procs,
            store_proc_key='process',
            venv=None,
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
