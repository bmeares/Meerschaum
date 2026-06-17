#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with the Webterm via the dashboard.
"""

import time
from typing import Optional, Tuple, Any

import meerschaum as mrsm
from meerschaum.api import CHECK_UPDATE, get_api_connector, webterm_port
from meerschaum.api.dash.sessions import is_session_authenticated, get_username_from_session
from meerschaum.api.dash.components import alert_from_success_tuple, console_div
from meerschaum.utils.typing import WebState
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum._internal.term.tools import is_webterm_running
from meerschaum.utils.threading import Thread, RLock
from meerschaum.utils.misc import is_tmux_available
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

MAX_WEBTERM_ATTEMPTS: int = 10
TMUX_IS_ENABLED: bool = (
    is_tmux_available() and mrsm.get_config('api', 'webterm', 'tmux', 'enabled')
)

_locks = {'webterm_thread': RLock()}

### Termux-style extra keys for mobile: index -> (label, tooltip).
WEBTERM_EXTRA_KEYS = {
    'esc': ('ESC', 'Escape'),
    'ctrl': ('CTRL', 'Control (sticky — applies to next key)'),
    'shift': ('SHIFT', 'Shift (sticky — applies to next key)'),
    'tab': ('TAB', 'Tab'),
    'up': ('↑', 'Up arrow'),
    'down': ('↓', 'Down arrow'),
    'left': ('←', 'Left arrow'),
    'right': ('→', 'Right arrow'),
}


def _webterm_key_button(index: str, grid_area: Optional[str] = None) -> Any:
    """
    Return one Termux-style key button. `webterm-key-btn` lets the focus-preserving
    listener (assets/webterm_keys.js) keep the soft keyboard up when tapped.
    """
    label, title = WEBTERM_EXTRA_KEYS[index]
    style = {
        'width': '100%',
        'min-width': '0',
        'padding': '2px 0',
        'font-size': '0.7rem',
        'line-height': '1.1',
    }
    if grid_area:
        style['grid-area'] = grid_area
    return dbc.Button(
        label,
        id={'type': 'webterm-key-button', 'index': index},
        color='light',
        outline=True,
        size='sm',
        title=title,
        n_clicks=0,
        className='webterm-key-btn',
        style=style,
    )


def build_webterm_extra_keys_row() -> Any:
    """
    Return the mobile-only Termux key row, sitting right above the webterm: the
    modifier/whitespace keys (ESC, CTRL, SHIFT, TAB) on the left, and the arrow keys
    stacked like a real keyboard (↑ on top, ← ↓ → below) on the right, filling the
    space up to the terminal controls. Hidden on md+ (physical keyboard).
    """
    mod_keys = html.Div(
        [_webterm_key_button(index) for index in ('esc', 'ctrl', 'shift', 'tab')],
        style={
            'display': 'grid',
            'grid-template-columns': 'repeat(2, 3.6em)',
            'gap': '3px',
            'align-content': 'flex-end',
        },
    )
    arrow_keys = html.Div(
        [
            _webterm_key_button('up', grid_area='up'),
            _webterm_key_button('left', grid_area='left'),
            _webterm_key_button('down', grid_area='down'),
            _webterm_key_button('right', grid_area='right'),
        ],
        style={
            'display': 'grid',
            'grid-template-columns': 'repeat(3, 1.4em)',
            'grid-template-areas': '". up ." "left down right"',
            'gap': '3px',
        },
    )
    return html.Div(
        [mod_keys, arrow_keys],
        className='d-md-none',
        style={
            'display': 'flex',
            'justify-content': 'space-between',
            'align-items': 'flex-end',
            'gap': '8px',
            'margin-top': '8px',  ### buffer above the key row
        },
    )


def get_webterm(state: WebState) -> Tuple[Any, Any]:
    """
    Start the webterm and return its iframe.
    """
    from meerschaum.api import _include_webterm
    if not _include_webterm:
        return console_div, []

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
        if is_webterm_running('localhost', webterm_port, session_id=(username or session_id)):
            return (
                [
                    html.Div(
                        [
                            ### Terminal controls on the top-right.
                            html.Div(
                                [
                                    dbc.Button(
                                        "⟳",
                                        color='black',
                                        size='sm',
                                        id='webterm-refresh-button',
                                        title='Refresh terminal',
                                    ),
                                    dbc.Button(
                                        '⛶',
                                        color='black',
                                        size='sm',
                                        id='webterm-fullscreen-button',
                                        title='Toggle fullscreen',
                                    ),
                                ] + [
                                    dbc.Button(
                                        html.B('+'),
                                        color='black',
                                        size='sm',
                                        id='webterm-new-tab-button',
                                        title='New terminal tab',
                                    ),
                                ] if TMUX_IS_ENABLED else [],
                                id='webterm-controls-div',
                                style={'text-align': 'right'},
                            ),
                            ### Termux-style keys along the bottom, just above the
                            ### webterm (mobile only).
                            build_webterm_extra_keys_row(),
                        ],
                        style={'display': 'flex', 'flex-direction': 'column'},
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
def start_webterm(webterm_port: Optional[int] = None) -> None:
    """
    Start the webterm thread.
    """
    from meerschaum.utils.packages import run_python_package

    def run():
        conn = get_api_connector()
        _ = run_python_package(
            'meerschaum',
            (
                ['start', 'webterm', '-i', str(conn)]
                + (
                    []
                    if not webterm_port
                    else ['-p', str(webterm_port)]
                )
            ),
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
