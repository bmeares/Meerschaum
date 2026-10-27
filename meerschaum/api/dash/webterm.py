#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The web terminal in the console: an iframe of the native terminal, `/webterm/<session id>`, which
the Rust server serves (xterm.js over a PTY; the API host never serves it).
"""

from typing import Optional, Tuple, Any

import meerschaum as mrsm
from meerschaum.api import CHECK_UPDATE
from meerschaum.api.dash.sessions import is_session_authenticated, get_username_from_session
from meerschaum.api.dash.components import alert_from_success_tuple, console_div, build_icon
from meerschaum.utils.typing import WebState
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import is_tmux_available
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

TMUX_IS_ENABLED: bool = (
    is_tmux_available() and mrsm.get_config('api', 'webterm', 'tmux', 'enabled')
)

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
            'align-items': 'flex-end',
            'gap': '12px',  ### a little space between the mod keys and the arrows
        },
    )


def webterm_enabled() -> bool:
    """
    Return whether the Rust server serves the terminal (the host config's `webterm`, which
    `--no-webterm` turns off).
    """
    from meerschaum.api.host import get_host_config
    return bool(get_host_config().get('webterm', False))


def build_webterm_iframe(session_id: str, id: str = 'webterm-iframe') -> Any:
    """
    Return the terminal's iframe for a session.
    """
    return html.Iframe(src=f"/webterm/{session_id}", id=id)


def get_webterm(state: WebState, fullscreen_button: bool = True) -> Tuple[Any, Any]:
    """
    Return the terminal's iframe (and its controls), or the console banner when there is none.
    The dashboard's controls include fullscreen (it takes the whole row), which has no meaning
    on the terminal's own page (`fullscreen_button=False`).
    """
    if not webterm_enabled():
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

    return (
        [
            html.Div(
                [
                    ### Termux-style keys on the left (mobile only): ESC/CTRL/
                    ### SHIFT/TAB then the arrow keys.
                    build_webterm_extra_keys_row(),
                    ### Terminal controls on the right, same row.
                    html.Div(
                        [
                            dbc.Button(
                                build_icon('refresh'),
                                color='black',
                                size='sm',
                                id='webterm-refresh-button',
                                title='Refresh terminal',
                            ),
                        ] + (
                            [
                                ### The icon follows the column's `data-fullscreen` (nav.css).
                                dbc.Button(
                                    [
                                        build_icon('fullscreen', 'webterm-icon-enter'),
                                        build_icon('fullscreen-exit', 'webterm-icon-exit'),
                                    ],
                                    color='black',
                                    size='sm',
                                    id='webterm-fullscreen-button',
                                    title='Fullscreen',
                                ),
                            ] if fullscreen_button else []
                        ) + (
                            [
                                dbc.Button(
                                    build_icon('plus'),
                                    color='black',
                                    size='sm',
                                    id='webterm-new-tab-button',
                                    title='New terminal tab',
                                ),
                            ] if TMUX_IS_ENABLED else []
                        ),
                        id='webterm-controls-div',
                        style={'margin-left': 'auto', 'text-align': 'right'},
                    ),
                ],
                style={
                    'display': 'flex',
                    'align-items': 'flex-end',
                    'gap': '8px',
                },
            ),
            build_webterm_iframe(session_id),
        ],
        []
    )
