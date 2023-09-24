#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute actions via the web interface.
"""

from __future__ import annotations
import platform, sys, io, os, shlex, time, copy
from dash.exceptions import PreventUpdate
from meerschaum.utils.threading import Thread
from meerschaum.utils.typing import SuccessTuple, Tuple, Dict, Any, WebState
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import remove_ansi
from meerschaum.actions import actions, get_shell
from meerschaum.api import debug, CHECK_UPDATE
from meerschaum.api.dash import (
    running_jobs, stopped_jobs, running_monitors, stopped_monitors, active_sessions
)
from meerschaum.api import get_api_connector
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple, console_div
from meerschaum.api.dash.webterm import get_webterm
from meerschaum.api.dash.pipes import pipes_from_state, keys_from_state
from meerschaum.api.dash.websockets import ws_send
from meerschaum.api._websockets import websockets
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
from meerschaum.config import get_config
from meerschaum.core import User

def execute_action(state: WebState):
    """
    Execute a Meerschaum action and capture its output.
    Format the output as an HTML `pre` object, and return a list of Alert objects.

    Parameters
    ----------
    state: WebState :
        The state of the dashboard.

    Returns
    -------
    A tuple of lists.
    The first list contains the children of the output div.
    The second contains alerts.

    """
    global running_jobs, stopped_jobs, running_monitors, stopped_monitors
    action, subaction, subaction_options, subaction_hidden, additional_subaction_text, flags = (
        state['action-dropdown.value'],
        state['subaction-dropdown.value'],
        state['subaction-dropdown.options'],
        state['subaction-dropdown-div.hidden'],
        state['subaction-dropdown-text.value'],
        state['flags-dropdown.value'],
    )
    if action is None:
        return [], []
    if subaction is None or subaction_hidden:
        subaction = None
    ### If the current value is not a member of the options, choose the first element (visible).
    elif subaction not in [o['value'] for o in subaction_options]:
        subaction = subaction_options[0]['value']
    if additional_subaction_text is None:
        additional_subaction_text = ''
    if flags is None:
        flags = []

    ### TODO add direct Input to parse if active_tab is 'text'

    session_id = state['session-store.data'].get('session-id', None)

    ### Check if actions are permitted by non-admin users.
    permissions = get_config('system', 'api', 'permissions')
    allow_non_admin = permissions.get('actions', {}).get('non_admin', False)
    if not allow_non_admin:
        username = active_sessions.get(session_id, {}).get('username', None)
        user = User(username, instance=get_api_connector())
        user_type = get_api_connector().get_user_type(user, debug=debug)
        if user_type != 'admin':
            msg = (
                f"User '{username}' is not authorized to perform actions on this instance.\n\n"
                + "To allow non-administrator users to execute actions,\n"
                + "  run the command `mrsm edit config system`.\n\n"
                + "Under the keys `api:permissions:actions`, set the value of \n"
                + "  `non_admin` to `true`."
            )
            return (
                [
                    html.Div(
                        html.Pre(msg, id='console-pre'),
                        id = 'console-div'
                    ),
                ],
                [alert_from_success_tuple(
                    (False, "Actions are not allowed on this Meerschaum instance.")
                )]
            )

    
    subactions = ([subaction] if subaction else []) + shlex.split(additional_subaction_text)

    keywords = {f : True for f in flags}
    keywords['debug'] = keywords.get('debug', debug)
    _ck, _mk, _lk, _params = keys_from_state(state, with_params=True)
    keywords['connector_keys'], keywords['metric_keys'], keywords['location_keys'] = (
        _ck, _mk, _lk,
    )
    keywords['params'] = _params
    keywords['mrsm_instance'] = get_web_connector(state) 

    def do_action() -> SuccessTuple:
        if sys.stdout is not cap:
            stdout = sys.stdout
            sys.stdout = cap
        try:
            success_tuple = actions[action](action=subactions, **keywords)
        except Exception as e:
            success_tuple = False, str(e)
        except KeyboardInterrupt:
            success_tuple = False, f"Action '{action}' was manually cancelled."
        stopped_jobs[session_id] = running_jobs.pop(session_id)
        stopped_monitors[session_id] = running_monitors.pop(session_id)
        return success_tuple

    cap = io.StringIO()
    _sentinel = object()
    def monitor_and_send_stdout():
        cap_buffer = ''
        while True:
            if not (sys.stdout) is cap:
                sys.stdout = cap
            if session_id not in running_jobs:
                text = cap.getvalue()
                print('Stopping monitoring and sending final message.', file=sys.stderr)
                ws_send(text, session_id)
                break
            text = cap.getvalue()
            if not text or len(text) == len(cap_buffer):
                continue
            cap_buffer = text
            ws_send(text, session_id)
            ### So we don't overwhelm the client.
            time.sleep(0.01)

    def use_stringio():
        try:
            LINES, COLUMNS = (
                os.environ.get('LINES', str(os.get_terminal_size().lines)),
                os.environ.get('COLUMNS', str(os.get_terminal_size().columns)),
            )
        except OSError:
            LINES, COLUMNS = '120', '100'
        os.environ['LINES'], os.environ['COLUMNS'] = '120', '100'
        stdout = sys.stdout
        success_tuple = use_process()
        os.environ['LINES'], os.environ['COLUMNS'] = LINES, COLUMNS
        text = cap.getvalue()
        return text, success_tuple


    def use_process():
        from meerschaum.utils.packages import run_python_package
        from meerschaum._internal.arguments._parse_arguments import parse_dict_to_sysargs
        max_buffer_size = 10000
        line_buffer = ''
        def send_line(line: str):
            nonlocal line_buffer
            line_buffer += line
            buff_start_idx = max(len(line_buffer) - max_buffer_size, 0)
            line_buffer = line_buffer[buff_start_idx:]
            ws_send(line_buffer, session_id)

        def do_process():
            keywords['action'] = [action] + subactions
            _sysargs = parse_dict_to_sysargs(keywords)

            ### Don't forget to pass the existing environment
            ### in case MRSM_CONFIG and MRSM_PATCH are set.
            try:
                _env = copy.deepcopy(os.environ)
            except Exception as e:
                _env = {}
            _env.update({'LINES': '120', 'COLUMNS': '100'})

            process = run_python_package(
                'meerschaum', _sysargs,
                line_callback = send_line,
                env = _env,
                foreground = False,
                universal_newlines = True,
                as_proc = True,
                capture_output = False,
                ### Store the `subprocess.Popen` process in case we need to terminate it later.
                store_proc_dict = running_jobs[session_id],
                venv = None,
                debug = debug,
            )
        action_thread = Thread(target=do_process)
        running_jobs[session_id] = {'parent_thread': action_thread,}
        action_thread.start()
        return True, "Success"


    def use_thread():
        action_thread = Thread(target=do_action, daemon=True)
        monitor_thread = Thread(target=monitor_and_send_stdout) 
        running_jobs[session_id] = action_thread
        running_monitors[session_id] = monitor_thread
        monitor_thread.start()
        action_thread.start()
        success_tuple = True, 'Success'
        return success_tuple

    text, success_tuple = use_stringio()

    return (
        [
            html.Div(
                html.Pre(text, id='console-pre'),
                id = 'console-div'
            ),
        ],
        [alert_from_success_tuple(success_tuple)]
    )


def stop_action(state: WebState):
    """
    Kill a running action.

    Parameters
    ----------
    state: WebState:
        The state of the dashboard.

    Returns
    -------
    A tuple of children and alerts.
    """
    global running_jobs
    session_id = state['session-store.data'].get('session-id', None)
    thread = running_jobs.get(session_id, {}).get('parent_thread', None)
    proc = running_jobs.get(session_id, {}).get('child_process', None)
    if proc is not None and proc.poll() is None:
        proc.terminate()
        thread.join()
    success_tuple = True, "Success"
    webterm_iframe, webterm_alerts = get_webterm(state)
    console_to_return = console_div if webterm_alerts else webterm_iframe
    return (
        [console_to_return],
        [alert_from_success_tuple(success_tuple)]
    )
