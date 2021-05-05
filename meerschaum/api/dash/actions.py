#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute actions via the web interface.
"""

from __future__ import annotations
import platform, sys, io, os, shlex, time
from dash.exceptions import PreventUpdate
from meerschaum.utils.threading import Thread
from meerschaum.utils.typing import SuccessTuple, Tuple, Dict, Any, WebState
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import remove_ansi
from meerschaum.actions import actions
from meerschaum.api import debug
from meerschaum.api.dash import running_jobs, stopped_jobs, running_monitors, stopped_monitors
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.pipes import pipes_from_state, keys_from_state
from meerschaum.api.dash.websockets import ws_send
from meerschaum.api._websockets import websockets
html = attempt_import('dash_html_components', warn=False)
#  capturer = attempt_import('capturer', lazy=False) if platform.system() != 'Windows' else None
capturer = None

def execute_action(state : WebState):
    """
    Execute a Meerschaum action and capture its output.
    Format the output as an HTML `pre` object, and return a list of Alert objects.
    """
    global running_jobs, stopped_jobs, running_monitors, stopped_monitors
    action, subaction, subaction_hidden, additional_subaction_text, flags = (
        state['action-dropdown.value'],
        state['subaction-dropdown.value'],
        state['subaction-dropdown-div.hidden'],
        state['subaction-dropdown-text.value'],
        state['flags-dropdown.value'],
    )
    if action is None:
        return [], []
    if subaction is None or subaction_hidden:
        subaction = None
    if additional_subaction_text is None:
        additional_subaction_text = ''
    if flags is None:
        flags = []

    ### TODO add direct Input to parse if active_tab is 'text'

    session_id = state['session-store.data'].get('session-id', None)
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
        ### allow the process time to execute before sending messages.
        #  time.sleep(0.5)
        cap_buffer = ''
        #  while sys.stdout is cap:
        while True:
            if not (sys.stdout) is cap:
                sys.stdout = cap
                #  print('lost cap!')
            #  data = in_q.get()
            #  print(data, file=sys.stderr)
            #  if data is _sentinel:
                #  print('Stopping monitoring.', data, file=sys.stderr)
                #  in_q.put(data)
                #  break
            if session_id not in running_jobs:
                text = cap.getvalue()
                print('Stopping monitoring and sending final message.', file=sys.stderr)
                ws_send(text, session_id)
                #  success_dict = stopped_jobs[session_id].join()
                #  print('Received success_dict:', success_dict, file=sys.stderr)
                #  del stopped_jobs[session_id]
                break
            #  print('Continuing...', file=sys.stderr)
            text = cap.getvalue()
            #  if len(cap_buffer) == len(text):
            #  print('found text:', text, file=sys.stderr)
            if not text or len(text) == len(cap_buffer):
                continue
            cap_buffer = text
            #  print('len(text):', len(text), file=sys.stderr)
            #  text = str(time.time())
            ws_send(text, session_id)
            ### So we don't overwhelm the client.
            time.sleep(0.01)

    def use_capture() -> Tuple[str, SuccessTuple]:
        return use_stringio()
        #  cap = capturer.CaptureOutput()
        #  with capturer.CaptureOutput() as cap:
            #  success_tuple = do_action()
        #  return cap.get_text(), success_tuple

    def use_stringio():
        LINES, COLUMNS = (
            os.environ.get('LINES', str(os.get_terminal_size().lines)),
            os.environ.get('COLUMNS', str(os.get_terminal_size().columns)),
        )
        os.environ['LINES'], os.environ['COLUMNS'] = '120', '100'
        stdout = sys.stdout
        #  sys.stdout = cap

        #  success_tuple = use_thread()
        success_tuple = use_process()
        #  success_tuple = do_action()

        #  sys.stdout = stdout
        os.environ['LINES'], os.environ['COLUMNS'] = LINES, COLUMNS
        text = cap.getvalue()
        return text, success_tuple

    def use_process():
        from meerschaum.utils.packages import run_python_package
        from meerschaum.actions.arguments._parse_arguments import parse_dict_to_sysargs
        line_buffer = ''
        def send_line(line : str):
            nonlocal line_buffer
            line_buffer += line
            ws_send(line_buffer, session_id)
        def do_process():
            keywords['action'] = [action] + subactions
            _sysargs = parse_dict_to_sysargs(keywords)
            run_python_package(
                #  'meerschaum', [action] + subactions,
                'meerschaum', _sysargs,
                line_callback = send_line,
                env = {'LINES' : '120', 'COLUMNS' : '100'},
                foreground = False,
                universal_newlines = True,
                debug = debug,
            )
        action_thread = Thread(target=do_process)
        running_jobs[session_id] = action_thread
        action_thread.start()
        return True, "Success"

    def use_thread():
        action_thread = Thread(target=do_action)
        monitor_thread = Thread(target=monitor_and_send_stdout) 
        running_jobs[session_id] = action_thread
        running_monitors[session_id] = monitor_thread
        monitor_thread.start()
        #  time.sleep(0.5)
        action_thread.start()
        #  success_tuple = action_thread.join()
        #  print('Done executing. Success tuple:', success_tuple, file=sys.stderr)
        success_tuple = True, 'Success'
        #  monitor_thread.join()
        return success_tuple

    text, success_tuple = use_capture() if capturer is not None else use_stringio()

    #  raise PreventUpdate
    return (
        [
            html.Div(
                html.Pre(text, id='console-pre'),
                id = 'console-div'
            ),
        ],
        [alert_from_success_tuple(success_tuple)]
    )

def check_input_interval(state : WebState):
    """
    Regularly check if the executing action is blocking on input.
    """
    print('check for input')
    return (state['content-div-right.children'], state['success-alert-div.children'])
