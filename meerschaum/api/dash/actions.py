#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute actions via the web interface.
"""

from __future__ import annotations
import platform, sys, io, os
from meerschaum.utils.typing import SuccessTuple, Tuple, Dict, Any
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.misc import remove_ansi
from meerschaum.actions import actions
from meerschaum.api import debug
from meerschaum.api.dash import web_state
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.pipes import pipes_from_state, keys_from_state
html = attempt_import('dash_html_components', warn=False)
#  capturer = attempt_import('capturer', lazy=False) if platform.system() != 'Windows' else None
capturer = None

def execute_action(state : Dict[str, Any]):
    """
    Execute a Meerschaum action and capture its output.
    Format the output as an HTML `pre` object, and return a list of Alert objects.
    """
    action, subaction, flags = (
        state['action-dropdown.value'],
        state['subaction-dropdown.value'],
        state['flags-dropdown.value'],
    )
    if action is None:
        return [], []
    if subaction is None:
        subaction = []
    if flags is None:
        flags = []

    ### TODO add direct Input to parse if active_tab is 'text'

    subactions = [subaction]

    keywords = {f : True for f in flags}
    keywords['debug'] = keywords.get('debug', debug)
    _ck, _mk, _lk, _params = keys_from_state(state, with_params=True)
    keywords['connector_keys'], keywords['metric_keys'], keywords['location_keys'] = (
        _ck, _mk, _lk,
    )
    keywords['params'] = _params
    keywords['mrsm_instance'] = web_state['web_instance_keys']

    def do_action() -> SuccessTuple:
        try:
            success_tuple = actions[action](action=subactions, **keywords)
        except Exception as e:
            success_tuple = False, str(e)
        except KeyboardInterrupt:
            success_tuple = False, f"Action '{action}' was manually cancelled."
        return success_tuple

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
        cap = io.StringIO()
        sys.stdout = cap
        success_tuple = do_action()
        sys.stdout = stdout
        os.environ['LINES'], os.environ['COLUMNS'] = LINES, COLUMNS
        return cap.getvalue(), success_tuple

    text, success_tuple = use_capture() if capturer is not None else use_stringio()

    return ([html.Pre(remove_ansi(text))], [alert_from_success_tuple(success_tuple)])
