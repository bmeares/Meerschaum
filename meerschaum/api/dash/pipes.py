#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with pipes via the web interface.
"""

from __future__ import annotations
import shlex
from dash.dependencies import Input, Output, State
from meerschaum.utils.typing import List, Optional, Dict, Any, Tuple, Union
from meerschaum.utils.misc import string_to_dict
from meerschaum.utils.packages import attempt_import
from meerschaum.api.dash import (
    dash_app, debug, _get_pipes
)
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple
dbc = attempt_import('dash_bootstrap_components', lazy=False)

def keys_from_state(
        state : Dict[str, Any],
        with_params : bool = False
    ) -> Union[
        Tuple[List[str], List[str], List[str]],
        Tuple[List[str], List[str], List[str], str],
    ]:
    """
    Read the current state and return the selected keys lists.
    """
    _filters = {
        'ck' : state[f"connector-keys-{state['pipes-filter-tabs.active_tab']}.value"],
        'mk' : state[f"metric-keys-{state['pipes-filter-tabs.active_tab']}.value"],
        'lk' : state[f"location-keys-{state['pipes-filter-tabs.active_tab']}.value"],
    }
    if state['pipes-filter-tabs.active_tab'] == 'input':
        try:
            #  params = string_to_dict(state['params-textarea.value'])
            params = string_to_dict(state['search-parameters-editor.value'])
        except Exception as e:
            params = None
    else:
        params = None

    for k in _filters:
        _filters[k] = [] if _filters[k] is None else _filters[k]
        if not isinstance(_filters[k], list):
            try:
                _filters[k] = shlex.split(_filters[k])
            except Exception as e:
                print(e)
                _filters[k] = []
    keys = [_filters['ck'], _filters['mk'], _filters['lk']]
    if with_params:
        keys.append(params)
    return tuple(keys)

def pipes_from_state(
        state : Dict[str, Any],
        **kw
    ):
    """
    Return a pipes dictionary or list from get_pipes.
    """
    _ck, _mk, _lk, _params = keys_from_state(state, with_params=True)
    try:
        _pipes = _get_pipes(
            _ck, _mk, _lk,
            params = _params,
            mrsm_instance = get_web_connector(state), 
            **kw
        )
    except Exception as e:
        return False, str(e)
    return _pipes

def get_pipes_cards(*keys):
    """
    Returns a tuple:
        - pipes as a list of cards
        - alert list
    """
    _pipes = pipes_from_state(*keys, as_list=True)
    alert = alert_from_success_tuple(_pipes)
    if not isinstance(_pipes, list):
        _pipes = []
    return ([dbc.Card(str(p)) for p in _pipes], [alert])

