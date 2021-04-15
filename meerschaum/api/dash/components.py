#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Custom components are defined here.
"""

from __future__ import annotations
from dash.dependencies import Input, Output, State
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.typing import SuccessTuple
from meerschaum.config.static import _static_config
from meerschaum.utils.misc import remove_ansi
dbc = attempt_import('dash_bootstrap_components', lazy=False)
dcc = attempt_import('dash_core_components', warn=False)
dash_ace = attempt_import('dash_ace', lazy=False)
html = attempt_import('dash_html_components', warn=False)

component_ids = {

}

go_button = dbc.Button('Go', id='go-button', color='primary')
show_pipes_button = dbc.Button('Get Pipes', id='show-pipes-button', color='secondary')

search_parameters_editor = dash_ace.DashAceEditor(
    id = 'search-parameters-editor',
    theme = 'monokai',
    mode = 'json',
    tabSize = 2,
    placeholder = '',
)

def alert_from_success_tuple(success : SuccessTuple):
    """
    Return an Alert from a SuccessTuple.
    """
    return dbc.Alert('', is_open=False) if not isinstance(success, tuple) else (
        dbc.Alert(
            remove_ansi(success[1]),
            id = 'success-alert',
            dismissable = True,
            fade = True,
            is_open = not (success[1] in _static_config()['system']['success']['ignore']),
            color = 'success' if success[0] else 'danger',
        )
    )

