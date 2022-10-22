#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the login page.
"""

from __future__ import annotations
from meerschaum.api import endpoints, no_auth, CHECK_UPDATE
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
import uuid
from meerschaum.utils.typing import Optional
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State
from meerschaum.api.dash import dash_app, debug, pipes, _get_pipes, active_sessions
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.routes._login import login
from fastapi_login.exceptions import InvalidCredentialsException
from fastapi.exceptions import HTTPException
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)

@dash_app.callback(
    Output('user-registration-disabled-collapse', 'is_open'),
    Input('show-user-registration-disabled-button', 'n_clicks'),
    State('user-registration-disabled-collapse', 'is_open'),
)
def show_registration_disabled_collapse(n_clicks, is_open):
    """
    Toggle the registration info div.
    """
    if n_clicks:
        return not is_open
    return is_open

@dash_app.callback(
    Output('session-store', 'data'),
    Output('username-input', 'className'),
    Output('location', 'pathname'),
    Input('username-input', 'n_submit'),
    Input('password-input', 'n_submit'),
    Input('login-button', 'n_clicks'),
    State('username-input', 'value'),
    State('password-input', 'value'),
)
def login_button_click(
        username_submit,
        password_submit,
        n_clicks,
        username,
        password,
    ):
    """
    When the user submits the login form, check the login.
    On successful login, set the session id.
    """
    form_class = 'form-control'
    ctx = dash.callback_context
    if not username or not password or not ctx.triggered:
        return {}, form_class, dash.no_update
    try:
        token_dict = login({'username' : username, 'password' : password})
        session_data = {'session-id': str(uuid.uuid4())}
        active_sessions[session_data['session-id']] = {'username': username}
    except HTTPException:
        form_class += ' is-invalid'
        session_data = None
    return session_data, form_class, (dash.no_update if not session_data else endpoints['dash'])
