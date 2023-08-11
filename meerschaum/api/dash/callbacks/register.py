#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the registration page.
"""

import uuid
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
from meerschaum.api.dash import dash_app, debug, active_sessions
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash.exceptions import PreventUpdate
from meerschaum.core import User
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.packages import attempt_import
dash = attempt_import('dash', check_update=CHECK_UPDATE)
from fastapi.exceptions import HTTPException

@dash_app.callback(
    [Output("register-username-input", "valid"), Output("register-username-input", "invalid")],
    [Input("register-username-input", "value")],
)
def validate_username(username):
    if not username:
        raise PreventUpdate
    valid = (len(username) >= STATIC_CONFIG['users']['min_username_length'])
    if not valid:
        return valid, not valid
    conn = get_api_connector()
    user = User(username=username, instance=conn)
    user_id = conn.get_user_id(user, debug=debug)
    valid = (user_id is None)
    return valid, not valid

@dash_app.callback(
    [Output("register-password-input", "valid"), Output("register-password-input", "invalid")],
    [Input("register-password-input", "value")],
)
def validate_password(password):
    if not password:
        raise PreventUpdate
    valid = (len(password) >= STATIC_CONFIG['users']['min_password_length'])
    return valid, not valid

@dash_app.callback(
    [Output("register-email-input", "valid"), Output("register-email-input", "invalid")],
    [Input("register-email-input", "value")],
)
def validate_email(email):
    if not email:
        raise PreventUpdate
    from meerschaum.utils.misc import is_valid_email
    valid = is_valid_email(email) is not None
    return valid, not valid

@dash_app.callback(
    Output('session-store', 'data'),
    Output('register-username-input', 'className'),
    Output('location', 'pathname'),
    Input('register-username-input', 'n_submit'),
    Input('register-password-input', 'n_submit'),
    Input('register-button', 'n_clicks'),
    State("register-username-input", "value"),
    State("register-password-input", "value"),
    State("register-email-input", "value"),
)
def register_button_click(
        username_submit,
        password_submit,
        n_clicks,
        username,
        password,
        email,
    ):
    if not n_clicks:
        raise PreventUpdate
    form_class = 'form-control'
    from meerschaum.api.routes._login import login
    conn = get_api_connector()
    if not username or not password:
        success, msg = False, "Invalid username or password."
        form_class += ' is-invalid'
        return {}, form_class, dash.no_update

    user = User(username, password, email=email, instance=conn)
    user_id = conn.get_user_id(user, debug=debug)
    if user_id is not None:
        form_class += ' is-invalid'
        return {}, form_class, dash.no_update
    success, msg = conn.register_user(user, debug=debug)
    if not success:
        form_class += ' is-invalid'
        return {}, form_class, dash.no_update
    try:
        token_dict = login({'username': username, 'password': password})
        session_data = {'session-id': str(uuid.uuid4())}
        active_sessions[session_data['session-id']] = {'username': username}
    except HTTPException as e:
        form_class += ' is-invalid'
        session_data = None
    return session_data, form_class, (dash.no_update if not session_data else endpoints['dash'])
