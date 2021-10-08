#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the registration page.
"""

from meerschaum.api import get_api_connector
from meerschaum.api.dash import dash_app, debug, active_sessions
from dash.dependencies import Input, Output, State, ALL, MATCH
from dash.exceptions import PreventUpdate
from meerschaum._internal.User import User
from meerschaum.config.static import _static_config

@dash_app.callback(
    [Output("username-input", "valid"), Output("username-input", "invalid")],
    [Input("username-input", "value")],
)
def validate_username(username):
    if not username:
        raise PreventUpdate
    valid = (len(username) >= _static_config()['users']['min_username_length'])
    if not valid:
        return valid, not valid
    conn = get_api_connector()
    user = User(username=username, instance=conn)
    user_id = conn.get_user_id(user, debug=debug)
    valid = (user_id is None)
    return valid, not valid

@dash_app.callback(
    [Output("password-input", "valid"), Output("password-input", "invalid")],
    [Input("password-input", "value")],
)
def validate_password(password):
    if not password:
        raise PreventUpdate
    valid = (len(password) >= _static_config()['users']['min_password_length'])
    return valid, not valid

@dash_app.callback(
    [Output("email-input", "valid"), Output("email-input", "invalid")],
    [Input("email-input", "value")],
)
def validate_email(email):
    print(f"EMAIL: {email}")
    if not email:
        raise PreventUpdate
    from meerschaum.utils.misc import is_valid_email
    valid = is_valid_email(email) is not None
    return valid, not valid
