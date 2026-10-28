#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the registration page (`/dash/register`).

They are public (a visitor registering has no session) and check for themselves whether an
account may be created now (`meerschaum.api.dash.registration.registration_allowed`): on an
instance without users (the first account is an administrator), or with
`api:permissions:registration:users` on. Otherwise they do nothing, not even a username lookup.
"""

from meerschaum.api import CHECK_UPDATE, endpoints
from meerschaum.api.dash import dash_app, debug
from meerschaum.api.dash.sessions import public_callback
from meerschaum.api.dash.components import alert_from_success_tuple
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from meerschaum.core import User
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.packages import attempt_import
dash = attempt_import('dash', check_update=CHECK_UPDATE)
from meerschaum.api.dash._support import authenticate_user


@public_callback
@dash_app.callback(
    [Output("register-username-input", "valid"), Output("register-username-input", "invalid")],
    [Input("register-username-input", "value")],
)
def validate_username(username):
    from meerschaum.api import get_api_connector
    from meerschaum.api.dash.registration import registration_allowed
    if not username:
        raise PreventUpdate
    valid = (len(username) >= STATIC_CONFIG['users']['min_username_length'])
    if not valid:
        return valid, not valid
    conn = get_api_connector()
    ### Nobody may register: say nothing about which usernames exist.
    if not registration_allowed(conn):
        raise PreventUpdate
    user = User(username=username, instance=conn)
    user_id = conn.get_user_id(user, debug=debug)
    valid = (user_id is None)
    return valid, not valid


@public_callback
@dash_app.callback(
    [Output("register-password-input", "valid"), Output("register-password-input", "invalid")],
    [Input("register-password-input", "value")],
)
def validate_password(password):
    if not password:
        raise PreventUpdate
    valid = (len(password) >= STATIC_CONFIG['users']['min_password_length'])
    return valid, not valid


@public_callback
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


@public_callback
@dash_app.callback(
    Output('session-store', 'data'),
    Output('register-username-input', 'className'),
    Output('mrsm-location', 'pathname'),
    Output('register-alert-div', 'children'),
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
    """
    Register the account (an administrator on an instance without users, else with the default
    role, if self-registration is on) and open the console signed in.
    """
    if not n_clicks:
        raise PreventUpdate
    from meerschaum.api.dash.registration import register_account, sign_in
    form_class = 'form-control'
    if not username or not password:
        return {}, form_class + ' is-invalid', dash.no_update, alert_from_success_tuple(
            (False, "Enter a username and a password.")
        )

    (success, message), _ = register_account(username, password, email=email)
    if not success:
        return {}, form_class + ' is-invalid', dash.no_update, alert_from_success_tuple(
            (False, message)
        )
    username = str(username).strip()
    if not authenticate_user(username, password):
        return {}, form_class + ' is-invalid', dash.no_update, alert_from_success_tuple(
            (False, "Sign in with the new account.")
        )
    from flask import request as flask_request
    session_data = sign_in(
        username,
        response=dash.callback_context.response,
        secure=flask_request.is_secure,
    )
    return session_data, form_class, endpoints['dash'], []
