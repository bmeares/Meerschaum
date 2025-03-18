#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the callbacks for the password reset page.
"""

import dash
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

from meerschaum.core.User import User
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.api import get_api_connector, debug
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.sessions import get_username_from_session


@dash_app.callback(
    Output('password-reset-alert-div', 'children'),
    Input('password-reset-button', 'n_clicks'),
    State('password-reset-input', 'value',),
    State('session-store', 'data'),
)
def password_reset_button_click(n_clicks, new_password_value, session_store_data):
    """
    Attempt the password reset with the form data.
    """
    if not n_clicks:
        raise PreventUpdate

    session_id = session_store_data.get('session-id', None)
    username = get_username_from_session(session_id)
    if not username:
        return alert_from_success_tuple(
            (False, "Invalid session. Are you logged in correctly?")
        )

    instance_connector = get_api_connector()
    user = User(username, new_password_value)
    success, msg = instance_connector.edit_user(user, debug=debug)
    return alert_from_success_tuple((success, msg))


@dash_app.callback(
    Output('password-reset-input', 'valid'),
    Output('password-reset-input', 'invalid'),
    Input('password-reset-input', 'value'),
)
def validate_new_password(new_password_value):
    if not new_password_value:
        raise PreventUpdate

    valid = (len(new_password_value) >= STATIC_CONFIG['users']['min_password_length'])
    return valid, not valid
 

@dash_app.callback(
    Output("password-reset-confirm-input", "valid"),
    Output("password-reset-confirm-input", "invalid"),
    Output("password-reset-button", 'disabled'),
    Input("password-reset-confirm-input", "value"),
    State("password-reset-input", "value"),
)
def validate_new_passwords_match(confirm_password_value, new_password_value):
    if not confirm_password_value:
        raise PreventUpdate

    new_password_is_valid, _ = validate_new_password(new_password_value)
    if not new_password_is_valid:
        raise PreventUpdate

    valid = confirm_password_value == new_password_value
    return valid, not valid, not valid
