#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the callbacks for the tokens page.
"""

from typing import Optional, List

import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import dash.html as html
import dash.dcc as dcc

from meerschaum.api import get_api_connector, debug
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.tokens import (
    get_tokens_cards,
    build_tokens_register_input_modal,
    build_tokens_register_output_modal,
)
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.daemon import get_new_daemon_name
from meerschaum.core import Token


@dash_app.callback(
    Output('tokens-output-div', 'children'),
    Output('tokens-register-input-modal', 'children'),
    Output('tokens-alert-div', 'children'),
    Input('tokens-refresh-button', 'n_clicks'),
)
def refresh_tokens_button_click(n_clicks: Optional[int]):
    """
    Build the tokens cards on load or refresh.
    """
    tokens_cards, alerts = get_tokens_cards()
    if not tokens_cards:
        return (
            [
                html.H4('No tokens registered.'),
                html.P('Click the `+` button to register a new token.'),
            ],
            alerts
        )

    return build_cards_grid(tokens_cards), build_tokens_register_input_modal(), alerts


@dash_app.callback(
    Output('tokens-register-input-modal', 'is_open'),
    Output('tokens-name-input', 'value'),
    Input('tokens-create-button', 'n_clicks'),
    prevent_inital_call=True,
)
def create_tokens_button_click(n_clicks: Optional[int]):
    """
    Open the tokens registration modal when the plus button is clicked.
    """
    if not n_clicks:
        raise PreventUpdate

    return True, get_new_daemon_name()


@dash_app.callback(
    Output("tokens-scopes-checklist-div", 'style'),
    Input("tokens-toggle-scopes-switch", 'value'),
    prevent_inital_call=True,
)
def toggle_token_scopes_checklist(value: bool):
    """
    Toggle the scopes checklist.
    """
    return {'display': 'none'} if value else {}


@dash_app.callback(
    Output('tokens-scopes-checklist', 'value'),
    Output('tokens-deselect-scopes-button', 'children'),
    Input('tokens-deselect-scopes-button', 'n_clicks'),
    State('tokens-deselect-scopes-button', 'children'),
    prevent_inital_call=True,
)
def deselect_scopes_click(n_clicks: Optional[int], name: str):
    """
    Set the value of the scopes checklist to an empty list.
    """
    if not n_clicks:
        raise PreventUpdate
    new_name = 'Select all' if name == 'Deselect all' else 'Deselect all'
    value = (
        []
        if name == 'Deselect all'
        else list(STATIC_CONFIG['tokens']['scopes'])
    )

    return value, new_name


@dash_app.callback(
    Output('tokens-register-input-modal', 'is_open'),
    Output('tokens-register-output-modal', 'is_open'),
    Output('tokens-register-output-modal', 'children'),
    Input('tokens-register-button', 'n_clicks'),
    State('tokens-name-input', 'value'),
    State('tokens-scopes-checklist', 'value'),
    prevent_inital_call=True,
)
def register_token_click(n_clicks: Optional[int], name: str, scopes: List[str]):
    """
    Register the token.
    """
    if not n_clicks:
        raise PreventUpdate

    token = Token(
        label=(name or None),
    )
    return False, True, build_tokens_register_output_modal(token)


@dash_app.callback(
    Output("tokens-refresh-button", "n_clicks"),
    Input("tokens-register-output-modal", "is_open"),
    State("tokens-refresh-button", "n_clicks"),
)
def register_token_modal_close_refresh(is_open: bool, n_clicks: int):
    """
    Refresh the cards when the registration modal changes visibility.
    """
    if not is_open or not n_clicks:
        raise PreventUpdate
    return n_clicks + 1
