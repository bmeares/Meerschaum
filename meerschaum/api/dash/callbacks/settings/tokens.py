#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the callbacks for the tokens page.
"""

from typing import Optional

import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import dash.html as html
import dash.dcc as dcc

from meerschaum.api import get_api_connector, debug
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.tokens import get_tokens_cards


@dash_app.callback(
    Output('tokens-output-div', 'children'),
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

    return build_cards_grid(tokens_cards), alerts


@dash_app.callback(
    Output('tokens-create-modal', 'is_open'),
    Input('tokens-create-button', 'n_clicks'),
    prevent_inital_call=True,
)
def create_tokens_button_click(n_clicks: Optional[int]):
    """
    Open the tokens registration modal when the plus button is clicked.
    """
    if not n_clicks:
        raise PreventUpdate

    return True


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
    Input('tokens-deselect-scopes-button', 'n_clicks'),
    prevent_inital_call=True,
)
def deselect_scopes_click(n_clicks: Optional[int]):
    """
    Set the value of the scopes checklist to an empty list.
    """
    if not n_clicks:
        raise PreventUpdate
    return []
