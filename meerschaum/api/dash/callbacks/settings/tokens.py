#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the callbacks for the tokens page.
"""

import json
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone

import dash
from dash.dependencies import Input, Output, State, MATCH, ALL
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import dash.html as html
import dash.dcc as dcc

import meerschaum as mrsm
from meerschaum.api import get_api_connector, debug
from meerschaum.api.dash import dash_app
from meerschaum.api.dash.sessions import get_user_from_session
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from meerschaum.api.dash.tokens import (
    get_tokens_cards,
    get_tokens_table,
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
    State('session-store', 'data'),
)
def refresh_tokens_button_click(
    n_clicks: Optional[int],
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    Build the tokens cards on load or refresh.
    """
    session_id = (session_data or {}).get('session-id', None)
    tokens_table, alerts = get_tokens_table(session_id)
    if not tokens_table:
        return (
            [
                html.H4('No tokens registered.'),
                html.P('Click the `+` button to register a new token.'),
            ],
            alerts
        )

    return tokens_table, build_tokens_register_input_modal(), alerts


@dash_app.callback(
    Output('tokens-register-input-modal', 'is_open'),
    Input('tokens-create-button', 'n_clicks'),
    prevent_initial_call=True,
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
    prevent_initial_call=True,
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
    prevent_initial_call=True,
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
    Output({'type': 'tokens-scopes-checklist', 'index': MATCH}, 'value'),
    Output({'type': 'tokens-deselect-scopes-button', 'index': MATCH}, 'children'),
    Input({'type': 'tokens-deselect-scopes-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'tokens-deselect-scopes-button', 'index': MATCH}, 'children'),
    prevent_initial_call=True,
)
def edit_token_deselect_scopes_click(n_clicks: Optional[int], name: str):
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
    State('tokens-expiration-datepickersingle', 'date'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def register_token_click(
    n_clicks: Optional[int],
    name: str,
    scopes: List[str],
    expiration: Optional[datetime] = None,
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    Register the token.
    """
    if not n_clicks:
        raise PreventUpdate

    session_id = (session_data or {}).get('session-id', None)
    token = Token(
        label=(name or None),
        user=get_user_from_session(session_id),
        expiration=(datetime.fromisoformat(f"{expiration}T00:00:00Z") if expiration is not None else None),
    )
    return False, True, build_tokens_register_output_modal(token)


@dash_app.callback(
    Output("tokens-refresh-button", "n_clicks"),
    Input("tokens-register-output-modal", "is_open"),
    Input({'type': 'tokens-edit-modal', 'index': ALL}, 'is_open'),
    Input({'type': 'tokens-invalidate-modal', 'index': ALL}, 'is_open'),
    Input({'type': 'tokens-delete-modal', 'index': ALL}, 'is_open'),
    State("tokens-refresh-button", "n_clicks"),
    prevent_initial_call=True,
)
def register_token_modal_close_refresh(
    register_is_open: bool,
    edit_is_open_list,
    invalidate_is_open_list,
    delete_is_open_list,
    n_clicks: int,
):
    """
    Refresh the cards when the registration, edit, invalidate, or delete modals changes visibility.
    """
    if any(
        edit_is_open_list
        + invalidate_is_open_list
        + delete_is_open_list
    ):
        raise PreventUpdate

    return (n_clicks or 0) + 1


@dash_app.callback(
    Output('tokens-register-clipboard', 'content'),
    Output('tokens-register-clipboard', 'n_clicks'),
    Output('tokens-register-copy-button', 'children'),
    Input('tokens-register-copy-button', 'n_clicks'),
    State('tokens-register-clipboard', 'n_clicks'),
    State('token-id-pre', 'children'),
    State('token-secret-pre', 'children'),
    prevent_initial_call=True,
)
def copy_token_button_click(
    n_clicks: int,
    clipboard_n_clicks: Optional[int],
    token_id: str,
    token_secret: str,
) -> Tuple[str, int, str]:
    """
    Copy the token's ID and secret to the clipboard.
    """
    if not n_clicks:
        raise PreventUpdate
    return (
        f"Client ID: {token_id}\nClient Secret: {token_secret}",
        (clipboard_n_clicks or 0) + 1,
        "Copied!",
    )


@dash_app.callback(
    Output('tokens-close-register-output-modal-button', 'disabled'),
    Output('tokens-register-output-modal', 'backdrop'),
    Input('tokens-register-clipboard', 'n_clicks'),
    prevent_initial_call=True,
)
def enable_close_button(n_clicks):
    """
    Enable the close button once the token has been copied.
    """
    if not n_clicks:
        raise PreventUpdate
    return False, True


@dash_app.callback(
    Output('tokens-register-output-modal', 'is_open'),
    Input('tokens-close-register-output-modal-button', 'n_clicks'),
    prevent_initial_call=True,
)
def close_register_output_modal(n_clicks: int) -> bool:
    """
    Close the register output modal when the Close button is clicked.
    """
    if not n_clicks:
        raise PreventUpdate
    return False


@dash_app.callback(
    Output({'type': 'tokens-edit-modal', 'index': MATCH}, 'is_open'),
    Input({'type': 'tokens-edit-button', 'index': MATCH}, 'n_clicks'),
    prevent_initial_call=True,
)
def edit_token_button_click(n_clicks: int):
    if not n_clicks:
        raise PreventUpdate
    return True


@dash_app.callback(
    Output({'type': 'tokens-edit-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'tokens-edit-alerts-div', 'index': MATCH}, 'children'),
    Input({'type': 'tokens-edit-submit-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'tokens-expiration-datepickersingle', 'index': MATCH}, 'date'),
    State({'type': 'tokens-scopes-checklist', 'index': MATCH}, 'value'),
    State({'type': 'tokens-name-input', 'index': MATCH}, 'value'),
    prevent_initial_call=True,
)
def edit_token_submit_button_click(
    n_clicks: int,
    expiration: Optional[datetime],
    scopes: List[str],
    label: str,
):
    if not n_clicks:
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate

    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    token_id = component_dict['index']

    token = Token(
        id=token_id,
        label=label,
        expiration=(datetime.fromisoformat(f"{expiration}T00:00:00Z") if expiration is not None else None),
        scopes=scopes,
        instance=get_api_connector(),
    )

    success, msg = token.edit(debug=debug)
    if not success:
        return dash.no_update, alert_from_success_tuple((success, msg))

    return False, dash.no_update


@dash_app.callback(
    Output({'type': 'tokens-invalidate-modal', 'index': MATCH}, 'is_open'),
    Input({'type': 'tokens-invalidate-button', 'index': MATCH}, 'n_clicks'),
    prevent_initial_call=True,
)
def invalidate_token_click(n_clicks: int):
    if not n_clicks:
        raise PreventUpdate
    return True


@dash_app.callback(
    Output({'type': 'tokens-delete-modal', 'index': MATCH}, 'is_open'),
    Input({'type': 'tokens-delete-button', 'index': MATCH}, 'n_clicks'),
    prevent_initial_call=True,
)
def invalidate_token_click(n_clicks: int):
    if not n_clicks:
        raise PreventUpdate
    return True



@dash_app.callback(
    Output({'type': 'tokens-edit-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'tokens-invalidate-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'tokens-invalidate-alerts-div', 'index': MATCH}, 'children'),
    Input({'type': 'tokens-invalidate-confirm-button', 'index': MATCH}, 'n_clicks'),
    prevent_initial_call=True,
)
def invalidate_token_confirm_click(n_clicks: int):
    if not n_clicks:
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate

    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    token_id = component_dict['index']

    token = Token(
        id=token_id,
        instance=get_api_connector(),
    )

    success, msg = token.invalidate(debug=debug)
    if not success:
        return dash.no_update, dash.no_update, alert_from_success_tuple((success, msg))

    return False, False, dash.no_update


@dash_app.callback(
    Output({'type': 'tokens-edit-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'tokens-delete-modal', 'index': MATCH}, 'is_open'),
    Output({'type': 'tokens-delete-alerts-div', 'index': MATCH}, 'children'),
    Input({'type': 'tokens-delete-confirm-button', 'index': MATCH}, 'n_clicks'),
    prevent_initial_call=True,
)
def delete_token_confirm_click(n_clicks: int):
    if not n_clicks:
        raise PreventUpdate

    ctx = dash.callback_context.triggered
    if ctx[0]['value'] is None:
        raise PreventUpdate

    component_dict = json.loads(ctx[0]['prop_id'].split('.' + 'n_clicks')[0])
    token_id = component_dict['index']

    token = Token(
        id=token_id,
        instance=get_api_connector(),
    )

    success, msg = token.delete(debug=debug)
    if not success:
        return dash.no_update, dash.no_update, alert_from_success_tuple((success, msg))

    return False, False, []
