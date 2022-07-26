#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the plugins page.
"""

from __future__ import annotations
import json
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api.dash import dash_app, debug, active_sessions
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
from meerschaum.core import Plugin
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import alert_from_success_tuple, build_cards_grid
from dash.exceptions import PreventUpdate
from meerschaum.api.dash.plugins import get_plugins_cards, is_plugin_owner

@dash_app.callback(
    Output('plugins-cards-div', 'children'),
    Input('search-plugins-input', 'value'),
    State('session-store', 'data'),
)
def search_plugins(text: Optional[str] = None, session_data: Optional[Dict[str, Any]] = None):
    cards, alerts = get_plugins_cards(search_term=text, session_data=session_data)
    return build_cards_grid(cards, num_columns=3)


@dash_app.callback(
    Output({'type': 'edit-alert-div', 'index': MATCH}, 'children'),
    Input({'type': 'edit-button', 'index': MATCH}, 'n_clicks'),
    State({'type': 'description-textarea', 'index': MATCH}, 'value'),
    State('session-store', 'data'),
)
def edit_plugin_description(
    n_clicks: Optional[int] = None,
    description: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    Edit a plugin's description and set the alert.
    """
    if n_clicks is None:
        raise PreventUpdate
    ctx = dash.callback_context
    prop_id = ctx.triggered[0]['prop_id']
    j = '.'.join(prop_id.split('.')[:-1])
    plugin_name = json.loads(j)['index']
    if not is_plugin_owner(plugin_name, session_data):
        success, msg = (
            False,
            f"Failed to update description for plugin '{plugin_name}'"
            + " due to insufficient permissions."
        )
    else:
        plugin = Plugin(plugin_name)
        plugin.user_id = get_api_connector().get_plugin_user_id(plugin, debug=debug)
        plugin.attributes = get_api_connector().get_plugin_attributes(plugin, debug=debug)
        plugin.attributes.update({'description': description})
        success, _msg = get_api_connector().register_plugin(plugin, debug=debug, force=True)
        msg = _msg if not success else "Successfully updated description."
    return [alert_from_success_tuple((success, msg))]

