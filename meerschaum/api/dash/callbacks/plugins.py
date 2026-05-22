#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the plugins page.
"""

from __future__ import annotations
import json
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api.dash import dash_app, debug
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
from meerschaum.core import Plugin
from meerschaum.utils.packages import attempt_import, import_dcc, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.plugins import (
    build_plugins_listing,
    build_plugin_detail,
    is_plugin_owner,
)


def _parse_plugin_name_from_pathname(pathname: Optional[str]) -> Optional[str]:
    """
    Return the plugin name from ``/dash/plugins/<name>`` or ``None`` for the listing.
    """
    if not pathname or not str(pathname).startswith('/dash/plugins'):
        return None
    suffix = str(pathname)[len('/dash/plugins'):].strip('/')
    return suffix or None


@dash_app.callback(
    Output('plugins-content-div', 'children'),
    Output('plugins-search-wrapper', 'style'),
    Input('plugins-location', 'pathname'),
    Input('search-plugins-input', 'value'),
    Input('plugins-pagination', 'active_page'),
    State('session-store', 'data'),
)
def render_plugins_page(
    pathname: Optional[str] = None,
    search_term: Optional[str] = None,
    active_page: Optional[int] = None,
    session_data: Optional[Dict[str, Any]] = None,
):
    """
    Render either the paginated plugins listing or a single plugin's detail page.
    """
    plugin_name = _parse_plugin_name_from_pathname(pathname)
    if plugin_name:
        return build_plugin_detail(plugin_name, session_data), {'display': 'none'}

    triggered_id = getattr(dash.callback_context, 'triggered_id', None)
    if triggered_id == 'search-plugins-input':
        page = 1
    else:
        page = int(active_page or 1)

    return (
        build_plugins_listing(
            search_term=search_term,
            session_data=session_data,
            page=page,
        ),
        {'display': 'block'},
    )


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
