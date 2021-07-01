#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the plugins page.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any
from meerschaum.api.dash import dash_app, debug, active_sessions
from meerschaum.api import get_api_connector, endpoints
from meerschaum._internal.Plugin import Plugin
from meerschaum.utils.packages import attempt_import
dash = attempt_import('dash', lazy=False)
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ALL, MATCH
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import alert_from_success_tuple
from dash.exceptions import PreventUpdate
import json

@dash_app.callback(
    Output('plugins-cards-div', 'children'),
    Input('search-plugins-input', 'value'),
    State('session-store', 'data'),
)
def search_plugins(text: Optional[str] = None, session_data: Optional[Dict[str, Any]] = None):
    #  print(session_data)
    return build_cards_div(search_term=text, session_data=session_data)


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
        print(f"{description=}")
        success, _msg = get_api_connector().register_plugin(plugin, debug=debug, force=True)
        msg = _msg if not success else "Successfully updated description."
    return [alert_from_success_tuple((success, msg))]

def is_plugin_owner(plugin_name: str, session_data: Dict['str', Any]) -> bool:
    """
    Check whether the currently logged in user is the owner of a plugin.
    """
    plugin = Plugin(plugin_name)
    _username = active_sessions.get(
        session_data.get('session-id', None), {}
    ).get('username', None)
    _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
    return (
        _username is not None
        and _username == _plugin_username
    )

def build_cards_div(
    search_term: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
) -> dbc.CardColumns:
    """
    Build the cards div.
    """
    cards = []
    if session_data is None:
        session_data = {}
    for plugin_name in sorted(get_api_connector().get_plugins(search_term=search_term)):
        plugin = Plugin(plugin_name)
        desc = get_api_connector().get_plugin_attributes(plugin).get(
            'description', 'No description provided.'
        )
        #  paragraph_list = []
        #  for line in desc.split('\n'):
            #  paragraph_list.append(line)
            #  paragraph_list.append(html.Br())
        desc_textarea_kw = dict(
            value=desc, readOnly=True, debounce=False, className='plugin-description',
            draggable=False, wrap='overflow',
            id={'type': 'description-textarea', 'index': plugin_name},
        )

        card_body_children = [html.H4(plugin_name)]

        if is_plugin_owner(plugin_name, session_data):
            desc_textarea_kw['readOnly'] = False
        card_body_children += [dbc.Textarea(**desc_textarea_kw)]
        if not desc_textarea_kw['readOnly']:
            card_body_children += [
                dbc.Button(
                    'Update description',
                    size="sm",
                    color="link",
                    id={'type': 'edit-button', 'index': plugin_name},
                ),
                html.Div(id={'type': 'edit-alert-div', 'index': plugin_name}),
            ]
        #  card_body_children = (
            #  card_body_children[:-1]
            #  + [dbc.Textarea(**desc_textarea_kw)]
            #  + [card_body_children[-1]]
        #  )
        _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
        card_children = [
            dbc.CardHeader([html.A('👤 ' + str(_plugin_username), href='#')]),
            dbc.CardBody(card_body_children),
            dbc.CardFooter([
                html.A('⬇️ Download source', href=(endpoints['plugins'] + '/' + plugin_name))
            ]),
        ]
        cards.append(
            dbc.Card(card_children, id=plugin_name + '_card', className='plugn-card')
        )
    return dbc.CardColumns(cards)

