#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with plugins via the web interface.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Tuple, SuccessTuple, Optional, WebState, Dict, Any
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin
from meerschaum.api.dash import dash_app, debug
from meerschaum.api.dash.sessions import get_username_from_session


def get_plugins_cards(
    state: Optional[WebState] = None,
    search_term: Optional[str] = None,
    session_data: Optional[Dict[str, Any]] = None,
) -> Tuple[List[dbc.Card], List[SuccessTuple]]:
    """
    Return the cards and alerts for plugins.
    """
    cards, alerts = [], []
    if session_data is None:
        session_data = {}
    for plugin_name in sorted(get_api_connector().get_plugins(search_term=search_term)):
        plugin = Plugin(plugin_name)
        desc = get_api_connector().get_plugin_attributes(plugin).get(
            'description', 'No description provided.'
        )
        desc_textarea_kw = {
            'value': desc,
            'readOnly': True,
            'debounce': False,
            'className': 'plugin-description',
            'draggable': False,
            'wrap': 'overflow',
            'placeholder': "Edit the plugin's description",
            'id': {'type': 'description-textarea', 'index': plugin_name},
        }

        card_body_children = [html.H4(plugin_name)]

        if is_plugin_owner(plugin_name, session_data):
            desc_textarea_kw['readOnly'] = False
        card_body_children.append(dbc.Textarea(**desc_textarea_kw))
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
        plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
        plugin_version = get_api_connector().get_plugin_version(plugin, debug=debug) or ' '
        card_children = [
            dbc.CardHeader(
                [
                    dbc.Row(
                        [
                            dbc.Col(html.A('👤 ' + str(plugin_username), href='#')),
                            dbc.Col(html.Pre(str(plugin_version), style={'text-align': 'right'})),
                        ],
                        justify = 'between',
                    ),
                ],
            ),
            dbc.CardBody(card_body_children),
            dbc.CardFooter([
                html.A('⬇️ Download', href=(endpoints['plugins'] + '/' + plugin_name))
            ]),
        ]
        cards.append(
            dbc.Card(card_children, id=plugin_name + '_card', className='plugin-card')
        )
    return cards, alerts


def is_plugin_owner(plugin_name: str, session_data: Dict['str', Any]) -> bool:
    """
    Check whether the currently logged in user is the owner of a plugin.
    """
    plugin = Plugin(plugin_name)
    session_id = session_data.get('session-id', None)
    if session_id is None:
        return False
    _username = get_username_from_session(session_id)
    _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
    return (
        _username is not None
        and _username == _plugin_username
    )
