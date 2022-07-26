#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with plugins via the web interface.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Tuple, SuccessTuple
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import get_api_connector, endpoints, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin
from meerschaum.api.dash import dash_app, debug, active_sessions


def get_plugins_cards(
        state: Optional[WebState] = None,
        search_term: Optional[str] = None,
        session_data: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[dbc.Card], List[SuccessTuple]]:
    cards, alerts = [], []
    if session_data is None:
        session_data = {}
    for plugin_name in sorted(get_api_connector().get_plugins(search_term=search_term)):
        plugin = Plugin(plugin_name)
        desc = get_api_connector().get_plugin_attributes(plugin).get(
            'description', 'No description provided.'
        )
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
        _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
        card_children = [
            dbc.CardHeader([html.A('👤 ' + str(_plugin_username), href='#')]),
            dbc.CardBody(card_body_children),
            dbc.CardFooter([
                html.A('⬇️ Download source', href=(endpoints['plugins'] + '/' + plugin_name))
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
    _username = active_sessions.get(
        session_data.get('session-id', None), {}
    ).get('username', None)
    _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
    return (
        _username is not None
        and _username == _plugin_username
    )

