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
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

@dash_app.callback(
    Output('plugins-cards-div', 'children'),
    Input('search-plugins-input', 'value'),
    State('session-store', 'data'),
)
def search_plugins(text: Optional[str] = None, session_data: Optional[Dict[str, Any]] = None):
    print(session_data)
    return build_cards_div(search_term=text, session_data=session_data)


#  @dash_app.callback(

#  )
#  def edit_plugin_description(

#  )

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
            value=desc, readOnly=True, debounce=True, className='plugin-description',
            draggable=False, wrap='overflow'
        )

        card_body_children = [html.H4(plugin_name)]
        _username = active_sessions.get(
            session_data.get('session-id', None), {}
        ).get('username', None)
        _plugin_username = get_api_connector().get_plugin_username(plugin, debug=debug)
        if (
            _username is not None
            and _username == _plugin_username
        ):
            desc_textarea_kw['readOnly'] = False
        card_body_children += [dbc.Textarea(**desc_textarea_kw)]
        if desc_textarea_kw['readOnly']:
            card_body_children += [dbc.Button('Edit description', size="sm", color="link")]
        #  card_body_children = (
            #  card_body_children[:-1]
            #  + [dbc.Textarea(**desc_textarea_kw)]
            #  + [card_body_children[-1]]
        #  )
        card_children = [
            dbc.CardHeader([html.A('👤 ' + str(_plugin_username), href='#')]),
            dbc.CardBody(card_body_children),
            dbc.CardFooter([
                html.A('⬇️ Download source', href=(endpoints['plugins'] + '/' + plugin_name))
            ]),
        ]
        cards.append(
            dbc.Card(card_children)
        )
    return dbc.CardColumns(cards)

