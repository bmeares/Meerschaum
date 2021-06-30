#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
View the available plugins hosted by this API instance.
"""

from __future__ import annotations
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from meerschaum.api import get_api_connector, endpoints
from meerschaum._internal.Plugin import Plugin
from meerschaum.utils.typing import Optional

search_box = dbc.Input(
    id="search-plugins-input",
    placeholder="Search for plugins...",
    type="text"
)

def build_cards_div(search_term: Optional[str] = None):
    """
    Build the cards div.
    """
    cards = []
    for plugin_name in sorted(get_api_connector().get_plugins(search_term=search_term)):
        plugin = Plugin(plugin_name)
        desc = get_api_connector().get_plugin_attributes(plugin).get(
            'description', 'No description provided.'
        )
        paragraph_list = []
        for line in desc.split('\n'):
            paragraph_list.append(line)
            paragraph_list.append(html.Br())

        card_body_children = [
            html.H4(plugin_name)] + paragraph_list + [
            html.A('⬇️ Download source', href=(endpoints['plugins'] + '/' + plugin_name)),
            dbc.Button('Edit description', size="sm", color="link")
        ]
        username = None
        if get_api_connector().get_plugin_username(plugin) == username and username is not None:
            pass
        card_children = [
            dbc.CardBody(card_body_children)
        ]
        cards.append(
            dbc.Card(card_children)
        )
    return dbc.CardColumns(cards)

layout = dbc.Container([
    html.Div([
        html.Br(),
        html.Div(
            dbc.Container([
                html.H2('Plugins'),
                html.P('Plugins extend the functionality of Meerschaum.'),
                html.A(
                    'To find out more, check out the plugins documentation.',
                    href='https://meerschaum.io/reference/plugins/using-plugins/'
                ),
            ]),
        className='jumbotron'
        ),
        search_box,
        html.Br(),
        html.Div(build_cards_div(), id='plugins-cards-div'),
    ])
])
