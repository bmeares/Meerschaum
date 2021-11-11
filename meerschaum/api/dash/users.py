#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with users.
"""

from __future__ import annotations

from meerschaum.api import debug
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple, Optional
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
dcc, html = import_dcc(), import_html()
dbc = attempt_import('dash_bootstrap_components', lazy=False)

def get_users_cards(state: WebState) -> Tuple[List[dbc.Card], List[SuccessTuple]]:
    """
    Return cards and alerts lists for users.
    """
    cards, alerts = [], [] 
    conn = get_web_connector(state)
    usernames = conn.get_users(debug=debug)
    for username in usernames:
        cards.append(dbc.Card(username))

    return cards, alerts