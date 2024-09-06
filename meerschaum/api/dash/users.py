#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with users.
"""

from __future__ import annotations

from meerschaum.api import debug, CHECK_UPDATE
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)


def get_users_cards(state: WebState) -> Tuple[List[dbc.Card], List[SuccessTuple]]:
    """
    Return the cards and alerts for users.
    """
    cards, alerts = [], [] 
    conn = get_web_connector(state)
    usernames = conn.get_users(debug=debug)
    for username in usernames:
        cards.append(
            dbc.Card([
                dbc.CardHeader(),
                dbc.CardBody([html.H5(username)]),
            ])
        )

    return cards, alerts
