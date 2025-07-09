#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash utility functions for constructing tokens components.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import meerschaum as mrsm
from meerschaum.api import debug, CHECK_UPDATE, get_api_connector
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import interval_str, round_time
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)


def get_tokens_cards() -> Tuple[List[dbc.Card], List[dbc.Alert]]:
    """
    Return the cards and alerts for tokens.
    """
    cards, alerts = [], [] 
    conn = get_api_connector()
    try:
        tokens = conn.get_tokens(debug=debug)
    except Exception as e:
        tokens = []
        alerts = [alert_from_success_tuple((False, f"Failed to fetch tokens from '{conn}':\n{e}"))]

    for token in tokens:
        try:
            cards.append(
                dbc.Card([
                    dbc.CardHeader(
                        [
                            html.H5(token.label),
                        ]
                    ),
                    dbc.CardBody(
                        [
                            html.Code(str(token.id), style={'color': '#999999'}),
                        ]
                    ),
                    dbc.CardFooter(
                        [
                            html.P(
                                get_creation_string(token),
                                style={'color': '#999999'},
                            ),
                            html.P(
                                get_expiration_string(token),
                                style={'color': '#999999'},
                            ),
                        ]
                    ),
                ])
            )
        except Exception as e:
            alerts.append(
                alert_from_success_tuple((False, f"Failed to load metadata for token:\n{e}"))
            )

    return cards, alerts


def get_creation_string(token: mrsm.core.Token) -> str:
    """
    Return the formatted string to represent the token's creation timestamp.
    """
    creation = token.creation
    if creation is None:
        return ''
    now = datetime.now(timezone.utc)
    delta = creation - now
    return 'Created ' + interval_str(creation - now, round_unit=True)


def get_expiration_string(token: mrsm.core.Token) -> str:
    """
    Return the formatted string to represent the token's expiration timestamp.
    """
    expiration = token.expiration
    if expiration is None:
        return 'Does not expire'
    now = datetime.now(timezone.utc)
    return 'Expires in ' + interval_str(expiration - now, round_unit=True)
