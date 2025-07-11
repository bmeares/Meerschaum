#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash utility functions for constructing tokens components.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, List

import meerschaum as mrsm
from meerschaum.api import debug, CHECK_UPDATE, get_api_connector
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import interval_str, round_time
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.core import Token
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


def build_tokens_register_input_modal() -> dbc.Modal:
    """
    Return the layout for the tokens register input modal.
    """
    now = datetime.now(timezone.utc)
    default_expiration_days = mrsm.get_config(
        'system', 'api', 'tokens', 'default_expiration_days',
    ) or 366
    default_expiration = round_time(
        now + timedelta(days=default_expiration_days),
        timedelta(days=1),
    )
    min_date_allowed = round_time(now + timedelta(days=1), timedelta(days=1))

    return [
        dbc.ModalHeader(html.H4("Register Token")),
        dbc.ModalBody([
            dbc.Form([
                dbc.Row(
                    [
                        dbc.Label("Name", width='auto'),
                        dbc.Col(
                            [
                                dbc.Input(
                                    placeholder="Enter token's label",
                                    id='tokens-name-input'
                                ),
                            ],
                            className="me-3",
                        ),
                        dbc.Label("Expiration", width='auto'),
                        dbc.Col(
                            dcc.DatePickerSingle(
                                date=default_expiration,
                                clearable=True,
                                min_date_allowed=min_date_allowed,
                                display_format="YYYY-MM-DD",
                                id='tokens-expiration-datepickersingle',
                            )
                        ),
                        dbc.Col(
                            dbc.Switch(
                                id="tokens-toggle-scopes-switch",
                                label="Grant all scopes",
                                value=True,
                            ),
                            className="me-3",
                        ),
                    ],
                    className='g-2',
                ),
                html.Br(),
                dbc.Row([
                ]),
                html.Div([
                    dbc.Button(
                        "Deselect all",
                        size='sm',
                        color='link',
                        id="tokens-deselect-scopes-button",
                        style={'text-decoration': 'none'},
                    ),
                    dbc.Row([
                        dbc.Label("Scopes", width='auto'),
                        dbc.Col([
                            dbc.Checklist(
                                options=[
                                    {"label": scope, "value": scope}
                                    for scope in STATIC_CONFIG['tokens']['scopes']
                                ],
                                value=list(STATIC_CONFIG['tokens']['scopes']),
                                id="tokens-scopes-checklist",
                                style={'columnCount': 3},
                            ),
                        ]),
                    ]),
                ], id='tokens-scopes-checklist-div', style={'display': 'none'}),
            ]),
        ]),
        dbc.ModalFooter([
            dbc.Button('Register', id='tokens-register-button'),
        ]),
    ]


def build_table_from_token(token: Token) -> dbc.Table:
    """
    Return a table with the token's metadata.
    """
    table_header = [html.Thead(html.Tr([html.Th("Attribute"), html.Th("Value")]))]
    table_header = []
    rows = [
        html.Tr([
            html.Td(html.B("Client ID")),
            html.Td(html.Pre(str(token.id))),
        ]),
    ]
    table_body = [html.Tbody(rows)]
    table = dbc.Table(
        table_header + table_body,
        id={'type': 'token-table', 'index': str(token.id)},
    )
    return table


def build_tokens_register_output_modal(token: Token) -> List[Any]:
    """
    Return the layout for the tokens register output modal.
    """
    success, msg = token.register(debug=debug)
    header_text = (
        "Registered token "
        if success
        else "Failed to register token "
    )
    body_children = (
        [
            build_table_from_token(token)
        ]
        if success
        else alert_from_success_tuple((False, msg))
    )
    return [
        dbc.ModalHeader(html.H4([header_text, html.B(token.label)])),
        dbc.ModalBody(body_children),
    ]
