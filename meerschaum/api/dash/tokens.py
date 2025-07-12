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
from meerschaum.api.dash.sessions import get_user_from_session
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import interval_str, round_time
from meerschaum.utils.dtypes import value_is_null
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.core import Token
from meerschaum.utils.daemon import get_new_daemon_name
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)


def get_tokens_table(session_id: Optional[str] = None) -> Tuple[dbc.Table, List[dbc.Alert]]:
    """
    Return the main tokens table.
    """
    conn = get_api_connector()
    user = get_user_from_session(session_id) if session_id is not None else None
    alerts = []
    try:
        tokens = conn.get_tokens(user=user, debug=debug)
    except Exception as e:
        tokens = []
        alerts = [alert_from_success_tuple((False, f"Failed to fetch tokens from '{conn}':\n{e}"))]

    if not tokens:
        return tokens, alerts

    table_header = [
        html.Thead(
            html.Tr([
                html.Th("Label"),
                html.Th("Client ID"),
                html.Th("Created"),
                html.Th("Expires in"),
                html.Th("Scopes"),
                html.Th("Edit"),
            ]),
        ),
    ]

    rows = [
        html.Tr([
            html.Td(str(token.label)),
            html.Td(html.Pre(str(token.id))),
            html.Td(get_creation_string(token).replace('Created ', '')),
            html.Td(get_expiration_string(token).replace('Expires ', '').capitalize()),
            html.Td([
                dbc.Button(
                    "View",
                    color='link',
                    style={'text-decoration': 'none'},
                    id={'type': 'tokens-view-scopes-button', 'index': str(token.id)},
                ),
                dbc.Collapse(
                    dbc.ListGroup(
                        [
                            dbc.ListGroupItem(scope)
                            for scope in token.scopes
                        ],
                        flush=True,
                    ),
                    id={'type': 'tokens-scopes-collapse', 'index': str(token.id)},
                )
            ]),
            html.Td([
                dbc.Button(
                    html.B("🖉"),
                    color='link',
                    size='sm',
                    id={'type': 'tokens-edit-button', 'index': str(token.id)},
                    style={'text-decoration': 'none'},
                ),
                dbc.Modal(
                    [
                        dbc.ModalHeader([
                            html.H4([
                                "Edit token ",
                                html.B(str(token.label))
                            ]),
                        ]),
                        dbc.ModalBody([
                            html.Div(id={'type': 'tokens-edit-alerts-div', 'index': str(token.id)}),

                            dbc.Form([
                                dbc.Row(
                                    [
                                        dbc.Label("Name", width='auto'),
                                        dbc.Col(
                                            [
                                                dbc.Input(
                                                    placeholder="Enter token's label",
                                                    value=str(token.label),
                                                    id={'type': 'tokens-name-input', 'index': str(token.id)},
                                                ),
                                            ],
                                            className="me-3",
                                        ),
                                        dbc.Label("Expiration", width='auto'),
                                        dbc.Col(
                                            dcc.DatePickerSingle(
                                                date=(token.expiration.to_pydatetime() if hasattr(token.expiration, 'to_pydatetime') else token.expiration),
                                                clearable=True,
                                                min_date_allowed=datetime.today().date(),
                                                display_format="YYYY-MM-DD",
                                                id={'type': 'tokens-expiration-datepickersingle', 'index': str(token.id)},
                                            )
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
                                        id={'type': "tokens-deselect-scopes-button", 'index': str(token.id)},
                                        style={'text-decoration': 'none'},
                                    ),
                                    html.Br(),
                                    dbc.Row([
                                        dbc.Label("Scopes", width='auto'),
                                        dbc.Col([
                                            dbc.Checklist(
                                                options=[
                                                    {"label": scope, "value": scope}
                                                    for scope in STATIC_CONFIG['tokens']['scopes']
                                                ],
                                                value=token.scopes,
                                                id={'type': "tokens-scopes-checklist", 'index': str(token.id)},
                                                style={'columnCount': 3},
                                            ),
                                        ]),
                                    ]),
                                ], id={'type': 'tokens-scopes-checklist-div', 'index': str(token.id)}),
                            ]),

                        ]),
                        dbc.ModalFooter([
                            dbc.Button(
                                "Edit",
                                id={'type': 'tokens-edit-submit-button', 'index': str(token.id)},
                            ),
                        ]),
                    ],
                    size='lg',
                    is_open=False,
                    id={'type': 'tokens-edit-modal', 'index': str(token.id)},
                ),
            ]),
        ])
        for token in tokens
    ]

    table_body = [html.Tbody(rows)]

    table = dbc.Table(
        table_header + table_body,
    )

    return table, alerts




def get_tokens_cards(session_id: Optional[str] = None) -> Tuple[List[dbc.Card], List[dbc.Alert]]:
    """
    Return the cards and alerts for tokens.
    """
    cards, alerts = [], [] 
    conn = get_api_connector()
    user = get_user_from_session(session_id) if session_id is not None else None
    try:
        tokens = conn.get_tokens(user=user, debug=debug)
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
    if value_is_null(str(creation)):
        return ''
    now = datetime.now(timezone.utc)
    return 'Created ' + interval_str(creation - now, round_unit=True)


def get_expiration_string(token: mrsm.core.Token) -> str:
    """
    Return the formatted string to represent the token's expiration timestamp.
    """
    expiration = token.expiration
    if value_is_null(str(expiration)):
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
                                    value=get_new_daemon_name(),
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
                    html.Br(),
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


def build_register_table_from_token(token: Token) -> dbc.Table:
    """
    Return a table with the token's metadata.
    """
    table_header = [html.Thead(html.Tr([html.Th("Attribute"), html.Th("Value")]))]
    table_header = []
    pre_style = {'white-space': 'pre-wrap', 'word-break': 'break-all'}
    rows = [
        html.Tr([
            html.Td(html.B("Client ID")),
            html.Td(html.Pre(
                str(token.id),
                style=pre_style,
                id='token-id-pre',
            )),
        ]),
        html.Tr([
            html.Td(html.B("Client Secret")),
            html.Td(html.Pre(
                str(token.secret),
                style=pre_style,
                id='token-secret-pre',
            )),
        ]),
        html.Tr([
            html.Td(html.B("API Key")),
            html.Td(html.Pre(token.get_api_key(), style=pre_style)),
        ]),
        html.Tr([
            html.Td(html.B("Expiration")),
            html.Td(
                html.Pre(token.expiration.isoformat(), style=pre_style)
                if token.expiration is not None
                else "Does not expire"
            ),
        ]),
        html.Tr([
            html.Td(html.B("Scopes")),
            html.Td(html.P(' '.join(token.scopes), style={'word-break': 'normal'})),
        ]),
        html.Tr([
            html.Td(html.B("User")),
            html.Td(token.user.username if token.user is not None else ""),
        ]),
    ]
    table_body = [html.Tbody(rows)]
    table = dbc.Table(
        table_header + table_body,
        id='tokens-register-table',
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
            dbc.Stack(
                [
                    html.Div([
                        html.P(html.B("Copy the token's details to dismiss.")),
                        html.P(html.I("The secret and API key will not be shown again.")),
                    ]),
                    html.Div([
                        dbc.Button(
                            "Copy token details",
                            id='tokens-register-copy-button',
                        ),
                    ], className="ms-auto"),
                    html.Div([
                        dcc.Clipboard(id="tokens-register-clipboard"),
                    ]),
                ],
                direction='horizontal',
                gap=2,
            ),
            build_register_table_from_token(token),
        ]
        if success
        else alert_from_success_tuple((False, msg))
    )
    return [
        dbc.ModalHeader(
            html.H4([header_text, html.B(token.label)]),
            close_button=False,
        ),
        dbc.ModalBody(body_children),
        dbc.ModalFooter([
            dbc.Button(
                "Close",
                id='tokens-close-register-output-modal-button',
                disabled=True,
            ),
        ]),
    ]
