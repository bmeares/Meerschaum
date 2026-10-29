#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Dash utility functions for constructing tokens components.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional

import meerschaum as mrsm
from meerschaum.api import debug, CHECK_UPDATE, get_api_connector
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.sessions import get_user_from_session
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.utils.misc import interval_str
from meerschaum.utils.dtypes import value_is_null, round_time
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.core import Token
from meerschaum.utils.daemon import get_new_daemon_name
dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)


def check_token_access(
    token_id: str,
    session_store_data: Optional[dict] = None,
) -> SuccessTuple:
    """
    Return whether a session may modify the token `token_id`.

    Mirrors the ownership rule enforced by the REST routes
    (`meerschaum.api.routes._tokens`): a token belongs to the user who created
    it, and only that user or an admin may edit, invalidate, or delete it.

    The token-management callbacks are reachable directly via
    `/dash/_dash-update-component` with an arbitrary token ID, so this must be
    checked in the callback rather than relied upon from the rendered page.

    Parameters
    ----------
    token_id: str
        The token ID taken from the triggering component's `index`.

    session_store_data: Optional[dict], default None
        The `session-store` component's `data` dict.

    Returns
    -------
    A `SuccessTuple` indicating whether the operation may proceed.
    """
    import uuid
    from meerschaum.utils.misc import is_uuid
    from meerschaum.api import no_auth
    from meerschaum.api.dash.access import get_viewer

    viewer = get_viewer(session_store_data)
    if not (viewer.signed_in or viewer.no_auth):
        return False, "You are not authenticated."

    if not is_uuid(token_id):
        return False, "Invalid token ID."

    conn = get_api_connector()
    token_model = conn.get_token_model(uuid.UUID(str(token_id)))
    if token_model is None:
        return False, "Token does not exist."

    ### Under `--no-auth`, the session has no real user (`get_user_from_session`
    ### returns `None`), so ownership cannot (and need not) be enforced.
    if no_auth:
        return True, "Success"

    session_id = (session_store_data or {}).get('session-id', None)
    user = get_user_from_session(session_id)
    user_id = conn.get_user_id(user, debug=debug) if user is not None else None

    if token_model.user_id and token_model.user_id != user_id:
        if user is None or conn.get_user_type(user, debug=debug) != 'admin':
            return False, "Cannot modify another user's token."

    return True, "Success"


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
                html.Th("Is Valid"),
                html.Th(""),
            ]),
        ),
    ]

    rows = [
        html.Tr([
            html.Td(str(token.label)),
            html.Td(html.Pre(str(token.id))),
            html.Td(get_creation_string(token).replace('Created ', '')),
            html.Td(get_expiration_string(token).replace('Expires in ', '')),
            html.Td(
                "✅" if token.is_valid else "❌",
                title=("Valid" if token.is_valid else "Invalid"),
            ),
            html.Td([
                dbc.Button(
                    html.B("⋮"),
                    color='link',
                    size='sm',
                    id={'type': 'tokens-context-button', 'index': str(token.id)},
                    title='Manage token',
                    style={'text-decoration': 'none'},
                ),
                build_manage_token_popover(token),
                build_edit_token_modal(token),
                build_invalidate_token_modal(token),
                build_delete_token_modal(token),
            ]),
        ])
        for token in tokens
    ]

    table_body = [html.Tbody(rows)]
    table = dbc.Table(table_header + table_body, responsive=True)
    return table, alerts


def build_manage_token_popover(token: Token) -> dbc.Popover:
    """
    Return the "Manage token" popover.
    """
    return dbc.Popover(
        [
            dbc.ButtonGroup(
                ([
                    dbc.Button(
                        "Edit",
                        outline=True,
                        color='light',
                        id={
                            'type': 'tokens-edit-button',
                            'index': str(token.id),
                        },
                    ),
                    dbc.Button(
                        "Invalidate",
                        outline=True,
                        color='warning',
                        id={
                            'type': 'tokens-invalidate-button',
                            'index': str(token.id),
                        },
                    ),
                ] if token.is_valid else []) + [
                dbc.Button(
                    "Delete",
                    color='danger',
                    outline=True,
                    id={
                        'type': 'tokens-delete-button',
                        'index': str(token.id),
                    },
                ),
            ]),
        ],
        body=True,
        trigger='legacy',
        autohide=True,
        id={'type': 'tokens-context-popover', 'index': str(token.id)},
        target={'type': 'tokens-context-button', 'index': str(token.id)},
    )


#: The label of the token forms' switch that stores the literal scope `*`.
GRANT_ALL_SCOPES_LABEL: str = "Grant all scopes, including custom and future ones"


def build_token_form(
    label: Optional[str] = None,
    expiration: Any = None,
    min_date_allowed: Any = None,
    grant_all: bool = True,
    scopes: Optional[List[str]] = None,
    token_id: Optional[str] = None,
) -> dbc.Form:
    """
    The fields of the New token and Edit token forms: Name and Expiration (side by side, stacked
    on phones), the "Grant all scopes" switch, and the scopes to grant instead (hidden while the
    switch is on). With `token_id`, the components' IDs are those of that token's Edit form
    (`{'type': ..., 'index': token_id}`), else the New token form's.
    """
    def cid(kind: str) -> Any:
        return kind if token_id is None else {'type': kind, 'index': str(token_id)}

    def label_for(text: str, kind: str) -> Any:
        return dbc.Label(text, html_for=kind) if token_id is None else dbc.Label(text)

    return dbc.Form([
        dbc.Row(
            [
                dbc.Col(
                    [
                        label_for('Name', 'tokens-name-input'),
                        dbc.Input(
                            placeholder="Enter token's label",
                            value=label,
                            autoComplete='off',
                            id=cid('tokens-name-input'),
                        ),
                    ],
                    xs=12,
                    md=6,
                ),
                dbc.Col(
                    [
                        dbc.Label('Expiration'),
                        dcc.DatePickerSingle(
                            date=expiration,
                            clearable=True,
                            min_date_allowed=min_date_allowed,
                            display_format="YYYY-MM-DD",
                            id=cid('tokens-expiration-datepickersingle'),
                        ),
                    ],
                    xs=12,
                    md=6,
                    class_name='tokens-expiration',
                ),
            ],
            class_name='g-3 tokens-form-row',
        ),
        dbc.Switch(
            id=cid('tokens-toggle-scopes-switch'),
            label=GRANT_ALL_SCOPES_LABEL,
            value=grant_all,
            class_name='tokens-grant-all',
        ),
        html.Div(
            [
                html.Div(
                    [
                        dbc.Label('Scopes', class_name='mb-0'),
                        dbc.Button(
                            "Deselect all",
                            size='sm',
                            color='link',
                            id=cid('tokens-deselect-scopes-button'),
                            class_name='tokens-deselect-scopes',
                        ),
                    ],
                    className='tokens-scopes-header',
                ),
                dbc.Checklist(
                    options=[
                        {"label": scope, "value": scope}
                        for scope in STATIC_CONFIG['tokens']['scopes']
                    ],
                    value=list(scopes or []),
                    id=cid('tokens-scopes-checklist'),
                    class_name='tokens-scopes-checklist',
                ),
            ],
            id=cid('tokens-scopes-checklist-div'),
            className='tokens-scopes',
            style=({'display': 'none'} if grant_all else {}),
        ),
    ], class_name='tokens-form')


def build_edit_token_modal(token: Token) -> dbc.Modal:
    """
    Return the Modal for editing the token.
    """
    ### A literal `*` grants every scope, so show the switch on and the checklist hidden.
    grants_all_scopes = '*' in (token.scopes or [])
    form = (
        build_token_form(
            label=str(token.label),
            expiration=(
                token.expiration.to_pydatetime()
                if hasattr(token.expiration, 'to_pydatetime')
                else token.expiration
            ),
            min_date_allowed=datetime.today().date(),
            grant_all=grants_all_scopes,
            scopes=(
                list(STATIC_CONFIG['tokens']['scopes'])
                if grants_all_scopes
                else list(token.scopes or [])
            ),
            token_id=str(token.id),
        )
        if token.is_valid
        else html.Div(id={'type': 'tokens-scopes-checklist-div', 'index': str(token.id)})
    )
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(["Edit token ", html.B(str(token.label))])),
            dbc.ModalBody([
                html.Div(id={'type': 'tokens-edit-alerts-div', 'index': str(token.id)}),
                form,
            ]),
            dbc.ModalFooter(
                [
                    html.Small(str(token.id), className='text-muted tokens-modal-id'),
                    dbc.Button(
                        "Submit",
                        id={'type': 'tokens-edit-submit-button', 'index': str(token.id)},
                    ),
                ],
                className='d-flex justify-content-between',
            ),
        ],
        size='lg',
        fullscreen='sm-down',
        scrollable=True,
        is_open=False,
        class_name='tokens-modal',
        id={'type': 'tokens-edit-modal', 'index': str(token.id)},
    )


def build_invalidate_token_modal(token: Token) -> dbc.Modal:
    """
    Return the Invalidate token modal.
    """
    return dbc.Modal(
        [
            dbc.ModalHeader([
                html.H4([
                    "Invalidate token ",
                    html.B(token.label),
                    "?"
                ]),
            ]),
            dbc.ModalBody([
                html.Div(
                    id={
                        'type': 'tokens-invalidate-alerts-div',
                        'index': str(token.id)
                    },
                ),
                html.P(
                    [
                        "Are you sure you want to invalidate token ",
                        html.B(token.label),
                        " (",
                        html.I(str(token.id)),
                        ")?",
                    ],
                ),
                html.P([html.B("This action cannot be undone!")]),
            ]),
            dbc.ModalFooter([
                dbc.Button(
                    "Invalidate",
                    color='danger',
                    id={
                        'type': 'tokens-invalidate-confirm-button',
                        'index': str(token.id),
                    },
                ),
            ]),
        ],
        id={
            'type': 'tokens-invalidate-modal',
            'index': str(token.id),
        },
    )


def build_delete_token_modal(token: Token) -> dbc.Modal:
    """
    Return the delete token modal.
    """
    return dbc.Modal(
        [
            dbc.ModalHeader([
                html.H4([
                    "Delete token ",
                    html.B(token.label),
                    "?"
                ]),
            ]),
            dbc.ModalBody([
                html.Div(
                    id={
                        'type': 'tokens-delete-alerts-div',
                        'index': str(token.id)
                    },
                ),
                html.P(
                    [
                        "Are you sure you want to delete token ",
                        html.B(token.label),
                        " (",
                        html.I(str(token.id)),
                        ")?",
                    ],
                ),
                html.P([html.B("This action cannot be undone!")]),
            ]),
            dbc.ModalFooter([
                dbc.Button(
                    "Delete",
                    color='danger',
                    id={
                        'type': 'tokens-delete-confirm-button',
                        'index': str(token.id),
                    },
                ),
            ]),
        ],
        id={
            'type': 'tokens-delete-modal',
            'index': str(token.id),
        },
    )


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


def default_token_scopes(user: Optional[mrsm.core.User] = None) -> Tuple[bool, List[str]]:
    """
    Return `(grant_all, scopes)`: where the New token form starts. "Grant all scopes" (`*`) is on
    for an administrator or a holder of `*` (and under `--no-auth`); anyone else starts from
    their own scopes, since a token carries at most its owner's.
    """
    all_scopes = list(STATIC_CONFIG['tokens']['scopes'])
    if user is None:
        return True, all_scopes
    ### Their effective permissions (their role's, or their own list): a token holds no more.
    from meerschaum.api.dash.access import viewer_for
    try:
        viewer = viewer_for(str(user.username))
    except Exception:
        return False, []
    if viewer.is_admin or viewer.no_auth or '*' in viewer.permissions:
        return True, all_scopes
    return False, [scope for scope in all_scopes if scope in viewer.permissions]


def build_tokens_register_input_modal(user: Optional[mrsm.core.User] = None) -> dbc.Modal:
    """
    Return the layout for the tokens register input modal (for `user`, the session's user).
    """
    grant_all, own_scopes = default_token_scopes(user)
    now = datetime.now(timezone.utc)
    default_expiration_days = mrsm.get_config(
        'api', 'tokens', 'default_expiration_days',
    ) or 366
    default_expiration = round_time(
        now + timedelta(days=default_expiration_days),
        timedelta(days=1),
    )
    min_date_allowed = round_time(now + timedelta(days=1), timedelta(days=1))

    return [
        dbc.ModalHeader(dbc.ModalTitle("New token")),
        dbc.ModalBody(
            build_token_form(
                label=get_new_daemon_name(),
                expiration=default_expiration,
                min_date_allowed=min_date_allowed,
                grant_all=grant_all,
                scopes=own_scopes,
            ),
        ),
        dbc.ModalFooter([
            dbc.Button('Register', id='tokens-register-button'),
        ]),
    ]


def build_register_table_from_token(token: Token) -> dbc.Table:
    """
    Return a table with the token's metadata.
    """
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
                disabled=success,
            ),
        ]),
    ]
