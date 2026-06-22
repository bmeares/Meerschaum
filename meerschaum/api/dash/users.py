#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with users via the web interface.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Tuple, SuccessTuple, Optional, WebState, Dict, Any
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import get_api_connector, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin
from meerschaum.api.dash import debug
from meerschaum.api.dash.components import build_cards_grid

USERS_PER_PAGE: int = 12
USERS_GRID_COLUMNS: int = 3


def _get_user_plugins(username: str) -> List[str]:
    """
    Return the names of plugins owned by ``username``.

    ponytail: O(plugins) username lookups — fine at typical instance scale.
    Add a connector-side `get_plugins(user=...)` filter if plugin counts grow large.
    """
    conn = get_api_connector()
    return sorted(
        name
        for name in (conn.get_plugins() or [])
        if conn.get_plugin_username(Plugin(name), debug=debug) == username
    )


def _build_user_card(username: str, *, detail: bool = False) -> dbc.Card:
    """
    Build a single user card. If ``detail`` is True, also list the user's plugins.
    """
    title_el = (
        html.H3(username, style={'margin': 0})
        if detail
        else html.H5(username, style={'margin': 0})
    )
    title_link = html.A(
        title_el,
        href='/dash/users/' + username,
        style={'text-decoration': 'none', 'color': 'inherit'},
    )

    body_children: List[Any] = []
    if detail:
        plugin_names = _get_user_plugins(username)
        body_children += [
            html.Hr(),
            html.H5('Plugins', style={'margin-bottom': '0.5em'}),
        ]
        if plugin_names:
            body_children.append(
                html.Ul([
                    html.Li(
                        html.A(name, href='/dash/plugins/' + name)
                    )
                    for name in plugin_names
                ])
            )
        else:
            body_children.append(
                html.P('No published plugins.', className='text-muted')
            )

    return dbc.Card(
        [
            dbc.CardHeader(html.Span('👤 ', className='text-muted'), style={'text-align': 'left'}),
            dbc.CardBody([title_link] + body_children),
        ],
        id=username + '_user_card',
        className='plugin-card' + (' plugin-card-detail' if detail else ''),
    )


def get_users_cards(
    state: Optional[WebState] = None,
    search_term: Optional[str] = None,
    page: int = 1,
    per_page: int = USERS_PER_PAGE,
) -> Tuple[List[dbc.Card], List[SuccessTuple], int, int]:
    """
    Return the cards, alerts, total page count, and total user count
    for the users listing.
    """
    alerts: List[SuccessTuple] = []
    all_names = sorted(get_api_connector().get_users(debug=debug) or [])
    if search_term:
        all_names = [name for name in all_names if search_term.lower() in name.lower()]
    total = len(all_names)
    if per_page <= 0:
        per_page = USERS_PER_PAGE
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_names = all_names[start:start + per_page]

    cards = [_build_user_card(name) for name in page_names]
    return cards, alerts, total_pages, total


def build_users_listing(
    search_term: Optional[str],
    page: int = 1,
) -> Tuple[Any, int, int]:
    """
    Build the users listing body (summary + cards grid) and return it
    along with the total page count and total user count.
    """
    cards, _alerts, total_pages, total = get_users_cards(search_term=search_term, page=page)

    if not cards:
        body = html.Div(
            [
                html.H4(
                    (
                        f"No users match '{search_term}'."
                        if search_term
                        else 'No users registered yet.'
                    ),
                    className='text-muted',
                    style={'text-align': 'center'},
                ),
            ],
            className='plugins-empty-state',
        )
        return body, total_pages, total

    summary_text = (
        f"Showing page {page} of {total_pages} ({total} user"
        + ('s' if total != 1 else '')
        + ')'
    )
    content = html.Div([
        html.P(summary_text, className='text-muted', style={'text-align': 'center'}),
        build_cards_grid(cards, num_columns=USERS_GRID_COLUMNS),
    ])
    return content, total_pages, total


def build_user_detail(username: str) -> Any:
    """
    Build the detail view for a single user (shareable URL).
    """
    back_button = dbc.Button(
        '← Back to users',
        href='/dash/users',
        color='link',
        size='lg',
        className='plugin-back-button',
    )

    existing = get_api_connector().get_users(debug=debug) or []
    if username not in existing:
        return html.Div([
            back_button,
            html.Br(),
            html.Br(),
            html.H3(f"404: User '{username}' not found."),
        ], className='plugin-detail-wrapper')

    return html.Div([
        back_button,
        html.Br(),
        html.Br(),
        _build_user_card(username, detail=True),
        html.Br(),
    ], className='plugin-detail-wrapper')
