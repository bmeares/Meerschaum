#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the users page.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional
from meerschaum.api.dash import dash_app
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import attempt_import, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
html = import_html(check_update=CHECK_UPDATE)
from dash.dependencies import Input, Output, State
from meerschaum.api.dash.users import build_users_listing, build_user_detail
from meerschaum.api.dash.sessions import is_state_authenticated


def _parse_username_from_pathname(pathname: Optional[str]) -> Optional[str]:
    """
    Return the username from ``/dash/users/<name>`` or ``None`` for the listing.
    """
    if not pathname or not str(pathname).startswith('/dash/users'):
        return None
    suffix = str(pathname)[len('/dash/users'):].strip('/')
    return suffix or None


@dash_app.callback(
    Output('users-content-div', 'children'),
    Output('users-search-wrapper', 'style'),
    Output('users-pagination', 'max_value'),
    Output('users-pagination', 'active_page'),
    Output('users-pagination', 'style'),
    Input('users-location', 'pathname'),
    Input('search-users-input', 'value'),
    Input('users-pagination', 'active_page'),
    State('session-store', 'data'),
)
def render_users_page(
    pathname: Optional[str] = None,
    search_term: Optional[str] = None,
    active_page: Optional[int] = None,
    session_store_data: Optional[dict] = None,
):
    """
    Render either the paginated users listing or a single user's detail page.
    """
    ### The user directory is not public: this callback is reachable directly via
    ### `/dash/_dash-update-component`, not only by navigating the page.
    if not is_state_authenticated(session_store_data):
        return (
            html.P("You are not authenticated."),
            {'display': 'none'},
            1,
            1,
            {'display': 'none'},
        )

    username = _parse_username_from_pathname(pathname)
    if username:
        return (
            build_user_detail(username),
            {'display': 'none'},
            1,
            1,
            {'display': 'none'},
        )

    triggered_id = getattr(dash.callback_context, 'triggered_id', None)
    if triggered_id == 'search-users-input':
        page = 1
    else:
        page = int(active_page or 1)

    content, total_pages, total = build_users_listing(search_term=search_term, page=page)
    show_search = bool(search_term) or total > 0
    return (
        content,
        {'display': 'block'} if show_search else {'display': 'none'},
        total_pages,
        page,
        {
            'justify-content': 'center',
            'display': 'flex' if total_pages > 1 else 'none',
        },
    )
