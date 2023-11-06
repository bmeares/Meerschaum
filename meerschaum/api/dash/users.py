#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with users.
"""

from __future__ import annotations

from meerschaum.api import debug, CHECK_UPDATE, get_api_connector, no_auth
from meerschaum.api.dash import active_sessions, authenticated_sessions, unauthenticated_sessions
from meerschaum.api.dash.connectors import get_web_connector
from meerschaum.utils.typing import WebState, SuccessTuple, List, Tuple, Optional
from meerschaum.utils.packages import attempt_import, import_html, import_dcc
from meerschaum.config import get_config
from meerschaum.core import User
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


def is_session_authenticated(session_id: str) -> bool:
    """
    Check is a session ID is active.
    If running in secure mode, check whether a session ID corresponds to an admin.

    Parameters
    ----------
    session_id: str
        The session UUID.

    Returns
    -------
    A bool whether the session is authenticated to perform actions.
    """
    if no_auth:
        return True
    if session_id in unauthenticated_sessions:
        return False
    if session_id in authenticated_sessions:
        return True
    permissions = get_config('system', 'api', 'permissions')
    allow_non_admin = permissions.get('actions', {}).get('non_admin', False)
    if allow_non_admin:
        return True
    conn = get_api_connector()
    username = active_sessions.get(session_id, {}).get('username', None)
    user = User(username, instance=conn)
    user_type = conn.get_user_type(user, debug=debug)
    is_auth = user_type == 'admin'
    if is_auth:
        authenticated_sessions[session_id] = username
    else:
        unauthenticated_sessions[session_id] = username
    return is_auth
