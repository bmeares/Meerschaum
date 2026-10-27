#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the users page: the table (or one user's page) and the administrators' Add user,
Change type, Reset password and Delete dialogs.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any
from meerschaum.api.dash import dash_app
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import attempt_import, import_html
dash = attempt_import('dash', lazy=False, check_update=CHECK_UPDATE)
html = import_html(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ALL
from dash.exceptions import PreventUpdate
from dash import no_update
from meerschaum.api.dash.components import alert_from_success_tuple
from meerschaum.api.dash.users import (
    build_users_listing,
    build_user_detail,
    viewer,
    get_user_rows,
    check_last_admin,
    validate_new_user,
    validate_password,
    add_user,
    change_user_type,
    reset_user_password,
    remove_user,
    end_user_sessions,
    normalize_user_type,
)


def _parse_username_from_pathname(pathname: Optional[str]) -> Optional[str]:
    """
    Return the username from ``/dash/users/<name>`` or ``None`` for the listing.
    """
    if not pathname or not str(pathname).startswith('/dash/users'):
        return None
    suffix = str(pathname)[len('/dash/users'):].strip('/')
    return suffix or None


HIDDEN = {'display': 'none'}


@dash_app.callback(
    Output('users-content-div', 'children'),
    Output('users-search-wrapper', 'style'),
    Output('users-pagination', 'max_value'),
    Output('users-pagination', 'active_page'),
    Output('users-pagination', 'style'),
    Output('users-add-button-div', 'hidden'),
    Output('users-header', 'style'),
    Input('users-location', 'pathname'),
    Input('search-users-input', 'value'),
    Input('users-pagination', 'active_page'),
    Input('users-refresh', 'data'),
    State('session-store', 'data'),
)
def render_users_page(
    pathname: Optional[str] = None,
    search_term: Optional[str] = None,
    active_page: Optional[int] = None,
    refresh: Any = None,
    session_store_data: Optional[dict] = None,
):
    """
    Render either the users table or a single user's page. Administrators see every user and
    their menus; other users see the table read-only, or only themselves on a `--private` or
    `--secure` server.
    """
    ### The user directory is not public: this callback is reachable directly via
    ### `/dash/_dash-update-component`, not only by navigating the page.
    who = viewer(session_store_data)
    if not who['active']:
        return (
            html.Div(
                [html.H5('Sign in to see the users'), dbc.Button('Sign in', href='/dash/login', color='primary')],
                className='console-empty',
            ),
            HIDDEN, 1, 1, HIDDEN, True, HIDDEN,
        )

    username = _parse_username_from_pathname(pathname)
    if username:
        return (
            build_user_detail(username, who),
            HIDDEN, 1, 1, HIDDEN, True, HIDDEN,
        )

    triggered_id = getattr(dash.callback_context, 'triggered_id', None)
    if triggered_id == 'search-users-input':
        page = 1
    else:
        page = int(active_page or 1)

    try:
        content, total_pages, total = build_users_listing(search_term=search_term, page=page, viewer_info=who)
    except Exception as e:
        content, total_pages, total = alert_from_success_tuple((False, f"Failed to read the users: {e}")), 1, 0
    show_search = who['sees_all'] and (bool(search_term) or total > 1)
    return (
        content,
        {} if show_search else HIDDEN,
        total_pages,
        min(page, total_pages),
        {
            'justify-content': 'center',
            'display': 'flex' if total_pages > 1 else 'none',
        },
        not who['admin'],
        {},
    )


DIALOG_OUTPUTS = (
    Output('users-add-modal', 'is_open'),
    Output('users-type-modal', 'is_open'),
    Output('users-password-modal', 'is_open'),
    Output('users-delete-modal', 'is_open'),
    Output('users-target', 'data'),
    Output('users-add-title', 'children'),
    Output('users-type-title', 'children'),
    Output('users-password-title', 'children'),
    Output('users-delete-title', 'children'),
    Output('users-delete-name', 'children'),
    Output('users-type-value', 'value'),
    Output('users-add-username', 'value'),
    Output('users-add-password', 'value'),
    Output('users-add-confirm', 'value'),
    Output('users-add-email', 'value'),
    Output('users-add-type', 'value'),
    Output('users-password-value', 'value'),
    Output('users-password-confirm', 'value'),
    Output('users-delete-input', 'value'),
    Output('users-add-alert', 'children'),
    Output('users-type-alert', 'children'),
    Output('users-password-alert', 'children'),
    Output('users-delete-alert', 'children'),
)


def _dialog(which: Optional[str] = None, username: Optional[str] = None, user_type: str = 'user'):
    """The outputs of `open_users_dialog`: `which` dialog open (or all closed), its fields fresh."""
    target = {'username': username, 'action': which} if which and which != 'add' else None
    return (
        which == 'add',
        which == 'type',
        which == 'password',
        which == 'delete',
        target,
        'Add user',
        ['Change the type of ', html.B(username or '')],
        ['Reset the password of ', html.B(username or '')],
        ['Delete user ', html.B(username or ''), '?'],
        username or '',
        user_type,
        '', '', '', '', 'user',
        '', '',
        '',
        [], [], [], [],
    )


@dash_app.callback(
    *DIALOG_OUTPUTS,
    Input('users-add-button', 'n_clicks'),
    Input({'type': 'user-action', 'action': ALL, 'index': ALL}, 'n_clicks'),
    Input('users-add-cancel', 'n_clicks'),
    Input('users-type-cancel', 'n_clicks'),
    Input('users-password-cancel', 'n_clicks'),
    Input('users-delete-cancel', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_users_dialog(add_clicks, action_clicks, *cancel_clicks_and_session):
    """Open the Add user dialog or a user's Change type / Reset password / Delete dialog."""
    session_store_data = cancel_clicks_and_session[-1]
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered[0]['value']:
        raise PreventUpdate
    trigger = ctx.triggered_id
    if trigger in ('users-add-cancel', 'users-type-cancel', 'users-password-cancel', 'users-delete-cancel'):
        return _dialog()
    if not viewer(session_store_data)['admin']:
        raise PreventUpdate
    if trigger == 'users-add-button':
        return _dialog('add')
    if not isinstance(trigger, dict) or trigger.get('type') != 'user-action':
        raise PreventUpdate
    username = trigger.get('index')
    action = trigger.get('action')
    if action not in ('type', 'password', 'delete'):
        raise PreventUpdate
    rows = {row['username']: row for row in get_user_rows()}
    if username not in rows:
        raise PreventUpdate
    return _dialog(action, username, rows[username]['type'])


def _admin_or_refusal(session_store_data):
    """The viewer when they are an administrator, else the alert of a refusal."""
    who = viewer(session_store_data)
    if not who['admin']:
        return None, alert_from_success_tuple((False, "Only administrators can change users."))
    return who, None


@dash_app.callback(
    Output('users-add-modal', 'is_open', allow_duplicate=True),
    Output('users-add-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children'),
    Output('users-refresh', 'data'),
    Input('users-add-submit', 'n_clicks'),
    State('users-add-username', 'value'),
    State('users-add-password', 'value'),
    State('users-add-confirm', 'value'),
    State('users-add-type', 'value'),
    State('users-add-email', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_add_user(n_clicks, username, password, confirm, user_type, email, refresh, session_store_data):
    """Register the new user."""
    if not n_clicks:
        raise PreventUpdate
    who, refusal = _admin_or_refusal(session_store_data)
    if refusal is not None:
        return no_update, refusal, no_update, no_update
    valid, message = validate_new_user(username, password, confirm, email)
    if not valid:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    try:
        success, message = add_user(username, password, user_type, email)
    except Exception as e:
        success, message = False, str(e)
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    return (
        False,
        [],
        alert_from_success_tuple((True, f"Added {normalize_user_type(user_type)} '{str(username).strip()}'.")),
        (refresh or 0) + 1,
    )


def _session_id(session_store_data) -> Optional[str]:
    return (session_store_data or {}).get('session-id', None) if isinstance(session_store_data, dict) else None


@dash_app.callback(
    Output('users-type-modal', 'is_open', allow_duplicate=True),
    Output('users-type-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('users-type-submit', 'n_clicks'),
    State('users-target', 'data'),
    State('users-type-value', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_change_type(n_clicks, target, user_type, refresh, session_store_data):
    """Make the user an admin or a regular user (never the only admin a regular user)."""
    if not n_clicks or not target or target.get('action') != 'type':
        raise PreventUpdate
    who, refusal = _admin_or_refusal(session_store_data)
    if refusal is not None:
        return no_update, refusal, no_update, no_update
    username = target.get('username')
    rows = get_user_rows()
    current = {row['username']: row for row in rows}.get(username)
    if current is None:
        return no_update, alert_from_success_tuple((False, f"User '{username}' does not exist.")), no_update, no_update
    user_type = normalize_user_type(user_type)
    if current['type'] == user_type:
        return False, [], alert_from_success_tuple((True, f"'{username}' is already a {user_type}.")), no_update
    allowed, message = check_last_admin(rows, username, new_type=user_type)
    if not allowed:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    try:
        success, message = change_user_type(username, user_type)
    except Exception as e:
        success, message = False, str(e)
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    ### The user's sessions carry the old rights: a demoted user signs in again, a promoted
    ### user's rights are read again (the one making the change keeps their session).
    end_user_sessions(
        username,
        keep_session_id=(_session_id(session_store_data) if username == who['username'] else None),
        refresh_only=(user_type == 'admin'),
    )
    return (
        False,
        [],
        alert_from_success_tuple((True, f"'{username}' is now {'an admin' if user_type == 'admin' else 'a user'}.")),
        (refresh or 0) + 1,
    )


@dash_app.callback(
    Output('users-password-modal', 'is_open', allow_duplicate=True),
    Output('users-password-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('users-password-submit', 'n_clicks'),
    State('users-target', 'data'),
    State('users-password-value', 'value'),
    State('users-password-confirm', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_reset_password(n_clicks, target, password, confirm, refresh, session_store_data):
    """Set the user's password and sign the user out of their other sessions."""
    if not n_clicks or not target or target.get('action') != 'password':
        raise PreventUpdate
    who, refusal = _admin_or_refusal(session_store_data)
    if refusal is not None:
        return no_update, refusal, no_update, no_update
    username = target.get('username')
    if username not in {row['username'] for row in get_user_rows()}:
        return no_update, alert_from_success_tuple((False, f"User '{username}' does not exist.")), no_update, no_update
    valid, message = validate_password(password, confirm)
    if not valid:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    try:
        success, message = reset_user_password(username, password)
    except Exception as e:
        success, message = False, str(e)
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    end_user_sessions(
        username,
        keep_session_id=(_session_id(session_store_data) if username == who['username'] else None),
    )
    return (
        False,
        [],
        alert_from_success_tuple((True, f"Reset the password of '{username}'.")),
        (refresh or 0) + 1,
    )


@dash_app.callback(
    Output('users-delete-submit', 'disabled'),
    Input('users-delete-input', 'value'),
    State('users-target', 'data'),
)
def enable_delete_user(typed_name, target):
    """Delete is enabled once the username has been typed."""
    return not target or (typed_name or '').strip() != target.get('username')


@dash_app.callback(
    Output('users-delete-modal', 'is_open', allow_duplicate=True),
    Output('users-delete-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('users-delete-submit', 'n_clicks'),
    State('users-target', 'data'),
    State('users-delete-input', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_delete_user(n_clicks, target, typed_name, refresh, session_store_data):
    """Delete the user (never the only admin) and end their sessions."""
    if not n_clicks or not target or target.get('action') != 'delete':
        raise PreventUpdate
    who, refusal = _admin_or_refusal(session_store_data)
    if refusal is not None:
        return no_update, refusal, no_update, no_update
    username = target.get('username')
    if (typed_name or '').strip() != username:
        return no_update, alert_from_success_tuple((False, "Type the username to delete the user.")), no_update, no_update
    rows = get_user_rows()
    if username not in {row['username'] for row in rows}:
        return no_update, alert_from_success_tuple((False, f"User '{username}' does not exist.")), no_update, no_update
    allowed, message = check_last_admin(rows, username, deleting=True)
    if not allowed:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    try:
        success, message = remove_user(username)
    except Exception as e:
        success, message = False, str(e)
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    end_user_sessions(username)
    return (
        False,
        [],
        alert_from_success_tuple((True, f"Deleted user '{username}'.")),
        (refresh or 0) + 1,
    )
