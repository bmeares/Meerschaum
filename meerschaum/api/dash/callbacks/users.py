#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Callbacks for the Users & Teams page: the Users table (or one user's page), the Teams table,
and their dialogs: Add user, Edit (type, role and permissions, teams, primary team), Reset
password and Delete for administrators; New team, Rename and Delete for administrators; Members
for administrators and the team's managers. Every change is checked again here, server side.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, Dict, List
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
from meerschaum.api.dash import access
from meerschaum.api.dash.users import (
    build_users_listing,
    build_user_detail,
    build_teams_table,
    build_team_members_editor,
    viewer,
    get_user_rows,
    visible_teams,
    may_manage_team,
    validate_team_name,
    team_pipe_counts,
    team_job_count,
    check_last_admin,
    validate_new_user,
    validate_password,
    add_user,
    change_user_type,
    set_user_rights,
    set_user_teams,
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
SHOWN: Dict[str, Any] = {}


def _session_id(session_store_data) -> Optional[str]:
    return (session_store_data or {}).get('session-id', None) if isinstance(session_store_data, dict) else None


@dash_app.callback(
    Output('users-content-div', 'children'),
    Output('users-search-wrapper', 'style'),
    Output('users-pagination', 'max_value'),
    Output('users-pagination', 'active_page'),
    Output('users-pagination', 'style'),
    Output('users-add-button-div', 'hidden'),
    Output('users-header', 'style'),
    Output('users-add-button', 'style'),
    Output('teams-new-button', 'style'),
    Output('users-tabs', 'style'),
    Input('users-location', 'pathname'),
    Input('search-users-input', 'value'),
    Input('users-pagination', 'active_page'),
    Input('users-refresh', 'data'),
    Input('users-tabs', 'active_tab'),
    State('session-store', 'data'),
)
def render_users_page(
    pathname: Optional[str] = None,
    search_term: Optional[str] = None,
    active_page: Optional[int] = None,
    refresh: Any = None,
    active_tab: Optional[str] = None,
    session_store_data: Optional[dict] = None,
):
    """
    Render the Users tab (the table, or a single user's page) or the Teams tab. Administrators
    see every user and team with their menus; others see the users and teams they share.
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
            HIDDEN, 1, 1, HIDDEN, True, HIDDEN, HIDDEN, HIDDEN, HIDDEN,
        )

    username = _parse_username_from_pathname(pathname)
    if username:
        return (
            build_user_detail(username, who),
            HIDDEN, 1, 1, HIDDEN, True, HIDDEN, HIDDEN, HIDDEN, HIDDEN,
        )

    if (active_tab or 'users') == 'teams':
        try:
            teams = visible_teams(access.list_teams(), who)
            users_count = len(get_user_rows(teams=[])) if who['admin'] else None
            content = build_teams_table(
                teams, who,
                pipe_counts=(team_pipe_counts() if who['admin'] or teams else {}),
                users_count=users_count,
            )
        except Exception as e:
            content = alert_from_success_tuple((False, f"Failed to read the teams: {e}"))
        return (
            content, HIDDEN, 1, 1, HIDDEN,
            not who['admin'], {}, HIDDEN, (SHOWN if who['admin'] else HIDDEN), SHOWN,
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
    show_search = bool(search_term) or total > 1
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
        (SHOWN if who['admin'] else HIDDEN),
        HIDDEN,
        SHOWN,
    )


### The tab is in the URL (`/dash/users`, `/dash/teams`), so a reload or a link opens it.
dash_app.clientside_callback(
    """
    function(tab) {
        if (!tab) return dash_clientside.no_update;
        const path = window.location.pathname.replace(/\\/+$/, '');
        if (!['/dash/users', '/dash/teams'].includes(path)) return dash_clientside.no_update;
        const target = tab === 'teams' ? '/dash/teams' : '/dash/users';
        if (path !== target) {
            window.history.replaceState(window.history.state, '', target + window.location.search);
        }
        return dash_clientside.no_update;
    }
    """,
    Output('users-content-div', 'title'),
    Input('users-tabs', 'active_tab'),
)


# ------------------------------------------------------------------------------------------------
# The users' dialogs
# ------------------------------------------------------------------------------------------------

DIALOG_OUTPUTS = (
    Output('users-add-modal', 'is_open'),
    Output('users-edit-modal', 'is_open'),
    Output('users-password-modal', 'is_open'),
    Output('users-delete-modal', 'is_open'),
    Output('users-target', 'data'),
    Output('users-add-title', 'children'),
    Output('users-edit-title', 'children'),
    Output('users-password-title', 'children'),
    Output('users-delete-title', 'children'),
    Output('users-delete-name', 'children'),
    Output('users-add-username', 'value'),
    Output('users-add-password', 'value'),
    Output('users-add-confirm', 'value'),
    Output('users-add-email', 'value'),
    Output('users-add-type', 'value'),
    Output('users-add-role', 'value'),
    Output('users-add-teams', 'options'),
    Output('users-add-teams', 'value'),
    Output('users-edit-type', 'value'),
    Output('users-edit-role', 'value'),
    Output({'type': 'users-edit-permissions', 'index': ALL}, 'value'),
    Output('users-edit-teams', 'options'),
    Output('users-edit-teams', 'value'),
    Output('users-edit-manages', 'options'),
    Output('users-edit-manages', 'value'),
    Output('users-edit-primary', 'options'),
    Output('users-edit-primary', 'value'),
    Output('users-password-value', 'value'),
    Output('users-password-confirm', 'value'),
    Output('users-delete-input', 'value'),
    Output('users-add-alert', 'children'),
    Output('users-edit-alert', 'children'),
    Output('users-password-alert', 'children'),
    Output('users-delete-alert', 'children'),
)


def _grid_values(permissions: List[str]) -> List[List[str]]:
    """The permissions grid's checklists' values (one list per group)."""
    return [
        [permission for permission, _ in items if permission in permissions]
        for _, items in access.PERMISSION_GROUPS
    ]


def _dialog(
    which: Optional[str] = None,
    row: Optional[Dict[str, Any]] = None,
    team_names: Optional[List[str]] = None,
):
    """The outputs of `open_users_dialog`: `which` dialog open (or all closed), its fields fresh."""
    username = (row or {}).get('username', None)
    target = {'username': username, 'action': which} if which and which != 'add' else None
    team_names = [name for name in (team_names or []) if name != access.EVERYONE]
    team_options = [{'label': name, 'value': name} for name in team_names]
    teams = dict((row or {}).get('teams') or {})
    role = 'default' if not (row or {}).get('explicit') else ((row or {}).get('role') or 'default')
    if role == 'admin':
        role = 'default'
    permissions = list((row or {}).get('permissions') or [])
    primary = (row or {}).get('primary_team', None) or access.EVERYONE
    primary_options = [{'label': access.EVERYONE, 'value': access.EVERYONE}] + [
        {'label': name, 'value': name} for name in sorted(teams)
    ]
    return (
        which == 'add',
        which == 'edit',
        which == 'password',
        which == 'delete',
        target,
        'Add user',
        ['Edit ', html.B(username or '')],
        ['Reset the password of ', html.B(username or '')],
        ['Delete user ', html.B(username or ''), '?'],
        username or '',
        '', '', '', '', 'user', 'default', team_options, [],
        (row or {}).get('type', 'user'),
        role,
        _grid_values(permissions),
        team_options,
        sorted(teams),
        team_options,
        sorted(team for team, team_role in teams.items() if team_role == 'manager'),
        primary_options,
        (primary if primary in [option['value'] for option in primary_options] else access.EVERYONE),
        '', '',
        '',
        [], [], [], [],
    )


@dash_app.callback(
    *DIALOG_OUTPUTS,
    Input('users-add-button', 'n_clicks'),
    Input({'type': 'user-action', 'action': ALL, 'index': ALL}, 'n_clicks'),
    Input('users-add-cancel', 'n_clicks'),
    Input('users-edit-cancel', 'n_clicks'),
    Input('users-password-cancel', 'n_clicks'),
    Input('users-delete-cancel', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_users_dialog(add_clicks, action_clicks, *cancel_clicks_and_session):
    """Open the Add user dialog or a user's Edit / Reset password / Delete dialog."""
    session_store_data = cancel_clicks_and_session[-1]
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered[0]['value']:
        raise PreventUpdate
    trigger = ctx.triggered_id
    if trigger in ('users-add-cancel', 'users-edit-cancel', 'users-password-cancel', 'users-delete-cancel'):
        return _dialog()
    if not viewer(session_store_data)['admin']:
        raise PreventUpdate
    try:
        teams = access.list_teams()
    except Exception:
        teams = []
    team_names = [team['name'] for team in teams]
    if trigger == 'users-add-button':
        return _dialog('add', team_names=team_names)
    if not isinstance(trigger, dict) or trigger.get('type') != 'user-action':
        raise PreventUpdate
    username = trigger.get('index')
    action = trigger.get('action')
    if action not in ('edit', 'password', 'delete'):
        raise PreventUpdate
    rows = {row['username']: row for row in get_user_rows(teams=teams)}
    if username not in rows:
        raise PreventUpdate
    return _dialog(action, rows[username], team_names=team_names)


### The role presets set the permissions grid; editing the grid makes the role Custom (or the
### preset it now equals). An administrator holds every permission: no grid.
dash_app.clientside_callback(
    """
    function(role, grids, type, presets) {
        const ctx = dash_clientside.callback_context;
        const triggered = (ctx.triggered || []).map(t => t.prop_id);
        const groups = (presets || {}).groups || [];
        const rights = {display: type === 'admin' ? 'none' : ''};
        if (triggered.some(t => t.startsWith('users-edit-role')) && role && role !== 'custom') {
            const wanted = (presets.roles || {})[role === 'default' ? presets.default : role] || [];
            const values = groups.map(items => items.filter(p => wanted.includes(p)));
            return [dash_clientside.no_update, values, rights];
        }
        if (triggered.some(t => t.includes('users-edit-permissions'))) {
            const chosen = new Set([].concat(...(grids || [])));
            let match = 'custom';
            for (const [name, perms] of Object.entries(presets.roles || {})) {
                if (perms.length === chosen.size && perms.every(p => chosen.has(p))) { match = name; break; }
            }
            return [match === role ? dash_clientside.no_update : match, dash_clientside.no_update, rights];
        }
        return [dash_clientside.no_update, dash_clientside.no_update, rights];
    }
    """,
    Output('users-edit-role', 'value', allow_duplicate=True),
    Output({'type': 'users-edit-permissions', 'index': ALL}, 'value', allow_duplicate=True),
    Output('users-edit-rights', 'style'),
    Input('users-edit-role', 'value'),
    Input({'type': 'users-edit-permissions', 'index': ALL}, 'value'),
    Input('users-edit-type', 'value'),
    State('users-role-presets', 'data'),
    prevent_initial_call=True,
)


### The primary team is one of the user's teams (or Everyone).
dash_app.clientside_callback(
    """
    function(teams, manages, current) {
        const names = Array.from(new Set([].concat(teams || [], manages || []))).sort();
        const options = [{label: 'Everyone', value: 'Everyone'}].concat(names.map(n => ({label: n, value: n})));
        const value = current === 'Everyone' ? current : (names.includes(current) ? current : (names[0] || 'Everyone'));
        return [options, value];
    }
    """,
    Output('users-edit-primary', 'options', allow_duplicate=True),
    Output('users-edit-primary', 'value', allow_duplicate=True),
    Input('users-edit-teams', 'value'),
    Input('users-edit-manages', 'value'),
    State('users-edit-primary', 'value'),
    prevent_initial_call=True,
)


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
    State('users-add-role', 'value'),
    State('users-add-teams', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_add_user(
    n_clicks, username, password, confirm, user_type, email, role, teams, refresh, session_store_data,
):
    """Register the new user (with the chosen role and teams)."""
    if not n_clicks:
        raise PreventUpdate
    who, refusal = _admin_or_refusal(session_store_data)
    if refusal is not None:
        return no_update, refusal, no_update, no_update
    valid, message = validate_new_user(username, password, confirm, email)
    if not valid:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    if role not in ('default', 'viewer', 'editor', 'operator', None):
        return no_update, alert_from_success_tuple((False, "Choose a role.")), no_update, no_update
    try:
        known = set(access.team_names())
    except Exception:
        known = set()
    teams = [team for team in (teams or []) if team in known]
    try:
        success, message = add_user(username, password, user_type, email, role=role, teams=teams)
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


@dash_app.callback(
    Output('users-edit-modal', 'is_open', allow_duplicate=True),
    Output('users-edit-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('users-edit-submit', 'n_clicks'),
    State('users-target', 'data'),
    State('users-edit-type', 'value'),
    State('users-edit-role', 'value'),
    State({'type': 'users-edit-permissions', 'index': ALL}, 'value'),
    State('users-edit-teams', 'value'),
    State('users-edit-manages', 'value'),
    State('users-edit-primary', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_edit_user(
    n_clicks, target, user_type, role, grids, teams, manages, primary, refresh, session_store_data,
):
    """
    Save a user's type (never demoting the only admin), role or permissions, teams and primary
    team; their sessions read the new rights at once (a demoted user signs in again).
    """
    if not n_clicks or not target or target.get('action') != 'edit':
        raise PreventUpdate
    who, refusal = _admin_or_refusal(session_store_data)
    if refusal is not None:
        return no_update, refusal, no_update, no_update
    username = target.get('username')
    try:
        all_teams = access.list_teams()
    except Exception as e:
        return no_update, alert_from_success_tuple((False, f"Could not read the teams: {e}")), no_update, no_update
    rows = get_user_rows(teams=all_teams)
    current = {row['username']: row for row in rows}.get(username)
    if current is None:
        return no_update, alert_from_success_tuple((False, f"User '{username}' does not exist.")), no_update, no_update
    user_type = normalize_user_type(user_type)
    allowed, message = check_last_admin(rows, username, new_type=user_type)
    if not allowed:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    known = {team['name'] for team in all_teams if not team['builtin']}
    teams = [team for team in (teams or []) if team in known]
    manages = [team for team in (manages or []) if team in known]
    permissions = sorted({
        permission for group in (grids or []) for permission in (group or [])
        if permission in access.GRID_PERMISSIONS
    })
    role = role if role in ('default', 'viewer', 'editor', 'operator', 'custom') else 'default'
    if primary not in set(teams) | set(manages) | {access.EVERYONE}:
        primary = access.EVERYONE

    try:
        if current['type'] != user_type:
            success, message = change_user_type(username, user_type)
            if not success:
                return no_update, alert_from_success_tuple((False, message)), no_update, no_update
        if user_type != 'admin':
            success, message = set_user_rights(username, role, permissions, primary_team=primary)
        else:
            success, message = set_user_rights(username, None, None, primary_team=primary) if (
                primary != current.get('primary_team')
            ) else (True, '')
        if not success:
            return no_update, alert_from_success_tuple((False, message)), no_update, no_update
        success, message = set_user_teams(username, teams, manages, current.get('teams') or {})
        if not success:
            return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    except Exception as e:
        return no_update, alert_from_success_tuple((False, str(e))), no_update, no_update

    ### A demoted user signs in again; anyone else's sessions read their new rights at once (the
    ### one making the change keeps their session).
    demoted = current['type'] == 'admin' and user_type != 'admin'
    end_user_sessions(
        username,
        keep_session_id=(_session_id(session_store_data) if username == who['username'] else None),
        refresh_only=not demoted,
    )
    words = 'an admin' if user_type == 'admin' else (
        'a user' if current['type'] == 'admin' else None
    )
    text = f"'{username}' is now {words}." if words else f"Saved '{username}'."
    return False, [], alert_from_success_tuple((True, text)), (refresh or 0) + 1


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
    from meerschaum.api import get_api_connector
    if username not in set(get_api_connector().get_users() or []):
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
    rows = get_user_rows(teams=[])
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


# ------------------------------------------------------------------------------------------------
# The teams' dialogs
# ------------------------------------------------------------------------------------------------

def _team_by_name(name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not name:
        return None
    try:
        return next((team for team in access.list_teams() if team['name'] == name), None)
    except Exception:
        return None


TEAM_DIALOG_OUTPUTS = (
    Output('teams-new-modal', 'is_open'),
    Output('teams-rename-modal', 'is_open'),
    Output('teams-delete-modal', 'is_open'),
    Output('teams-members-modal', 'is_open'),
    Output('teams-target', 'data'),
    Output('teams-new-title', 'children'),
    Output('teams-rename-title', 'children'),
    Output('teams-delete-title', 'children'),
    Output('teams-members-title', 'children'),
    Output('teams-delete-name', 'children'),
    Output('teams-delete-consequence', 'children'),
    Output('teams-members-body', 'children'),
    Output('teams-new-name', 'value'),
    Output('teams-rename-name', 'value'),
    Output('teams-delete-input', 'value'),
    Output('teams-new-alert', 'children'),
    Output('teams-rename-alert', 'children'),
    Output('teams-delete-alert', 'children'),
    Output('teams-members-alert', 'children'),
)


def _team_dialog(which: Optional[str] = None, team: Optional[Dict[str, Any]] = None,
                 consequence: str = '', members_body: Any = None):
    name = (team or {}).get('name', '')
    target = {'team': name, 'action': which} if which and which != 'new' else None
    return (
        which == 'new',
        which == 'rename',
        which == 'delete',
        which == 'members',
        target,
        'New team',
        ['Rename team ', html.B(name)],
        ['Delete team ', html.B(name), '?'],
        ['Members of ', html.B(name)],
        name,
        consequence,
        members_body if members_body is not None else [],
        '', name, '',
        [], [], [], [],
    )


def delete_consequence(name: str) -> str:
    """`3 pipes and 1 job move to Everyone.`: what deleting a team does."""
    pipes = team_pipe_counts().get(name, 0)
    jobs = team_job_count(name)
    parts = []
    if pipes:
        parts.append(f"{pipes} pipe{'s' if pipes != 1 else ''}")
    if jobs:
        parts.append(f"{jobs} job{'s' if jobs != 1 else ''}")
    if not parts:
        return "The team has no pipes or jobs."
    return f"{' and '.join(parts)} move to {access.EVERYONE}."


@dash_app.callback(
    *TEAM_DIALOG_OUTPUTS,
    Input('teams-new-button', 'n_clicks'),
    Input({'type': 'team-action', 'action': ALL, 'index': ALL}, 'n_clicks'),
    Input('teams-new-cancel', 'n_clicks'),
    Input('teams-rename-cancel', 'n_clicks'),
    Input('teams-delete-cancel', 'n_clicks'),
    Input('teams-members-close', 'n_clicks'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def open_teams_dialog(new_clicks, action_clicks, *cancel_clicks_and_session):
    """New team, or a team's Rename / Delete (administrators) or Members (and its managers)."""
    session_store_data = cancel_clicks_and_session[-1]
    ctx = dash.callback_context
    if not ctx.triggered or not ctx.triggered[0]['value']:
        raise PreventUpdate
    trigger = ctx.triggered_id
    if trigger in ('teams-new-cancel', 'teams-rename-cancel', 'teams-delete-cancel', 'teams-members-close'):
        return _team_dialog()
    who = viewer(session_store_data)
    if trigger == 'teams-new-button':
        if not who['admin']:
            raise PreventUpdate
        return _team_dialog('new')
    if not isinstance(trigger, dict) or trigger.get('type') != 'team-action':
        raise PreventUpdate
    team = _team_by_name(trigger.get('index'))
    action = trigger.get('action')
    if team is None or action not in ('rename', 'delete', 'members'):
        raise PreventUpdate
    if action in ('rename', 'delete') and (not who['admin'] or team['builtin']):
        raise PreventUpdate
    if action == 'members':
        if not may_manage_team(who, team):
            raise PreventUpdate
        return _team_dialog('members', team, members_body=_members_body(team))
    if action == 'delete':
        return _team_dialog('delete', team, consequence=delete_consequence(team['name']))
    return _team_dialog('rename', team)


def _members_body(team: Dict[str, Any]) -> Any:
    from meerschaum.api import get_api_connector
    try:
        user_names = sorted(str(name) for name in (get_api_connector().get_users() or []))
    except Exception:
        user_names = []
    return build_team_members_editor(team, user_names)


@dash_app.callback(
    Output('teams-new-modal', 'is_open', allow_duplicate=True),
    Output('teams-new-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('teams-new-submit', 'n_clicks'),
    State('teams-new-name', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_new_team(n_clicks, name, refresh, session_store_data):
    """Create a team (administrators)."""
    if not n_clicks:
        raise PreventUpdate
    if not viewer(session_store_data)['admin']:
        return no_update, alert_from_success_tuple((False, "Only administrators can create teams.")), no_update, no_update
    try:
        existing = access.team_names()
    except Exception as e:
        return no_update, alert_from_success_tuple((False, str(e))), no_update, no_update
    valid, message = validate_team_name(name, existing)
    if not valid:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    name = str(name).strip()
    success, message = access.create_team(name)
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    access.forget()
    return False, [], alert_from_success_tuple((True, f"Created team '{name}'.")), (refresh or 0) + 1


@dash_app.callback(
    Output('teams-rename-modal', 'is_open', allow_duplicate=True),
    Output('teams-rename-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('teams-rename-submit', 'n_clicks'),
    State('teams-target', 'data'),
    State('teams-rename-name', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_rename_team(n_clicks, target, new_name, refresh, session_store_data):
    """Rename a team (administrators; never Everyone). Its pipes and jobs follow the new name."""
    if not n_clicks or not target or target.get('action') != 'rename':
        raise PreventUpdate
    if not viewer(session_store_data)['admin']:
        return no_update, alert_from_success_tuple((False, "Only administrators can rename teams.")), no_update, no_update
    team = _team_by_name(target.get('team'))
    if team is None or team['builtin']:
        return no_update, alert_from_success_tuple((False, "This team cannot be renamed.")), no_update, no_update
    new_name = str(new_name or '').strip()
    if new_name == team['name']:
        return False, [], no_update, no_update
    others = [name for name in access.team_names() if name != team['name']]
    valid, message = validate_team_name(new_name, others)
    if not valid:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    success, message = access.rename_team(team['name'], new_name)
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    access.forget()
    return (
        False, [],
        alert_from_success_tuple((True, f"Renamed team '{team['name']}' to '{new_name}'.")),
        (refresh or 0) + 1,
    )


@dash_app.callback(
    Output('teams-delete-submit', 'disabled'),
    Input('teams-delete-input', 'value'),
    State('teams-target', 'data'),
)
def enable_delete_team(typed_name, target):
    """Delete is enabled once the team's name has been typed."""
    return not target or (typed_name or '').strip() != target.get('team')


@dash_app.callback(
    Output('teams-delete-modal', 'is_open', allow_duplicate=True),
    Output('teams-delete-alert', 'children', allow_duplicate=True),
    Output('users-alert-div', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('teams-delete-submit', 'n_clicks'),
    State('teams-target', 'data'),
    State('teams-delete-input', 'value'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def submit_delete_team(n_clicks, target, typed_name, refresh, session_store_data):
    """Delete a team (administrators; never Everyone): its pipes and jobs move to Everyone."""
    if not n_clicks or not target or target.get('action') != 'delete':
        raise PreventUpdate
    if not viewer(session_store_data)['admin']:
        return no_update, alert_from_success_tuple((False, "Only administrators can delete teams.")), no_update, no_update
    team = _team_by_name(target.get('team'))
    if team is None or team['builtin']:
        return no_update, alert_from_success_tuple((False, "This team cannot be deleted.")), no_update, no_update
    if (typed_name or '').strip() != team['name']:
        return no_update, alert_from_success_tuple((False, "Type the team's name to delete it.")), no_update, no_update
    success, message = access.delete_team(team['name'])
    if not success:
        return no_update, alert_from_success_tuple((False, message)), no_update, no_update
    access.forget()
    return False, [], alert_from_success_tuple((True, f"Deleted team '{team['name']}'.")), (refresh or 0) + 1


@dash_app.callback(
    Output('teams-members-body', 'children', allow_duplicate=True),
    Output('teams-members-alert', 'children', allow_duplicate=True),
    Output('users-refresh', 'data', allow_duplicate=True),
    Input('team-add-button', 'n_clicks', allow_optional=True),
    Input({'type': 'team-member-role', 'index': ALL}, 'value'),
    Input({'type': 'team-member-remove', 'index': ALL}, 'n_clicks'),
    State('team-add-member', 'value', allow_optional=True),
    State('team-add-role', 'value', allow_optional=True),
    State('teams-target', 'data'),
    State('users-refresh', 'data'),
    State('session-store', 'data'),
    prevent_initial_call=True,
)
def edit_team_members(add_clicks, roles, remove_clicks, new_member, new_role, target, refresh, session_store_data):
    """Add a member, change a member's role, or remove a member (the team's managers and admins)."""
    ctx = dash.callback_context
    if not target or target.get('action') != 'members' or not ctx.triggered:
        raise PreventUpdate
    trigger = ctx.triggered_id
    who = viewer(session_store_data)
    team = _team_by_name(target.get('team'))
    if team is None or not may_manage_team(who, team):
        return no_update, alert_from_success_tuple((False, "You may not change this team's members.")), no_update
    from meerschaum.api import get_api_connector
    users = set(str(name) for name in (get_api_connector().get_users() or []))
    if trigger == 'team-add-button':
        if not add_clicks or not new_member:
            raise PreventUpdate
        if new_member not in users:
            return no_update, alert_from_success_tuple((False, f"User '{new_member}' does not exist.")), no_update
        role = 'manager' if new_role == 'manager' else 'member'
        if team['builtin'] and role != 'manager':
            raise PreventUpdate
        success, message = access.set_team_member(team['name'], new_member, role)
    elif isinstance(trigger, dict) and trigger.get('type') == 'team-member-role':
        username = trigger.get('index')
        value = next((item['value'] for item in ctx.triggered if item.get('value')), None)
        if username not in team['members'] or value not in ('member', 'manager'):
            raise PreventUpdate
        if team['members'].get(username) == value:
            raise PreventUpdate
        ### Everyone's members are every user: a manager made a member is just removed.
        if team['builtin'] and value == 'member':
            success, message = access.set_team_member(team['name'], username, None)
        else:
            success, message = access.set_team_member(team['name'], username, value)
    elif isinstance(trigger, dict) and trigger.get('type') == 'team-member-remove':
        if not any(remove_clicks or []):
            raise PreventUpdate
        username = trigger.get('index')
        if username not in team['members']:
            raise PreventUpdate
        success, message = access.set_team_member(team['name'], username, None)
    else:
        raise PreventUpdate
    access.forget()
    updated = _team_by_name(team['name']) or team
    return (
        _members_body(updated),
        (alert_from_success_tuple((False, message)) if not success else []),
        (refresh or 0) + 1,
    )
