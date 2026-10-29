#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's **Users & Teams** page (`/dash/users`, `/dash/teams`):

- **Users**: the users of the API's user store as a table (User, Type, Role, Teams, Email) and,
  for administrators, Add user, Edit (type, role preset and permissions grid, teams, primary
  team), Reset password and Delete;
- **Teams**: the teams (Everyone first), with New team, Rename and Delete for administrators and
  Members for administrators and the team's managers.

Who sees what (`meerschaum.api.dash.access`): administrators see every user; other users see
themselves and the members of their teams (a manager of Everyone sees every user). An email
address is shown to administrators and to the managers of a team the user is in (AC13).

The instance connector's own user and team methods do the work (`register_user`, `edit_user`,
`delete_user`, `get_users`, `mrsm.Team`); nothing here writes the stores directly.
"""

from __future__ import annotations

import json
import re

import meerschaum as mrsm
from meerschaum.utils.typing import List, Tuple, SuccessTuple, Optional, WebState, Dict, Any
from meerschaum.utils.packages import import_dcc, import_html
from meerschaum.api import get_api_connector, CHECK_UPDATE
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.core import Plugin, User
from meerschaum.api.dash import debug
from meerschaum.api.dash import access

USERS_PER_PAGE: int = 50
USERS_GRID_COLUMNS: int = 3

#: The user types the console offers (`None` in the users table reads as `user`).
USER_TYPES: Tuple[str, str] = ('user', 'admin')

#: The role choices of the Add user and Edit dialogs: `(value, label)` (`default` is no role of
#: the user's own).
ROLE_CHOICES: Tuple[Tuple[str, str], ...] = (
    ('default', 'Default'),
    ('viewer', 'Viewer'),
    ('editor', 'Editor'),
    ('operator', 'Operator'),
    ('custom', 'Custom'),
)

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")


########################################################################################
# Reading users.
########################################################################################

def _users_table_pipe(conn) -> Optional[mrsm.Pipe]:
    """
    A read-only view of the instance's users table (`mrsm_users`), or `None` when the instance
    keeps its users elsewhere (an API instance answers through its routes).
    """
    if getattr(conn, 'type', None) == 'sql':
        return mrsm.Pipe(
            'mrsm', 'users',
            instance=conn,
            target='mrsm_users',
            temporary=True,
            static=True,
        )
    get_users_pipe = getattr(conn, 'get_users_pipe', None)
    if not callable(get_users_pipe) or getattr(conn, 'type', None) == 'api':
        return None
    try:
        return get_users_pipe()
    except Exception:
        return None


def normalize_user_type(user_type: Optional[str]) -> str:
    """`admin`, or `user` for every other type (`None` included)."""
    return 'admin' if str(user_type or '').strip().lower() == 'admin' else 'user'


def _attributes_of(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def get_user_rows(conn=None, teams: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    The instance's users, sorted by username: `[{'username', 'type', 'email', 'role',
    'permissions', 'explicit', 'teams', 'primary_team'}]` (`type` is `admin` or `user`; `role` a
    preset name, `custom` or `admin`; `teams` is `{team: 'member' | 'manager'}` without
    Everyone; `email` is `None` when the instance does not say).
    """
    conn = conn if conn is not None else get_api_connector()
    names = sorted(str(name) for name in (conn.get_users(debug=debug) or []))
    details: Dict[str, Dict[str, Any]] = {}
    pipe = _users_table_pipe(conn)
    if pipe is not None:
        try:
            docs = pipe.get_docs(
                select_columns=['username', 'email', 'user_type', 'attributes'], debug=debug,
            ) or []
            details = {str(doc.get('username')): doc for doc in docs if doc.get('username') is not None}
        except Exception:
            details = {}
    if teams is None:
        try:
            teams = access.list_teams(conn)
        except Exception:
            teams = []
    memberships = access.teams_by_user(teams)
    rows = []
    for name in names:
        doc = details.get(name)
        user = User(name, instance=conn)
        if doc is not None and 'user_type' in doc:
            user_type = doc.get('user_type')
        else:
            try:
                user_type = conn.get_user_type(user, debug=debug)
            except Exception:
                user_type = None
        if doc is not None and 'attributes' in doc:
            attributes = _attributes_of(doc.get('attributes'))
        else:
            try:
                attributes = conn.get_user_attributes(user, debug=debug) or {}
            except Exception:
                attributes = {}
        email = (doc or {}).get('email', None)
        try:
            rights = access.user_rights(user_type, attributes)
        except Exception:
            rights = {'role': 'admin' if normalize_user_type(user_type) == 'admin' else 'custom',
                      'permissions': set(), 'explicit': False}
        user_teams = memberships.get(name, {})
        primary = attributes.get('primary_team', None)
        if primary and access.core().is_everyone(primary):
            primary = access.EVERYONE
        elif not primary or primary not in user_teams:
            primary = sorted(user_teams)[0] if user_teams else access.EVERYONE
        rows.append({
            'username': name,
            'type': normalize_user_type(user_type),
            'email': (str(email) if email not in (None, '') else None),
            'role': rights['role'],
            'permissions': sorted(rights['permissions']),
            'explicit': bool(rights['explicit']),
            'teams': dict(sorted(user_teams.items())),
            'primary_team': primary,
            'attributes': attributes,
        })
    return rows


def admin_usernames(rows: List[Dict[str, Any]]) -> List[str]:
    """The administrators among `rows`."""
    return [row['username'] for row in rows if row.get('type') == 'admin']


def check_last_admin(
    rows: List[Dict[str, Any]],
    username: str,
    new_type: Optional[str] = None,
    deleting: bool = False,
) -> SuccessTuple:
    """
    Refuse to delete or demote the instance's only administrator (whoever asks, the
    administrator included): it would leave nobody able to manage users and jobs.
    """
    admins = admin_usernames(rows)
    removes_admin = deleting or normalize_user_type(new_type) != 'admin'
    if username in admins and len(admins) <= 1 and removes_admin:
        verb = 'delete' if deleting else 'demote'
        return False, (
            f"Cannot {verb} '{username}': it is the only admin. Make another user an admin first."
        )
    return True, "Success"


########################################################################################
# Who is asking.
########################################################################################

def viewer(session_store_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Who is looking at the Users & Teams page: `{'username', 'active', 'admin', 'sees_all',
    'viewer'}` (`viewer` is the `access.Viewer`). Under `--no-auth` everyone is an
    administrator. Administrators are read again (not a session's cached flag), since these are
    user-management decisions.
    """
    from meerschaum.api import no_auth
    if no_auth:
        who = access.viewer_for(None)
        return {'username': None, 'active': True, 'admin': True, 'sees_all': True, 'viewer': who}
    session_id = (
        (session_store_data or {}).get('session-id', None)
        if isinstance(session_store_data, dict) else None
    )
    from meerschaum.api.dash.sessions import get_username_from_session
    username = get_username_from_session(str(session_id)) if session_id else None
    if not username:
        return {'username': None, 'active': False, 'admin': False, 'sees_all': False,
                'viewer': access.ANONYMOUS}
    who = access.viewer_for(username)
    if not who.signed_in:
        return {'username': None, 'active': False, 'admin': False, 'sees_all': False,
                'viewer': access.ANONYMOUS}
    return {
        'username': username,
        'active': True,
        'admin': who.is_admin,
        'sees_all': who.is_admin,
        'viewer': who,
    }


def visible_user_rows(
    rows: List[Dict[str, Any]],
    who: Dict[str, Any],
    teams: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    The users a viewer may see: every user for administrators and managers of Everyone, else
    themselves and the members of their teams (other than Everyone).
    """
    if who.get('admin') or who.get('sees_all'):
        return list(rows)
    username = who.get('username')
    if not username:
        return []
    if username in access.everyone_managers(teams or []):
        return list(rows)
    viewer_teams = set((who.get('viewer') or access.ANONYMOUS).own_teams)
    return [
        row for row in rows
        if row['username'] == username or (set(row['teams']) & viewer_teams)
    ]


def email_visible(who: Dict[str, Any], row: Dict[str, Any]) -> bool:
    """Whether the viewer may see this user's email address (AC13)."""
    viewer_object = who.get('viewer') or access.ANONYMOUS
    return access.can_see_email(viewer_object, row['username'], list(row.get('teams', {})))


def sees_any_email(who: Dict[str, Any], teams: Optional[List[Dict[str, Any]]] = None) -> bool:
    """Whether the Email column has anything for this viewer: administrators and managers."""
    viewer_object = who.get('viewer') or access.ANONYMOUS
    if who.get('admin') or viewer_object.is_admin or viewer_object.no_auth:
        return True
    if viewer_object.managed_teams:
        return True
    return bool(who.get('username')) and who.get('username') in access.everyone_managers(teams or [])


########################################################################################
# Changing users.
########################################################################################

def validate_new_user(username: str, password: str, confirm: Optional[str], email: Optional[str]) -> SuccessTuple:
    """Check the Add user form before asking the instance."""
    username = str(username or '').strip()
    if not username:
        return False, "Enter a username."
    try:
        from meerschaum.connectors.sql._users import valid_username
        valid, message = valid_username(username)
    except Exception:
        valid, message = True, ''
    if not valid:
        return False, message.strip()
    password_valid, password_message = validate_password(password, confirm)
    if not password_valid:
        return False, password_message
    if email and not EMAIL_REGEX.match(str(email).strip()):
        return False, f"'{email}' is not a valid email address."
    return True, "Success"


def validate_password(password: Optional[str], confirm: Optional[str]) -> SuccessTuple:
    """A password long enough, typed twice the same."""
    from meerschaum._internal.static import STATIC_CONFIG
    min_length = STATIC_CONFIG['users']['min_password_length']
    if not password or len(password) < min_length:
        return False, f"Passwords must have at least {min_length} characters."
    if confirm is not None and password != confirm:
        return False, "The passwords do not match."
    return True, "Success"


def add_user(
    username: str,
    password: str,
    user_type: str,
    email: Optional[str],
    conn=None,
    role: Optional[str] = None,
    permissions: Optional[List[str]] = None,
    teams: Optional[List[str]] = None,
) -> SuccessTuple:
    """
    Register a user with the instance's `register_user`: the default role unless `role` names a
    preset (or `custom` with `permissions`), and a member of `teams`.
    """
    conn = conn if conn is not None else get_api_connector()
    attributes: Dict[str, Any] = {}
    if normalize_user_type(user_type) != 'admin' and role not in (None, '', 'default'):
        try:
            attributes = access.attributes_with_rights({}, role, permissions)
        except Exception as e:
            return False, f"Invalid role: {e}"
    attributes = {key: value for key, value in attributes.items() if value is not None}
    user = User(
        str(username).strip(),
        password,
        type=('admin' if normalize_user_type(user_type) == 'admin' else None),
        email=(str(email).strip() or None) if email else None,
        attributes=(attributes or None),
        instance=conn,
    )
    success, message = conn.register_user(user, debug=debug)
    if not success:
        return success, message
    for team in (teams or []):
        if not team or team == access.EVERYONE:
            continue
        team_success, team_message = access.set_team_member(team, str(username).strip(), 'member', conn=conn)
        if not team_success:
            return False, f"Added '{username}', but not to team '{team}': {team_message}"
    return success, message


def change_user_type(username: str, user_type: str, conn=None) -> SuccessTuple:
    """Make a user an admin or a regular user (`edit_user` with only the type set)."""
    conn = conn if conn is not None else get_api_connector()
    ### An empty password, email and attributes leave them as they are.
    user = User(username, '', type=normalize_user_type(user_type), email='', instance=conn)
    return conn.edit_user(user, debug=debug)


def set_user_rights(
    username: str,
    role: Optional[str],
    permissions: Optional[List[str]] = None,
    primary_team: Optional[str] = None,
    conn=None,
) -> SuccessTuple:
    """
    Store a user's role (a preset, `custom` with `permissions`, or `default`) and primary team,
    keeping their other attributes.
    """
    conn = conn if conn is not None else get_api_connector()
    user = User(username, instance=conn)
    try:
        current = conn.get_user_attributes(user, debug=debug) or {}
    except Exception as e:
        return False, f"Could not read '{username}': {e}"
    attributes = access.attributes_with_rights(current, role, permissions)
    if primary_team is not None:
        attributes['primary_team'] = primary_team
    user = User(username, '', type='', email='', attributes=attributes, instance=conn)
    setter = getattr(conn, 'set_user_attributes', None)
    if callable(setter):
        return setter(user, attributes, debug=debug)
    return conn.edit_user(user, debug=debug)


def set_user_teams(
    username: str,
    member_of: List[str],
    manager_of: List[str],
    current: Dict[str, str],
    conn=None,
) -> SuccessTuple:
    """Make the user's teams (other than Everyone) `member_of`, managing `manager_of`."""
    conn = conn if conn is not None else get_api_connector()
    wanted = {team: ('manager' if team in manager_of else 'member') for team in member_of if team != access.EVERYONE}
    for team in sorted(set(manager_of) - set(wanted) - {access.EVERYONE}):
        wanted[team] = 'manager'
    for team, role in sorted(wanted.items()):
        if current.get(team) == role:
            continue
        success, message = access.set_team_member(team, username, role, conn=conn)
        if not success:
            return False, message
    for team in sorted(set(current) - set(wanted)):
        success, message = access.set_team_member(team, username, None, conn=conn)
        if not success:
            return False, message
    return True, "Success"


def reset_user_password(username: str, password: str, conn=None) -> SuccessTuple:
    """Set a user's password (`edit_user` with only the password set)."""
    conn = conn if conn is not None else get_api_connector()
    user = User(username, password, type='', email='', instance=conn)
    return conn.edit_user(user, debug=debug)


def remove_user(username: str, conn=None) -> SuccessTuple:
    """Delete a user with the instance's `delete_user` (their pipes stay with their teams)."""
    conn = conn if conn is not None else get_api_connector()
    return conn.delete_user(User(username, instance=conn), debug=debug)


def end_user_sessions(
    username: str,
    keep_session_id: Optional[str] = None,
    refresh_only: bool = False,
) -> int:
    """
    Sign a user out of the console everywhere (a deleted, demoted or reset user must not keep a
    session that still carries the old rights), except `keep_session_id`. With `refresh_only`
    (a promotion, or new rights), every session is kept and only its rights are read again.
    Returns how many sessions ended.
    """
    from meerschaum.api.dash import sessions
    access.forget(username)
    ended = 0
    session_ids: List[str] = []
    try:
        if sessions._use_instance_store():
            pipe = sessions.get_instance_sessions_pipe()
            if pipe.exists():
                for doc in pipe.get_docs(select_columns=['session_id', 'data']) or []:
                    try:
                        data = json.loads(doc.get('data') or '{}')
                    except Exception:
                        continue
                    if isinstance(data, dict) and data.get('username') == username:
                        session_ids.append(str(doc.get('session_id')))
        else:
            from meerschaum.api import get_cache_connector
            cache = get_cache_connector()
            if cache is None:
                session_ids = [
                    str(session_id)
                    for session_id, data in list(getattr(sessions, '_active_sessions', {}).items())
                    if isinstance(data, dict) and data.get('username') == username
                ]
            else:
                prefix = sessions.SESSION_KEY_TEMPLATE.format(session_id='')
                for key in cache.client.scan_iter(match=prefix + '*'):
                    key = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                    try:
                        data = json.loads(cache.get(key) or '{}')
                    except Exception:
                        continue
                    if isinstance(data, dict) and data.get('username') == username:
                        session_ids.append(key[len(prefix):])
    except Exception:
        session_ids = []

    for session_id in session_ids:
        try:
            if refresh_only or (keep_session_id and session_id == str(keep_session_id)):
                data = sessions.get_session_data(session_id, use_cache=False) or {}
                for key in ('admin', 'authenticated'):
                    data.pop(key, None)
                sessions.set_session(session_id, data)
                continue
            sessions.delete_session(session_id)
            ended += 1
        except Exception:
            continue
    access.forget(username)
    return ended


########################################################################################
# Teams.
########################################################################################

def team_pipe_counts(conn=None) -> Dict[str, int]:
    """How many of the API instance's pipes each team has (`None` key: Everyone's)."""
    conn = conn if conn is not None else get_api_connector()
    counts: Dict[Optional[str], int] = {}
    try:
        for _, _, _, parameters in access.registered_pipes(conn):
            team = access.access_fields(parameters).get('team')
            counts[team] = counts.get(team, 0) + 1
    except Exception:
        return {}
    return counts


def team_job_count(team: str) -> int:
    """How many of this root's jobs belong to `team` (`meerschaum.jobs.get_team_jobs`)."""
    try:
        from meerschaum.jobs import get_team_jobs
        return len(get_team_jobs(str(team)) or [])
    except Exception:
        return 0


def visible_teams(teams: List[Dict[str, Any]], who: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The teams a viewer sees: every team for administrators, else Everyone and their own."""
    if who.get('admin'):
        return list(teams)
    username = who.get('username')
    return [team for team in teams if team['builtin'] or username in team['members']]


def may_manage_team(who: Dict[str, Any], team: Dict[str, Any]) -> bool:
    """Whether the viewer may change the team's members: administrators and its managers."""
    if who.get('admin'):
        return True
    return team['members'].get(who.get('username') or '') == 'manager'


def validate_team_name(name: Optional[str], existing: List[str]) -> SuccessTuple:
    """The username rules, not Everyone, and not taken (names compare without case)."""
    name = str(name or '').strip()
    if not name:
        return False, "Enter the team's name."
    if name.lower() == access.EVERYONE.lower():
        return False, f"'{access.EVERYONE}' is the built-in team of every user."
    if name.lower() in {str(team).lower() for team in existing}:
        return False, f"A team named '{name}' exists."
    try:
        from meerschaum.connectors.sql._users import valid_username
        valid, message = valid_username(name)
    except Exception:
        valid, message = True, ''
    if not valid:
        return False, message.replace('Username', 'Team name').replace('Usernames', 'Team names').strip()
    return True, "Success"


########################################################################################
# The page.
########################################################################################

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


def build_user_type_badge(user_type: str) -> Any:
    return dbc.Badge(
        user_type,
        color=('warning' if user_type == 'admin' else 'secondary'),
        text_color=('dark' if user_type == 'admin' else None),
        class_name='user-type-badge',
    )


def role_label(row: Dict[str, Any]) -> str:
    """`Admin`, `Editor`, `Custom`, ...: the row's role in words."""
    if row.get('type') == 'admin' or row.get('role') == 'admin':
        return 'Admin'
    return dict(ROLE_CHOICES).get(row.get('role') or 'custom', str(row.get('role')).capitalize())


def build_role_cell(row: Dict[str, Any]) -> Any:
    """The Role cell: the role, marked `default` when the user has no role of their own."""
    children: List[Any] = [html.Span(role_label(row), className='user-role')]
    if row.get('type') != 'admin' and not row.get('explicit'):
        children.append(dbc.Badge('default', color='secondary', class_name='ms-1 user-role-default'))
    return children


def build_teams_cell(teams: Dict[str, str]) -> Any:
    """The user's teams (a manager's marked), or Everyone."""
    if not teams:
        return html.Span(access.EVERYONE, className='text-muted user-team')
    return [
        html.Span(
            [team] + ([html.Span(' manager', className='user-team-role')] if role == 'manager' else []),
            className='user-team',
        )
        for team, role in teams.items()
    ]


def build_user_row_menu(username: str) -> Any:
    """The `⋮` menu of a user's row (administrators only)."""
    item = lambda label, action, **kw: dbc.DropdownMenuItem(
        label, id={'type': 'user-action', 'action': action, 'index': username}, **kw,
    )
    return dbc.DropdownMenu(
        [
            item('Edit', 'edit'),
            item('Reset password', 'password'),
            dbc.DropdownMenuItem(divider=True),
            item('Delete', 'delete', class_name='text-danger'),
        ],
        label='⋮',
        toggle_class_name='job-row-menu-toggle',
        color='link',
        size='sm',
        align_end=True,
        menu_variant='dark',
        class_name='user-row-menu',
    )


def build_users_table(
    rows: List[Dict[str, Any]],
    admin: bool = False,
    me: Optional[str] = None,
    who: Optional[Dict[str, Any]] = None,
    show_email: Optional[bool] = None,
) -> Any:
    """
    The users as a table: User, then for administrators Type and Role; Teams; Email (for
    administrators and team managers, each address only where they may see it); and the
    administrators' menu.
    """
    who = who or {'admin': admin, 'username': me, 'viewer': access.ANONYMOUS}
    show_email = admin if show_email is None else show_email
    header = [html.Th('User')]
    if admin:
        header += [html.Th('Type'), html.Th('Role')]
    header.append(html.Th('Teams'))
    if show_email:
        header.append(html.Th('Email'))
    if admin:
        header.append(html.Th(html.Span('Actions', className='visually-hidden')))
    body = []
    for row in rows:
        name = row['username']
        cells = [
            html.Td([
                html.A(name, href='/dash/users/' + name, className='console-name-link'),
            ] + ([' ', dbc.Badge('you', color='info', class_name='ms-1')] if me and name == me else [])),
        ]
        if admin:
            cells += [
                html.Td(build_user_type_badge(row['type'])),
                html.Td(build_role_cell(row), className='user-role-cell'),
            ]
        cells.append(html.Td(build_teams_cell(row.get('teams') or {}), className='user-teams'))
        if show_email:
            email = row['email'] if (admin or email_visible(who, row)) else None
            cells.append(html.Td(email or '', className='user-email'))
        if admin:
            cells.append(html.Td(build_user_row_menu(name), className='users-row-menu-cell'))
        body.append(html.Tr(cells, id={'type': 'user-row', 'index': name}))
    return html.Div(
        dbc.Table(
            [html.Thead(html.Tr(header)), html.Tbody(body)],
            hover=True,
            size='sm',
            class_name='console-table users-table',
            id='users-table',
        ),
        className='console-table-scroll',
    )


def get_users_cards(
    state: Optional[WebState] = None,
    search_term: Optional[str] = None,
    page: int = 1,
    per_page: int = 12,
) -> Tuple[List[dbc.Card], List[SuccessTuple], int, int]:
    """
    Return the cards, alerts, total page count, and total user count for the users listing
    (the web terminal page's Users button): the users the session's viewer may see.
    """
    alerts: List[SuccessTuple] = []
    who = viewer((state or {}).get('session-store.data', None) if isinstance(state, dict) else None)
    rows = visible_user_rows(get_user_rows(), who)
    all_names = sorted(row['username'] for row in rows)
    if search_term:
        all_names = [name for name in all_names if search_term.lower() in name.lower()]
    total = len(all_names)
    if per_page <= 0:
        per_page = 12
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_names = all_names[start:start + per_page]

    cards = [_build_user_card(name) for name in page_names]
    return cards, alerts, total_pages, total


def build_users_listing(
    search_term: Optional[str],
    page: int = 1,
    viewer_info: Optional[Dict[str, Any]] = None,
) -> Tuple[Any, int, int]:
    """
    Build the users table for a viewer (everyone, or the users they may see) and return it
    along with the total page count and total user count.
    """
    viewer_info = viewer_info or {'admin': False, 'sees_all': False, 'username': None}
    try:
        teams = access.list_teams()
    except Exception:
        teams = []
    rows = visible_user_rows(get_user_rows(teams=teams), viewer_info, teams)
    show_email = sees_any_email(viewer_info, teams)
    if search_term:
        term = search_term.lower()
        rows = [
            row for row in rows
            if term in row['username'].lower()
            or (show_email and email_visible(viewer_info, row) and term in str(row['email'] or '').lower())
            or any(term in team.lower() for team in (row.get('teams') or {}))
        ]
    total = len(rows)
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    page_rows = rows[(page - 1) * USERS_PER_PAGE:page * USERS_PER_PAGE]
    if not page_rows:
        title = f"No users match '{search_term}'." if search_term else 'No users'
        return html.Div(html.H5(title), className='console-empty'), total_pages, total
    return (
        build_users_table(
            page_rows,
            admin=bool(viewer_info.get('admin')),
            me=viewer_info.get('username'),
            who=viewer_info,
            show_email=show_email,
        ),
        total_pages,
        total,
    )


def _build_user_card(
    username: str,
    *,
    detail: bool = False,
    row: Optional[Dict[str, Any]] = None,
    show_email: bool = False,
    show_type: bool = False,
) -> dbc.Card:
    """
    Build a single user card. If ``detail`` is True, also list the user's teams and plugins.
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

    header = [title_link]
    if row is not None and show_type:
        header = [
            html.Div(
                [
                    title_link,
                    html.Div(
                        [build_user_type_badge(row['type']),
                         html.Span(role_label(row), className='user-role ms-2')],
                        className='d-flex align-items-center',
                    ),
                ],
                className='d-flex align-items-center justify-content-between gap-2',
            )
        ]
    body_children: List[Any] = []
    if show_email and row is not None and row.get('email'):
        body_children.append(html.P(row['email'], className='text-muted user-email mb-2'))
    if detail:
        if row is not None:
            body_children += [
                html.H5('Teams', style={'margin-bottom': '0.5em'}),
                html.Div(build_teams_cell(row.get('teams') or {}), className='user-teams mb-3'),
            ]
        plugin_names = _get_user_plugins(username)
        body_children += [
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
            dbc.CardHeader(header),
            dbc.CardBody(body_children),
        ],
        id=username + '_user_card',
        class_name='plugin-card' + (' plugin-card-detail' if detail else ''),
    )


def build_user_detail(username: str, viewer_info: Optional[Dict[str, Any]] = None) -> Any:
    """
    Build the detail view for a single user (shareable URL): only a user the viewer may see.
    """
    viewer_info = viewer_info or {'sees_all': False}
    back_button = dbc.Button(
        '← Users',
        href='/dash/users',
        color='link',
        size='lg',
        class_name='plugin-back-button',
    )
    try:
        teams = access.list_teams()
    except Exception:
        teams = []
    rows = {row['username']: row for row in visible_user_rows(get_user_rows(teams=teams), viewer_info, teams)}
    if username not in rows:
        return html.Div([
            back_button,
            html.Br(),
            html.Br(),
            html.H3(f"404: User '{username}' not found."),
        ], className='plugin-detail-wrapper')

    row = rows[username]
    is_self = username == viewer_info.get('username')
    return html.Div([
        back_button,
        html.Br(),
        html.Br(),
        _build_user_card(
            username,
            detail=True,
            row=row,
            show_email=bool(viewer_info.get('admin') or is_self or email_visible(viewer_info, row)),
            show_type=bool(viewer_info.get('admin') or is_self),
        ),
        html.Br(),
    ], className='plugin-detail-wrapper')


def build_teams_table(
    teams: List[Dict[str, Any]],
    who: Dict[str, Any],
    pipe_counts: Optional[Dict[Optional[str], int]] = None,
    users_count: Optional[int] = None,
) -> Any:
    """
    The teams: Team, Members, Managers and Pipes, with each team's `⋮` menu (Members for its
    managers and administrators; Rename and Delete for administrators, never for Everyone).
    """
    pipe_counts = pipe_counts or {}
    header = [html.Th('Team'), html.Th('Members'), html.Th('Managers'), html.Th('Pipes'),
              html.Th(html.Span('Actions', className='visually-hidden'))]
    body = []
    for team in teams:
        name = team['name']
        members = team['members']
        managers = sorted(username for username, role in members.items() if role == 'manager')
        if team['builtin']:
            members_text = (f"{users_count} users" if users_count is not None else 'Every user')
        else:
            count = len(members)
            members_text = f"{count} member{'s' if count != 1 else ''}"
        items = []
        if may_manage_team(who, team):
            items.append(dbc.DropdownMenuItem(
                'Members', id={'type': 'team-action', 'action': 'members', 'index': name},
            ))
        if who.get('admin') and not team['builtin']:
            items += [
                dbc.DropdownMenuItem(
                    'Rename', id={'type': 'team-action', 'action': 'rename', 'index': name},
                ),
                dbc.DropdownMenuItem(divider=True),
                dbc.DropdownMenuItem(
                    'Delete', id={'type': 'team-action', 'action': 'delete', 'index': name},
                    class_name='text-danger',
                ),
            ]
        menu = dbc.DropdownMenu(
            items,
            label='⋮',
            toggle_class_name='job-row-menu-toggle',
            color='link',
            size='sm',
            align_end=True,
            menu_variant='dark',
            class_name='team-row-menu',
        ) if items else ''
        body.append(html.Tr(
            [
                html.Td([html.Span(name, className='team-name')] + (
                    [dbc.Badge('built-in', color='secondary', class_name='ms-1')] if team['builtin'] else []
                )),
                html.Td(members_text, className='team-members'),
                html.Td(', '.join(managers), className='team-managers'),
                html.Td(f"{pipe_counts.get(None if team['builtin'] else name, 0):,}", className='team-pipes'),
                html.Td(menu, className='users-row-menu-cell'),
            ],
            id={'type': 'team-row', 'index': name},
        ))
    return html.Div(
        dbc.Table(
            [html.Thead(html.Tr(header)), html.Tbody(body)],
            hover=True,
            size='sm',
            class_name='console-table teams-table',
            id='teams-table',
        ),
        className='console-table-scroll',
    )


def build_team_members_editor(team: Dict[str, Any], user_names: List[str]) -> Any:
    """
    A team's members in the Members dialog: each with Member / Manager and Remove, then Add (a
    user and a role). Everyone lists only its managers (every user is a member).
    """
    rows = []
    for username, role in sorted(team['members'].items()):
        if team['builtin'] and role != 'manager':
            continue
        rows.append(html.Tr([
            html.Td(username, className='team-member-name'),
            html.Td(dbc.Select(
                id={'type': 'team-member-role', 'index': username},
                options=[{'label': 'Member', 'value': 'member'}, {'label': 'Manager', 'value': 'manager'}]
                if not team['builtin'] else [{'label': 'Manager', 'value': 'manager'},
                                               {'label': 'Member', 'value': 'member'}],
                value=role,
                size='sm',
                class_name='dbc_dark custom-select custom-select-sm',
            )),
            html.Td(dbc.Button(
                'Remove', id={'type': 'team-member-remove', 'index': username},
                color='link', size='sm', class_name='text-danger',
            ), className='users-row-menu-cell'),
        ], id={'type': 'team-member-row', 'index': username}))
    listed = set(team['members'])
    candidates = [name for name in user_names if name not in listed]
    add_row = html.Div(
        [
            dcc.Dropdown(
                id='team-add-member',
                options=[{'label': name, 'value': name} for name in candidates],
                value=None,
                placeholder='User',
                searchable=True,
                clearable=True,
                className='dbc_dark team-add-member',
            ),
            dbc.Select(
                id='team-add-role',
                options=(
                    [{'label': 'Manager', 'value': 'manager'}] if team['builtin']
                    else [{'label': 'Member', 'value': 'member'}, {'label': 'Manager', 'value': 'manager'}]
                ),
                value=('manager' if team['builtin'] else 'member'),
                size='sm',
                class_name='dbc_dark custom-select custom-select-sm team-add-role',
            ),
            dbc.Button('Add', id='team-add-button', color='primary', size='sm', n_clicks=0),
        ],
        className='team-add-row',
    )
    table = dbc.Table(
        [html.Tbody(rows)], size='sm', class_name='console-table team-members-table',
        id='team-members-table',
    ) if rows else None
    return html.Div([table, add_row])


########################################################################################
# The dialogs (one of each per page, aimed at a user or a team by a store).
########################################################################################

def _modal(modal_id: str, title_id: str, body: List[Any], footer: List[Any], size: Optional[str] = None) -> dbc.Modal:
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id=title_id)),
            dbc.ModalBody(body),
            dbc.ModalFooter(footer),
        ],
        id=modal_id,
        is_open=False,
        fullscreen='sm-down',
        size=size,
        class_name='users-modal',
    )


def _field(label: str, control: Any, field_id: Optional[str] = None) -> Any:
    return html.Div([dbc.Label(label, html_for=field_id), control], className='new-job-field')


def build_permissions_grid(grid_id: Any, values: Optional[List[str]] = None) -> Any:
    """The permissions as checklists by group (the Edit dialog's grid)."""
    values = list(values or [])
    groups = []
    for group, items in access.PERMISSION_GROUPS:
        groups.append(html.Fieldset(
            [
                html.Legend(group, className='users-permissions-legend'),
                dbc.Checklist(
                    id={'type': grid_id, 'index': group},
                    options=[{'label': label, 'value': permission} for permission, label in items],
                    value=[permission for permission, _ in items if permission in values],
                    class_name='users-permissions-checklist',
                ),
            ],
            className='users-permissions-group',
        ))
    return html.Div(groups, className='users-permissions-grid', id=f'{grid_id}-div')


def build_users_modals() -> List[Any]:
    """The Add user, Edit, Reset password and Delete dialogs, and the teams' dialogs."""
    type_options = [{'label': 'User', 'value': 'user'}, {'label': 'Admin', 'value': 'admin'}]
    role_options = [{'label': label, 'value': value} for value, label in ROLE_CHOICES]
    return [
        dcc.Store(id='users-target', data=None),
        dcc.Store(id='teams-target', data=None),
        _modal(
            'users-add-modal', 'users-add-title',
            [
                html.Div(id='users-add-alert'),
                _field('Username', dbc.Input(id='users-add-username', autoComplete='off'), 'users-add-username'),
                _field('Password', dbc.Input(id='users-add-password', type='password', autoComplete='new-password'), 'users-add-password'),
                _field('Confirm password', dbc.Input(id='users-add-confirm', type='password', autoComplete='new-password'), 'users-add-confirm'),
                _field('Type', dbc.RadioItems(id='users-add-type', options=type_options, value='user', inline=True)),
                _field('Role', dbc.Select(id='users-add-role', options=[
                    option for option in role_options if option['value'] != 'custom'
                ], value='default', class_name='dbc_dark custom-select'), 'users-add-role'),
                _field('Teams', dcc.Dropdown(id='users-add-teams', options=[], value=[], multi=True,
                                             placeholder=access.EVERYONE, className='dbc_dark')),
                _field('Email', dbc.Input(id='users-add-email', type='email', placeholder='name@example.com', autoComplete='off'), 'users-add-email'),
            ],
            [
                dbc.Button('Cancel', id='users-add-cancel', color='secondary'),
                dbc.Button('Add user', id='users-add-submit', color='primary'),
            ],
        ),
        _modal(
            'users-edit-modal', 'users-edit-title',
            [
                html.Div(id='users-edit-alert'),
                _field('Type', dbc.RadioItems(id='users-edit-type', options=type_options, value='user', inline=True)),
                html.Div(
                    [
                        _field('Role', dbc.Select(id='users-edit-role', options=role_options, value='default',
                                                  class_name='dbc_dark custom-select'), 'users-edit-role'),
                        build_permissions_grid('users-edit-permissions'),
                    ],
                    id='users-edit-rights',
                ),
                _field('Teams', dcc.Dropdown(id='users-edit-teams', options=[], value=[], multi=True,
                                             placeholder=access.EVERYONE, className='dbc_dark')),
                _field('Manages', dcc.Dropdown(id='users-edit-manages', options=[], value=[], multi=True,
                                               placeholder='No teams', className='dbc_dark')),
                _field('Primary team', dbc.Select(id='users-edit-primary', options=[], value=access.EVERYONE,
                                                  class_name='dbc_dark custom-select'), 'users-edit-primary'),
            ],
            [
                dbc.Button('Cancel', id='users-edit-cancel', color='secondary'),
                dbc.Button('Save', id='users-edit-submit', color='primary'),
            ],
            size='lg',
        ),
        _modal(
            'users-password-modal', 'users-password-title',
            [
                html.Div(id='users-password-alert'),
                _field('New password', dbc.Input(id='users-password-value', type='password', autoComplete='new-password'), 'users-password-value'),
                _field('Confirm password', dbc.Input(id='users-password-confirm', type='password', autoComplete='new-password'), 'users-password-confirm'),
            ],
            [
                dbc.Button('Cancel', id='users-password-cancel', color='secondary'),
                dbc.Button('Reset password', id='users-password-submit', color='primary'),
            ],
        ),
        _modal(
            'users-delete-modal', 'users-delete-title',
            [
                html.Div(id='users-delete-alert'),
                dbc.Label(['Type ', html.Code(id='users-delete-name'), ' to confirm']),
                dbc.Input(id='users-delete-input', autoComplete='off'),
            ],
            [
                dbc.Button('Cancel', id='users-delete-cancel', color='secondary'),
                dbc.Button('Delete', id='users-delete-submit', color='danger', disabled=True),
            ],
        ),
        _modal(
            'teams-new-modal', 'teams-new-title',
            [
                html.Div(id='teams-new-alert'),
                _field('Name', dbc.Input(id='teams-new-name', autoComplete='off'), 'teams-new-name'),
            ],
            [
                dbc.Button('Cancel', id='teams-new-cancel', color='secondary'),
                dbc.Button('Create team', id='teams-new-submit', color='primary'),
            ],
        ),
        _modal(
            'teams-rename-modal', 'teams-rename-title',
            [
                html.Div(id='teams-rename-alert'),
                _field('New name', dbc.Input(id='teams-rename-name', autoComplete='off'), 'teams-rename-name'),
            ],
            [
                dbc.Button('Cancel', id='teams-rename-cancel', color='secondary'),
                dbc.Button('Rename', id='teams-rename-submit', color='primary'),
            ],
        ),
        _modal(
            'teams-delete-modal', 'teams-delete-title',
            [
                html.Div(id='teams-delete-alert'),
                html.P(id='teams-delete-consequence'),
                dbc.Label(['Type ', html.Code(id='teams-delete-name'), ' to confirm']),
                dbc.Input(id='teams-delete-input', autoComplete='off'),
            ],
            [
                dbc.Button('Cancel', id='teams-delete-cancel', color='secondary'),
                dbc.Button('Delete', id='teams-delete-submit', color='danger', disabled=True),
            ],
        ),
        _modal(
            'teams-members-modal', 'teams-members-title',
            [
                html.Div(id='teams-members-alert'),
                html.Div(id='teams-members-body'),
            ],
            [
                dbc.Button('Done', id='teams-members-close', color='secondary'),
            ],
        ),
    ]
