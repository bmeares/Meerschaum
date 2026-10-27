#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's Users page: the users of the API instance as a table (username, type, email) and,
for administrators, Add user, Change type, Reset password and Delete. The instance connector's
own user methods do the work (`register_user`, `edit_user`, `delete_user`, `get_users`,
`get_user_type`); nothing here writes the users table directly.
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

USERS_PER_PAGE: int = 50
USERS_GRID_COLUMNS: int = 3

#: The user types the console offers (`None` in the users table reads as `user`).
USER_TYPES: Tuple[str, str] = ('user', 'admin')

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


def get_user_rows(conn=None) -> List[Dict[str, Any]]:
    """
    The instance's users, sorted by username: `[{'username', 'type', 'email'}]` (`type` is `admin`
    or `user`; `email` is `None` when the instance does not say).
    """
    conn = conn if conn is not None else get_api_connector()
    names = sorted(str(name) for name in (conn.get_users(debug=debug) or []))
    details: Dict[str, Dict[str, Any]] = {}
    pipe = _users_table_pipe(conn)
    if pipe is not None:
        try:
            docs = pipe.get_docs(select_columns=['username', 'email', 'user_type'], debug=debug) or []
            details = {str(doc.get('username')): doc for doc in docs if doc.get('username') is not None}
        except Exception:
            details = {}
    rows = []
    for name in names:
        doc = details.get(name)
        if doc is not None and 'user_type' in doc:
            user_type = doc.get('user_type')
        else:
            try:
                user_type = conn.get_user_type(User(name, instance=conn), debug=debug)
            except Exception:
                user_type = None
        email = (doc or {}).get('email', None)
        rows.append({
            'username': name,
            'type': normalize_user_type(user_type),
            'email': (str(email) if email not in (None, '') else None),
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
    Who is looking at the Users page: `{'username', 'active', 'admin', 'sees_all'}`. Under
    `--no-auth` everyone is an administrator. An administrator is checked against the users
    table (not the session's cached flag), since these are user-management decisions.
    """
    from meerschaum.api import no_auth, private
    from meerschaum.api.dash.sessions import get_username_from_session
    if no_auth:
        return {'username': None, 'active': True, 'admin': True, 'sees_all': True}
    session_id = (session_store_data or {}).get('session-id', None) if isinstance(session_store_data, dict) else None
    username = get_username_from_session(str(session_id)) if session_id else None
    if not username:
        return {'username': None, 'active': False, 'admin': False, 'sees_all': False}
    conn = get_api_connector()
    try:
        is_admin = normalize_user_type(conn.get_user_type(User(username, instance=conn), debug=debug)) == 'admin'
    except Exception:
        is_admin = False
    secure = not mrsm.get_config('api', 'permissions', 'actions', 'non_admin', warn=False)
    return {
        'username': username,
        'active': True,
        'admin': is_admin,
        'sees_all': is_admin or not (private or secure),
    }


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


def add_user(username: str, password: str, user_type: str, email: Optional[str], conn=None) -> SuccessTuple:
    """Register a user with the instance's `register_user`."""
    conn = conn if conn is not None else get_api_connector()
    from meerschaum._internal.static import STATIC_CONFIG
    user = User(
        str(username).strip(),
        password,
        type=('admin' if normalize_user_type(user_type) == 'admin' else None),
        email=(str(email).strip() or None) if email else None,
        attributes={'scopes': list(STATIC_CONFIG['tokens']['scopes'])},
        instance=conn,
    )
    return conn.register_user(user, debug=debug)


def change_user_type(username: str, user_type: str, conn=None) -> SuccessTuple:
    """Make a user an admin or a regular user (`edit_user` with only the type set)."""
    conn = conn if conn is not None else get_api_connector()
    ### An empty password, email and attributes leave them as they are.
    user = User(username, '', type=normalize_user_type(user_type), email='', instance=conn)
    return conn.edit_user(user, debug=debug)


def reset_user_password(username: str, password: str, conn=None) -> SuccessTuple:
    """Set a user's password (`edit_user` with only the password set)."""
    conn = conn if conn is not None else get_api_connector()
    user = User(username, password, type='', email='', instance=conn)
    return conn.edit_user(user, debug=debug)


def remove_user(username: str, conn=None) -> SuccessTuple:
    """Delete a user with the instance's `delete_user`."""
    conn = conn if conn is not None else get_api_connector()
    return conn.delete_user(User(username, instance=conn), debug=debug)


def end_user_sessions(
    username: str,
    keep_session_id: Optional[str] = None,
    refresh_only: bool = False,
) -> int:
    """
    Sign a user out of the console everywhere (a deleted, demoted or reset user must not keep a
    session that still carries the old rights), except `keep_session_id`, whose cached rights
    are dropped instead so they are read again. With `refresh_only` (a promotion), every session
    is kept and only its cached rights are dropped. Returns how many sessions ended.
    """
    from meerschaum.api.dash import sessions
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
    return ended


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


def build_user_row_menu(username: str) -> Any:
    """The `⋮` menu of a user's row (administrators only)."""
    item = lambda label, action, **kw: dbc.DropdownMenuItem(
        label, id={'type': 'user-action', 'action': action, 'index': username}, **kw,
    )
    return dbc.DropdownMenu(
        [
            item('Change type', 'type'),
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


def build_users_table(rows: List[Dict[str, Any]], admin: bool = False, me: Optional[str] = None) -> Any:
    """
    The users as a table: their usernames and, for administrators, each user's type, email and
    menu. Other users see neither types nor email addresses (theirs are on their own page): who
    the administrators are and how to reach users is the administrators' to know.
    """
    header = [html.Th('Username')]
    if admin:
        header += [
            html.Th('Type'),
            html.Th('Email'),
            html.Th(html.Span('Actions', className='visually-hidden')),
        ]
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
                html.Td(row['email'] or '', className='user-email'),
                html.Td(build_user_row_menu(name), className='users-row-menu-cell'),
            ]
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
    (the web terminal page's Users button).
    """
    alerts: List[SuccessTuple] = []
    all_names = sorted(get_api_connector().get_users(debug=debug) or [])
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
    Build the users table for a viewer (everyone, or only themselves) and return it along with
    the total page count and total user count.
    """
    viewer_info = viewer_info or {'admin': False, 'sees_all': True, 'username': None}
    rows = get_user_rows()
    if not viewer_info.get('sees_all'):
        rows = [row for row in rows if row['username'] == viewer_info.get('username')]
    if search_term:
        term = search_term.lower()
        admin = bool(viewer_info.get('admin'))
        rows = [
            row for row in rows
            if term in row['username'].lower() or (admin and term in str(row['email'] or '').lower())
        ]
    total = len(rows)
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    page_rows = rows[(page - 1) * USERS_PER_PAGE:page * USERS_PER_PAGE]
    if not page_rows:
        title = f"No users match '{search_term}'." if search_term else 'No users'
        return html.Div(html.H5(title), className='console-empty'), total_pages, total
    return (
        build_users_table(page_rows, admin=bool(viewer_info.get('admin')), me=viewer_info.get('username')),
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

    header = [title_link]
    if row is not None and show_type:
        header = [
            html.Div(
                [title_link, build_user_type_badge(row['type'])],
                className='d-flex align-items-center justify-content-between gap-2',
            )
        ]
    body_children: List[Any] = []
    if show_email and row is not None and row.get('email'):
        body_children.append(html.P(row['email'], className='text-muted user-email mb-2'))
    if detail:
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
    Build the detail view for a single user (shareable URL).
    """
    viewer_info = viewer_info or {'sees_all': True}
    back_button = dbc.Button(
        '← Users',
        href='/dash/users',
        color='link',
        size='lg',
        class_name='plugin-back-button',
    )

    rows = {row['username']: row for row in get_user_rows()}
    visible = viewer_info.get('sees_all') or username == viewer_info.get('username')
    if username not in rows or not visible:
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
        _build_user_card(
            username,
            detail=True,
            row=rows[username],
            show_email=bool(viewer_info.get('admin') or username == viewer_info.get('username')),
            show_type=bool(viewer_info.get('admin') or username == viewer_info.get('username')),
        ),
        html.Br(),
    ], className='plugin-detail-wrapper')


########################################################################################
# The administrators' dialogs (one of each per page, aimed at a user by `users-target`).
########################################################################################

def _modal(modal_id: str, title_id: str, body: List[Any], footer: List[Any]) -> dbc.Modal:
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle(id=title_id)),
            dbc.ModalBody(body),
            dbc.ModalFooter(footer),
        ],
        id=modal_id,
        is_open=False,
        fullscreen='sm-down',
        class_name='users-modal',
    )


def _field(label: str, control: Any) -> Any:
    return html.Div([dbc.Label(label), control], className='new-job-field')


def build_users_modals() -> List[Any]:
    """The Add user, Change type, Reset password and Delete dialogs."""
    type_options = [{'label': 'User', 'value': 'user'}, {'label': 'Admin', 'value': 'admin'}]
    return [
        dcc.Store(id='users-target', data=None),
        _modal(
            'users-add-modal', 'users-add-title',
            [
                html.Div(id='users-add-alert'),
                _field('Username', dbc.Input(id='users-add-username', autoComplete='off')),
                _field('Password', dbc.Input(id='users-add-password', type='password', autoComplete='new-password')),
                _field('Confirm password', dbc.Input(id='users-add-confirm', type='password', autoComplete='new-password')),
                _field('Type', dbc.RadioItems(id='users-add-type', options=type_options, value='user', inline=True)),
                _field('Email', dbc.Input(id='users-add-email', type='email', placeholder='name@example.com', autoComplete='off')),
            ],
            [
                dbc.Button('Cancel', id='users-add-cancel', color='secondary'),
                dbc.Button('Add user', id='users-add-submit', color='primary'),
            ],
        ),
        _modal(
            'users-type-modal', 'users-type-title',
            [
                html.Div(id='users-type-alert'),
                _field('Type', dbc.RadioItems(id='users-type-value', options=type_options, value='user', inline=True)),
            ],
            [
                dbc.Button('Cancel', id='users-type-cancel', color='secondary'),
                dbc.Button('Change type', id='users-type-submit', color='primary'),
            ],
        ),
        _modal(
            'users-password-modal', 'users-password-title',
            [
                html.Div(id='users-password-alert'),
                _field('New password', dbc.Input(id='users-password-value', type='password', autoComplete='new-password')),
                _field('Confirm password', dbc.Input(id='users-password-confirm', type='password', autoComplete='new-password')),
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
    ]
