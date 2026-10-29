#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Users & Teams (`/dash/users`, `/dash/teams`), and one user's page (`/dash/users/{name}`), built
per request: the Users and Teams tabs, with Add user and New team for administrators.
"""

from __future__ import annotations
from meerschaum.api import CHECK_UPDATE
from meerschaum.utils.packages import import_html, import_dcc
html, dcc = import_html(check_update=CHECK_UPDATE), import_dcc(check_update=CHECK_UPDATE)
import dash_bootstrap_components as dbc
from meerschaum.api.dash.components import build_pages_navbar
from meerschaum.api.dash.users import build_users_modals


def tab_of_path(pathname) -> str:
    """`teams` for `/dash/teams`, else `users`."""
    return 'teams' if str(pathname or '').rstrip('/').startswith('/dash/teams') else 'users'


def role_presets() -> dict:
    """
    What the Edit dialog's role select and permissions grid need in the browser: each preset's
    permissions, the default role, and the grid's groups (their permissions, in order).
    """
    from meerschaum.api.dash import access
    roles = {}
    for role in access.ROLE_LABELS:
        try:
            roles[role] = sorted(access.role_permissions(role))
        except Exception:
            continue
    try:
        default = access.default_role()
    except Exception:
        default = 'editor'
    return {
        'roles': roles,
        'default': default,
        'groups': [[permission for permission, _ in items] for _, items in access.PERMISSION_GROUPS],
    }


def layout(pathname=None, href=None):
    """The Users & Teams page with the tab of its path."""
    tab = tab_of_path(pathname)
    search_box = dbc.Input(
        id="search-users-input",
        placeholder="Search users",
        type="search",
        debounce=True,
        autoComplete='off',
    )
    return [
        build_pages_navbar(active='users'),
        dcc.Location(id='users-location'),
        dbc.Container([
            html.Div(
                [
                    html.H3('Users & Teams'),
                    html.Div(
                        [
                            dbc.Button('Add user', id='users-add-button', color='primary'),
                            dbc.Button('New team', id='teams-new-button', color='primary'),
                        ],
                        id='users-add-button-div',
                        hidden=True,
                        className='console-page-actions',
                    ),
                ],
                id='users-header',
                className='console-page-header',
            ),
            dbc.Tabs(
                [
                    dbc.Tab(label='Users', tab_id='users', id='users-tab-users'),
                    dbc.Tab(label='Teams', tab_id='teams', id='users-tab-teams'),
                ],
                id='users-tabs',
                active_tab=tab,
                class_name='users-tabs',
            ),
            html.Div(search_box, id='users-search-wrapper', className='console-search'),
            html.Div(id='users-alert-div'),
            dbc.Pagination(
                id='users-pagination',
                max_value=1,
                active_page=1,
                first_last=True,
                previous_next=True,
                fully_expanded=False,
                style={'justify-content': 'center', 'display': 'none'},
            ),
            html.Div([], id='users-content-div'),
            dcc.Store(id='users-refresh', data=0),
            dcc.Store(id='users-role-presets', data=role_presets()),
            *build_users_modals(),
        ], class_name='console-page users-page'),
    ]
