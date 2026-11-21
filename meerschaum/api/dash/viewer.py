#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Who is looking at a console page, with what rights: the `Viewer` of a console session.

A viewer is read from the session's user (their type, effective permissions, teams and team
grants, `meerschaum.core.access`'s rules, the same as the API server's), narrowed to the team the
navbar's Team switcher acts as (`mrsm-team`), and, for an isolated team's member, to what the server
projects for native execution (`meerschaum.api.dash._native_execution`). It is the one
implementation of that computation: `meerschaum.api.dash.sessions`' authorization helpers and
`meerschaum.api.dash.access`' pipe, Pipeline and job rules both ask it. It imports nothing from the
Dash app and no console page.

- `resolve_viewer(session)`: the viewer of a callback's `session-store` data, or of the current
  request; anonymous outside a request (`SYSTEM` is asked for by name, `current_viewer()`).
- `viewer_for(username)`: a user's base viewer, read now (no team switcher, no projection).
- `make_viewer(...)`: a viewer with given rights, without reading the instance.
- `forget(username)`: drop cached viewers after a change of rights.
"""

from __future__ import annotations

import contextvars
import threading
import time
from dataclasses import dataclass, field, replace

from typing import Set
from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, Tuple
from meerschaum.api.dash.connectors_file import refresh_connectors
from meerschaum.api.dash.session_store import (
    SESSION_COOKIE_NAME,
    get_username_from_session,
    valid_session_id,
)

#: The built-in team every user belongs to (a pipe without a team is Everyone's).
EVERYONE: str = 'Everyone'

#: How long (seconds) a viewer is reused.
VIEWER_TTL_SECONDS: float = 5.0

#: The role presets (`api:permissions:default_role` names one of the first three).
ROLE_LABELS: Dict[str, str] = {
    'viewer': 'Viewer',
    'editor': 'Editor',
    'operator': 'Operator',
}

#: The permissions grid of the Users & Teams page: `(group, [(permission, label), ...])`.
PERMISSION_GROUPS: Tuple[Tuple[str, Tuple[Tuple[str, str], ...]], ...] = (
    ('Pipes', (
        ('pipes:read', 'Read'),
        ('pipes:register', 'Register'),
        ('pipes:write', 'Edit and sync'),
        ('pipes:define_sql', 'Set SQL definitions'),
        ('pipes:drop', 'Drop'),
        ('pipes:delete', 'Delete'),
    )),
    ('Jobs', (
        ('jobs:read', 'Read'),
        ('logs:read', 'Read logs'),
        ('jobs:execute', 'Start'),
        ('jobs:write', 'Create and edit'),
        ('jobs:stop', 'Stop'),
        ('jobs:pause', 'Pause'),
        ('jobs:delete', 'Delete'),
    )),
    ('Actions', (
        ('actions:execute', 'Run actions'),
        ('webterm:use', 'Use the terminal'),
    )),
    ('Data', (
        ('connectors:read', 'List connectors'),
        ('sql:read', 'Read SQL'),
        ('instance:read', 'Read the instance'),
        ('instance:chain', 'Chain instances'),
    )),
    ('Plugins', (
        ('plugins:read', 'Read'),
        ('plugins:write', 'Publish'),
        ('plugins:delete', 'Delete'),
    )),
    ('Account', (
        ('users:read', 'Read own account'),
        ('users:write', 'Edit own account'),
        ('users:register', 'Register users'),
        ('users:delete', 'Delete own account'),
    )),
)

#: Every permission of the grid, in order.
GRID_PERMISSIONS: Tuple[str, ...] = tuple(
    permission for _, items in PERMISSION_GROUPS for permission, _ in items
)


def core():
    """`meerschaum.core.access`: the access rules (shared with the API server)."""
    import meerschaum.core.access as _access
    return _access


# ------------------------------------------------------------------------------------------------
# The viewer
# ------------------------------------------------------------------------------------------------

#: The cookie storing the viewer's active "acting as" team (empty / absent = All teams).
TEAM_COOKIE_NAME: str = 'mrsm-team'


@dataclass
class Viewer:
    """Who is looking at a console page, with what rights."""

    username: Optional[str] = None
    signed_in: bool = False
    is_admin: bool = False
    permissions: frozenset = field(default_factory=frozenset)
    teams: Dict[str, str] = field(default_factory=dict)
    primary_team: Optional[str] = None
    no_auth: bool = False
    principal: Any = None
    #: What the viewer's teams grant and withhold (`meerschaum.core.access.team_grants`).
    grants: Dict[str, Any] = field(default_factory=dict)
    #: Fresh Main-only execution projection; never retained in the base Viewer cache.
    native_execution: Any = None
    #: When narrowed via the navbar Team switcher (`mrsm-team` cookie), the team being acted as,
    #: plus the underlying real identity so the switcher always knows what teams may be chosen.
    acting_team: Optional[str] = None
    real_is_admin: Optional[bool] = None
    base_teams: Optional[Dict[str, str]] = None
    base_is_isolated: Optional[bool] = None
    base_primary_team: Optional[str] = None

    @property
    def is_real_admin(self) -> bool:
        """Whether the underlying signed-in account is an administrator (even when acting as a team)."""
        return bool(self.is_admin if self.real_is_admin is None else self.real_is_admin)

    @property
    def real_teams(self) -> Dict[str, str]:
        """The account's real team memberships before any navbar team narrowing."""
        return dict(self.teams if self.base_teams is None else self.base_teams)

    @property
    def real_primary_team(self) -> Optional[str]:
        """The account's real primary team before any navbar team narrowing."""
        return self.primary_team if self.acting_team is None else self.base_primary_team

    @property
    def real_is_isolated(self) -> bool:
        """Whether the underlying account is isolated before any navbar team narrowing."""
        return bool(self.is_isolated if self.base_is_isolated is None else self.base_is_isolated)

    @property
    def is_isolated(self) -> bool:
        """Whether the viewer is walled off from Everyone (all of their teams are isolated)."""
        if self.is_admin or self.no_auth:
            return False
        return bool((self.grants or {}).get('isolated', False))

    @property
    def default_instance(self) -> Optional[str]:
        """The default instance of the viewer's team, if it has one."""
        instance = (self.grants or {}).get('instance', None)
        return str(instance) if instance else None

    def can_use_plugin(self, name: str) -> bool:
        """Whether the viewer may see and use the plugin `name`."""
        if not self.is_isolated:
            return True
        return str(name) in set((self.grants or {}).get('plugins', None) or [])

    @property
    def can_write_plugins(self) -> bool:
        """Whether the viewer may install, edit, upload and uninstall plugins."""
        if self.is_admin or self.no_auth:
            return True
        return bool(self.signed_in and (self.grants or {}).get('plugins_write', False))

    def can(self, permission: str) -> bool:
        """Whether the viewer holds `permission` (administrators hold every permission)."""
        if self.is_admin or self.no_auth:
            return True
        return permission in self.permissions or '*' in self.permissions

    def manages(self, team: Optional[str]) -> bool:
        """Whether the viewer manages `team` (administrators manage every team)."""
        if self.is_admin or self.no_auth:
            return True
        return bool(team) and self.teams.get(str(team)) == 'manager'

    def in_team(self, team: Optional[str]) -> bool:
        """Whether the viewer is in `team` (everyone is in Everyone)."""
        if not team or str(team) == EVERYONE:
            return (not self.is_isolated) or self.teams.get(EVERYONE) == 'manager'
        return str(team) in self.teams

    @property
    def managed_teams(self) -> List[str]:
        return sorted(team for team, role in self.teams.items() if role == 'manager')

    @property
    def own_teams(self) -> List[str]:
        """The viewer's teams other than Everyone, sorted."""
        return sorted(team for team in self.teams if team != EVERYONE)

    @property
    def active(self) -> bool:
        """Whether the viewer is signed in or running under `--no-auth`."""
        return bool(self.no_auth or self.signed_in)

    @property
    def can_read_pipes(self) -> bool:
        """Whether the viewer may view pipes (`pipes:read`, or `--no-auth`)."""
        return bool(self.no_auth or (self.signed_in and self.can('pipes:read')))

    @property
    def can_act(self) -> bool:
        """Whether the viewer may run actions (`actions:execute`, or `--no-auth`)."""
        return bool(self.no_auth or (self.signed_in and self.can('actions:execute')))

    def can_write_pipes(self) -> Tuple[bool, str]:
        """`(allowed, reason)`: whether the viewer may edit pipes' registrations (`pipes:write`)."""
        if not self.active:
            return False, "Sign in to edit pipes."
        if not self.can('pipes:write'):
            return False, "Your account may not edit pipes."
        return True, ''

    def can_register_pipes(self) -> Tuple[bool, str]:
        """`(allowed, reason)`: whether the viewer may register new pipes."""
        from meerschaum.config import get_config
        if not get_config('api', 'permissions', 'registration', 'pipes', warn=False):
            return False, (
                "This server does not allow registering pipes "
                "(`api:permissions:registration:pipes`)."
            )
        if not self.active:
            return False, "Sign in to register pipes."
        if not self.can('pipes:register'):
            return False, "Your account may not register pipes."
        return True, ''

    def can_define_sql(self) -> Tuple[bool, str]:
        """`(allowed, reason)`: whether the viewer may set or change SQL definitions."""
        if not self.can('pipes:define_sql'):
            return False, "Your account may not set SQL definitions."
        return True, ''

    @property
    def can_view_connectors(self) -> bool:
        """
        Whether the viewer may see the Connectors page: a signed-in user with `connectors:read`
        (only an administrator under `--secure`), or anyone with `--no-auth`.
        """
        if self.no_auth:
            return True
        if not self.signed_in or not self.can('connectors:read'):
            return False
        return self.is_admin or not secure_mode()

    @property
    def can_see_connector_details(self) -> bool:
        """Whether the viewer may see masked connector settings (administrators, or `--no-auth`)."""
        return bool(self.no_auth or (self.signed_in and self.is_admin))

    @property
    def can_test_connectors(self) -> bool:
        """Whether the viewer may test connections (administrators, or `--no-auth`)."""
        return self.can_see_connector_details

    def narrowed(self, **changes: Any) -> 'Viewer':
        """A copy of the viewer with `changes` (fewer rights) and its rules' `Principal` again."""
        narrowed = replace(self, **changes)
        narrowed.principal = principal_of(narrowed)
        return narrowed

    def to_json(self) -> Dict[str, Any]:
        return {
            'username': self.username,
            'signed_in': self.signed_in,
            'admin': self.is_admin,
            'permissions': sorted(self.permissions),
            'teams': dict(self.teams),
            'primary_team': self.primary_team,
            'grants': dict(self.grants or {}),
            'acting_team': self.acting_team,
        }


ANONYMOUS = Viewer()

#: The console itself reading for a check of its own (never shown as is): sees every pipe.
SYSTEM = Viewer(username=None, signed_in=True, is_admin=True)

_viewer_cache: Dict[str, Tuple[float, Viewer]] = {}
_teams_cache: Optional[Tuple[float, List[Any]]] = None
_viewer_lock = threading.Lock()
_invalidation_hooks: List[Any] = []


def register_invalidation_hook(fn: Any) -> None:
    """
    Register `fn(username: Optional[str])`, called whenever `forget()` runs (`None`: everyone),
    for a cache derived from users' rights.
    """
    with _viewer_lock:
        if fn not in _invalidation_hooks:
            _invalidation_hooks.append(fn)


def forget(username: Optional[str] = None) -> None:
    """
    Drop the cached viewers (of one user, or all) and the cached teams: their rights are read
    again. Then call every `register_invalidation_hook` callback with `username`.
    """
    global _teams_cache
    with _viewer_lock:
        _teams_cache = None
        if username is None:
            _viewer_cache.clear()
        else:
            for key in [key for key, (_, viewer) in _viewer_cache.items() if viewer.username == username]:
                _viewer_cache.pop(key, None)
        hooks = list(_invalidation_hooks)
    for hook in hooks:
        try:
            hook(username)
        except Exception as e:
            from meerschaum.utils.warnings import warn
            warn(f"Could not drop a cache of the console's rights: {e}", stack=False)


def _cached_teams(conn: Any = None, *, fresh: bool = False) -> List[Any]:
    """Cached team records on the API instance (TTL `VIEWER_TTL_SECONDS`)."""
    global _teams_cache
    now = time.monotonic()
    if conn is None and not fresh:
        with _viewer_lock:
            if _teams_cache is not None and now - _teams_cache[0] < VIEWER_TTL_SECONDS:
                return list(_teams_cache[1])
    from meerschaum.api import get_api_connector
    target = conn if conn is not None else get_api_connector()
    try:
        records = list(target.get_teams() or [])
    except Exception:
        return []
    if conn is None:
        with _viewer_lock:
            _teams_cache = (now, list(records))
    return records


#: What the console's router renders a page for (`components.console_nav_context`):
#: `{'pathname', 'session_id'}` and, when given, `'acting_team'` (overrides the `mrsm-team` cookie).
nav_context: contextvars.ContextVar = contextvars.ContextVar('mrsm_console_nav', default=None)


def request_acting_team() -> Optional[str]:
    """
    The active "acting as" team for the current web request (`mrsm-team` cookie, or an explicit
    override in `nav_context`), or `None` for the full All-teams view.
    """
    ctx = nav_context.get()
    if isinstance(ctx, dict) and 'acting_team' in ctx:
        val = ctx.get('acting_team')
        return str(val).strip() or None if val is not None else None
    raw = None
    try:
        import dash
        raw = (dash.callback_context.cookies or {}).get(TEAM_COOKIE_NAME)
    except Exception:
        pass
    if not raw:
        try:
            import flask
            if flask.has_request_context():
                raw = flask.request.cookies.get(TEAM_COOKIE_NAME)
        except Exception:
            return None
    if not raw:
        return None
    from urllib.parse import unquote
    cleaned = unquote(str(raw)).strip()
    return cleaned or None


def available_acting_teams(viewer: Optional[Viewer], conn: Any = None) -> List[Dict[str, str]]:
    """
    The options for the navbar Team switcher (`[{'label': ..., 'value': ...}]`), or `[]` when the
    viewer has nothing to switch between (`<= 1` choice, anonymous, or `no_auth`).
    """
    if viewer is None or not viewer.signed_in or viewer.no_auth:
        return []
    if viewer.is_real_admin:
        try:
            records = _cached_teams(conn)
        except Exception:
            return []
        names = sorted(
            {
                str(getattr(record, 'name', None) or (record.get('name') if isinstance(record, dict) else ''))
                for record in records
            }
            - {'', EVERYONE}
            - {
                str(getattr(record, 'name', None) or (record.get('name') if isinstance(record, dict) else ''))
                for record in records
                if core().is_everyone(
                    str(getattr(record, 'name', None) or (record.get('name') if isinstance(record, dict) else ''))
                )
            },
            key=str.lower,
        )
        if not names:
            return []
        return (
            [{'label': 'All teams (Admin)', 'value': ''}]
            + [{'label': name, 'value': name} for name in names]
            + [{'label': EVERYONE, 'value': EVERYONE}]
        )

    own = sorted((t for t in viewer.real_teams if t != EVERYONE), key=str.lower)
    in_everyone = not viewer.real_is_isolated
    if not own or (len(own) == 1 and not in_everyone):
        return []
    options = [{'label': 'All teams', 'value': ''}] + [{'label': name, 'value': name} for name in own]
    if in_everyone:
        options.append({'label': EVERYONE, 'value': EVERYONE})
    return options


#: What acting as a team withholds: actions and the terminal run on the server under the
#: account's real identity, which the team view cannot narrow (switch to All teams to use them).
ACTING_WITHHELD: frozenset = frozenset({'actions:execute', 'webterm:use'})


def _acting_permissions(viewer: Viewer, grants: Dict[str, Any], isolated: bool) -> frozenset:
    """The permissions of `viewer` acting as a team with `grants` (never more than its own)."""
    permissions = frozenset(viewer.permissions) - ACTING_WITHHELD
    if isolated:
        permissions = permissions - frozenset(core().withheld_permissions(grants))
    if secure_mode() and not viewer.no_auth:
        permissions = permissions - SECURE_WITHHELD
    return permissions


def apply_acting_team(
    viewer: Viewer,
    requested_team: Optional[str],
    conn: Any = None,
    *,
    fresh: bool = False,
) -> Viewer:
    """
    Narrow `viewer` to act as `requested_team` (never widens permissions or grants).

    - Administrators may act as any registered team (with `manager` role in that team and
      `is_admin=False` so every team restriction applies) or as `Everyone`.
    - Non-administrators may act as any team they belong to (keeping their real role in that
      team) or as `Everyone` when they are not isolated.
    - Unknown or unauthorized team names are ignored safely (returns `viewer` unchanged).
    """
    if not requested_team or not viewer.signed_in or viewer.no_auth:
        return viewer
    target = str(requested_team).strip()
    if not target:
        return viewer

    real_is_admin = viewer.is_real_admin
    real_teams = viewer.real_teams
    real_is_isolated = viewer.real_is_isolated

    if core().is_everyone(target):
        if not real_is_admin and real_is_isolated:
            return viewer
        projected = Viewer(
            username=viewer.username,
            signed_in=True,
            is_admin=False,
            permissions=_acting_permissions(viewer, {}, False),
            teams={EVERYONE: 'member'},
            primary_team=EVERYONE,
            no_auth=False,
            grants={},
            acting_team=EVERYONE,
            real_is_admin=real_is_admin,
            base_teams=real_teams,
            base_is_isolated=real_is_isolated,
            base_primary_team=viewer.real_primary_team,
        )
        projected.principal = principal_of(projected)
        return projected

    try:
        records = _cached_teams(conn, fresh=fresh)
    except Exception:
        return viewer
    canonical = None
    for record in records:
        name = str(
            getattr(record, 'name', None)
            or (record.get('name') if isinstance(record, dict) else '')
        )
        if name and name.lower() == target.lower() and not core().is_everyone(name):
            canonical = name
            break

    if real_is_admin:
        if canonical is None:
            return viewer
        team_name = canonical
        role = 'manager'
    else:
        matched = None
        for existing_name, existing_role in real_teams.items():
            if existing_name != EVERYONE and existing_name.lower() == target.lower():
                matched = (existing_name, existing_role)
                break
        ### Fail closed: a team whose record cannot be read now (renamed, deleted, or the
        ### teams unreadable) is never acted as (its grants would be empty and unisolated).
        if matched is None or canonical is None:
            return viewer
        team_name = canonical
        role = matched[1]

    try:
        grants = dict(core().team_grants([(team_name, role)], records, team_name) or {})
    except Exception:
        return viewer
    ### Acting as one of an isolated account's teams never leaves its wall.
    if not real_is_admin and real_is_isolated:
        grants['isolated'] = True
        base_grants = viewer.grants or {}
        for key in ('connectors', 'plugins'):
            if key in grants:
                allowed = set(base_grants.get(key, None) or [])
                grants[key] = [item for item in (grants[key] or []) if item in allowed]
        if not base_grants.get('plugins_write', False):
            grants.pop('plugins_write', None)
    isolated = bool(grants.get('isolated', False))
    teams = {team_name: role} if isolated else {EVERYONE: 'member', team_name: role}
    permissions = _acting_permissions(viewer, grants, isolated)

    projected = Viewer(
        username=viewer.username,
        signed_in=True,
        is_admin=False,
        permissions=permissions,
        teams=teams,
        primary_team=team_name,
        no_auth=False,
        grants=grants,
        acting_team=team_name,
        real_is_admin=real_is_admin,
        base_teams=real_teams,
        base_is_isolated=real_is_isolated,
        base_primary_team=viewer.real_primary_team,
    )
    projected.principal = principal_of(projected)
    return projected


#: What a non-administrator may not do from the console on a server started with `--secure`
#: (`api:permissions:actions:non_admin: false`): run actions, open the terminal, or change
#: anything (pipes, Pipelines, jobs). They still read what their permissions let them read.
SECURE_WITHHELD: frozenset = frozenset({
    'actions:execute', 'webterm:use',
    'pipes:write', 'pipes:register', 'pipes:define_sql', 'pipes:drop', 'pipes:delete',
    'jobs:write', 'jobs:execute', 'jobs:stop', 'jobs:pause', 'jobs:delete',
})


def _host_config() -> Dict[str, Any]:
    """The configuration the server gave this host (`MRSM_API_HOST_CONFIG`), or `{}`."""
    try:
        from meerschaum.api import get_uvicorn_config
        config = get_uvicorn_config()
        return config if isinstance(config, dict) else {}
    except Exception:
        return {}


def secure_mode() -> bool:
    """
    Whether non-administrators may not act from the console: the server runs with `--secure`
    (it passes `actions_non_admin: false`), or `api:permissions:actions:non_admin` is set to
    `false` in the configuration (the server reads it the same way).
    """
    if _host_config().get('actions_non_admin', True) is False:
        return True
    try:
        from meerschaum.config import get_config
        return get_config('api', 'permissions', 'actions', 'non_admin', warn=False) is False
    except Exception:
        return False


def default_role() -> str:
    """
    The role of users without permissions of their own: the server's (it resolves
    `api:permissions:default_role`, `editor` by default, or `operator` when Meerschaum 4's
    `api:permissions:actions:non_admin: true` is set), else the same rule here
    (`meerschaum.core.access.default_role`).
    """
    role = _host_config().get('default_role', None)
    if role in ROLE_LABELS:
        return str(role)
    return str(core().default_role())


def role_name(role: Any) -> str:
    """The name of a role (`meerschaum.core.access` role values, enums or strings), lowercase."""
    role = getattr(role, 'value', None) or getattr(role, 'name', None) or role
    return str(role or 'member').lower()


def role_permissions(role: str) -> Set[str]:
    """The permissions of a role preset (`viewer`, `editor`, `operator`)."""
    return {str(permission) for permission in core().role_permissions(str(role))}


def all_permissions() -> List[str]:
    """Every permission (`ALL_PERMISSIONS`), in the grid's order first."""
    names = [str(permission) for permission in core().ALL_PERMISSIONS]
    ordered = [permission for permission in GRID_PERMISSIONS if permission in names]
    return ordered + [permission for permission in names if permission not in ordered]


def session_id_of(session_data: Optional[Dict[str, Any]]) -> Optional[str]:
    """The valid session ID of a `session-store`'s data, or `None`."""
    if isinstance(session_data, dict):
        session_id = valid_session_id(session_data.get('session-id', None))
        if session_id:
            return session_id
    return None


def in_request() -> bool:
    """Whether this runs for a web request (a page or a callback)."""
    try:
        import flask
        return bool(flask.has_request_context())
    except Exception:
        return False


def request_session_id() -> Optional[str]:
    """The session of the current request: a callback's `session-store`, else the cookie."""
    try:
        import dash
        context = dash.callback_context
        for mapping in (getattr(context, 'states', None) or {}, getattr(context, 'inputs', None) or {}):
            data = mapping.get('session-store.data', None)
            session_id = session_id_of(data)
            if session_id:
                return session_id
    except Exception:
        pass
    try:
        import flask
        if flask.has_request_context():
            body = flask.request.get_json(silent=True)
            if isinstance(body, dict):
                for key in ('state', 'inputs'):
                    for item in body.get(key, None) or []:
                        if isinstance(item, dict) and item.get('id') == 'session-store':
                            session_id = session_id_of(item.get('value', None))
                            if session_id:
                                return session_id
            return valid_session_id(flask.request.cookies.get(SESSION_COOKIE_NAME, None))
    except Exception:
        pass
    return None


def _user_teams(conn: Any, username: str) -> Dict[str, str]:
    """`{team: 'member' | 'manager'}` of a user (Everyone included)."""
    from meerschaum.core import User
    teams: Dict[str, str] = {EVERYONE: 'member'}
    for team, role in (conn.get_user_teams(User(username, instance=conn)) or []):
        if team is None:
            continue
        role = str(getattr(role, 'value', role) or 'member').lower()
        name = EVERYONE if core().is_everyone(str(team)) else str(team)
        teams[name] = 'manager' if role == 'manager' else teams.get(name, 'member')
    return teams


def _primary_team(user: Any, attributes: Dict[str, Any], teams: Dict[str, str]) -> str:
    """The user's primary team (`meerschaum.core.access.primary_team`)."""
    pairs = [(team, role) for team, role in teams.items() if team != EVERYONE]
    return str(core().primary_team(attributes, pairs))


def _effective_permissions(user: Any, user_type: Optional[str], attributes: Dict[str, Any]) -> frozenset:
    """
    The user's effective permissions (`meerschaum.core.access.user_permissions`: their own list,
    else their stored role, else the default role); under `--secure` a non-administrator only
    reads (`SECURE_WITHHELD`).
    """
    names = {
        str(permission)
        for permission in core().user_permissions(user_type, attributes, default_role())
    }
    if '*' in names:
        names = set(all_permissions())
    if secure_mode() and str(user_type or '').lower() != 'admin':
        names -= SECURE_WITHHELD
    return frozenset(names)


def principal_of(viewer: Viewer) -> Any:
    """The rules' `Principal` of a viewer (`meerschaum.core.access.Principal`)."""
    return core().Principal(
        username=str(viewer.username or ''),
        is_admin=bool(viewer.is_admin or viewer.no_auth),
        permissions=sorted(viewer.permissions),
        teams=[(team, role) for team, role in viewer.teams.items()
               if team != EVERYONE or role == 'manager'],
        grants=dict(viewer.grants or {}),
    )


def make_viewer(
    username: Optional[str],
    admin: bool = False,
    role: Optional[str] = None,
    permissions: Optional[Iterable[str]] = None,
    teams: Optional[Dict[str, str]] = None,
    primary_team: Optional[str] = None,
    grants: Optional[Dict[str, Any]] = None,
) -> Viewer:
    """
    A signed-in viewer with the given rights, without reading the instance (tests, and code that
    already knows who is asking): an administrator, a role preset's permissions (`role`), or
    explicit `permissions` (default: the default role's); `teams` is `{team: 'member' |
    'manager'}` (Everyone is added).
    """
    if admin:
        names = frozenset(all_permissions())
    elif permissions is not None:
        names = frozenset(str(permission) for permission in permissions)
    else:
        names = frozenset(role_permissions(role or default_role()))
    if not admin and (grants or {}).get('isolated', False):
        names = names - frozenset(core().withheld_permissions(grants))
    team_roles: Dict[str, str] = {EVERYONE: 'member'}
    for team, team_role in (teams or {}).items():
        name = EVERYONE if core().is_everyone(str(team)) else str(team)
        team_roles[name] = 'manager' if str(team_role).lower() == 'manager' else 'member'
    if primary_team is None:
        own = sorted(team for team in team_roles if team != EVERYONE)
        primary_team = own[0] if own else EVERYONE
    viewer = Viewer(
        username=(str(username) if username else None),
        signed_in=True,
        is_admin=bool(admin),
        permissions=names,
        teams=team_roles,
        primary_team=primary_team,
        grants=dict(grants or {}),
    )
    viewer.principal = principal_of(viewer)
    return viewer


def _team_grants(conn: Any, teams: Dict[str, str], primary: Optional[str]) -> Dict[str, Any]:
    """The grants of a user's teams (`meerschaum.core.access.team_grants`)."""
    pairs = [(team, role) for team, role in teams.items() if team != EVERYONE]
    if not pairs:
        return {}
    return core().team_grants(pairs, conn.get_teams() or [], primary)


def viewer_for(username: Optional[str]) -> Viewer:
    """The viewer for a username (its type, effective permissions and teams), read now."""
    from meerschaum.api import get_api_connector, no_auth
    from meerschaum.core import User
    ### Connectors another process registered (a team's database) are picked up here: one `stat`.
    try:
        refresh_connectors()
    except Exception:
        pass
    if no_auth:
        viewer = Viewer(username=username, signed_in=True, is_admin=True, no_auth=True,
                        permissions=frozenset(all_permissions()))
        viewer.principal = principal_of(viewer)
        return viewer
    if not username:
        return ANONYMOUS
    conn = get_api_connector()
    user = User(str(username), instance=conn)
    ### Fail closed: a user whose record cannot be read has no rights.
    try:
        user_id = conn.get_user_id(user)
    except Exception:
        user_id = None
    if user_id is None:
        return Viewer(username=str(username), signed_in=False)
    user.user_id = user_id
    try:
        user_type = conn.get_user_type(user)
        attributes = conn.get_user_attributes(user) or {}
    except Exception:
        return Viewer(username=str(username), signed_in=False)
    user._attributes = attributes
    user.type = user_type
    is_admin = str(user_type or '').lower() == 'admin'
    ### Fail closed: when the teams cannot be read the viewer is walled off (they see nothing of
    ### Everyone's) instead of being treated as an ordinary member of Everyone.
    try:
        teams = _user_teams(conn, str(username))
        primary = _primary_team(user, attributes, teams)
        grants = {} if is_admin else _team_grants(conn, teams, primary)
    except Exception:
        teams = {EVERYONE: 'member'}
        primary = EVERYONE
        grants = {} if is_admin else {'isolated': True}
    permissions = frozenset(all_permissions()) if is_admin else _effective_permissions(
        user, user_type, attributes,
    )
    if not is_admin and grants.get('isolated', False):
        ### Actions and the terminal run with the server's credentials, outside the team's wall
        ### (5.1: a team that runs code keeps them, as its own sandboxed processes).
        permissions = permissions - frozenset(core().withheld_permissions(grants))
    viewer = Viewer(
        username=str(username),
        signed_in=True,
        is_admin=is_admin,
        permissions=permissions,
        teams=teams,
        primary_team=primary,
        grants=grants,
    )
    viewer.principal = principal_of(viewer)
    return viewer


def resolve_viewer(
    session: Optional[Dict[str, Any]] = None,
    *,
    session_id: Optional[str] = None,
    fresh: bool = False,
) -> Viewer:
    """
    The viewer of a console session: `session_id`, else the `session-store` data `session`, else
    the current request's session (a callback's `session-store`, else the session cookie).
    Without any of them (outside a web request, or a visitor who is not signed in) the viewer is
    `ANONYMOUS`; a check of the console's own asks for `SYSTEM` by name (`current_viewer()`).

    - `fresh=False` (default) reuses the user's base viewer for `VIEWER_TTL_SECONDS`;
      `fresh=True` reads the user and the teams again (the session lookup keeps its own few
      seconds of cache, so a logout may take that long to apply).
    - The viewer is narrowed to the request's acting team (the `mrsm-team` cookie, or
      `nav_context`), on every page alike.
    - An isolated team's member is projected for native execution on every call.
    """
    from meerschaum.api import no_auth
    if no_auth:
        return viewer_for(None)
    sid = (
        valid_session_id(session_id)
        if session_id is not None
        else (session_id_of(session) or request_session_id())
    )
    if not sid:
        return ANONYMOUS
    now = time.monotonic()
    entry = None
    if not fresh:
        with _viewer_lock:
            entry = _viewer_cache.get(sid, None)
    if entry is not None and now - entry[0] < VIEWER_TTL_SECONDS:
        base_viewer = entry[1]
    else:
        username = get_username_from_session(sid)
        base_viewer = viewer_for(username) if username else ANONYMOUS
        with _viewer_lock:
            if len(_viewer_cache) > 4096:
                _viewer_cache.clear()
            _viewer_cache[sid] = (now, base_viewer)
    acting_team = request_acting_team()
    viewer = apply_acting_team(base_viewer, acting_team, fresh=fresh) if acting_team else base_viewer
    ### An isolated team's member: what the server projects for native execution, read on every
    ### call and never cached.
    from meerschaum.api.dash._native_execution import project
    return project(viewer, sid)


def current_viewer() -> Viewer:
    """
    The viewer of the current request (a callback's or a page's); outside a web request, the
    local trust boundary, `SYSTEM` (sees every pipe), as for the CLI and Python.
    """
    if not in_request():
        return SYSTEM
    return resolve_viewer()
