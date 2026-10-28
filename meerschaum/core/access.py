#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The access model: permissions, roles, teams, pipe access and locks.

The rules are the core's (`meerschaum_core::access`), so the web console and the Python API
apply exactly what the API server applies. Two kinds of rules live here:

- **Who may do what**: permissions, roles, teams, and a pipe's owner, team and visibility. The
  API server, the MCP server and the web console enforce these for the users they serve. Local
  command-line and Python access with the instance's own credentials sees everything.
- **Locks** (`parameters['locks']`): every write path refuses what a pipe's locks refuse,
  locally too. `-y`, `--force` and `--allow-protected` never bypass a lock.

The model is open by default: a pipe without an `access` key belongs to the team Everyone (every
user is a member) and anyone who can see it may change it within their permissions.

Permissions
-----------
The permissions are the token scopes (`ALL_PERMISSIONS`). A user's effective permissions:

1. an administrator (user type `admin`) holds every permission;
2. explicit permissions (`attributes['scopes']`) are used as listed (`'*'` is every permission);
3. otherwise the role in `attributes['role']`, else the default role
   (`api:permissions:default_role`, `'editor'` unless configured).

Roles (`ROLES`): `'viewer'` (reads pipes, jobs, logs, plugins and the connectors list; their own
account), `'editor'` (viewer, plus registering, editing and syncing pipes, SQL definitions
included, starting jobs and read-only SQL), `'operator'` (editor, plus actions, the web terminal
and every job control) and `'admin'` (the user type: everything).

Pipe access
-----------
`parameters['access']`: `owner` (a username or `None`), `team` (a team name; `None` is
Everyone), `visibility` (`'team'`, `'private'` or `'everyone'`) and `edit` (`'team'` or
`'owner'`). See `can_see()`, `can_edit()`, `can_manage_access()` and `can_lock()`.

Locks
-----
`parameters['locks']`: a list of `'parameters'`, `'sync'`, `'clear'`, `'drop'`, `'delete'` or
`'all'`. `check_lock(locks, op)` says whether a write is refused; the writes (`op`) are
`'edit'`, `'sync'`, `'replace'`, `'deduplicate'`, `'clear'`, `'drop'`, `'rebuild'`, `'schema'`
(table schema changes: `verify schema`, repartitioning, dropping indices), `'delete'` and
`'delete_and_drop'`.

Examples
--------
>>> from meerschaum.core.access import Principal, can_see
>>> alice = Principal('alice', permissions=['pipes:read'], teams=[('ops', 'member')])
>>> can_see(alice, {'team': 'ops', 'visibility': 'team'})
True
>>> can_see(alice, {'team': 'data', 'visibility': 'team'})
False
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import meerschaum as mrsm

__all__ = (
    'Permission',
    'ALL_PERMISSIONS',
    'PERMISSION_DESCRIPTIONS',
    'ROLES',
    'DEFAULT_ROLE',
    'EVERYONE',
    'TEAM_ROLES',
    'VISIBILITIES',
    'EDIT_MODES',
    'LOCKS',
    'LOCK_OPS',
    'ACCESS_KEY',
    'LOCKS_KEY',
    'Principal',
    'role_permissions',
    'resolve_default_role',
    'default_role',
    'effective_permissions',
    'user_permissions',
    'user_role',
    'attributes_with_role',
    'attributes_with_permissions',
    'token_permissions',
    'get_pipe_access',
    'parse_access',
    'get_locks',
    'parse_locks',
    'locks_with',
    'locks_without',
    'check_lock',
    'lock_refusal',
    'is_lock_refusal',
    'can_see',
    'can_edit',
    'can_manage_access',
    'can_lock',
    'can_see_email',
    'can_manage_team',
    'can_administer_teams',
    'check_access_change',
    'access_for_new_pipe',
    'primary_team',
    'valid_team_name',
    'same_team',
    'is_everyone',
    'sql_definition_changed',
    'get_principal',
    'filter_visible_pipes',
)


def _core():
    from meerschaum import _core as core
    return core


class Permission:
    """
    The permission names (the token scopes), e.g. `Permission.PIPES_READ`.
    """
    PIPES_READ: str = 'pipes:read'
    PIPES_WRITE: str = 'pipes:write'
    PIPES_REGISTER: str = 'pipes:register'
    PIPES_DEFINE_SQL: str = 'pipes:define_sql'
    PIPES_DROP: str = 'pipes:drop'
    PIPES_DELETE: str = 'pipes:delete'
    ACTIONS_EXECUTE: str = 'actions:execute'
    WEBTERM_USE: str = 'webterm:use'
    CONNECTORS_READ: str = 'connectors:read'
    SQL_READ: str = 'sql:read'
    JOBS_READ: str = 'jobs:read'
    JOBS_WRITE: str = 'jobs:write'
    JOBS_EXECUTE: str = 'jobs:execute'
    JOBS_STOP: str = 'jobs:stop'
    JOBS_PAUSE: str = 'jobs:pause'
    JOBS_DELETE: str = 'jobs:delete'
    LOGS_READ: str = 'logs:read'
    INSTANCE_READ: str = 'instance:read'
    INSTANCE_CHAIN: str = 'instance:chain'
    PLUGINS_READ: str = 'plugins:read'
    PLUGINS_WRITE: str = 'plugins:write'
    PLUGINS_DELETE: str = 'plugins:delete'
    USERS_READ: str = 'users:read'
    USERS_WRITE: str = 'users:write'
    USERS_REGISTER: str = 'users:register'
    USERS_DELETE: str = 'users:delete'


def _static_scopes() -> Dict[str, str]:
    from meerschaum._internal.static import STATIC_CONFIG
    return dict(STATIC_CONFIG['tokens']['scopes'])


### The token scopes of `static.toml` (the core asserts the same list).
PERMISSION_DESCRIPTIONS: Dict[str, str] = _static_scopes()
ALL_PERMISSIONS: Tuple[str, ...] = tuple(PERMISSION_DESCRIPTIONS)
ROLES: Tuple[str, ...] = ('viewer', 'editor', 'operator', 'admin')
DEFAULT_ROLE: str = 'editor'
EVERYONE: str = 'Everyone'
TEAM_ROLES: Tuple[str, ...] = ('member', 'manager')
VISIBILITIES: Tuple[str, ...] = ('team', 'private', 'everyone')
EDIT_MODES: Tuple[str, ...] = ('team', 'owner')
LOCKS: Tuple[str, ...] = ('parameters', 'sync', 'clear', 'drop', 'delete', 'all')
LOCK_OPS: Tuple[str, ...] = (
    'edit', 'sync', 'replace', 'deduplicate', 'clear', 'drop', 'rebuild', 'schema', 'delete',
    'delete_and_drop',
)
ACCESS_KEY: str = 'access'
LOCKS_KEY: str = 'locks'
_ACCESS_FIELDS = {'owner', 'team', 'visibility', 'edit', 'edit_mode'}


@dataclass
class Principal:
    """
    Who is asking: a user as the API server, the MCP server or the web console sees them.

    Parameters
    ----------
    username: str
        The username (empty for an anonymous caller).

    is_admin: bool, default False
        Whether the user is an administrator (every permission).

    permissions: List[str], default []
        The effective permissions (a token's already intersected with its user's).

    teams: List[Tuple[str, str]], default []
        The user's teams as `(team, role)` pairs, `role` being `'member'` or `'manager'`
        (Everyone is implied).
    """
    username: str = ''
    is_admin: bool = False
    permissions: List[str] = field(default_factory=list)
    teams: List[Tuple[str, str]] = field(default_factory=list)

    def has(self, permission: str) -> bool:
        """Whether the principal holds `permission` (administrators hold all)."""
        return self.is_admin or permission in self.permissions or '*' in self.permissions

    def team_role(self, team: Optional[str]) -> Optional[str]:
        """The principal's role in `team` (`None` is Everyone, where every user is a member)."""
        roles = [role for name, role in self.teams if same_team(name, team)]
        if 'manager' in roles:
            return 'manager'
        if roles or is_everyone(team):
            return 'member'
        return None

    def is_member(self, team: Optional[str]) -> bool:
        """Whether the principal is in `team` (any role)."""
        return self.team_role(team) is not None

    def is_manager(self, team: Optional[str]) -> bool:
        """Whether the principal manages `team`."""
        return self.team_role(team) == 'manager'

    @property
    def team_names(self) -> List[str]:
        """The names of the principal's teams (Everyone first)."""
        out = [EVERYONE]
        for name, _ in self.teams:
            if not any(same_team(name, existing) for existing in out):
                out.append(name)
        return out

    def to_dict(self) -> Dict[str, Any]:
        """The principal as a dictionary."""
        return {
            'username': self.username,
            'is_admin': self.is_admin,
            'permissions': list(self.permissions),
            'teams': [tuple(t) for t in self.teams],
        }


def _principal_arg(principal: Union[Principal, Dict[str, Any], Any]) -> Any:
    if isinstance(principal, Principal):
        return principal.to_dict()
    return principal


def _access_arg(access: Any) -> Any:
    """An access value from a dict, a pipe, or a pipe's parameters."""
    if access is None:
        return None
    if hasattr(access, 'parameters') and hasattr(access, 'connector_keys'):
        return get_pipe_access(access)
    ### A pipe's parameters (any key besides the access keys): their `access`.
    if isinstance(access, dict) and not set(access) <= _ACCESS_FIELDS:
        return access.get(ACCESS_KEY, None)
    return access


def role_permissions(role: str) -> List[str]:
    """
    Return the permissions of a role preset (`'viewer'`, `'editor'`, `'operator'`, `'admin'`).
    """
    return list(_core().access_role_permissions(role))


def resolve_default_role(
    configured: Optional[str] = None,
    legacy_actions_non_admin: Optional[bool] = None,
) -> str:
    """
    Return the default role from `api:permissions:default_role` (`configured`) and v4's
    `api:permissions:actions:non_admin` (`True` set in the configuration maps to `'operator'`).
    A value that names no default role gives `'viewer'`.
    """
    return _core().access_resolve_default_role(configured, legacy_actions_non_admin)


def default_role() -> str:
    """
    Return this host's default role (`api:permissions:default_role`, see
    `resolve_default_role()`).
    """
    from meerschaum.config import get_config
    configured = get_config('api', 'permissions', 'default_role', warn=False)
    legacy = get_config('api', 'permissions', 'actions', 'non_admin', warn=False)
    return resolve_default_role(
        str(configured) if configured is not None else None,
        bool(legacy) if legacy is not None else None,
    )


def effective_permissions(
    user_type: Optional[str] = None,
    scopes: Optional[Sequence[str]] = None,
    default_role: Optional[str] = None,
) -> List[str]:
    """
    Return a user's effective permissions from their type and explicit scopes (`None`: the
    `default_role`'s permissions).
    """
    return list(_core().access_effective_permissions(user_type, scopes, default_role))


def user_permissions(
    user_type: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    default_role: Optional[str] = None,
) -> List[str]:
    """
    Return a user's effective permissions from their stored type and attributes
    (`attributes['scopes']`, else `attributes['role']`, else `default_role`).
    """
    if default_role is None:
        default_role = globals()['default_role']()
    return list(_core().access_user_permissions(user_type, attributes, default_role))


def user_role(
    user_type: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
    default_role: Optional[str] = None,
) -> str:
    """
    Return a user's role: `'admin'` for administrators; the preset their explicit permissions
    equal, else `'custom'`; else their stored role or the default role.
    """
    if default_role is None:
        default_role = globals()['default_role']()
    return _core().access_user_role(user_type, attributes, default_role)


def attributes_with_role(attributes: Optional[Dict[str, Any]], role: str) -> Dict[str, Any]:
    """
    Return a user's attributes with a role preset set (explicit `scopes` removed, so the
    preset's permissions apply).
    """
    return dict(_core().access_attributes_with_role(attributes or {}, role))


def attributes_with_permissions(
    attributes: Optional[Dict[str, Any]],
    permissions: Iterable[str],
) -> Dict[str, Any]:
    """
    Return a user's attributes with explicit permissions (`scopes`) and the matching `role`
    (`'custom'` unless they equal a preset).
    """
    return dict(_core().access_attributes_with_permissions(attributes or {}, list(permissions)))


def token_permissions(token_scopes: Iterable[str], user_permissions: Iterable[str]) -> List[str]:
    """
    Return a token's permissions when it is used: its scopes intersected with its user's current
    effective permissions.
    """
    return list(_core().access_token_permissions(list(token_scopes), list(user_permissions)))


def get_pipe_access(pipe_or_parameters: Any) -> Dict[str, Any]:
    """
    Return the access of a pipe (or of its parameters): `{'owner', 'team', 'visibility',
    'edit'}`, with the defaults for missing keys (`team` `None` is Everyone).
    """
    parameters = pipe_or_parameters
    if hasattr(pipe_or_parameters, 'connector_keys'):
        parameters = pipe_or_parameters._attributes.get('parameters', {})
        if not pipe_or_parameters.temporary:
            parameters = pipe_or_parameters.attributes.get('parameters', {})
    return dict(_core().access_pipe_access(dict(parameters or {})))


def parse_access(value: Any) -> Dict[str, Any]:
    """
    Parse an access value strictly (raises `ValueError` on unknown keys, visibilities or edit
    modes) and return it normalized.
    """
    return dict(_core().access_parse_access(value))


def get_locks(pipe_or_parameters: Any) -> List[str]:
    """
    Return the locks of a pipe (its registered parameters) or of a parameters dictionary.
    """
    parameters = pipe_or_parameters
    if hasattr(pipe_or_parameters, 'connector_keys'):
        parameters = pipe_or_parameters.attributes.get('parameters', {})
    if isinstance(parameters, dict):
        return list(_core().access_locks(dict(parameters)))
    return list(_core().access_locks(parameters))


def parse_locks(value: Union[str, Iterable[str], None]) -> List[str]:
    """
    Parse locks a person gave (`'drop,sync'`, `['sync']`; empty is `['all']`); raises
    `ValueError` on an unknown name.
    """
    if value is None:
        value = []
    return list(_core().access_parse_locks(value))


def locks_with(current: Optional[Iterable[str]], added: Optional[Iterable[str]]) -> List[str]:
    """Return `current` locks with `added` (`'all'` absorbs the rest)."""
    return list(_core().access_locks_with(list(current or []), list(added or [])))


def locks_without(current: Optional[Iterable[str]], removed: Optional[Iterable[str]]) -> List[str]:
    """
    Return `current` locks without `removed` (removing `'all'` removes every lock; removing one
    kind from `'all'` keeps the others).
    """
    return list(_core().access_locks_without(list(current or []), list(removed or [])))


def check_lock(locks: Optional[Iterable[str]], op: str) -> Optional[str]:
    """
    Return `None` when `locks` allow the write `op` (see `LOCK_OPS`), else why not
    (`'it is locked (sync)'`).
    """
    return _core().access_check_lock(list(locks or []), op)


def lock_refusal(pipe: 'mrsm.Pipe', locks: Optional[Iterable[str]], op: str) -> Optional[str]:
    """
    Return the refusal message of the write `op` on `pipe` under `locks` (`None` when allowed):
    it names the pipe, the lock and how to unlock it.
    """
    return _core().access_lock_refusal(pipe, list(locks or []), op)


def is_lock_refusal(message: str) -> bool:
    """Whether a message is a lock refusal."""
    return bool(_core().access_is_lock_refusal(str(message)))


def can_see(principal: Union[Principal, Dict[str, Any]], access: Any = None) -> bool:
    """
    Whether `principal` can see a pipe with `access` (a dict, a pipe, or a pipe's parameters):
    administrators and the owner always; `'everyone'`: every user; `'team'`: the team's members;
    `'private'`: the team's managers.
    """
    return bool(_core().access_can('see', _principal_arg(principal), _access_arg(access)))


def can_edit(principal: Union[Principal, Dict[str, Any]], access: Any = None) -> bool:
    """
    Whether `principal` may change a pipe with `access` (within their permissions): edit
    `'team'`: anyone who can see it; edit `'owner'`: the owner, the team's managers and
    administrators.
    """
    return bool(_core().access_can('edit', _principal_arg(principal), _access_arg(access)))


def can_manage_access(principal: Union[Principal, Dict[str, Any]], access: Any = None) -> bool:
    """
    Whether `principal` may change a pipe's access: its owner, its team's managers and
    administrators.
    """
    return bool(
        _core().access_can('manage_access', _principal_arg(principal), _access_arg(access))
    )


def can_lock(principal: Union[Principal, Dict[str, Any]], access: Any = None) -> bool:
    """
    Whether `principal` may lock or unlock a pipe through the API or the console: its owner,
    its team's managers and administrators.
    """
    return bool(_core().access_can('lock', _principal_arg(principal), _access_arg(access)))


def can_see_email(
    viewer: Union[Principal, Dict[str, Any]],
    target_user_teams: Optional[Iterable[Tuple[str, str]]] = None,
    target_username: Optional[str] = None,
) -> bool:
    """
    Whether `viewer` may see the email address of a user in `target_user_teams`:
    administrators, the managers of a team the user is in, and (with `target_username`) the user
    themself.
    """
    return bool(_core().access_can_see_email(
        _principal_arg(viewer),
        [tuple(t) for t in (target_user_teams or [])],
        target_username,
    ))


def can_manage_team(principal: Union[Principal, Dict[str, Any]], team: str) -> bool:
    """Whether `principal` may add and remove members of `team`: its managers and administrators."""
    return bool(_core().access_can_manage_team(_principal_arg(principal), team))


def can_administer_teams(principal: Union[Principal, Dict[str, Any]]) -> bool:
    """Whether `principal` may create, rename and delete teams: administrators."""
    return bool(_core().access_can_administer_teams(_principal_arg(principal)))


def check_access_change(
    principal: Union[Principal, Dict[str, Any]],
    old: Any,
    new: Any,
) -> Optional[str]:
    """
    Return `None` when `principal` may change a pipe's access from `old` to `new` through the
    API or the console, else why not.
    """
    return _core().access_check_access_change(
        _principal_arg(principal), _access_arg(old), _access_arg(new)
    )


def access_for_new_pipe(
    principal: Union[Principal, Dict[str, Any]],
    requested: Optional[Dict[str, Any]] = None,
    primary_team: str = EVERYONE,
) -> Dict[str, Any]:
    """
    Return the access of a pipe `principal` registers through the API or the console: the owner
    is the caller (administrators may name another) and the team defaults to `primary_team`.
    """
    return dict(_core().access_for_new_pipe(_principal_arg(principal), requested, primary_team))


def primary_team(
    attributes: Optional[Dict[str, Any]],
    user_teams: Optional[Iterable[Tuple[str, str]]],
) -> str:
    """
    Return a user's primary team: `attributes['primary_team']` when the user is in it, else
    their first team other than Everyone, else Everyone.
    """
    return _core().access_primary_team(attributes, [tuple(t) for t in (user_teams or [])])


def valid_team_name(name: str) -> mrsm.SuccessTuple:
    """Return whether a team name is valid (the username rules, and not `'Everyone'`)."""
    return tuple(_core().access_valid_team_name(name))


def same_team(a: Optional[str], b: Optional[str]) -> bool:
    """Whether two team names name the same team (without case; `None` is Everyone)."""
    return bool(_core().access_same_team(a, b))


def is_everyone(team: Optional[str]) -> bool:
    """Whether a team name means Everyone (`None`, empty, or `'Everyone'` in any case)."""
    return team is None or not str(team).strip() or str(team).strip().lower() == 'everyone'


def sql_definition_changed(
    connector_keys: str,
    old: Dict[str, Any],
    new: Dict[str, Any],
) -> bool:
    """
    Whether going from `old` to `new` parameters sets or changes a pipe's SQL definition
    (through the API this needs `pipes:define_sql`).
    """
    return bool(_core().access_sql_definition_changed(str(connector_keys), old or {}, new or {}))


def get_principal(
    user: Union['mrsm.core.User', str],
    instance: Union[str, 'mrsm.connectors.InstanceConnector', None] = None,
    debug: bool = False,
) -> Principal:
    """
    Return the principal of a user on an instance: their type, effective permissions and teams.

    Parameters
    ----------
    user: Union[User, str]
        The user (or username).

    instance: Union[str, InstanceConnector, None], default None
        The instance holding the user (the user's own, else the configured instance).

    Returns
    -------
    A `Principal`.
    """
    from meerschaum.core.User import User
    if not isinstance(user, User):
        user = User(str(user), instance=instance)
    if instance is not None and user._instance_keys is None:
        user._instance_keys = str(instance)
    username = user.username
    is_admin = (user.type or '').lower() == 'admin'
    if not user.type:
        try:
            is_admin = (user.instance_connector.get_user_type(user, debug=debug) or '') == 'admin'
        except Exception:
            is_admin = False
    return Principal(
        username=username,
        is_admin=is_admin,
        permissions=list(user.permissions),
        teams=list(user.teams),
    )


def filter_visible_pipes(
    principal: Union[Principal, Dict[str, Any]],
    pipes: Iterable['mrsm.Pipe'],
) -> List['mrsm.Pipe']:
    """
    Return the pipes `principal` can see (`can_see()` on each pipe's access).
    """
    return [pipe for pipe in pipes if can_see(principal, get_pipe_access(pipe))]
