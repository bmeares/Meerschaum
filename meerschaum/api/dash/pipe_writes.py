#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Every change the console makes to a pipe's registration, with the access model's checks run on
the server side of the callback (`meerschaum.api.dash.access`, `meerschaum.api.dash.sql_guard`):

- `save_parameters`: a pipe's parameters (the Parameters and Definition tabs, `fetch`, its
  parents and children): edit rights, the right to change `access` and `locks`, the
  `pipes:define_sql` permission and the SQL guard for a SQL definition, and its locks;
- `register_pipe`: a new pipe (New pipe, Copy): `pipes:register`, the connector's `teams`, the
  same SQL checks, and its `access` (the viewer owns it, in their primary team by default);
- `set_access` / `set_locks`: Manage > Access… and Locks…;
- `connector_allows`: a connector's `teams` attribute (which teams may use it for pipes).

Locks are enforced again by the core on every write, whoever writes.
"""

from __future__ import annotations

from copy import deepcopy

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple, Tuple
from meerschaum.api.dash import access
from meerschaum.api.dash import sql_guard


def _viewer(viewer: Any = None) -> Any:
    return viewer if viewer is not None else access.current_viewer()


def _targets_changed(old: Dict[str, Any], new: Dict[str, Any]) -> bool:
    return any((old or {}).get(key) != (new or {}).get(key) for key in sql_guard.TARGET_KEYS)


def connector_allows(viewer: Any, connector_keys: str, team: Optional[str]) -> SuccessTuple:
    """
    Whether a pipe of `team` (`None`: Everyone) may use the connector `connector_keys` through
    the console: its `teams` attribute lists the teams that may (every team when it has none).
    Administrators may use every connector.
    """
    if viewer.no_auth or viewer.is_admin:
        return True, ''
    try:
        connector = mrsm.get_connector(str(connector_keys))
    except Exception:
        return True, ''
    teams = getattr(connector, 'teams', None)
    if teams is None and hasattr(connector, '__dict__'):
        teams = connector.__dict__.get('teams', None)
    if not teams:
        return True, ''
    if isinstance(teams, str):
        teams = [part.strip() for part in teams.split(',') if part.strip()]
    allowed = {str(name).strip().lower() for name in teams}
    name = (team or access.EVERYONE)
    if str(name).lower() in allowed:
        return True, ''
    return False, f"The connector '{connector_keys}' is not available to the team '{name}'."


def _check_sql(viewer: Any, pipe: mrsm.Pipe, old: Dict[str, Any], new: Dict[str, Any]) -> SuccessTuple:
    """`pipes:define_sql` and the SQL guard for a changed definition or target (non-admins)."""
    changed = access.sql_definition_changed(str(pipe.connector_keys), old or {}, new or {})
    if changed and not viewer.can('pipes:define_sql'):
        return False, "Your account may not set SQL definitions."
    if changed or _targets_changed(old, new) or not old:
        refusal = sql_guard.check_parameters(viewer, pipe, new or {}, check_definitions=changed)
        if refusal:
            return False, refusal
    return True, ''


def referenced_refs(pipe: mrsm.Pipe, parameters: Dict[str, Any]) -> List[str]:
    """
    The `pipe_ref`s of the pipes a parameters tree references: its `{{ Pipe(...) }}` templates
    (in any string, attribute chains included) and its `reference` / `references`.
    """
    from meerschaum import _core
    from meerschaum.utils.pipes import get_pipe_from_value
    refs: List[str] = []

    def _texts(value: Any):
        if isinstance(value, str):
            if '{{' in value:
                yield value
        elif isinstance(value, dict):
            for item in value.values():
                yield from _texts(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                yield from _texts(item)

    for text in _texts(parameters or {}):
        try:
            refs.extend(_core.extract_pipe_references(
                text, str(pipe.instance_keys), str(pipe.connector_keys), str(pipe.metric_key),
                (None if pipe.location_key is None else str(pipe.location_key)),
            ))
        except Exception:
            continue
    values = (parameters or {}).get('references', (parameters or {}).get('reference', None)) or []
    if isinstance(values, (str, dict)):
        values = [values]
    for value in values if isinstance(values, list) else []:
        try:
            other = get_pipe_from_value(value, _pipe=pipe)
        except Exception:
            continue
        if other is not None:
            refs.append(access._ref(other.instance_keys, other.connector_keys, other.metric_key, other.location_key))
    return list(dict.fromkeys(refs))


def _check_references(viewer: Any, pipe: mrsm.Pipe, old: Dict[str, Any], new: Dict[str, Any]) -> SuccessTuple:
    """
    The pipes the new parameters reference that the old ones did not (`{{ Pipe() }}` templates,
    `reference(s)`) must be pipes the viewer may see, as the API server requires: a template's
    attribute chain or an inherited definition would read a hidden pipe's parameters.
    """
    if viewer.no_auth or viewer.is_admin:
        return True, ''
    from meerschaum.api.dash.engine import split_ref
    before = set(referenced_refs(pipe, old or {}))
    for ref in referenced_refs(pipe, new or {}):
        if ref in before:
            continue
        instance_keys, connector_keys, metric_key, location_key = split_ref(ref)
        if access.visible_pipe(connector_keys, metric_key, location_key, instance_keys, viewer=viewer) is None:
            return False, f"Refusing to reference Pipe('{connector_keys}', '{metric_key}'): it is a pipe you cannot see."
    return True, ''


def save_parameters(pipe: mrsm.Pipe, parameters: Dict[str, Any], viewer: Any = None) -> SuccessTuple:
    """Replace a pipe's parameters with `parameters` after the console's checks."""
    viewer = _viewer(viewer)
    if not isinstance(parameters, dict):
        return False, "The parameters must be a dictionary."
    try:
        old = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    except Exception as e:
        return False, f"Could not read {pipe}: {e}"
    new = deepcopy(parameters)
    allowed, message = access.check_parameters_change(viewer, pipe, old, new)
    if not allowed:
        return False, message
    allowed, message = _check_sql(viewer, pipe, old, new)
    if not allowed:
        return False, message
    allowed, message = _check_references(viewer, pipe, old, new)
    if not allowed:
        return False, message
    if (old.get('access') or None) != (new.get('access') or None):
        team = access.access_fields(new).get('team')
        allowed, message = connector_allows(viewer, str(pipe.connector_keys), team)
        if not allowed:
            return False, message
    pipe.parameters = new
    return pipe.edit()


def update_parameters(pipe: mrsm.Pipe, patch: Dict[str, Any], viewer: Any = None) -> SuccessTuple:
    """Merge `patch` into a pipe's parameters (one level deep for `fetch`) and save them."""
    try:
        parameters = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    except Exception as e:
        return False, f"Could not read {pipe}: {e}"
    for key, value in (patch or {}).items():
        if isinstance(value, dict) and isinstance(parameters.get(key), dict):
            parameters[key] = {**parameters[key], **value}
        else:
            parameters[key] = value
    return save_parameters(pipe, parameters, viewer=viewer)


def new_access(
    viewer: Any,
    team: Optional[str] = None,
    visibility: Optional[str] = None,
    owner: Optional[str] = None,
) -> Dict[str, Any]:
    """
    The `access` of a pipe the viewer registers: they own it (an administrator may name another
    owner), in `team` (default: their primary team), visible as `visibility` (default `team`).
    """
    team = str(team).strip() if team else None
    if team and team != access.EVERYONE and team not in access.assignable_teams(viewer):
        raise ValueError(f"You are not a member of the team '{team}'.")
    return access.new_pipe_access(viewer, team=team, visibility=visibility, owner=owner)


def register_pipe(
    pipe: mrsm.Pipe,
    parameters: Dict[str, Any],
    viewer: Any = None,
    team: Optional[str] = None,
    visibility: Optional[str] = None,
    owner: Optional[str] = None,
) -> SuccessTuple:
    """
    Register `pipe` with `parameters` (its `access` set by `new_access`) after the console's
    checks: `pipes:register` (and `api:permissions:registration:pipes`), a team the viewer may
    give it, the connector's `teams`, and the SQL checks for a definition or a target.
    """
    from meerschaum.config import get_config
    viewer = _viewer(viewer)
    if not get_config('api', 'permissions', 'registration', 'pipes', warn=False):
        return False, "This server does not allow registering pipes (`api:permissions:registration:pipes`)."
    if not viewer.can('pipes:register'):
        return False, "Your account may not register pipes."
    team = str(team).strip() if team else None
    if team and team != access.EVERYONE and team not in access.assignable_teams(viewer):
        return False, f"You are not a member of the team '{team}'."
    parameters = deepcopy(parameters or {})
    parameters['access'] = new_access(viewer, team=team, visibility=visibility, owner=owner)
    allowed, message = connector_allows(
        viewer, str(pipe.connector_keys), access.access_fields(parameters).get('team'),
    )
    if not allowed:
        return False, message
    allowed, message = _check_sql(viewer, pipe, {}, parameters)
    if not allowed:
        return False, message
    allowed, message = _check_references(viewer, pipe, {}, parameters)
    if not allowed:
        return False, message
    new_pipe = mrsm.Pipe(
        pipe.connector_keys, pipe.metric_key, pipe.location_key,
        instance=pipe.instance_keys, parameters=parameters,
    )
    try:
        return new_pipe.register()
    except Exception as e:
        return False, f"Failed to register {new_pipe}: {e}"


def set_access(
    pipe: mrsm.Pipe,
    owner: Optional[str],
    team: Optional[str],
    visibility: Optional[str],
    edit: Optional[str],
    viewer: Any = None,
) -> SuccessTuple:
    """Manage > Access…: the owner, team, visibility and edit mode of a pipe."""
    viewer = _viewer(viewer)
    try:
        parameters = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    except Exception as e:
        return False, f"Could not read {pipe}: {e}"
    if not access.can_manage_access(viewer, parameters):
        return False, "Only the pipe's owner, its team's managers and administrators may change its access."
    visibility = visibility if visibility in {value for value, _ in access.VISIBILITIES} else 'team'
    edit = edit if edit in {value for value, _ in access.EDIT_MODES} else 'team'
    team = None if (not team or team == access.EVERYONE) else str(team)
    owner = (str(owner).strip() or None) if owner else None
    if owner is not None:
        from meerschaum.api import get_api_connector
        if owner not in set(str(name) for name in (get_api_connector().get_users() or [])):
            return False, f"User '{owner}' does not exist."
    parameters['access'] = {'owner': owner, 'team': team, 'visibility': visibility, 'edit': edit}
    setter = getattr(pipe, 'set_access', None)
    allowed, message = access.check_parameters_change(
        viewer, pipe, pipe.get_parameters(apply_symlinks=False) or {}, parameters,
    )
    if not allowed:
        return False, message
    allowed, message = connector_allows(viewer, str(pipe.connector_keys), team)
    if not allowed:
        return False, message
    if callable(setter):
        try:
            return setter(team=team, visibility=visibility, edit=edit, owner=owner)
        except TypeError:
            pass
    pipe.parameters = parameters
    return pipe.edit()


def set_locks(pipe: mrsm.Pipe, locks: List[str], viewer: Any = None) -> SuccessTuple:
    """Manage > Locks…: lock and unlock a pipe (its owner, team managers and admins)."""
    viewer = _viewer(viewer)
    try:
        parameters = pipe.get_parameters(apply_symlinks=False, refresh=True) or {}
    except Exception as e:
        return False, f"Could not read {pipe}: {e}"
    if not access.can_lock(viewer, parameters):
        return False, "Only the pipe's owner, its team's managers and administrators may lock or unlock it."
    known = [lock for lock, _ in access.LOCKS]
    wanted = [lock for lock in known if lock in set(locks or [])]
    current = access.pipe_locks(parameters)
    added = [lock for lock in wanted if lock not in current]
    removed = [lock for lock in current if lock not in wanted]
    if not added and not removed:
        return True, f"{pipe} is unchanged."
    if removed and hasattr(pipe, 'unlock'):
        success, message = pipe.unlock(*removed)
        if not success:
            return False, message
    if added and hasattr(pipe, 'lock'):
        success, message = pipe.lock(*added)
        if not success:
            return False, message
    if not hasattr(pipe, 'lock'):
        new_parameters = deepcopy(parameters)
        new_parameters['locks'] = wanted
        pipe.parameters = new_parameters
        return pipe.edit()
    if not wanted:
        return True, f"Unlocked {pipe}."
    return True, f"Locked {pipe} against {', '.join(wanted)}."


def claim_new_pipe(
    pipe: mrsm.Pipe,
    viewer: Any = None,
    team: Optional[str] = None,
    visibility: Optional[str] = None,
) -> SuccessTuple:
    """
    Give a pipe the console just registered for the viewer (a saved Pipeline's registry pipe:
    New pipeline, Copy) the access of a new pipe: the viewer owns it, in `team` (default: their
    primary team), visible as `visibility`. Only for a pipe registered by this request.
    """
    viewer = _viewer(viewer)
    if viewer.no_auth:
        return True, ''
    team = str(team).strip() if team else None
    if team and team != access.EVERYONE and team not in access.assignable_teams(viewer):
        return False, f"You are not a member of the team '{team}'."
    try:
        parameters = deepcopy(pipe.get_parameters(apply_symlinks=False, refresh=True) or {})
    except Exception as e:
        return False, f"Could not read {pipe}: {e}"
    parameters['access'] = new_access(viewer, team=team, visibility=visibility)
    pipe.parameters = parameters
    return pipe.edit()
