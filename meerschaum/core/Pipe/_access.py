#! /usr/bin/env python3
# vim:fenc=utf-8

"""
A pipe's access (owner, team, visibility, edit mode) and locks (`meerschaum.core.access`).

`parameters['access']` says who may see and change a pipe through the API server, the MCP
server and the web console; local access with the instance's credentials sees everything.
`parameters['locks']` refuses writes everywhere (`-y`, `--force` and `--allow-protected` never
bypass a lock).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import meerschaum as mrsm

_UNSET = object()

### Instance types whose write methods run in the core, which checks locks itself.
_CORE_INSTANCE_TYPES = ('sql', 'api', 'valkey')


def _raw_parameters(self) -> Dict[str, Any]:
    """The pipe's raw parameters (the registered ones patched with the in-memory ones)."""
    if self.temporary:
        return dict(self._attributes.get('parameters', None) or {})
    return dict(self.attributes.get('parameters', None) or {})


@property
def access(self) -> Dict[str, Any]:
    """
    The pipe's access: `{'owner', 'team', 'visibility', 'edit'}` (`team` `None` is
    `Everyone`). Change it with `set_access()`.
    """
    from meerschaum.core.access import get_pipe_access
    return get_pipe_access(_raw_parameters(self))


@property
def owner(self) -> Optional[str]:
    """The username of the pipe's owner (`None`: no owner)."""
    return self.access.get('owner', None)


@property
def team(self) -> str:
    """The name of the pipe's team (`'Everyone'` when it names none)."""
    from meerschaum.core.access import EVERYONE
    return self.access.get('team', None) or EVERYONE


@property
def visibility(self) -> str:
    """Who can see the pipe: `'team'` (default), `'private'` or `'everyone'`."""
    return self.access.get('visibility', 'team')


@property
def edit_mode(self) -> str:
    """Who can change the pipe: `'team'` (default: anyone who can see it) or `'owner'`."""
    return self.access.get('edit', 'team')


@property
def locks(self) -> List[str]:
    """
    The pipe's locks (`parameters['locks']`): `'parameters'`, `'sync'`, `'clear'`, `'drop'`,
    `'delete'` or `'all'`. Change them with `lock()` and `unlock()`.
    """
    from meerschaum.core.access import get_locks
    return get_locks(_raw_parameters(self))


def _is_registered(self, debug: bool = False) -> bool:
    if self.temporary:
        return False
    try:
        return self.get_id(debug=debug) is not None
    except Exception:
        return False


def _set_in_memory(self, key: str, value: Any) -> None:
    parameters = self._attributes.get('parameters', None)
    if parameters is None:
        self._attributes['parameters'] = {}
        parameters = self._attributes['parameters']
    if value is None:
        parameters.pop(key, None)
    else:
        parameters[key] = value


def _after_write(self, debug: bool = False) -> None:
    """Forget the cached attributes so the next read sees what was written."""
    try:
        self._invalidate_cache(hard=True, debug=debug)
        self._clear_cache_key('_attributes_sync_time', debug=debug)
    except Exception:
        pass


def lock(self, *locks: str, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Lock the pipe against writes. Every write path refuses what a lock refuses, locally too:

    - `'parameters'`: parameter edits (changing `locks` stays allowed);
    - `'sync'`: syncs, replace windows, deduplication and verification syncs;
    - `'clear'`: row deletes (clears, replace windows, rebuilds);
    - `'drop'`: drops, rebuilds and table schema changes (`verify schema`, repartitioning);
    - `'delete'`: deleting (unregistering) the pipe;
    - `'all'` (the default): all of them.

    A registered pipe is saved at once; an unregistered one keeps the locks in memory until it
    is registered.

    Parameters
    ----------
    *locks: str
        The locks to add (names or comma-separated text). Without any, `'all'`.

    Returns
    -------
    A `SuccessTuple`.

    Examples
    --------
    >>> pipe.lock('drop', 'delete')
    (True, "Locked Pipe('sql:main', 'weather') (drop, delete).")
    >>> pipe.clear()
    (True, 'Success')
    >>> pipe.drop()
    (False, "Cannot drop Pipe('sql:main', 'weather'): it is locked (drop). Unlock it with ...")
    """
    from meerschaum.core.access import parse_locks, locks_with
    try:
        wanted = parse_locks(list(locks))
    except ValueError as e:
        return False, str(e)
    new = locks_with(self.locks, wanted)
    if not _is_registered(self, debug=debug):
        _set_in_memory(self, 'locks', new)
        return True, f"Locked {self} ({', '.join(new)}); saved when it is registered."
    result = self.instance_connector.lock_pipe(self, wanted, debug=debug)
    if result[0]:
        ### The in-memory parameters follow (they patch the registered ones on reads).
        _set_in_memory(self, 'locks', new)
    _after_write(self, debug=debug)
    return tuple(result)


def unlock(self, *locks: str, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Remove locks from the pipe (without any, every lock). Removing one kind from `'all'` keeps
    the other kinds.

    Parameters
    ----------
    *locks: str
        The locks to remove (names or comma-separated text). Without any, `'all'`.

    Returns
    -------
    A `SuccessTuple`.
    """
    from meerschaum.core.access import parse_locks, locks_without
    try:
        removed = parse_locks(list(locks))
    except ValueError as e:
        return False, str(e)
    new = locks_without(self.locks, removed)
    if not _is_registered(self, debug=debug):
        _set_in_memory(self, 'locks', new or None)
        return True, f"Unlocked {self}" + (f" (still locked: {', '.join(new)})." if new else ".")
    result = self.instance_connector.unlock_pipe(self, removed, debug=debug)
    if result[0]:
        _set_in_memory(self, 'locks', new or None)
    _after_write(self, debug=debug)
    return tuple(result)


def set_access(
    self,
    team: Any = _UNSET,
    visibility: Any = _UNSET,
    edit: Any = _UNSET,
    owner: Any = _UNSET,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Set who owns the pipe and who may see and change it (`parameters['access']`). Arguments not
    given keep their current values; `None` clears the owner or the team (back to
    `Everyone`).

    The API server, the MCP server and the web console enforce access; through an `api:`
    instance only the pipe's owner, its team's managers and administrators may change it. A
    `parameters` lock refuses the change.

    Parameters
    ----------
    team: Optional[str]
        The pipe's team (`None` or `'Everyone'`: every user's).

    visibility: str
        `'team'` (its team's members and its owner), `'private'` (its owner, its team's
        managers and administrators) or `'everyone'` (every user).

    edit: str
        `'team'` (anyone who can see it may change it within their permissions) or `'owner'`
        (its owner, its team's managers and administrators).

    owner: Optional[str]
        The owner's username (`None`: no owner).

    Returns
    -------
    A `SuccessTuple`.

    Examples
    --------
    >>> pipe.set_access(team='ops', visibility='private')
    (True, "Set the access of Pipe('sql:main', 'weather'): team ops, visibility private, edit team.")
    >>> pipe.team, pipe.visibility
    ('ops', 'private')
    """
    from meerschaum.core.access import parse_access
    current = dict(self.access)
    new = dict(current)
    if team is not _UNSET:
        new['team'] = team
    if visibility is not _UNSET:
        new['visibility'] = visibility
    if edit is not _UNSET:
        new['edit'] = edit
    if owner is not _UNSET:
        new['owner'] = owner
    try:
        new = parse_access(new)
    except ValueError as e:
        return False, str(e)
    if not _is_registered(self, debug=debug):
        _set_in_memory(self, 'access', new)
        return True, f"Set the access of {self}; saved when it is registered."
    result = self.instance_connector.set_pipe_access(self, new, debug=debug)
    if result[0]:
        _set_in_memory(self, 'access', new)
    _after_write(self, debug=debug)
    return tuple(result)


def _checks_locks_itself(self) -> bool:
    """
    Whether the pipe's instance connector writes through the core, which refuses what locks
    refuse (SQL, API, Valkey and native plugin instances). A custom Python instance connector's
    methods run here, so the façade checks for it.
    """
    conn = self.instance_connector
    return (
        getattr(conn, 'type', None) in _CORE_INSTANCE_TYPES
        or bool(getattr(conn, 'IS_NATIVE', False))
    )


def _registered_parameters(self, debug: bool = False) -> Optional[Dict[str, Any]]:
    """The registered parameters, read now (`None` when the pipe is not registered)."""
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    if self.temporary:
        return None
    conn = self.instance_connector
    with Venv(get_connector_plugin(conn)):
        attributes = conn.get_pipe_attributes(self, debug=debug)
    if not isinstance(attributes, dict) or not attributes:
        return None
    parameters = attributes.get('parameters', None) or {}
    if isinstance(parameters, str):
        import json
        try:
            parameters = json.loads(parameters)
        except Exception:
            parameters = {}
    return dict(parameters) if isinstance(parameters, dict) else {}


def _lock_refusal(self, op: str, always: bool = False, debug: bool = False) -> Optional[str]:
    """
    The refusal of the write `op` (`meerschaum.core.access.LOCK_OPS`) under the pipe's locks,
    read now from its instance (`None` when allowed). Unless `always`, only for instances whose
    writes do not go through the core (which checks them itself).
    """
    from meerschaum.core.access import lock_refusal
    if self.connector_keys == 'mrsm' and self.metric_key != 'pipeline':
        return None
    if not always and _checks_locks_itself(self):
        return None
    registered = _registered_parameters(self, debug=debug)
    parameters = registered if registered is not None else (self._attributes.get('parameters', None) or {})
    return lock_refusal(self, parameters.get('locks', None), op)


def _edit_lock_refusal(self, patch: bool = False, debug: bool = False) -> Optional[str]:
    """
    The refusal of saving the in-memory parameters under a registered `parameters` lock (only
    `locks` may change), for instances whose writes do not go through the core.
    """
    from meerschaum import _core
    from meerschaum.config._patch import apply_patch_to_config
    if self.temporary or _checks_locks_itself(self):
        return None
    if self.connector_keys == 'mrsm' and self.metric_key != 'pipeline':
        return None
    registered = _registered_parameters(self, debug=debug)
    if registered is None:
        return None
    raw = dict(self._attributes.get('parameters', None) or {})
    new = apply_patch_to_config(registered, raw) if patch else raw
    return _core.access_edit_refusal(self, registered, new)
