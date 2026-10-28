#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Teams, pipe locks and pipe access for instance connectors (`meerschaum.core.access`).

These methods call the core's implementation for the connector's instance: teams live in the
internal pipe `mrsm:teams` (target `mrsm_teams`) beside users and tokens; an `api:` instance
asks its server, which checks who may change what. Teams cross this boundary as dictionaries
`{'id', 'name', 'members': {username: 'member' | 'manager'}, 'attributes'}`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

import meerschaum as mrsm


def _carrier(self) -> Tuple['mrsm.Pipe', Dict[str, Any]]:
    """A pipe that only carries this instance to the core, and the core's context for it."""
    from meerschaum.core.Pipe._status import _status_context
    carrier = mrsm.Pipe('mrsm', 'teams', instance=self, temporary=True)
    return carrier, _status_context(carrier)


def _team_record(team: Any) -> Dict[str, Any]:
    if isinstance(team, dict):
        return team
    to_dict = getattr(team, 'to_dict', None)
    if callable(to_dict):
        return to_dict()
    return {'name': str(getattr(team, 'name', team))}


def _username(user: Any) -> str:
    return str(getattr(user, 'username', user))


def get_teams(self, debug: bool = False, **kwargs: Any) -> List[Dict[str, Any]]:
    """
    Return every team on this instance, `Everyone` first.
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return list(_core.get_teams(carrier, context, debug=debug))


def get_team(self, name: str, debug: bool = False, **kwargs: Any) -> Optional[Dict[str, Any]]:
    """
    Return the team named `name` (any case), or `None`.
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return _core.get_team(carrier, context, str(name), debug=debug)


def register_team(self, team: Any, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
    """
    Register a new team (a `Team` or a dictionary; its new id is written back to it).
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    record = _team_record(team)
    success, message, registered = _core.register_team(carrier, context, record, debug=debug)
    if success:
        new_id = (registered or {}).get('id', None)
        if isinstance(team, dict):
            team['id'] = new_id
        elif hasattr(team, '_id'):
            team._id = new_id
    return success, message


def edit_team(self, team: Any, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
    """
    Save a team's members and attributes.
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return tuple(_core.edit_team(carrier, context, _team_record(team), debug=debug))


def delete_team(self, name: Any, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
    """
    Delete a team: its pipes and saved Pipelines move to `Everyone`.
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return tuple(_core.delete_team(carrier, context, str(getattr(name, 'name', name)), debug=debug))


def rename_team(
    self,
    old_name: Any,
    new_name: str,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Rename a team, in the pipes, saved Pipelines and primary teams that name it too.
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return tuple(_core.rename_team(
        carrier, context, str(getattr(old_name, 'name', old_name)), str(new_name), debug=debug,
    ))


def get_user_teams(self, user: Any, debug: bool = False, **kwargs: Any) -> List[Tuple[str, str]]:
    """
    Return a user's teams as `(team, role)` pairs (`Everyone` first).
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return [tuple(pair) for pair in _core.get_user_teams(carrier, context, _username(user), debug=debug)]


def get_team_pipes_count(self, name: str, debug: bool = False, **kwargs: Any) -> int:
    """
    Return how many pipes (and saved Pipelines) name a team (what renaming or deleting it
    changes).
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return int(_core.team_pipes_count(carrier, context, str(name), debug=debug))


def get_user_permissions(self, user: Any, debug: bool = False, **kwargs: Any) -> List[str]:
    """
    Return a user's effective permissions on this instance.
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return list(_core.get_user_permissions(carrier, context, _username(user), debug=debug))


def get_user_role(self, user: Any, debug: bool = False, **kwargs: Any) -> str:
    """
    Return a user's role on this instance (`'viewer'`, `'editor'`, `'operator'`, `'admin'` or
    `'custom'`).
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return str(_core.get_user_role(carrier, context, _username(user), debug=debug))


def set_user_attributes(
    self,
    user: Any,
    attributes: Dict[str, Any],
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Replace a user's attributes (and nothing else).
    """
    from meerschaum import _core
    carrier, context = _carrier(self)
    return tuple(_core.set_user_attributes(
        carrier, context, _username(user), dict(attributes or {}), debug=debug,
    ))


def lock_pipe(self, pipe: 'mrsm.Pipe', locks: Any, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
    """
    Add locks to a registered pipe (`parameters['locks']`).
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._status import _status_context
    return tuple(_core.pipe_lock(pipe, _status_context(pipe), list(locks), debug=debug))


def unlock_pipe(self, pipe: 'mrsm.Pipe', locks: Any, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
    """
    Remove locks from a registered pipe (removing `'all'` removes every lock).
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._status import _status_context
    return tuple(_core.pipe_unlock(pipe, _status_context(pipe), list(locks), debug=debug))


def set_pipe_access(
    self,
    pipe: 'mrsm.Pipe',
    access: Dict[str, Any],
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Store a registered pipe's access (`parameters['access']`).
    """
    from meerschaum import _core
    from meerschaum.core.Pipe._status import _status_context
    return tuple(_core.pipe_set_access(pipe, _status_context(pipe), dict(access), debug=debug))
