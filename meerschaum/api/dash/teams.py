#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The teams of the API instance as the Users & Teams page edits them: their records and members
(`list_teams`), and creating, renaming and deleting them with their connector reservations
(`meerschaum.api.dash.team_setup`). Who may do that is checked by the callers
(`meerschaum.api.dash.access`).
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple
from meerschaum.api.dash.viewer import EVERYONE, core, role_name


def _conn(conn: Any = None) -> Any:
    from meerschaum.api import get_api_connector
    return conn if conn is not None else get_api_connector()


def get_teams(conn: Any = None) -> List[Any]:
    """The instance's teams (`mrsm.get_teams()`), Everyone first."""
    return list(mrsm.get_teams(instance=_conn(conn)))


def team_names(conn: Any = None) -> List[str]:
    """The names of the instance's teams, Everyone first."""
    return [str(getattr(team, 'name', team)) for team in get_teams(conn)]


def team_info(team: Any) -> Dict[str, Any]:
    """
    `{name, id, builtin, members: {username: 'member' | 'manager'}, attributes}` of a team (`mrsm.Team`):
    its listed members (Everyone lists only the users with a role of their own there).
    """
    name = str(getattr(team, 'name', team))
    builtin = core().is_everyone(name)
    roles = getattr(team, 'roles', None) or {}
    members = {
        str(username): ('manager' if role_name(role) == 'manager' else 'member')
        for username, role in dict(roles).items()
    }
    return {
        'name': EVERYONE if builtin else name,
        'id': (str(getattr(team, 'id', None) or '') or None),
        'builtin': builtin,
        'members': members,
        'attributes': dict(getattr(team, 'attributes', None) or {}),
    }


def list_teams(conn: Any = None) -> List[Dict[str, Any]]:
    """Every team of the instance as `team_info()` dicts (Everyone first, always present)."""
    teams = [team_info(team) for team in get_teams(conn)]
    if not any(team['builtin'] for team in teams):
        teams.insert(0, {'name': EVERYONE, 'id': None, 'builtin': True, 'members': {}})
    return teams


def teams_by_user(teams: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """`{username: {team: role}}` of the teams' listed members (Everyone not included)."""
    out: Dict[str, Dict[str, str]] = {}
    for team in teams:
        if team['builtin']:
            continue
        for username, role in team['members'].items():
            out.setdefault(username, {})[team['name']] = role
    return out


def everyone_managers(teams: List[Dict[str, Any]]) -> List[str]:
    """The managers of Everyone (they manage every user)."""
    for team in teams:
        if team['builtin']:
            return sorted(username for username, role in team['members'].items() if role == 'manager')
    return []


def _team(name: str, conn: Any = None) -> Any:
    return mrsm.Team(str(name), instance=_conn(conn))


def _result(value: Any, default_message: str) -> SuccessTuple:
    if isinstance(value, tuple) and len(value) == 2:
        return bool(value[0]), str(value[1])
    return (value is not False), default_message


def create_team(name: str, conn: Any = None) -> SuccessTuple:
    """
    Register a team (administrators only, checked by the caller). Connectors still reserved for
    its name are never adopted.
    """
    from meerschaum.api.dash import team_setup
    refused = team_setup.refuse_reserved(str(name))
    if refused:
        return False, refused
    try:
        return _result(_team(name, conn).register(), f"Created team '{name}'.")
    except Exception as e:
        return False, str(e)


def rename_team(name: str, new_name: str, conn: Any = None) -> SuccessTuple:
    """
    Rename a team (its pipes, jobs and connector reservations follow). A name that connectors
    are still reserved for is refused.
    """
    from meerschaum.api.dash import team_setup
    if str(name).strip().lower() != str(new_name).strip().lower():
        try:
            exists = _team(new_name, conn).exists()
        except Exception:
            exists = False
        refused = None if exists else team_setup.refuse_reserved(str(new_name))
        if refused:
            return False, refused
    try:
        success, message = _result(
            _team(name, conn).rename(str(new_name)),
            f"Renamed team '{name}' to '{new_name}'.",
        )
    except Exception as e:
        return False, str(e)
    if success:
        note = team_setup.move_reservations(str(name), str(new_name))
        if note:
            message += '\n' + note
    return success, message


def delete_team(name: str, conn: Any = None) -> SuccessTuple:
    """
    Delete a team (its pipes and jobs move to Everyone). Its connectors and databases stay, and
    their reservations are retired so no later team of its name inherits them.
    """
    from meerschaum.api.dash import team_setup
    try:
        team = _team(name, conn)
        team_id = None
        try:
            team_id = team.id
        except Exception:
            team_id = None
        success, message = _result(team.delete(), f"Deleted team '{name}'.")
    except Exception as e:
        return False, str(e)
    if success:
        note = team_setup.move_reservations(
            str(name), team_setup.retired_reservation(str(name), str(team_id) if team_id else None),
        )
        if note:
            message += '\n' + note
    return success, message


def set_team_member(team: str, username: str, role: Optional[str], conn: Any = None) -> SuccessTuple:
    """Add `username` to `team` as `role` (`member` or `manager`), or remove them (`None`)."""
    try:
        team_object = _team(team, conn)
        if role is None:
            return _result(team_object.remove_members(username), f"Removed '{username}' from '{team}'.")
        return _result(team_object.add_members(username, role=role), f"'{username}' is a {role} of '{team}'.")
    except Exception as e:
        return False, str(e)
