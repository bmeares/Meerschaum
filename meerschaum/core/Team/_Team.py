#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the `Team` class: a named group of users on an instance.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import meerschaum as mrsm

__all__ = ('Team', 'get_teams')

EVERYONE: str = 'Everyone'


def _role(role: Any) -> str:
    text = str(getattr(role, 'value', role) or 'member').strip().lower()
    if text in ('manager', 'managers'):
        return 'manager'
    if text in ('member', 'members'):
        return 'member'
    raise ValueError(f"Invalid team role '{role}': use 'member' or 'manager'.")


def _usernames(usernames: Iterable[Any]) -> List[str]:
    """Usernames given as names, `User` objects or comma-separated text."""
    out: List[str] = []
    for item in usernames:
        if isinstance(item, (list, tuple, set)):
            out.extend(_usernames(item))
            continue
        name = str(getattr(item, 'username', item))
        for part in name.split(','):
            part = part.strip()
            if part and part not in out:
                out.append(part)
    return out


class Team:
    """
    A team of users on an instance (the API server's user store).

    Every user is a member of the built-in team `Everyone`, which cannot be deleted or left.
    Administrators create more teams; a user can be in several, as a `'member'` or a
    `'manager'`. Managers add and remove members of their team and manage the access of its
    pipes. Renaming a team renames it in the pipes and jobs that name it; deleting one moves
    them to `Everyone`.

    Parameters
    ----------
    name: str
        The team's name (the username rules; names are unique without case).

    instance: Union[str, InstanceConnector, None], default None
        The instance that holds the team (the configured instance by default). Through an
        `api:` instance the server checks who may change the team.

    members: Optional[Iterable[str]], default None
        Members to add (in memory, until `register()` or `edit()`).

    managers: Optional[Iterable[str]], default None
        Managers to add (in memory, until `register()` or `edit()`).

    attributes: Optional[Dict[str, Any]], default None
        Free-form attributes.

    Examples
    --------
    >>> team = mrsm.Team('ops', instance='sql:local', members=['alice'], managers=['mia'])
    >>> team.register()
    (True, "Registered team 'ops'.")
    >>> team.add_members('bob')
    (True, "Edited team 'ops'.")
    >>> team.members
    ['alice', 'mia', 'bob']
    >>> team.managers
    ['mia']
    """

    def __init__(
        self,
        name: str,
        instance: Union[str, 'mrsm.connectors.InstanceConnector', None] = None,
        members: Optional[Iterable[Any]] = None,
        managers: Optional[Iterable[Any]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        team_id: Optional[str] = None,
    ):
        self.name = str(name).strip()
        if instance is None:
            from meerschaum.config import get_config
            instance = get_config('meerschaum', 'instance', patch=True)
        if not isinstance(instance, str):
            self._instance_connector = instance
            self._instance_keys = str(instance)
        else:
            self._instance_keys = instance
        self._id = team_id
        self._roles: Dict[str, str] = {}
        self._attributes: Dict[str, Any] = dict(attributes or {})
        self._loaded = False
        for username in _usernames(members or []):
            self._roles[username] = 'member'
        for username in _usernames(managers or []):
            self._roles[username] = 'manager'
        self._pending = bool(self._roles) or bool(self._attributes)

    # --------------------------------------------------------------------------------------
    # The instance
    # --------------------------------------------------------------------------------------

    @property
    def instance_keys(self) -> str:
        """The keys of the instance holding the team."""
        return self._instance_keys

    @property
    def instance_connector(self) -> 'mrsm.connectors.InstanceConnector':
        """The instance connector holding the team."""
        if '_instance_connector' not in self.__dict__:
            from meerschaum.connectors.parse import parse_instance_keys
            self._instance_connector = parse_instance_keys(self._instance_keys)
        return self._instance_connector

    @property
    def is_everyone(self) -> bool:
        """Whether this is the built-in team `Everyone`."""
        return self.name.lower() == EVERYONE.lower() or not self.name

    def _load(self, refresh: bool = False, debug: bool = False) -> Optional[Dict[str, Any]]:
        """Read the team from the instance (its record, or `None` when it does not exist)."""
        if self._loaded and not refresh:
            return None
        record = self.instance_connector.get_team(self.name, debug=debug)
        self._loaded = True
        if not record:
            return None
        if not self._pending:
            self._apply_record(record)
        else:
            ### Names compare without case: the stored spelling wins.
            self.name = str(record.get('name', None) or self.name)
            self._id = record.get('id', None) or self._id
        return record

    def _apply_record(self, record: Dict[str, Any]) -> None:
        self.name = str(record.get('name', self.name) or self.name)
        self._id = record.get('id', None) or record.get('team_id', None) or self._id
        members = record.get('members', None) or {}
        self._roles = {
            str(username): _role(role)
            for username, role in (
                members.items() if isinstance(members, dict) else ((m, 'member') for m in members)
            )
        }
        self._attributes = dict(record.get('attributes', None) or {})

    def refresh(self, debug: bool = False) -> 'Team':
        """Read the team again from the instance (in-memory changes are dropped)."""
        self._pending = False
        self._loaded = False
        self._load(refresh=True, debug=debug)
        return self

    def exists(self, debug: bool = False) -> bool:
        """
        Whether the team exists on the instance (`Everyone` always does). An existing team's
        `name` takes its stored spelling (`Team('OPS').name` is `'ops'`).
        """
        if self.is_everyone:
            return True
        return self._load(refresh=True, debug=debug) is not None

    # --------------------------------------------------------------------------------------
    # Members
    # --------------------------------------------------------------------------------------

    @property
    def id(self) -> Optional[str]:
        """The team's id (`None` before it is registered)."""
        self._load()
        return self._id

    @property
    def roles(self) -> Dict[str, str]:
        """`{username: 'member' | 'manager'}` of the listed members."""
        self._load()
        return dict(self._roles)

    @property
    def members(self) -> List[str]:
        """
        The usernames of the members (managers included). For `Everyone`, every registered
        user.
        """
        self._load()
        if self.is_everyone:
            users = list(self.instance_connector.get_users() or [])
            return users + [u for u in self._roles if u not in users]
        return list(self._roles)

    @property
    def managers(self) -> List[str]:
        """The usernames of the managers."""
        self._load()
        return [username for username, role in self._roles.items() if role == 'manager']

    @property
    def attributes(self) -> Dict[str, Any]:
        """The team's free-form attributes (edit them, then call `edit()`)."""
        self._load()
        self._pending = True
        return self._attributes

    def role_of(self, user: Any) -> Optional[str]:
        """A user's role in the team (`'member'` for anyone in `Everyone`), or `None`."""
        username = str(getattr(user, 'username', user))
        role = self.roles.get(username, None)
        if role is None and self.is_everyone:
            return 'member'
        return role

    def to_dict(self) -> Dict[str, Any]:
        """The team as stored: `{'id', 'name', 'members': {username: role}, 'attributes'}`."""
        return {
            'id': self._id,
            'name': EVERYONE if self.is_everyone else self.name,
            'members': dict(self._roles),
            'attributes': dict(self._attributes),
        }

    def _write(self, debug: bool = False) -> mrsm.SuccessTuple:
        record = self.to_dict()
        result = self.instance_connector.edit_team(record, debug=debug)
        if result[0]:
            self._pending = False
        return result

    def add_members(self, *usernames: Any, role: str = 'member', debug: bool = False) -> mrsm.SuccessTuple:
        """
        Add users to the team as `role` (`'member'` or `'manager'`; an existing member takes the
        new role). A registered team is saved at once.
        """
        names = _usernames(usernames)
        if not names:
            return False, "No users to add."
        role = _role(role)
        self._load(debug=debug)
        for username in names:
            self._roles[username] = role
        self._pending = True
        if not self.is_everyone and not self._id:
            return True, f"Added {len(names)} user(s) to team '{self.name}' (not registered yet)."
        return self._write(debug=debug)

    def add_managers(self, *usernames: Any, debug: bool = False) -> mrsm.SuccessTuple:
        """Make users managers of the team (`add_members(..., role='manager')`)."""
        return self.add_members(*usernames, role='manager', debug=debug)

    def remove_members(self, *usernames: Any, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Remove users from the team (a manager of `Everyone` becomes a plain member: nobody leaves
        `Everyone`). A registered team is saved at once.
        """
        names = _usernames(usernames)
        if not names:
            return False, "No users to remove."
        self._load(debug=debug)
        missing = [u for u in names if u not in self._roles]
        if missing and not self.is_everyone:
            return False, (
                f"Not in team '{self.name}': " + ', '.join(f"'{u}'" for u in missing) + "."
            )
        for username in names:
            self._roles.pop(username, None)
        self._pending = True
        if not self.is_everyone and not self._id:
            return True, f"Removed {len(names)} user(s) from team '{self.name}' (not registered yet)."
        return self._write(debug=debug)

    # --------------------------------------------------------------------------------------
    # Lifecycle
    # --------------------------------------------------------------------------------------

    def register(self, debug: bool = False) -> mrsm.SuccessTuple:
        """Register the team (administrators only through the API)."""
        if self.is_everyone:
            return False, f"'{EVERYONE}' is the built-in team of every user: edit it instead."
        record = self.to_dict()
        result = self.instance_connector.register_team(record, debug=debug)
        if result[0]:
            self._id = record.get('id', None) or self._id
            self._pending = False
            self._loaded = True
        return result

    def edit(self, debug: bool = False) -> mrsm.SuccessTuple:
        """Save the team's members and attributes."""
        self._load(debug=debug)
        return self._write(debug=debug)

    def rename(self, new_name: str, debug: bool = False) -> mrsm.SuccessTuple:
        """Rename the team (the pipes and jobs that name it follow)."""
        old_name = self.name
        result = self.instance_connector.rename_team(old_name, str(new_name).strip(), debug=debug)
        if result[0]:
            self.name = str(new_name).strip()
            _rename_jobs(old_name, self.name, debug=debug)
        return result

    def delete(self, debug: bool = False) -> mrsm.SuccessTuple:
        """Delete the team (its pipes and jobs move to `Everyone`)."""
        result = self.instance_connector.delete_team(self.name, debug=debug)
        if result[0]:
            _rename_jobs(self.name, None, debug=debug)
            self._id = None
            self._loaded = False
        return result

    def get_pipes(self, debug: bool = False) -> List['mrsm.Pipe']:
        """The pipes (and saved Pipelines' registry pipes) whose access names this team."""
        from meerschaum.core.access import get_pipe_access, same_team
        pipes = mrsm.get_pipes(instance=self.instance_connector, as_list=True, debug=debug)
        return [
            pipe for pipe in pipes
            if same_team(get_pipe_access(pipe).get('team', None), None if self.is_everyone else self.name)
        ]

    def __repr__(self) -> str:
        return f"Team('{self.name}')"

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other: Any) -> bool:
        from meerschaum.core.access import same_team
        return (
            isinstance(other, Team)
            and same_team(self.name, other.name)
            and self.instance_keys == other.instance_keys
        )

    def __hash__(self) -> int:
        return hash((self.name.lower(), self.instance_keys))


def _rename_jobs(old_name: str, new_name: Optional[str], debug: bool = False) -> None:
    """Rename (or, with `new_name=None`, move to Everyone) the local jobs that name a team."""
    try:
        from meerschaum.jobs._access import move_team_jobs
    except Exception:
        return
    try:
        success, message = move_team_jobs(old_name, new_name)
    except Exception as e:
        success, message = False, str(e)
    if not success:
        from meerschaum.utils.warnings import warn
        warn(f"Could not update the jobs of team '{old_name}':\n{message}", stack=False)


def get_teams(
    instance: Union[str, 'mrsm.connectors.InstanceConnector', None] = None,
    debug: bool = False,
) -> List[Team]:
    """
    Return the teams of an instance, `Everyone` first.

    Parameters
    ----------
    instance: Union[str, InstanceConnector, None], default None
        The instance (the configured instance by default).

    Returns
    -------
    A list of `Team` objects.

    Examples
    --------
    >>> [team.name for team in mrsm.get_teams('sql:local')]
    ['Everyone', 'ops']
    """
    if instance is None:
        from meerschaum.config import get_config
        instance = get_config('meerschaum', 'instance', patch=True)
    if isinstance(instance, str):
        from meerschaum.connectors.parse import parse_instance_keys
        instance = parse_instance_keys(instance)
    teams = []
    for record in (instance.get_teams(debug=debug) or []):
        if isinstance(record, Team):
            teams.append(record)
            continue
        if not isinstance(record, dict):
            record = {'name': str(getattr(record, 'name', record))}
        team = Team(str(record.get('name', '')), instance=instance)
        team._apply_record(record)
        team._loaded = True
        teams.append(team)
    return teams
