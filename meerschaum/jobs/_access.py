#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Who a job belongs to, and who may see it.

A job started through the API server or the web console records who started it: `owner` (the
user's name) and `team` (their primary team; `None` is Everyone). A job started by another job
(for example `start pipeline NAME` run from the console's Execute box) belongs to that job's
owner and team. A job created locally (the CLI, Compose, systemd, Python) records neither and belongs to
Everyone.

Administrators see every job and owners their own; a team's job is visible to the team's members,
and Everyone's to every user. A console run (an action, Pipeline run or plugin action started
from the console) is private: its owner, the managers of its team and administrators.

The rule is the Rust core's (`meerschaum_core::job_access`), the one the API server applies.
Locally nothing is filtered: the host's files are the trust boundary.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, Dict, Iterable, List, Optional, Tuple, Union, SuccessTuple
from meerschaum.jobs import _native

#: The built-in team every user is a member of.
EVERYONE: str = 'Everyone'

#: `(username, is_admin, [(team, 'member' | 'manager'), ...])`: who is asking.
_ViewerParts = Tuple[Optional[str], bool, List[Tuple[str, str]]]


def _role_name(role: Any) -> str:
    """`'manager'` or `'member'` from a role (a string, an enum, a bool `is_manager`)."""
    if isinstance(role, bool):
        return 'manager' if role else 'member'
    for attr in ('value', 'name'):
        value = getattr(role, attr, None)
        if isinstance(value, str):
            role = value
            break
    return 'manager' if str(role).strip().lower() in ('manager', 'managers') else 'member'


def _team_pairs(teams: Any) -> List[Tuple[str, str]]:
    """
    `[(team, 'member' | 'manager'), ...]` from `{team: role}`, `(team, role)` pairs, dicts with
    `team` (or `name`) and `role`, team names, or objects with `name` and `role`.
    """
    if not teams:
        return []
    items: Iterable[Any] = teams.items() if isinstance(teams, dict) else teams
    pairs: List[Tuple[str, str]] = []
    for item in items:
        if isinstance(item, (tuple, list)) and len(item) >= 2:
            team, role = item[0], item[1]
        elif isinstance(item, dict):
            team = item.get('team', item.get('name', None))
            role = item.get('role', 'member')
        elif isinstance(item, str):
            team, role = item, 'member'
        else:
            team, role = getattr(item, 'name', None), getattr(item, 'role', 'member')
        if team is None:
            continue
        pairs.append((str(getattr(team, 'name', team)), _role_name(role)))
    return pairs


def _user_parts(user: Any, instance: Any = None) -> _ViewerParts:
    """
    `(username, is_admin, teams)` of a `User` or a username, read from its instance (or
    `instance`) with `meerschaum.core.access.get_principal`. A user whose record cannot be read
    is no administrator and in no team (they still see their own jobs and Everyone's).
    """
    from meerschaum.core.access import get_principal
    username = getattr(user, 'username', user)
    username = str(username) if username else None
    if not username:
        return None, False, []
    try:
        principal = get_principal(user, instance=instance)
    except Exception:
        return username, False, []
    return (
        principal.username or username,
        bool(principal.is_admin),
        _team_pairs(principal.teams),
    )


def _viewer_parts(principal_or_user: Any, instance: Any = None) -> _ViewerParts:
    """
    `(username, is_admin, teams)` of whoever asks: `None` (anonymous), a
    `meerschaum.core.access.Principal`, the console's viewer, a dictionary (`username`,
    `is_admin` or `admin`, `teams`), a `meerschaum.core.User` or a username.
    """
    if principal_or_user is None:
        return None, False, []
    from meerschaum.core import User
    if isinstance(principal_or_user, (str, User)):
        return _user_parts(principal_or_user, instance=instance)
    if isinstance(principal_or_user, dict):
        get = principal_or_user.get
    else:
        def get(key: str, default: Any = None) -> Any:
            return getattr(principal_or_user, key, default)
    if get('no_auth', False):
        return (get('username', None) or None), True, _team_pairs(get('teams', None))
    ### A console viewer who is not signed in is anonymous.
    if get('signed_in', True) is False:
        return None, False, []
    teams = get('teams', None)
    if callable(teams):
        teams = teams()
    return (
        (str(get('username', '') or '') or None),
        bool(get('is_admin', get('admin', False))),
        _team_pairs(teams),
    )


def _job_properties(job: Any) -> Dict[str, Any]:
    """A job's properties (a `Job`, a native job, or anything with `daemon.properties`)."""
    core = getattr(job, '_core', None)
    if core is not None and hasattr(core, 'properties'):
        return core.properties() or {}
    daemon = getattr(job, 'daemon', None)
    properties = getattr(daemon, 'properties', None) if daemon is not None else None
    if isinstance(properties, dict):
        return properties
    getter = getattr(job, 'get_daemon_properties', None)
    if callable(getter):
        return getter() or {}
    return {}


def job_access(job: Any) -> Dict[str, Any]:
    """
    Return who a job belongs to: `{'owner', 'team', 'private'}`.

    `owner` is the user who started it through the API server or the console (`None` for a local
    job), `team` its team (`None`: Everyone) and `private` whether it is a console run (visible to
    its owner, its team's managers and administrators only). A `systemd` or `api:` job reports
    what its executor recorded.
    """
    core = getattr(job, '_core', None)
    if core is not None and hasattr(core, 'access'):
        return core.access()
    return _native.core().jobs_access_of(str(getattr(job, 'name', '')), _job_properties(job))


def _can_see(parts: _ViewerParts, job: Any) -> bool:
    username, is_admin, teams = parts
    if is_admin:
        return True
    return bool(_native.core().jobs_can_see(
        str(getattr(job, 'name', '')),
        _job_properties(job),
        username=username,
        is_admin=False,
        teams=teams,
    ))


def can_see_job(principal_or_user: Any, job: Any, instance: Any = None) -> bool:
    """
    Return whether `principal_or_user` may see `job` (see `filter_visible_jobs`).
    """
    return _can_see(_viewer_parts(principal_or_user, instance=instance), job)


def filter_visible_jobs(
    principal_or_user: Any,
    jobs: Union[Dict[str, Any], Iterable[Any]],
    instance: Any = None,
) -> Union[Dict[str, Any], List[Any]]:
    """
    Return the jobs a person may see.

    Administrators see every job and owners their own. A job started through the API server or
    the console belongs to its owner's primary team, and its team's members see it; a local job
    (the CLI, Compose, systemd) is Everyone's, and every user sees it. A console run is private:
    its owner, the managers of its team and administrators.

    Parameters
    ----------
    principal_or_user: Any
        Who is asking: a `meerschaum.core.access.Principal`, the console's viewer, a dictionary
        (`username`, `is_admin`, `teams`), a `meerschaum.core.User` or a username (read from
        `instance`), or `None` (anonymous: only Everyone's jobs).

    jobs: Union[Dict[str, Job], Iterable[Job]]
        The jobs to filter, e.g. `meerschaum.jobs.get_jobs()`.

    instance: Any, default None
        The instance a `User` or a username is read from (default: the user's own, else the
        configured instance). A user who cannot be read is no administrator and in no team.

    Returns
    -------
    The visible jobs, in the same shape as `jobs` (a dictionary or a list).

    Examples
    --------
    >>> from meerschaum.jobs import get_jobs, filter_visible_jobs
    >>> visible = filter_visible_jobs(mrsm.User('bob'), get_jobs())
    """
    parts = _viewer_parts(principal_or_user, instance=instance)
    if isinstance(jobs, dict):
        return {name: job for name, job in jobs.items() if _can_see(parts, job)}
    return [job for job in jobs if _can_see(parts, job)]


def ownership_properties(owner: Optional[str] = None, team: Optional[str] = None) -> Dict[str, Any]:
    """
    Return the properties that record who started a job, to pass as `Job(..., _properties=...)`.

    The API server and the console record the caller (`owner`) and their primary team (`team`;
    `None` or `'Everyone'`: Everyone) on the jobs they start.

    Examples
    --------
    >>> ownership_properties('alice', 'Data')
    {'owner': 'alice', 'team': 'Data', 'mrsm_owner': 'alice'}
    """
    return _native.core().jobs_ownership_properties(owner, team)


def get_team_jobs(team: str) -> List[str]:
    """
    Return the names of this root's jobs (local and `systemd`) whose team is `team` (names compare
    without case; `'Everyone'`: the jobs without a team), e.g. for a deletion's confirmation.
    """
    return list(_native.core().jobs_team_jobs(str(team)))


def move_team_jobs(team: str, to: Optional[str] = None) -> SuccessTuple:
    """
    Move this root's jobs of `team` to the team `to` (a rename), or to Everyone (`to=None`, as
    deleting the team does).

    Renaming or deleting a team through `mrsm.Team`, the CLI, the API server or the console calls
    this for the root where it runs.

    Returns
    -------
    A `SuccessTuple` naming how many jobs moved (and which could not be updated).
    """
    moved, failed = _native.core().jobs_retag_team(str(team), to)
    target = f"team '{to}'" if to and str(to).strip().lower() != EVERYONE.lower() else EVERYONE
    msg = f"Moved {len(moved)} job{'' if len(moved) == 1 else 's'} to {target}."
    if failed:
        msg += " Could not update " + ', '.join(f"'{name}' ({why})" for name, why in failed) + '.'
    return not failed, msg
