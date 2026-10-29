#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The access model of an `api:` instance (teams, users' permissions, a pipe's access and locks).

Teams, permissions, access and locks of an `api:` instance live on its server, which decides who
may see and change what. The methods inherited from `InstanceConnector`
(`meerschaum/connectors/instance/_teams.py`) reach it through the Rust client's routes
(`/teams/*`, `/users/{username}/permissions`, `/pipes/{keys}/access`, `/lock`, `/unlock`);
the ones here use routes that answer in one request what the generic implementation would read
pipe by pipe.
"""

from __future__ import annotations

import urllib.parse
from typing import Any, Dict, List

import meerschaum as mrsm


def _segment(name: Any) -> str:
    return urllib.parse.quote(str(getattr(name, 'name', name)).strip(), safe='')


def _body(response: Any) -> Dict[str, Any]:
    """The `{"success", "message", ...}` body of a v5 route, or raise with the server's message."""
    try:
        body = response.json()
    except Exception:
        body = None
    if not isinstance(body, dict):
        body = {}
    if response.ok and body.get('success', True):
        return body
    message = body.get('message') or body.get('detail') or response.text
    raise RuntimeError(str(message))


def get_team_pipes_count(self, name: str, debug: bool = False, **kwargs: Any) -> int:
    """
    Return how many pipes (and saved Pipelines) name a team, as far as you can see them (what
    deleting the team gives to `Everyone`): `GET /teams/{name}?counts=true`.
    """
    response = self.get(f"/teams/{_segment(name)}", params={'counts': 'true'}, debug=debug)
    return int(_body(response).get('pipes', 0) or 0)


def get_team_jobs_count(self, name: str, debug: bool = False, **kwargs: Any) -> int:
    """
    Return how many of the server's jobs belong to a team: `GET /teams/{name}?counts=true`.
    """
    response = self.get(f"/teams/{_segment(name)}", params={'counts': 'true'}, debug=debug)
    return int(_body(response).get('jobs', 0) or 0)


def get_users_permissions(self, debug: bool = False, **kwargs: Any) -> List[Dict[str, Any]]:
    """
    Return every user as the server describes them: `username`, `user_type`, `role`,
    `permissions`, `teams` (`[{'name', 'role'}]`), `primary_team` and `email` (`None` unless it
    is yours, you are an administrator, or you manage a team the user is in):
    `GET /users/permissions`.
    """
    response = self.get('/users/permissions', debug=debug)
    return list(_body(response).get('users', None) or [])


def get_user_permissions_record(
    self,
    user: Any,
    debug: bool = False,
    **kwargs: Any
) -> Dict[str, Any]:
    """
    Return one user as `get_users_permissions()` describes them (`{}` for an unknown user):
    `GET /users/{username}/permissions`.
    """
    username = str(getattr(user, 'username', user))
    response = self.get(f"/users/{_segment(username)}/permissions", debug=debug)
    if response.status_code == 404:
        return {}
    return dict(_body(response).get('user', None) or {})


def get_pipe_access(self, pipe: mrsm.Pipe, debug: bool = False, **kwargs: Any) -> Dict[str, Any]:
    """
    Return a pipe's `access` (`owner`, `team`, `visibility`, `edit`), its `locks` and what you
    `may` do with it (`edit`, `manage_access`, `lock`): `GET /pipes/{keys}/access`.
    """
    from meerschaum.connectors.api._pipes import pipe_r_url
    if pipe.connector_keys == 'mrsm' and pipe.metric_key == 'pipeline':
        r_url = f"/pipelines/{_segment(pipe.location_key)}/access"
    else:
        r_url = pipe_r_url(pipe) + '/access'
    response = self.get(
        r_url,
        params={'instance_keys': self.get_pipe_instance_keys(pipe)},
        debug=debug,
    )
    body = _body(response)
    return {key: body.get(key, None) for key in ('access', 'locks', 'may')}
