#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The isolated team a request to the API host runs for (5.1).

The host is one process shared by every team: plugin routes, pages and console callbacks of all
teams run in it. While it serves a request of a user who acts for an isolated team
(`meerschaum.core.access` rules: every team of theirs other than Everyone is isolated; not an
administrator), `get_connector()` refuses and `get_config()` omits the connectors outside that
team's set (`meerschaum._internal.team_scope`). The team is decided here, from the request's
credentials (an `Authorization` header) or its console session cookie, and set for the duration of
the request, including the threads it starts.

This is a guard rail, not isolation: code in this process can read its memory, its environment
and the server's files.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional

#: How long the team of a user is reused (the identity cache of the Rust server is as long).
TTL_SECONDS: float = 5.0

_cache: Dict[str, Any] = {}
_lock = threading.Lock()


def _header(scope, name: bytes) -> Optional[str]:
    for key, value in scope.get('headers', []):
        if key.lower() == name:
            return value.decode('latin-1')
    return None


def _cookie(scope, name: str) -> Optional[str]:
    raw = _header(scope, b'cookie')
    if not raw:
        return None
    for pair in raw.split(';'):
        key, _, value = pair.strip().partition('=')
        if key == name:
            return value
    return None


def username_of_request(scope) -> Optional[str]:
    """The user a request authenticates as (its `Authorization`, else its console session)."""
    from meerschaum.api.host._auth import principal_from_authorization
    authorization = _header(scope, b'authorization')
    if authorization:
        principal = principal_from_authorization(authorization)
        if principal is None:
            return None
        username = getattr(principal, 'username', None)
        if username:
            return str(username)
        ### A token acts as its owner.
        user = getattr(principal, 'user', None)
        if user is not None and getattr(user, 'username', None):
            return str(user.username)
        user_id = getattr(principal, 'user_id', None)
        if user_id is not None:
            from meerschaum.api import get_api_connector
            name = get_api_connector().get_username(user_id)
            return str(name) if name else None
        return None
    from meerschaum.api.dash.sessions import (
        SESSION_COOKIE_NAME, get_username_from_session, valid_session_id,
    )
    session_id = valid_session_id(_cookie(scope, SESSION_COOKIE_NAME))
    if not session_id:
        return None
    return get_username_from_session(session_id)


def team_scope_of_user(username: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    The scope (`team`, `connectors`, `plugins`, `instance`) of the isolated team `username` acts
    for, or `None` for the open view. Cached for `TTL_SECONDS`.
    """
    if not username:
        return None
    now = time.monotonic()
    with _lock:
        entry = _cache.get(username, None)
    if entry is not None and now - entry[0] < TTL_SECONDS:
        return entry[1]
    from meerschaum.api.dash.access import viewer_for
    from meerschaum.api import get_api_connector
    from meerschaum import _core
    viewer = viewer_for(username)
    scope = None
    if viewer.is_isolated:
        conn = get_api_connector()
        records = [
            (team.to_dict() if hasattr(team, 'to_dict') else team)
            for team in (conn.get_teams() or [])
        ]
        connectors = _core.get_config('meerschaum', 'connectors', warn=False) or {}
        scope = _core.access_team_scope(
            list(viewer.teams.items()),
            records,
            viewer.primary_team,
            connectors,
            None,
            False,
        )
        if scope is None:
            ### Walled off, but no team record could be read: no connector at all.
            scope = {'team': username, 'connectors': [], 'plugins': []}
    with _lock:
        if len(_cache) > 4096:
            _cache.clear()
        _cache[username] = (now, scope)
    return scope


def forget(username: Optional[str] = None) -> None:
    """Drop cached team scopes (for one user, or all)."""
    with _lock:
        if username is None:
            _cache.clear()
        else:
            _cache.pop(str(username), None)


def _register_access_invalidation() -> None:
    try:
        from meerschaum.api.dash.access import register_invalidation_hook
        register_invalidation_hook(forget)
    except Exception:
        pass


_register_access_invalidation()


class TeamScopeMiddleware:
    """
    A pure ASGI middleware that runs each request of an isolated team's member with that team's
    scope (see the module documentation). The host's internal routes run without one.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        from meerschaum.api.host import INTERNAL_PREFIX
        if scope.get('type') not in ('http', 'websocket') or str(scope.get('path', '')).startswith(
            INTERNAL_PREFIX
        ):
            await self.app(scope, receive, send)
            return
        from starlette.concurrency import run_in_threadpool
        from meerschaum._internal import team_scope

        def _resolve():
            try:
                username = username_of_request(scope)
            except Exception:
                ### Credentials that cannot be read: the request is anonymous.
                username = None
            if not username:
                return None
            try:
                team = team_scope_of_user(username)
            except Exception:
                ### Fail closed: a user whose teams cannot be read gets no connector.
                return team_scope.TeamScope('unknown', ())
            return None if team is None else team_scope.scope_for(team)

        resolved = await run_in_threadpool(_resolve)
        token = team_scope._enter(resolved)
        try:
            await self.app(scope, receive, send)
        finally:
            team_scope._exit(token)
