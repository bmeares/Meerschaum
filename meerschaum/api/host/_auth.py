#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Authentication in the API host.

`ScopedAuth`, `CurrentScopes` and the `fastapi_login` manager are `meerschaum.api._oauth2`'s,
with the same secret file as the Rust server, so a JWT issued by either side validates on the
other. v4 registers the manager's user loader next to the `/login` route, which the host does
not serve, so the host installs the same loader here.
"""

from __future__ import annotations

import uuid

from meerschaum.utils.typing import Any, Dict, Optional, Union

_active_user_ids: Dict[str, Any] = {}
_installed = {'loader': False}


def load_user_or_token_from_manager(username_or_token_id: str):
    """
    The manager's user loader (`routes/_login.py`): a `Token` for a UUID subject, else a `User`
    with its ID from the cache or the instance.
    """
    import meerschaum as mrsm
    from meerschaum.core import User
    from meerschaum.api import get_api_connector, get_cache_connector
    from meerschaum.utils.misc import is_uuid, is_int

    cache_conn = get_cache_connector()
    api_conn = get_api_connector()

    if is_uuid(username_or_token_id):
        return api_conn.get_token(username_or_token_id)

    username = username_or_token_id
    cached_user_id = (
        _active_user_ids.get(username)
        if cache_conn is None
        else cache_conn.get(f'mrsm:users:{username}:id')
    )
    if isinstance(cached_user_id, str):
        if is_int(cached_user_id):
            cached_user_id = int(cached_user_id)
        elif is_uuid(cached_user_id):
            cached_user_id = uuid.UUID(cached_user_id)

    user = User(username, instance=api_conn, user_id=cached_user_id)
    if cached_user_id is not None:
        return user

    user_id = api_conn.get_user_id(user)
    if user_id is not None:
        user._user_id = user_id
        expires_seconds = mrsm.get_config('api', 'cache', 'session_expires_minutes') * 60
        if cache_conn is not None:
            cache_conn.set(f'mrsm:users:{username}:id', str(user_id), ex=expires_seconds)
        else:
            _active_user_ids[username] = user_id
    return user


def install_user_loader() -> None:
    """
    Register the user loader on `meerschaum.api._oauth2.manager` (once).
    """
    if _installed['loader']:
        return
    from meerschaum.api._oauth2 import manager
    manager.user_loader()(load_user_or_token_from_manager)
    _installed['loader'] = True


def decode_internal_token(token: Optional[str]) -> Union[Dict[str, Any], None]:
    """
    Validate an internal token from the Rust server (`X-Mrsm-Host-Token`): an HS256 JWT signed
    with the API secret whose `mrsm_host` claim is `mcp`. Return its claims, or `None`.
    """
    if not token:
        return None
    from meerschaum.api.host import TOKEN_CLAIM
    from meerschaum.api._oauth2 import SECRET
    from meerschaum.utils.packages import attempt_import
    jose_jwt = attempt_import('jose.jwt', lazy=False, check_update=False)
    try:
        claims = jose_jwt.decode(token, SECRET, algorithms=['HS256'])
    except Exception:
        return None
    if not isinstance(claims, dict) or claims.get(TOKEN_CLAIM) != 'mcp':
        return None
    return claims
