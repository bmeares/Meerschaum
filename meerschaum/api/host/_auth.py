#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Authentication in the API host.

`ScopedAuth`, `CurrentScopes` and the login `manager` (`meerschaum.api._login`) are `meerschaum.api._oauth2`'s,
with the same secret file as the Rust server, so a JWT issued by either side validates on the
other. v4 registers the manager's user loader next to the `/login` route, which the host does
not serve, so the host installs the same loader here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from meerschaum.utils.typing import Any, Dict, Optional, Union

_installed = {'loader': False}


def load_user_or_token_from_manager(username_or_token_id: str):
    """
    Resolve the JWT's current principal, rejecting deleted users and invalid tokens.
    """
    from meerschaum.core import User
    from meerschaum.api import get_api_connector
    from meerschaum.utils.misc import is_uuid

    api_conn = get_api_connector()

    if is_uuid(username_or_token_id):
        token = api_conn.get_token(username_or_token_id)
        if token is None or not token.is_valid:
            return None
        if token.expiration is not None and token.expiration <= datetime.now(timezone.utc):
            return None
        return token

    user = User(username_or_token_id, instance=api_conn)
    user_id = api_conn.get_user_id(user)
    if user_id is None:
        return None
    user._user_id = user_id
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
    from meerschaum.api._oauth2 import decode_jwt
    try:
        claims = decode_jwt(token)
    except Exception:
        return None
    if not isinstance(claims, dict) or claims.get(TOKEN_CLAIM) != 'mcp':
        return None
    return claims


def mint_internal_token(minutes: int = 5) -> str:
    """
    Return a token the Rust server accepts as the host's (`X-Mrsm-Host-Token`): an HS256 JWT signed
    with the API secret whose `mrsm_host` claim is `mcp`, valid for `minutes`.
    """
    from datetime import timedelta
    from meerschaum.api.host import TOKEN_CLAIM
    from meerschaum.api._oauth2 import manager
    return manager.create_access_token(
        data={TOKEN_CLAIM: 'mcp'},
        expires=timedelta(minutes=minutes),
        scopes=['*'],
    )
