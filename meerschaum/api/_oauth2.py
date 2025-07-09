#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define JWT authorization here.
"""

import os
import base64
import functools
import inspect
from typing import List, Optional, Union

from meerschaum.api import endpoints, CHECK_UPDATE, no_auth, debug
from meerschaum.api._tokens import optional_token, get_token_from_authorization
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.packages import attempt_import
from meerschaum.core import User, Token

fastapi, starlette = attempt_import('fastapi', 'starlette', lazy=False, check_update=CHECK_UPDATE)
fastapi_responses = attempt_import('fastapi.responses', lazy=False, check_update=CHECK_UPDATE)
fastapi_login = attempt_import('fastapi_login', check_update=CHECK_UPDATE)
from fastapi import Depends, HTTPException, Request
from starlette import status


class CustomOAuth2PasswordRequestForm:
    def __init__(
        self,
        grant_type: str = fastapi.Form(None, regex="password|client_credentials"),
        username: Optional[str] = fastapi.Form(None),
        password: Optional[str] = fastapi.Form(None),
        scope: str = fastapi.Form(" ".join(STATIC_CONFIG['tokens']['scopes'])),
        client_id: Optional[str] = fastapi.Form(None),
        client_secret: Optional[str] = fastapi.Form(None),
        authorization: Optional[str] = fastapi.Header(None),
    ):
        self.grant_type = grant_type
        self.username = username
        self.password = password
        self.scope = scope
        self.client_id = client_id
        self.client_secret = client_secret

        if (
            not username
            and not password
            and not self.client_id
            and not self.client_secret
            and authorization
        ):
            try:
                scheme, credentials = authorization.split()
                if credentials.startswith('mrsm-key:'):
                    credentials = credentials[len('mrsm-key:'):]
                if scheme.lower() in ('basic', 'bearer'):
                    decoded_credentials = base64.b64decode(credentials).decode('utf-8')
                    _client_id, _client_secret = decoded_credentials.split(':', 1)
                    self.client_id = _client_id
                    self.client_secret = _client_secret
                    self.grant_type = 'client_credentials'
            except ValueError:
                pass


async def optional_user(request: Request) -> Optional[User]:
    """
    FastAPI dependency that returns a User if logged in, otherwise None.
    """
    if no_auth:
        return None
    return await manager(request)
    try:
        return await manager(request)
    except HTTPException:
        return None


async def load_user_or_token(
    request: Request,
    users: bool = True,
    tokens: bool = True,
) -> Union[User, Token, None]:
    """
    Load the current user or token.
    """
    authorization = request.headers.get('authorization', request.headers.get('Authorization', None))
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
        )
    authorization = authorization.replace('Basic ', '').replace('Bearer ', '')
    if not authorization.startswith('mrsm-key:'):
        if not users:
            raise HTTPException(
                status=status.HTTP_401_UNAUTHORIZED,
                detail="Users not authenticated for this endpoint.",
            )
        return await manager(request)
    if not tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Tokens not authenticated for this endpoint.",
        )
    return get_token_from_authorization(authorization)


def ScopedAuth(scopes: List[str]):
    """
    Dependency factory for authenticating with either a user session or a scoped token.
    """
    async def _authenticate(
        user_or_token: Union[User, Token, None] = Depends(
            load_user_or_token,
        ),
    ) -> Union[User, Token, None]:
        if no_auth:
            return None

        if not user_or_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated.",
                headers={"WWW-Authenticate": "Basic"},
            )

        fresh_scopes = user_or_token.get_scopes(refresh=True, debug=debug)
        if '*' in fresh_scopes:
            return user_or_token

        for scope in scopes:
            if scope not in fresh_scopes:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Missing required scope: '{scope}'",
                )
        
        return user_or_token
    return _authenticate


LoginManager = fastapi_login.LoginManager
def generate_secret_key() -> bytes:
    """
    Read or generate the secret keyfile.
    """
    from meerschaum.config._paths import API_SECRET_KEY_PATH
    if not API_SECRET_KEY_PATH.exists():
        secret_key = os.urandom(24).hex()
        with open(API_SECRET_KEY_PATH, 'w+', encoding='utf-8') as f:
            f.write(secret_key)
    else:
        with open(API_SECRET_KEY_PATH, 'r', encoding='utf-8') as f:
            secret_key = f.read()

    return secret_key.encode('utf-8')


SECRET = generate_secret_key()
manager = LoginManager(SECRET, token_url=endpoints['login'])
