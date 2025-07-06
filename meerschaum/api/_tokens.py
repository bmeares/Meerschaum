#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the authentication logic for Meerschaum Tokens.
"""

import base64
import uuid
from typing import Optional, Union, List
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette import status

import meerschaum as mrsm
from meerschaum.api import (
    get_api_connector,
    manager,
    no_auth,
)
from meerschaum.core import Token, User
from meerschaum.core.User import verify_password


http_bearer = HTTPBearer(auto_error=False, scheme_name="APIKey")

def _get_token_from_creds(auth_creds: HTTPAuthorizationCredentials) -> Token:
    """
    Helper function to decode and verify a token from credentials.
    Raises HTTPException on failure.
    """
    try:
        credential_string = base64.b64decode(auth_creds.credentials).decode('utf-8')
        token_id_str, secret = credential_string.split(':', 1)
        token_id = uuid.UUID(token_id_str)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token format. Expected Base64-encoded 'token_id:secret'.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    conn = get_api_connector()
    token = conn.get_token(token_id)

    if not token or not token.is_valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or revoked token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if token.expiration and token.expiration < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not verify_password(secret, token.secret_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid secret.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return token


def get_current_token(
    auth_creds: Optional[HTTPAuthorizationCredentials] = Depends(http_bearer),
) -> Token:
    """
    FastAPI dependency to authenticate a request with a Meerschaum Token.
    This dependency will fail if no token is provided.
    """
    if auth_creds is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return _get_token_from_creds(auth_creds)


async def optional_token(
    auth_creds: Optional[HTTPAuthorizationCredentials] = Depends(http_bearer),
) -> Optional[Token]:
    """
    FastAPI dependency that returns a Token if provided, otherwise None.
    """
    if not auth_creds:
        return None
    try:
        return _get_token_from_creds(auth_creds)
    except HTTPException as e:
        return None


async def optional_user(request: Request) -> Optional[User]:
    """
    FastAPI dependency that returns a User if logged in, otherwise None.
    """
    if no_auth:
        return None
    try:
        return await manager(request)
    except HTTPException:
        return None


def ScopedAuth(scopes: List[str]):
    """
    Dependency factory for authenticating with either a user session or a scoped token.
    """
    async def _authenticate(
        user: Optional[User] = Depends(optional_user),
        token: Optional[Token] = Depends(optional_token),
    ) -> Union[User, Token, None]:
        if no_auth:
            return None
        print(f"{user=}")
        print(f"{token=}")
        if user:
            return user
        
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        for scope in scopes:
            if scope not in token.scopes:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Token is missing required scope: '{scope}'",
                )
        
        return token
    return _authenticate
