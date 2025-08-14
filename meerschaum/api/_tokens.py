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
    debug,
)
from meerschaum.core import Token, User
from meerschaum.core.User import verify_password


http_bearer = HTTPBearer(auto_error=False, scheme_name="APIKey")


def get_token_from_authorization(authorization: str) -> Token:
    """
    Helper function to decode and verify a token from credentials.
    Raises HTTPException on failure.
    """
    if authorization.startswith('mrsm-key:'):
        authorization = authorization[len('mrsm-key:'):]
    try:
        credential_string = base64.b64decode(authorization).decode('utf-8')
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

    if token.get_expiration_status(debug=debug):
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
    return get_token_from_authorization(auth_creds.credentials)


async def optional_token(
    auth_creds: Optional[HTTPAuthorizationCredentials] = Depends(http_bearer),
) -> Optional[Token]:
    """
    FastAPI dependency that returns a Token if provided, otherwise None.
    """
    if not auth_creds:
        return None

    try:
        return get_token_from_authorization(auth_creds.credentials)
    except HTTPException as e:
        return None
