#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage access and refresh tokens.
"""

import uuid
from datetime import datetime, timedelta, timezone
from typing import Union

import fastapi
from fastapi import Request, status, Response
from fastapi_login.exceptions import InvalidCredentialsException
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

from meerschaum.api import endpoints, get_api_connector, app, debug, manager, no_auth
from meerschaum.core import User
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.typing import Dict, Any
from meerschaum.utils.misc import is_uuid
from meerschaum.core.User import verify_password
from meerschaum.utils.warnings import warn
from meerschaum.api._oauth2 import CustomOAuth2PasswordRequestForm


@manager.user_loader()
def load_user(username: str) -> User:
    """
    Create the `meerschaum.core.User` object from the username.
    """
    return User(username, instance=get_api_connector())


@app.post(endpoints['login'], tags=['Users'])
def login(
    data: CustomOAuth2PasswordRequestForm = fastapi.Depends()
) -> Dict[str, Any]:
    """
    Login and set the session token.
    """
    username, password = (
        (data.get('username', None), data.get('password', None))
        if isinstance(data, dict)
        else (data.username, data.password)
    )
    client_id, client_secret = (
        (data.get('client_id', None), data.get('client_secret', None))
        if isinstance(data, dict)
        else (data.client_id, data.client_secret)
    )
    grant_type = (
        data.get('grant_type', None)
        if isinstance(data, dict)
        else data.grant_type
    )
    if not grant_type:
        grant_type = (
            'password'
            if username and password
            else 'client_credentials'
        )

    expires_dt: Union[datetime, None] = None
    if grant_type == 'password':
        user = User(str(username), str(password), instance=get_api_connector())
        correct_password = no_auth or verify_password(
            str(password),
            get_api_connector().get_user_password_hash(user, debug=debug)
        )
        if not correct_password:
            raise InvalidCredentialsException

    elif grant_type == 'client_credentials':
        if not is_uuid(str(client_id)):
            raise InvalidCredentialsException
        token_id = uuid.UUID(client_id)
        correct_password = no_auth or verify_password(
            str(client_secret),
            str(get_api_connector().get_token_secret_hash(token_id, debug=debug))
        )
        if not correct_password:
            raise InvalidCredentialsException
    else:
        raise InvalidCredentialsException

    expires_minutes = STATIC_CONFIG['api']['oauth']['token_expires_minutes']
    expires_delta = timedelta(minutes=expires_minutes)
    expires_dt = datetime.now(timezone.utc).replace(tzinfo=None) + expires_delta
    access_token = manager.create_access_token(
        data={
            'sub': (username if grant_type == 'password' else client_id),
        },
        expires=expires_delta
    )

    return {
        'access_token': access_token,
        'token_type': 'bearer',
        'expires': expires_dt,
    }


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Log validation errors as warnings.
    """
    warn(f"Validation error: {exc.errors()}", stack=False)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()},
    )
