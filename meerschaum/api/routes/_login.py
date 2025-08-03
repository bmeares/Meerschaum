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

import meerschaum as mrsm
from meerschaum.api import (
    endpoints,
    get_api_connector,
    get_cache_connector,
    app,
    debug,
    manager,
    no_auth,
)
from meerschaum.core import User
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.utils.typing import Dict, Any
from meerschaum.utils.misc import is_uuid, is_int
from meerschaum.core.User import verify_password
from meerschaum.utils.warnings import warn
from meerschaum.api._oauth2 import CustomOAuth2PasswordRequestForm


USER_ID_CACHE_EXPIRES_SECONDS: int = mrsm.get_config('system', 'api', 'cache', 'session_expires_minutes') * 60
_active_user_ids = {}

@manager.user_loader()
def load_user_or_token_from_manager(username_or_token_id: str) -> User:
    """
    Create the `meerschaum.core.User` object from the username.
    """
    cache_conn = get_cache_connector()
    api_conn = get_api_connector()

    is_token = is_uuid(username_or_token_id)

    if is_token:
        print("Returning token.")
        return api_conn.get_token(username_or_token_id)

    username = username_or_token_id

    cached_user_id = (
        _active_user_ids.get(username)
        if cache_conn is None
        else cache_conn.get(f'mrsm:users:{username}:id')
    )
    print(f"{username=}")
    print(f"{cached_user_id=}")
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
        if cache_conn is not None:
            cache_conn.set(f'mrsm:users:{username}:id', str(user_id), ex=USER_ID_CACHE_EXPIRES_SECONDS)
        else:
            _active_user_ids[username] = user_id
    return user



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

    allowed_scopes = []
    type_ = None
    expires_dt: Union[datetime, None] = None
    sub_id = None
    if grant_type == 'password':
        user = User(str(username), str(password), instance=get_api_connector())
        correct_password = no_auth or verify_password(
            str(password),
            get_api_connector().get_user_password_hash(user, debug=debug)
        )
        if not correct_password:
            raise InvalidCredentialsException

        allowed_scopes = user.get_scopes(debug=debug)
        type_ = get_api_connector().get_user_type(user, debug=debug)
        sub_id = username

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

        allowed_scopes = get_api_connector().get_token_scopes(token_id, debug=debug)
        sub_id = client_id

    else:
        raise InvalidCredentialsException

    requested_scopes = data.scope.split()
    if '*' in allowed_scopes:
        final_scopes = requested_scopes or ['*']
    else:
        final_scopes = [
            s for s in requested_scopes if s in allowed_scopes
        ] if requested_scopes else allowed_scopes

    expires_minutes = STATIC_CONFIG['api']['oauth']['token_expires_minutes']
    expires_delta = timedelta(minutes=expires_minutes)
    expires_dt = datetime.now(timezone.utc).replace(tzinfo=None) + expires_delta
    access_token = manager.create_access_token(
        data={
            'sub': sub_id,
            'scopes': final_scopes,
            'type': type_,
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
