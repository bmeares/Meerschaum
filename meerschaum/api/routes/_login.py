#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage access and refresh tokens.
"""

from datetime import datetime, timedelta, timezone
import fastapi
from fastapi_login.exceptions import InvalidCredentialsException
from fastapi.security import OAuth2PasswordRequestForm
from starlette.responses import Response, JSONResponse
from meerschaum.api import endpoints, get_api_connector, app, debug, manager, no_auth
from meerschaum.core import User
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.typing import Dict, Any


@manager.user_loader()
def load_user(
        username: str
    ) -> User:
    """
    Create the `meerschaum.core.User` object from the username.
    """
    return User(username, instance=get_api_connector())


@app.post(endpoints['login'], tags=['Users'])
def login(
        data: OAuth2PasswordRequestForm = fastapi.Depends()
    ) -> Dict[str, Any]:
    """
    Login and set the session token.
    """
    username, password = (
        (data['username'], data['password']) if isinstance(data, dict)
        else (data.username, data.password)
    ) if not no_auth else ('no-auth', 'no-auth')

    from meerschaum.core.User._User import verify_password
    user = User(username, password)
    correct_password = no_auth or verify_password(
        password,
        get_api_connector().get_user_password_hash(user, debug=debug)
    )
    if not correct_password:
        raise InvalidCredentialsException

    expires_minutes = STATIC_CONFIG['api']['oauth']['token_expires_minutes']
    expires_delta = timedelta(minutes=expires_minutes)
    expires_dt = datetime.now(timezone.utc).replace(tzinfo=None) + expires_delta
    access_token = manager.create_access_token(
        data = {'sub': username},
        expires = expires_delta
    )
    return {
        'access_token': access_token,
        'token_type': 'bearer',
        'expires' : expires_dt,
    }
