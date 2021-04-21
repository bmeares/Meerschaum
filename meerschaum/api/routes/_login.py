#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage access and refresh tokens.
"""

import fastapi, datetime
from fastapi_login.exceptions import InvalidCredentialsException
from fastapi.security import OAuth2PasswordRequestForm
from starlette.responses import Response, JSONResponse
from meerschaum.api import endpoints, get_api_connector, app, debug, manager
from meerschaum._internal.User import User
from meerschaum.config.static import _static_config

@manager.user_loader
def load_user(
        username: str
    ) -> User:
    """
    Create the User object from the username.
    """
    return User(username, instance=get_api_connector())

@app.post(endpoints['login'])
def login(
        data : OAuth2PasswordRequestForm = fastapi.Depends()
    ) -> JSONResponse:
    """
    Login and set the session token.
    """
    username, password = (
        (data['username'], data['password']) if isinstance(data, dict)
        else (data.username, data.password)
    )

    from meerschaum._internal.User._User import get_pwd_context
    user = User(username, password)
    correct_password = get_pwd_context().verify(
        password, get_api_connector().get_user_password_hash(user, debug=debug)
    )
    if not correct_password:
        raise InvalidCredentialsException

    expires_minutes = _static_config()['api']['oauth']['token_expires_minutes']
    expires_delta = datetime.timedelta(minutes=expires_minutes)
    expires_dt = datetime.datetime.utcnow() + expires_delta
    access_token = manager.create_access_token(
        data = dict(sub=username),
        expires = expires_delta
    )
    #  response.set_cookie(key="user_id", value=get_api_connector().get_user_id(user))
    return {
        'access_token': access_token,
        'token_type': 'bearer',
        'expires' : expires_dt,
    }


