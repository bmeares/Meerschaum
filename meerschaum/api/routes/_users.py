#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing users
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Optional, Union, SuccessTuple, Any, Mapping, Sequence
)

from meerschaum.utils.packages import attempt_import
from meerschaum.api import (
    fastapi, app, endpoints, get_connector, pipes, get_pipe,
    get_pipes_sql, manager
)
from meerschaum.api.tables import get_tables
from starlette.responses import Response, JSONResponse
from meerschaum import User
import os, pathlib, datetime

sqlalchemy = attempt_import('sqlalchemy')
typing = attempt_import('typing')
users_endpoint = endpoints['mrsm'] + '/users'

from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login.exceptions import InvalidCredentialsException

@manager.user_loader
def load_user(
        username: str
    ) -> meerschaum.User:
    return User(username, repository=get_connector())

@app.post('/mrsm/login')
def login(
        response : Response,
        data : OAuth2PasswordRequestForm = fastapi.Depends()
    ) -> JSONResponse:
    """
    Login and set the session token
    """
    username = data.username
    password = data.password

    from meerschaum.User._User import get_pwd_context
    user = User(username, password)
    correct_password = get_pwd_context().verify(
        password, get_connector().get_user_password_hash(user)
    )
    if not correct_password:
        raise InvalidCredentialsException

    expires = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
    access_token = manager.create_access_token(
        data = dict(sub=username),
        expires_delta = datetime.timedelta(minutes=15)
    )
    #  response.set_cookie(key="user_id", value=get_connector().get_user_id(user))
    return {'access_token': access_token, 'token_type': 'bearer', 'expires' : expires}

@app.get(users_endpoint + "/me")
def read_current_user(
        curr_user : str = fastapi.Depends(manager),
    ) -> Mapping[str, Union[str, int]]:
    return {"username" : curr_user.username, 'user_id' : curr_user.user_id}

@app.get(users_endpoint)
def get_users() -> Sequence[str]:
    """
    Return a list of registered users
    """
    return get_connector().get_users()

@app.post(users_endpoint + "/{username}/register")
def register_user(
        username : str,
        password : str,
        email : str = None,
        attributes : dict = None
    ) -> SuccessTuple:
    """
    Register a new user
    """
    from meerschaum.config import get_config
    allow_users = get_config('system', 'api', 'allow_registration', 'users', patch=True)
    if not allow_users:
        return False, (
            "The administrator for this server has not allowed user registration.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config` and search for 'allow_registration'. " +
            " Under the keys system:api:allow_registration, you can toggle various registration types."
        )
    user = User(username, password, email=email, attributes=attributes)
    return get_connector().register_user(user)

@app.post(users_endpoint + "/{username}/edit")
def edit_user(
        username : str,
        password : str,
        email : str = None,
        attributes : dict = None,
        curr_user : str = fastapi.Depends(manager),
    ) -> SuccessTuple:
    """
    Edit an existing user
    """
    user = User(username)
    user_type = get_connector().get_user_type(curr_user)
    if user_type == 'admin' or curr_user.username == user.username:
        return get_connector().edit_user(user)

    return False, f"Cannot edit user '{user}': Permission denied"

@app.get(users_endpoint + "/{username}/id")
def get_user_id(
        username : str,
    ) -> Optional[int]:
    """
    Get a user's ID
    """
    return User(username, repository=get_connector()).user_id

@app.post(users_endpoint + "/{username}/delete")
def delete_user(
        username : str,
        curr_user : str = fastapi.Depends(manager),
    ) -> SuccessTuple:
    """
    Delete a user
    """
    user = User(username)
    user_type = get_connector().get_user_type(curr_user)
    if user_type == 'admin' or curr_user.username == user.username:
        return get_connector().delete_user(user)

    return False, f"Cannot delete user '{user}': Permission denied"
