#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing users
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Optional, Union, SuccessTuple, Any, Mapping, Sequence, Dict, List
)

from meerschaum.utils.packages import attempt_import
from meerschaum.api import (
    fastapi, app, endpoints, get_connector, pipes, get_pipe,
    get_pipes_sql, manager, debug
)
from meerschaum.api.tables import get_tables
from starlette.responses import Response, JSONResponse
from meerschaum._internal.User import User
import os, pathlib, datetime

import meerschaum._internal.User
sqlalchemy = attempt_import('sqlalchemy')
users_endpoint = endpoints['users']

from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login.exceptions import InvalidCredentialsException

@manager.user_loader
def load_user(
        username: str
    ) -> User:
    return User(username, repository=get_connector())

@app.post(endpoints['login'])
def login(
        response : Response,
        data : OAuth2PasswordRequestForm = fastapi.Depends()
        #  data : OAuth2PasswordRequestForm
    ) -> JSONResponse:
    """
    Login and set the session token
    """
    username = data.username
    password = data.password

    from meerschaum._internal.User._User import get_pwd_context
    user = User(username, password)
    correct_password = get_pwd_context().verify(
        password, get_connector().get_user_password_hash(user, debug=debug)
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
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> Mapping[str, Union[str, int]]:
    """
    Return attributes of the current User.
    """
    return {
        'username' : curr_user.username,
        'user_id' : get_connector().get_user_id(curr_user),
        'user_type' : get_connector().get_user_type(curr_user),
        'attributes' : get_connector().get_user_attributes(curr_user),
    }

@app.get(users_endpoint)
def get_users() -> Sequence[str]:
    """
    Return a list of registered users
    """
    return get_connector().get_users(debug=debug)

@app.post(users_endpoint + "/{username}/register")
def register_user(
        username : str,
        password : str,
        type : Optional[str] = None,
        email : Optional[str] = None,
        attributes : Optional[Dict[str, Any]] = None
    ) -> SuccessTuple:
    """
    Register a new user
    """
    from meerschaum.config import get_config
    allow_users = get_config('system', 'api', 'permissions', 'registration', 'users', patch=True)
    if not allow_users:
        return False, (
            "The administrator for this server has not allowed user registration.\n\n" +
            "Please contact the system administrator, or if you are running this server, " +
            "open the configuration file with `edit config system` and search for 'permissions'. " +
            " Under the keys api:permissions:registration, you can toggle various registration types."
        )
    user = User(username, password, type=type, email=email, attributes=attributes)
    return get_connector().register_user(user, debug=debug)

@app.post(users_endpoint + "/{username}/edit")
def edit_user(
        username : str,
        password : str,
        type : Optional[str] = None,
        email : Optional[str] = None,
        attributes : Optional[Dict[str, Any]] = None,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> SuccessTuple:
    """
    Edit an existing user
    """
    user = User(username, password, email=email, attributes=attributes)
    user_type = get_connector().get_user_type(curr_user)
    if user_type == 'admin' and type is not None:
        user.type = type
    if user_type == 'admin' or curr_user.username == user.username:
        return get_connector().edit_user(user, debug=debug)

    return False, f"Cannot edit user '{user}': Permission denied"

@app.get(users_endpoint + "/{username}/id")
def get_user_id(
        username : str,
    ) -> Optional[int]:
    """
    Get a user's ID
    """
    return get_connector().get_user_id(User(username), debug=debug)

@app.get(users_endpoint + "/{username}/attributes")
def get_user_attributes(
        username : str,
    ) -> Optional[Dict[str, Any]]:
    """
    Get a user's attributes.
    """
    return get_connector().get_user_attributes(User(username), debug=debug)

@app.post(users_endpoint + "/{username}/delete")
def delete_user(
        username : str,
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> SuccessTuple:
    """
    Delete a user
    """
    user = User(username)
    user_type = get_connector().get_user_type(curr_user, debug=debug)
    if user_type == 'admin' or curr_user.username == user.username:
        return get_connector().delete_user(user, debug=debug)

    return False, f"Cannot delete user '{user}': Permission denied"
