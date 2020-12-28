#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing users
"""

from meerschaum.utils.misc import attempt_import
from meerschaum.api import fastapi, fast_api, endpoints, get_connector, pipes, get_pipe, get_pipes_sql, manager
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
    ):
    return username

@fast_api.post('/mrsm/login')
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
    correct_password = get_pwd_context().verify(password, get_connector().get_user_password_hash(user))
    if not correct_password:
        raise InvalidCredentialsException

    access_token = manager.create_access_token(
        data = dict(sub=username),
        expires_delta = datetime.timedelta(minutes=15)
    )
    response.set_cookie(key="user_id", value=get_connector().get_user_id(user))
    return {'access_token': access_token, 'token_type': 'bearer'}

@fast_api.get(users_endpoint + "/me")
def read_current_user(
        logged_in_username : str = fastapi.Depends(manager),
        user_id : typing.Optional[int] = fastapi.Cookie(None)
    ):
    return {"username" : logged_in_username, 'user_id' : user_id}

@fast_api.get(users_endpoint)
def get_users(
        logged_in_username : str = fastapi.Depends(manager),
        user_id : typing.Optional[int] = fastapi.Cookie(None)
    ) -> list:
    """
    Return a list of registered users
    """
    return get_connector().get_users()

@fast_api.post(users_endpoint + "/{username}/register")
def register_user(
        username : str,
        password : str,
        email : str = None,
        attributes : dict = None,
        logged_in_username : str = fastapi.Depends(manager),
        user_id : typing.Optional[int] = fastapi.Cookie(None)
    ):
    """
    Register a new user
    """
    user = User(username, password, email=email, attributes=attributes)
    return get_connector().register_user(user)

@fast_api.post(users_endpoint + "/{username}/edit")
def edit_user(
        username : str,
        password : str,
        email : str = None,
        attributes : dict = None,
        logged_in_username : str = fastapi.Depends(manager),
        user_id : typing.Optional[int] = fastapi.Cookie(None)
    ):
    """
    Edit an existing user
    """
    user = User(username, password, email=email, attributes=attributes)
    return get_connector().edit_user(user)

@fast_api.get(users_endpoint + "/{username}/id")
def get_user_id(
        username : str,
        logged_in_username : str = fastapi.Depends(manager),
        user_id : typing.Optional[int] = fastapi.Cookie(None)
    ):
    """
    Get a user's ID
    """
    user = User(username, '')
    return get_connector().get_user_id(user)

@fast_api.post(users_endpoint + "/{username}/delete")
def delete_user(
        username : str,
        logged_in_username : str = fastapi.Depends(manager),
        user_id : typing.Optional[int] = fastapi.Cookie(None)
    ):
    """
    Delete a user
    """
    user = User(username, '')
    return get_connector().delete_user(user)

