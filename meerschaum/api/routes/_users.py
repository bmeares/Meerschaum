#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Routes for managing users
"""

from meerschaum.utils.misc import attempt_import
from meerschaum.api import fastapi, fast_api, endpoints, get_connector, pipes, get_pipe, get_pipes_sql
from meerschaum.api.tables import get_tables
import os, pathlib

sqlalchemy = attempt_import('sqlalchemy')
users_endpoint = endpoints['mrsm'] + '/users'

security = fastapi.security.HTTPBasic()

from fastapi.security import OAuth2PasswordRequestForm
from fastapi_login.exceptions import InvalidCredentialsException
from fastapi_login import LoginManager
def generate_secret_key():
    """
    Read or generate the keyfile
    """
    from meerschaum.config._paths import API_RESOURCES_PATH
    keyfilepath = pathlib.Path(os.path.join(API_RESOURCES_PATH, '.api_secret_key'))
    if not keyfilepath.exists():
        secret_key = os.urandom(24).hex()
        with open(keyfilepath, 'w') as f:
            f.write(secret_key)
    else:
        with open(keyfilepath, 'r') as f:
            secret_key = f.read()

    return secret_key
SECRET = generate_secret_key()
manager = LoginManager(SECRET, tokenUrl='/auth/token')

@manager.user_loader
def load_user(email: str):
    user = fake_db.get(email)
    return user



@fast_api.get('/mrsm/login')
def login(
        data : OAuth2PasswordRequestForm = fastapi.Depends()
        #  response : fastapi.Response,
        #  credentials : fastapi.security.HTTPBasicCredentials = fastapi.Depends(security)
    ):
    """
    Login and set the session token
    """
    username = data.username
    password = data.password


    import uuid, datetime, secrets
    from meerschaum import User
    user = User(credentials.username, credentials.password)
    correct_password = secrets.compare_digest(
        user.password_hash,
        get_connector().get_user_password_hash(user)
    )
    if not correct_password:
        raise HTTPException(
            status_code = fastapi.status.HTTP_401_UNAUTHORIZED,
            detail = "Incorrect username or password",
            headers = {"WWW-Authenticate": "Basic"},
        )

    response.set_cookie(key="session", value=str(uuid.uuid1()))
    response.set_cookie(key="username", value=user.username)
    response.set_cookie(key="login_time", value=datetime.datetime.utcnow())
    return True, "Success"

@fast_api.get(users_endpoint + "/me")
def read_current_user(
    ):
    return {"username": credentials.username, "password": credentials.password}

@fast_api.get(users_endpoint)
def get_users() -> list:
    """
    Return a list of registered users
    """
    return get_connector().get_users()

@fast_api.post(users_endpoint + "/{username}/register")
def register_user(
        username : str,
        password : str,
        email : str = None,
        attributes : dict = None
    ):
    """
    Register a new user
    """
    from meerschaum import User
    user = User(username, password, email=email, attributes=attributes)
    return get_connector().register_user(user)

@fast_api.post(users_endpoint + "/{username}/edit")
def edit_user(
        username : str,
        password : str,
        email : str = None,
        attributes : dict = None
    ):
    """
    Edit an existing user
    """
    from meerschaum import User
    user = User(username, password, email=email, attributes=attributes)
    return get_connector().edit_user(user)


