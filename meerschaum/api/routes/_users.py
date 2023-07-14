#, manager! /usr/bin/env python
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
    fastapi, app, endpoints, get_api_connector, pipes, get_pipe,
    manager, debug, check_allow_chaining, DISALLOW_CHAINING_MESSAGE,
    no_auth, private,
)
from meerschaum.utils.misc import string_to_dict
from meerschaum.config import get_config
from meerschaum.api.tables import get_tables
from starlette.responses import Response, JSONResponse
from meerschaum.core import User
import os, pathlib, datetime

import meerschaum.core
sqlalchemy = attempt_import('sqlalchemy')
users_endpoint = endpoints['users']

import fastapi
from fastapi import HTTPException, Form

@app.get(users_endpoint + "/me", tags=['Users'])
def read_current_user(
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> Dict[str, Union[str, int]]:
    """
    Get information about the currently logged-in user.
    """
    return {
        'username': (
            curr_user.username if curr_user is not None else 'no_auth'
        ),
        'user_id': (
            get_api_connector().get_user_id(curr_user)
            if curr_user is not None else -1
        ),
        'user_type': (
            get_api_connector().get_user_type(curr_user)
            if curr_user is not None else 'admin'
        ),
        'attributes': (
            get_api_connector().get_user_attributes(curr_user)
            if curr_user is not None else {}
        ),
    }

@app.get(users_endpoint, tags=['Users'])
def get_users(
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> List[str]:
    """
    Get a list of the registered users.
    """
    return get_api_connector().get_users(debug=debug)


@app.post(users_endpoint + "/register", tags=['Users'])
def register_user(
        username: str = Form(None),
        password: str = Form(None),
        attributes: str = Form(None),
        type: str = Form(None),
        email: str = Form(None),
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> SuccessTuple:
    """
    Register a new user.
    """
    if username is None or password is None:
        raise HTTPException(status_code=406, detail="A username and password must be submitted.")

    if attributes is not None:
        try:
            attributes = string_to_dict(attributes)
        except Exception as e:
            return False, f"Invalid dictionary string received for attributes."

    allow_users = get_config('system', 'api', 'permissions', 'registration', 'users')
    if not allow_users:
        return False, (
            "The administrator for this server has not allowed user registration.\n\n"
            + "Please contact the system administrator, or if you are running this server, "
            + "open the configuration file with `edit config system` and search for 'permissions'. "
            + " Under the keys api:permissions:registration, "
            + "you can toggle various registration types."
        )
    if type == 'admin':
        return False, (
            "New users cannot be of type 'admin' when using the API connector. " +
            "Register a normal user first, then edit the user from an authorized account, " +
            "or use a SQL connector instead."
        )
    user = User(username, password, type=type, email=email, attributes=attributes)
    return get_api_connector().register_user(user, debug=debug)


@app.post(users_endpoint + "/edit", tags=['Users'])
def edit_user(
        username: str = Form(None),
        password: str = Form(None),
        type: str = Form(None),
        email: str = Form(None),
        attributes: str = Form(None),
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> SuccessTuple:
    """
    Edit an existing user.
    """
    if attributes is not None:
        try:
            attributes = string_to_dict(attributes)
        except Exception as e:
            return False, f"Invalid dictionary string received for attributes."

    user = User(username, password, email=email, attributes=attributes)
    user_type = get_api_connector().get_user_type(curr_user) if curr_user is not None else 'admin'
    if user_type == 'admin' and type is not None:
        user.type = type
    if user_type == 'admin' or curr_user.username == user.username:
        return get_api_connector().edit_user(user, debug=debug)

    return False, f"Cannot edit user '{user}': Permission denied"


@app.get(users_endpoint + "/{username}/id", tags=['Users'])
def get_user_id(
        username : str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> Union[int, None]:
    """
    Get a user's ID.
    """
    return get_api_connector().get_user_id(User(username), debug=debug)


@app.get(users_endpoint + "/{username}/attributes", tags=['Users'])
def get_user_attributes(
        username : str,
        curr_user = (
            fastapi.Depends(manager) if private else None
        ),
    ) -> Union[Dict[str, Any], None]:
    """
    Get a user's attributes.
    """
    return get_api_connector().get_user_attributes(User(username), debug=debug)

@app.delete(users_endpoint + "/{username}", tags=['Users'])
def delete_user(
        username: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> SuccessTuple:
    """
    Delete a user.
    """
    user = User(username)
    user_type = (
        get_api_connector().get_user_type(curr_user, debug=debug)
        if curr_user is not None else 'admin'
    )
    if user_type == 'admin' or curr_user.username == user.username:
        return get_api_connector().delete_user(user, debug=debug)

    return False, f"Cannot delete user '{user}': Permission denied"

###################################
# Internal API Chaining functions #
###################################

@app.get(users_endpoint + '/{username}/password_hash', tags=['Users'])
def get_user_password_hash(
        username: str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> str:
    """
    If configured to allow chaining, return a user's password_hash.
    """
    if not check_allow_chaining():
        raise HTTPException(status_code=403, detail=DISALLOW_CHAINING_MESSAGE)
    return get_api_connector().get_user_password_hash(User(username), debug=debug)

@app.get(users_endpoint + '/{username}/type', tags=['Users'])
def get_user_type(
        username : str,
        curr_user = (
            fastapi.Depends(manager) if not no_auth else None
        ),
    ) -> str:
    """
    If configured to allow chaining, return a user's type.
    """
    if not check_allow_chaining():
        raise HTTPException(status_code=403, detail=DISALLOW_CHAINING_MESSAGE)
    return get_api_connector().get_user_type(User(username))
