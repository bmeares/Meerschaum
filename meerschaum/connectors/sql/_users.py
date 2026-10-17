#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the SQL Connector
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Optional, Any, Dict, List, Union


def get_users_pipe(self) -> mrsm.Pipe:
    """
    Return the internal metadata pipe for users management.
    """
    if '_users_pipe' in self.__dict__:
        return self._users_pipe

    cache_connector = self.__dict__.get('_cache_connector', None)
    self._users_pipe = mrsm.Pipe(
        'mrsm', 'users',
        temporary=True,
        cache=True,
        cache_connector_keys=cache_connector,
        static=True,
        null_indices=False,
        enforce=False,
        autoincrement=True,
        columns={
            'primary': 'user_id',
        },
        dtypes={
            'user_id': 'int',
            'username': 'string',
            'attributes': 'json',
            'user_type': 'string',
        },
        indices={
            'unique': 'username',
        },
    )
    return self._users_pipe


def register_user(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Register a new user."""
    return self._rust_method('register_user')(user)


def valid_username(username: str) -> SuccessTuple:
    """Verify that a given username is valid."""
    from meerschaum._internal.static import STATIC_CONFIG
    fail_reasons = []

    min_length = STATIC_CONFIG['users']['min_username_length']
    if len(username) < min_length:
        fail_reasons.append(f"Usernames must have at least {min_length} characters.")

    max_length = STATIC_CONFIG['users']['max_username_length']
    if len(username) > max_length:
        fail_reasons.append(f"Usernames must contain {max_length} or fewer characters.")

    acceptable_chars = {'_', '-', '.', '@'}
    for c in username:
        if not c.isalnum() and c not in acceptable_chars:
            fail_reasons.append(
                (
                    "Usernames may only contain alphanumeric characters " +
                    "and the following special characters: "
                    + str(list(acceptable_chars))
                )
            )
            break

    if len(fail_reasons) > 0:
        msg = f"Username '{username}' is invalid for the following reasons:" + '\n'
        for reason in fail_reasons:
            msg += f" - {reason}" + '\n'
        return False, msg

    return True, "Success"


def edit_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Update an existing user's metadata."""
    return self._rust_method('edit_user')(user)


def get_user_id(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Optional[int]:
    """If a user is registered, return the `user_id`."""
    return self._rust_method('get_user_id')(user)

def get_user_attributes(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Union[Dict[str, Any], None]:
    """
    Return the user's attributes.
    """
    return self._rust_method('get_user_attributes')(user)

def delete_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> SuccessTuple:
    """Delete a user's record from the users table."""
    return self._rust_method('delete_user')(user)


def get_users(
    self,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """
    Get the registered usernames.
    """
    return self._rust_method('get_users')()


def get_user_password_hash(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Optional[str]:
    """
    Return the password has for a user.
    **NOTE**: This may be dangerous and is only allowed if the security settings explicity allow it.
    """
    return self._rust_method('get_user_password_hash')(user)


def get_user_type(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Optional[str]:
    """
    Return the user's type.
    """
    return self._rust_method('get_user_type')(user)
