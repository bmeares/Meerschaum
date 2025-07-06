#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define high-level user-management methods for instance connectors.
"""

from __future__ import annotations

import uuid
from typing import Any, Union, Optional, List, Dict

import meerschaum as mrsm
from meerschaum.core import User


def get_users_pipe(self) -> 'mrsm.Pipe':
    """
    Return the pipe used for users registration.
    """
    return mrsm.Pipe(
        'mrsm', 'users',
        instance=self,
        target='mrsm_users',
        temporary=True,
        static=True,
        null_indices=False,
        columns={
            'primary': 'user_id',
        },
        dtypes={
            'user_id': 'uuid',
            'username': 'string',
            'password_hash': 'string',
            'email': 'string',
            'user_type': 'string',
            'attributes': 'json',
        },
        indices={
            'unique': 'username',
        },
    )


def register_user(
    self,
    user: User,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Register a new user to the users pipe.
    """
    users_pipe = self.get_users_pipe()
    user.user_id = uuid.uuid4()
    sync_success, sync_msg = users_pipe.sync(
        [{
            'user_id': user.user_id,
            'username': user.username,
            'email': user.email,
            'password_hash': user.password_hash,
            'user_type': user.type,
            'attributes': user.attributes,
        }],
        check_existing=False,
        debug=debug,
    )
    if not sync_success:
        return False, f"Failed to register user '{user.username}':\n{sync_msg}"

    return True, "Success"


def get_user_id(self, user: User, debug: bool = False) -> Union[uuid.UUID, None]:
    """
    Return a user's ID from the username.
    """
    users_pipe = self.get_users_pipe()
    result_df = users_pipe.get_data(['user_id'], params={'username': user.username}, limit=1)
    if result_df is None or len(result_df) == 0:
        return None
    return result_df['user_id'][0]


def get_username(self, user_id: Any, debug: bool = False) -> Any:
    """
    Return the username from the given ID.
    """
    users_pipe = self.get_users_pipe()
    return users_pipe.get_value('username', {'user_id': user_id}, debug=debug)


def get_users(
    self,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """
    Get the registered usernames.
    """
    users_pipe = self.get_users_pipe()
    df = users_pipe.get_data()
    if df is None:
        return []

    return list(df['username'])


def edit_user(self, user: User, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Edit the attributes for an existing user.
    """
    users_pipe = self.get_users_pipe()
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)

    doc = {'user_id': user_id}
    if user.email != '':
        doc['email'] = user.email
    if user.password_hash != '':
        doc['password_hash'] = user.password_hash
    if user.type != '':
        doc['user_type'] = user.type
    if user.attributes:
        doc['attributes'] = user.attributes

    sync_success, sync_msg = users_pipe.sync([doc], debug=debug)
    if not sync_success:
        return False, f"Failed to edit user '{user.username}':\n{sync_msg}"

    return True, "Success"


def delete_user(self, user: User, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Delete a user from the users table.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    users_pipe = self.get_users_pipe()
    clear_success, clear_msg = users_pipe.clear(params={'user_id': user_id}, debug=debug)
    if not clear_success:
        return False, f"Failed to delete user '{user}':\n{clear_msg}"
    return True, "Success"


def get_user_password_hash(self, user: User, debug: bool = False) -> Union[uuid.UUID, None]:
    """
    Get a user's password hash from the users table.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    users_pipe = self.get_users_pipe()
    result_df = users_pipe.get_data(['password_hash'], params={'user_id': user_id}, debug=debug)
    if result_df is None or len(result_df) == 0:
        return None

    return result_df['password_hash'][0]


def get_user_type(self, user: User, debug: bool = False) -> Union[str, None]:
    """
    Get a user's type from the users table.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    users_pipe = self.get_users_pipe()
    result_df = users_pipe.get_data(['user_type'], params={'user_id': user_id}, debug=debug)
    if result_df is None or len(result_df) == 0:
        return None

    return result_df['user_type'][0]


def get_user_attributes(self, user: User, debug: bool = False) -> Union[Dict[str, Any], None]:
    """
    Get a user's attributes from the users table.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    users_pipe = self.get_users_pipe()
    result_df = users_pipe.get_data(['attributes'], params={'user_id': user_id}, debug=debug)
    if result_df is None or len(result_df) == 0:
        return None

    return result_df['attributes'][0]
