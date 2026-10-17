#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define methods for managing users (the Rust client, `meerschaum._core.ValkeyConnector`).
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Union, SuccessTuple, Dict, List

USERS_TABLE: str = 'mrsm_users'
USER_PREFIX: str = 'mrsm_user'


def get_users_pipe(self):
    """
    Return the pipe which stores the registered users.
    """
    return mrsm.Pipe(
        'mrsm', 'users',
        columns=['user_id'],
        temporary=True,
        target=USERS_TABLE,
        instance=self,
    )


@classmethod
def get_user_key(cls, user_id_or_username: str, sub_key: str, by_username: bool = False) -> str:
    """
    Return the key to store metadata about a user.

    Parameters
    ----------
    user_id_or_username: str
        The user ID or username of the given user.
        If `by_username` is `True`, then provide the username.

    sub_key: str
        The key suffix, e.g. `'attributes'`.

    by_username: bool, default False
        If `True`, then treat `user_id_or_username` as a username.

    Returns
    -------
    A key to store information about a user.

    Examples
    --------
    >>> get_user_key('deadbeef', 'attributes')
    'mrsm_user:user_id:deadbeef:attributes'
    >>> get_user_key('foo', 'user_id', by_username=True)
    'mrsm_user:username:foo:user_id'
    """
    from meerschaum import _core
    return _core.ValkeyConnector.get_user_key(user_id_or_username, sub_key, by_username)


@classmethod
def get_user_keys_vals(
    cls,
    user: 'mrsm.core.User',
    mutable_only: bool = False,
) -> Dict[str, str]:
    """
    Return a dictionary containing keys and values to set for the user.

    Parameters
    ----------
    user: mrsm.core.User
        The user for which to generate the keys.

    mutable_only: bool, default False
        If `True`, only return keys which may be edited.

    Returns
    -------
    A dictionary mapping a user's keys to values.
    """
    from meerschaum import _core
    return _core.ValkeyConnector.get_user_keys_vals(
        user,
        user.password_hash,
        mutable_only,
    )


def register_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Register a new user (a new 12-character `user_id` is set on `user`).
    """
    return self._rust_client().register_user(user)


def get_user_id(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Union[str, None]:
    """
    Return the user ID for a username.
    """
    return self._rust_client().get_user_id(user)


def edit_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Update a user's mutable attributes.
    """
    return self._rust_client().edit_user(user)


def get_user_attributes(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Union[Dict[str, Any], None]:
    """
    Return the user's attributes.
    """
    return self._rust_client().get_user_attributes(user)


def delete_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> SuccessTuple:
    """
    Delete a user's keys and its row in the users pipe.
    """
    return self._rust_client().delete_user(user)


def get_users(
    self,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """
    Get the registered usernames.
    """
    return self._rust_client().get_users()


def get_user_password_hash(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return the password hash for a user.
    """
    return self._rust_client().get_user_password_hash(user)


def get_user_type(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return the user's type.
    """
    return self._rust_client().get_user_type(user)
