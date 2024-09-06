#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define methods for managing users.
"""

from __future__ import annotations

import json

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
    key_type = 'user_id' if not by_username else 'username'
    return cls.get_entity_key(USER_PREFIX, key_type, user_id_or_username, sub_key)


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
    user_attributes_str = json.dumps(user.attributes, separators=(',', ':'))
    mutable_keys_vals = {
        cls.get_user_key(user.user_id, 'attributes'): user_attributes_str,
        cls.get_user_key(user.user_id, 'email'): user.email,
        cls.get_user_key(user.user_id, 'type'): user.type,
        cls.get_user_key(user.user_id, 'password_hash'): user.password_hash,
    }
    if mutable_only:
        return mutable_keys_vals

    immutable_keys_vals = {
        cls.get_user_key(user.user_id, 'username'): user.username,
        cls.get_user_key(user.username, 'user_id', by_username=True): user.user_id,
    }

    return {**immutable_keys_vals, **mutable_keys_vals}


def register_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Register a new user.
    """
    from meerschaum.utils.misc import generate_password

    user.user_id = generate_password(12)
    users_pipe = self.get_users_pipe()
    keys_vals = self.get_user_keys_vals(user)

    try:
        sync_success, sync_msg = users_pipe.sync(
            [
                {
                    'user_id': user.user_id,
                    'username': user.username,
                },
            ],
            check_existing=False,
            debug=debug,
        )
        if not sync_success:
            return sync_success, sync_msg

        for key, val in keys_vals.items():
            if val is not None:
                self.set(key, val)

        success, msg = True, "Success"
    except Exception as e:
        success = False
        import traceback
        traceback.print_exc()
        msg = f"Failed to register '{user.username}':\n{e}"

    if not success:
        for key in keys_vals:
            try:
                self.client.delete(key)
            except Exception:
                pass

    return success, msg


def get_user_id(self, user: 'mrsm.core.User', debug: bool = False) -> Union[str, None]:
    """
    Return the ID for a user, or `None`.
    """
    username_user_id_key = self.get_user_key(user.username, 'user_id', by_username=True)
    try:
        user_id = self.get(username_user_id_key)
    except Exception:
        user_id = None
    return user_id


def edit_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Edit the attributes for an existing user.
    """
    keys_vals = self.get_user_keys_vals(user, mutable_only=True)
    try:
        old_keys_vals = {
            key: self.get(key)
            for key in keys_vals
        }
    except Exception as e:
        return False, f"Failed to edit user:\n{e}"

    try:
        for key, val in keys_vals.items():
            self.set(key, val)
        success, msg = True, "Success"
    except Exception as e:
        success = False
        msg = f"Failed to edit user:\n{e}"

    if not success:
        try:
            for key, old_val in old_keys_vals.items():
                self.set(key, old_val)
        except Exception:
            pass

    return success, msg


def get_user_attributes(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Union[Dict[str, Any], None]:
    """
    Return the user's attributes.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    user_id_attributes_key = self.get_user_key(user_id, 'attributes')
    try:
        return json.loads(self.get(user_id_attributes_key))
    except Exception:
        return None


def delete_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> SuccessTuple:
    """
    Delete a user's keys.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    users_pipe = self.get_users_pipe()
    keys_vals = self.get_user_keys_vals(user)
    try:
        old_keys_vals = {
            key: self.get(key)
            for key in keys_vals
        }
    except Exception as e:
        return False, f"Failed to delete user:\n{e}"

    clear_success, clear_msg = users_pipe.clear(params={'user_id': user_id})
    if not clear_success:
        return clear_success, clear_msg

    try:
        for key in keys_vals:
            self.client.delete(key)
        success, msg = True, "Success"
    except Exception as e:
        success = False
        msg = f"Failed to delete user:\n{e}"

    if not success:
        try:
            for key, old_val in old_keys_vals.items():
                self.set(key, old_val)
        except Exception:
            pass

    return success, msg


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


def get_user_password_hash(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return the password has for a user.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    user_id_password_hash_key = self.get_user_key(user_id, 'password_hash')
    try:
        return self.get(user_id_password_hash_key)
    except Exception:
        return None


def get_user_type(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return the user's type.
    """
    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    user_id_type_key = self.get_user_key(user_id, 'type')
    try:
        return self.get(user_id_type_key)
    except Exception:
        return None
