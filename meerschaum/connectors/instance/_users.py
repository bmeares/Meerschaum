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
    if '_users_pipe' in self.__dict__:
        return self._users_pipe

    cache_connector = self.__dict__.get('_cache_connector', None)
    self._users_pipe = mrsm.Pipe(
        'mrsm', 'users',
        instance=self,
        target='mrsm_users',
        temporary=True,
        cache=True,
        cache_connector_keys=cache_connector,
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
    return self._users_pipe


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


def _literal_param_safe(text: str) -> bool:
    """
    Whether the `params` grammar reads `text` as itself: no leading `_` (a negation), no
    null-like text and nothing the SQL keyword blocklist refuses.
    """
    lower = text.strip().lower()
    return (
        bool(text)
        and text.strip() == text
        and not text.startswith('_')
        and lower not in ('none', 'na', 'nan', 'nat', 'natz', '<na>', 'null')
        and '--' not in lower
        and ';' not in lower
        and 'drop ' not in lower
    )


def _exact_row(
    users_pipe: 'mrsm.Pipe',
    column: str,
    value: Any,
    select: List[str],
    debug: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    The first row of the users pipe whose `column` is exactly `value`.

    A `params` filter is not an exact match: a leading `_` negates it (a username `_alice` once
    matched every other user's row) and null-like text matches nulls. The filter is only used
    for values it reads as themselves, and the rows it returns are checked.
    """
    if value is None:
        return None
    text = str(value)
    columns = list(select) + ([column] if column not in select else [])
    params = {column: text} if _literal_param_safe(text) else None
    df = users_pipe.get_data(columns, params=params, debug=debug)
    if df is None or len(df) == 0 or column not in df.columns:
        return None
    ### A Polars or a pandas frame (the `meerschaum:dataframe` setting).
    to_dicts = getattr(df, 'to_dicts', None)
    rows = to_dicts() if callable(to_dicts) else df.to_dict(orient='records')
    for row in rows:
        found = row.get(column, None)
        if not _is_null(found) and str(found) == text:
            return row
    return None


def _is_null(value: Any) -> bool:
    """Whether a cell is null (`None`, `NaN`, `NaT`, `pd.NA`)."""
    if value is None:
        return True
    try:
        return bool(value != value)
    except Exception:
        return True


def _resolve_user_id(self, user: User, debug: bool = False) -> Any:
    return user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)


def get_user_id(self, user: User, debug: bool = False) -> Union[uuid.UUID, None]:
    """
    Return a user's ID from the username (matched exactly).
    """
    users_pipe = self.get_users_pipe()
    row = _exact_row(users_pipe, 'username', user.username, ['user_id'], debug=debug)
    return row.get('user_id', None) if row else None


def get_username(self, user_id: Any, debug: bool = False) -> Any:
    """
    Return the username from the given ID (matched exactly).
    """
    users_pipe = self.get_users_pipe()
    row = _exact_row(users_pipe, 'user_id', user_id, ['username'], debug=debug)
    return row.get('username', None) if row else None


def _user_field(self, user: User, field: str, debug: bool = False) -> Any:
    """
    A field of the user's row (`None` for an unknown user, never the row of a null `user_id`).
    """
    user_id = _resolve_user_id(self, user, debug=debug)
    if user_id is None:
        return None
    users_pipe = self.get_users_pipe()
    row = _exact_row(users_pipe, 'user_id', user_id, [field], debug=debug)
    return row.get(field, None) if row else None


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
    user_id = _resolve_user_id(self, user, debug=debug)
    if user_id is None:
        return False, (
            f"User '{user.username}' does not exist. "
            f"Register user '{user.username}' before editing."
        )

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
    user_id = _resolve_user_id(self, user, debug=debug)
    if user_id is None:
        return False, f"User '{user.username}' is not registered and cannot be deleted."
    if not _literal_param_safe(str(user_id)):
        return False, f"Cannot delete user '{user.username}': its ID cannot be matched exactly."
    users_pipe = self.get_users_pipe()
    clear_success, clear_msg = users_pipe.clear(params={'user_id': user_id}, debug=debug)
    if not clear_success:
        return False, f"Failed to delete user '{user}':\n{clear_msg}"
    ### The user leaves every team; their pipes keep their team, without an owner.
    try:
        from meerschaum import _core
        from meerschaum.connectors.instance._teams import _carrier
        carrier, context = _carrier(self)
        forgotten, forgotten_msg = _core.forget_user_access(
            carrier, context, user.username, debug=debug,
        )
    except Exception as e:
        forgotten, forgotten_msg = False, str(e)
    if not forgotten:
        from meerschaum.utils.warnings import warn
        warn(forgotten_msg, stack=False)
    return True, "Success"


def get_user_password_hash(self, user: User, debug: bool = False) -> Union[uuid.UUID, None]:
    """
    Get a user's password hash from the users table.
    """
    return _user_field(self, user, 'password_hash', debug=debug)


def get_user_type(self, user: User, debug: bool = False) -> Union[str, None]:
    """
    Get a user's type from the users table.
    """
    return _user_field(self, user, 'user_type', debug=debug)


def get_user_attributes(self, user: User, debug: bool = False) -> Union[Dict[str, Any], None]:
    """
    Get a user's attributes from the users table.
    """
    return _user_field(self, user, 'attributes', debug=debug)
