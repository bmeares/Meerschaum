#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the SQL Connector
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Optional, Any, Dict, List, Union


def register_user(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Register a new user."""
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors
    sqlalchemy = attempt_import('sqlalchemy')

    valid_tuple = valid_username(user.username)
    if not valid_tuple[0]:
        return valid_tuple

    old_id = self.get_user_id(user, debug=debug)

    if old_id is not None:
        return False, f"User '{user}' already exists."

    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    import json
    bind_variables = {
        'username': user.username,
        'email': user.email,
        'password_hash': user.password_hash,
        'user_type': user.type,
        'attributes': (
            json.dumps(user.attributes) if self.flavor not in json_flavors else user.attributes
        ),
    }
    if old_id is not None:
        return False, f"User '{user.username}' already exists."
    if old_id is None:
        query = (
            sqlalchemy.insert(tables['users']).
            values(**bind_variables)
        )

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register user '{user}'."
    return True, f"Successfully registered user '{user}'."


def valid_username(username: str) -> SuccessTuple:
    """Verify that a given username is valid."""
    from meerschaum.config.static import STATIC_CONFIG
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
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.sql import json_flavors
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']

    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)
    if user_id is None:
        return False, (
            f"User '{user.username}' does not exist. " +
            f"Register user '{user.username}' before editing."
        )
    user.user_id = user_id

    import json
    valid_tuple = valid_username(user.username)
    if not valid_tuple[0]:
        return valid_tuple

    bind_variables = {
        'user_id' : user_id,
        'username' : user.username,
    }
    if user.password != '':
        bind_variables['password_hash'] = user.password_hash
    if user.email != '':
        bind_variables['email'] = user.email
    if user.attributes is not None and user.attributes != {}:
        bind_variables['attributes'] = (
            json.dumps(user.attributes) if self.flavor in ('duckdb',)
            else user.attributes
        )
    if user.type != '':
        bind_variables['user_type'] = user.type

    query = (
        sqlalchemy
        .update(users_tbl)
        .values(**bind_variables)
        .where(users_tbl.c.user_id == user_id)
    )

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to edit user '{user}'."
    return True, f"Successfully edited user '{user}'."


def get_user_id(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Optional[int]:
    """If a user is registered, return the `user_id`."""
    ### ensure users table exists
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']

    query = (
        sqlalchemy.select(users_tbl.c.user_id)
        .where(users_tbl.c.username == user.username)
    )

    result = self.value(query, debug=debug)
    if result is not None:
        return int(result)
    return None

def get_user_attributes(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> Union[Dict[str, Any], None]:
    """
    Return the user's attributes.
    """
    ### ensure users table exists
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']

    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)

    query = (
        sqlalchemy.select(users_tbl.c.attributes)
        .where(users_tbl.c.user_id == user_id)
    )

    result = self.value(query, debug=debug)
    if result is not None and not isinstance(result, dict):
        try:
            result = dict(result)
            _parsed = True
        except Exception as e:
            _parsed = False
        if not _parsed:
            try:
                import json
                result = json.loads(result)
                _parsed = True
            except Exception as e:
                _parsed = False
        if not _parsed:
            warn(f"Received unexpected type for attributes: {result}")
    return result

def delete_user(
    self,
    user: 'mrsm.core.User',
    debug: bool = False
) -> SuccessTuple:
    """Delete a user's record from the users table."""
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)

    if user_id is None:
        return False, f"User '{user.username}' is not registered and cannot be deleted."

    query = sqlalchemy.delete(users_tbl).where(users_tbl.c.user_id == user_id)

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to delete user '{user}'."

    query = sqlalchemy.delete(plugins).where(plugins.c.user_id == user_id)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to delete plugins of user '{user}'."

    return True, f"Successfully deleted user '{user}'"


def get_users(
    self,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """
    Get the registered usernames.
    """
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select(users_tbl.c.username)

    return list(self.read(query, debug=debug)['username'])


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
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.sql.tables import get_tables
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if user.user_id is not None:
        user_id = user.user_id
        if debug:
            dprint(f"Already given user_id: {user_id}")
    else:
        if debug:
            dprint("Fetching user_id...")
        user_id = self.get_user_id(user, debug=debug)

    if user_id is None:
        return None

    query = sqlalchemy.select(users_tbl.c.password_hash).where(users_tbl.c.user_id == user_id)

    return self.value(query, debug=debug)


def get_user_type(
    self,
    user: 'mrsm.core.User',
    debug: bool = False,
    **kw: Any
) -> Optional[str]:
    """
    Return the user's type.
    """
    from meerschaum.connectors.sql.tables import get_tables
    users_tbl = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    user_id = user.user_id if user.user_id is not None else self.get_user_id(user, debug=debug)

    if user_id is None:
        return None

    query = sqlalchemy.select(users_tbl.c.user_type).where(users_tbl.c.user_id == user_id)

    return self.value(query, debug=debug)
