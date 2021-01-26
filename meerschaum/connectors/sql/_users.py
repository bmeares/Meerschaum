#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the SQL Connector
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, Any

def register_user(
        self,
        user : meerschaum._internal.User,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Register a new user
    """
    from meerschaum.utils.warnings import warn, error, info
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    valid_tuple = valid_username(user.username)
    if not valid_tuple[0]: return valid_tuple

    old_id = self.get_user_id(user, debug=debug)

    if old_id is not None:
        return False, f"User '{user}' already exists."

    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    json = attempt_import('json')
    bind_variables = {
        'user_id' : old_id,
        'username' : user.username,
        'email' : user.email,
        'password_hash' : user.password_hash,
        'attributes' : json.dumps(user.attributes),
    }
    if old_id is not None:
        return False, f"User '{username}' already exists"
    if old_id is None:
        query = (
            sqlalchemy.insert(tables['users']).
            values(**bind_variables)
        )

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register user '{user}'"
    return True, f"Successfully registered user '{user}'"

def valid_username(username : str) -> SuccessTuple:
    """
    Verify that a given username is valid
    """
    fail_reasons = []

    min_length = 4
    if len(username) < min_length:
        fail_reasons.append(f"Usernames must have at least {min_length} characters")

    max_length = 26
    if len(username) > max_length:
        fail_reasons.append(f"Usernames must contain {max_length} or fewer characters")

    acceptable_chars = {'_', '-'}
    for c in username:
        if not c.isalnum() and c not in acceptable_chars:
            fail_reasons.append(
                (
                    f"Usernames may only contain alphanumeric characters and the following special characters: "
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
        user : meerschaum._internal.User,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Update an existing user
    """
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']

    if user.user_id is None: user_id = user.user_id
    else: user_id = self.get_user_id(user, debug=debug)

    import json
    valid_tuple = valid_username(user.username)
    if not valid_tuple[0]: return valid_tuple

    bind_variables = {
        'user_id' : user_id,
        'username' : user.username,
        'email' : user.email,
        'password_hash' : user.password_hash,
        'attributes' : json.dumps(user.attributes),
    }

    query = sqlalchemy.update(users).values(**bind_variables).where(users.c.user_id == user_id)

    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to edit user '{user}'"
    return True, f"Successfully edited user '{user}'"

def get_user_id(
        self,
        user : meerschaum._internal.User,
        debug : bool = False
    ) -> Optional[int]:
    ### ensure users table exists
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']

    query = (
        sqlalchemy.select([users.c.user_id]).
        where(users.c.username == user.username)
    )

    result = self.value(query, debug=debug)
    if result is not None: return int(result)
    return None

def delete_user(
        self,
        user : meerschaum._internal.User,
        debug : bool = False
    ) -> SuccessTuple:
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']
    plugins = get_tables(mrsm_instance=self, debug=debug)['plugins']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if user.user_id is not None: user_id = user.user_id
    else: user_id = self.get_user_id(user, debug=debug)

    if user_id is None:
        return False, f"User '{user.username}' is not registered and cannot be deleted."

    query = sqlalchemy.delete(users).where(users.c.user_id == user_id)

    result = self.exec(query, debug=True)
    if result is None: return False, f"Failed to delete user '{user}'"

    query = sqlalchemy.delete(plugins).where(plugins.c.user_id == user_id)
    result = self.exec(query, debug=True)
    if result is None: return False, f"Failed to delete plugins of user '{user}'"

    return True, f"Successfully deleted user '{user}'"

def get_users(
        self,
        debug : bool = False,
        **kw : Any
    ) -> list:
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    query = sqlalchemy.select([users.c.username])

    return list(self.read(query, debug=debug)['username'])

def get_user_password_hash(
        self,
        user : meerschaum._internal.User,
        debug : bool = False,
        **kw : Any
    ) -> str:
    """
    Return a user's password hash
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if user.user_id is not None:
        user_id = user.user_id
        if debug: dprint(f"Already given user_id: {user_id}")
    else:
        if debug: dprint(f"Fetching user_id...")
        user_id = self.get_user_id(user, debug=debug)

    if user_id is None: return None

    query = sqlalchemy.select([users.c.password_hash]).where(users.c.user_id == user_id)

    return self.value(query, debug=debug)

def get_user_type(
        self,
        user : meerschaum._internal.User,
        debug : bool = False,
        **kw : Any
    ) -> Optional[str]:
    """
    Return a user's type
    """
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if user.user_id is not None: user_id = user.user_id
    else: user_id = self.get_user_id(user, debug=debug)

    if user_id is None: return None

    query = sqlalchemy.select([users.c.user_type]).where(users.c.user_id == user_id)

    return self.value(query, debug=debug)
