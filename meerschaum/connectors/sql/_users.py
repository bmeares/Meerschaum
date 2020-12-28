#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the SQL Connector
"""

def register_user(
        self,
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Register a new user
    """
    from meerschaum.utils.warnings import warn, error, info

    valid_tuple = valid_username(user.username)
    if not valid_tuple[0]: return valid_tuple

    old_id = self.get_user_id(user, debug=debug)

    if old_id is not None:
        return False, f"User '{user}' already exists."

    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    import json
    bind_variables = {
        'user_id' : old_id,
        'username' : user.username,
        'email' : user.email,
        'password_hash' : user.password_hash,
        'attributes' : json.dumps(user.attributes),
    }
    if old_id is None:
        query = f"""
        INSERT INTO users (
            username,
            email,
            password_hash,
            attributes
        ) VALUES (
            %(username)s,
            %(email)s,
            %(password_hash)s,
            %(attributes)s
        );
        """
    else: ### NOTE: this 
        return False, f"User '{username}' already exists."

    result = self.exec(query, bind_variables, debug=debug)
    if result is None:
        return False, f"Failed to register user '{user}'"
    return True, f"Successfully registered user '{user}'"

def valid_username(username : str) -> tuple:
    """
    Verify that a given username is valid
    """
    fail_reasons = []

    min_length = 4
    if len(username) < min_length: fail_reasons.append(f"Usernames must have at least {min_length} characters")

    max_length = 26
    if len(username) > max_length: fail_reasons.append(f"Usernames must contain {max_length} or fewer characters")

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
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Update an existing user
    """
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
    

    query = f"""
    UPDATE users
    SET username = %(username)s,
        password_hash = %(password_hash)s,
        email = %(email)s,
        attributes = %(attributes)s
    WHERE user_id = %(user_id)s
    """

    result = self.exec(query, bind_variables, debug=debug)
    if result is None:
        return False, f"Failed to edit user '{user}'"
    return True, f"Successfully edited user '{user}'"

def get_user_id(
        self,
        user : 'meerschaum.User',
        debug : bool = False
    ) -> int:
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']

    query = f"""
    SELECT user_id
    FROM users
    WHERE username = %s
    """
    result = self.value(query, params=(user.username,), debug=debug)
    if result is not None: return int(result)
    return None

def delete_user(
        self,
        user : 'meerschaum.User',
        debug : bool = False
    ) -> tuple:
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    users = get_tables(mrsm_instance=self, debug=debug)['users']

    if user.user_id is not None: user_id = user.user_id
    else: user_id = self.get_user_id(user, debug=debug)

    if user_id is None: return False, f"User '{user.username}' is not registered and cannot be deleted."

    query = f"""
    DELETE
    FROM users
    WHERE user_id = %s
    """
    result = self.value(query, (user_id,), debug=debug)
    return True, f"Successfully deleted user '{user}'"

def get_users(
        self,
        debug : bool = False,
        **kw
    ) -> list:
    ### ensure users table exists
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(mrsm_instance=self, debug=debug)

    q = f"""
    SELECT username
    FROM users
    """
    return list(self.read(q, debug=debug)['username'])

def get_user_password_hash(
        self,
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> str:
    """
    Return a user's password hash
    """
    if user.user_id is not None: user_id = user.user_id
    else: user_id = self.get_user_id(user, debug=debug)

    if user_id is None: return False, f"User '{user.username}' is not registered and cannot be deleted."

    query = f"""
    SELECT password_hash
    FROM users
    WHERE user_id = %s
    """

    return self.value(query, (user_id,), debug=debug)

