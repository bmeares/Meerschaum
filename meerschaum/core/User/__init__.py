#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manager users' metadata via the User class
"""

from typing import Optional, List

import meerschaum as mrsm
from meerschaum.core.User._User import User, hash_password, verify_password


__all__ = (
    'User',
    'hash_password',
    'verify_password',
    'is_user_allowed_to_execute',
)

NECESSARY_SCOPES: List[str] = ['actions:execute', 'jobs:execute']


def is_user_allowed_to_execute(
    user: Optional[User],
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Return a `SuccessTuple` indicating whether a given user is allowed to execute actions.
    """
    if user is None:
        return True, "Success"

    user_type = (
        user.instance_connector.get_user_type(user, debug=debug)
        if isinstance(user, User)
        else 'token'
    )

    if user_type == 'admin':
        return True, "Success"

    allow_non_admin = mrsm.get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_non_admin:
        return False, "The administrator for this server has not allowed users to perform actions."

    if hasattr(user, 'get_scopes'):
        scopes = user.get_scopes(refresh=True, debug=debug)
        for necessary_scope in NECESSARY_SCOPES:
            if necessary_scope not in scopes:
                return False, f"Missing scope '{necessary_scope}'."

        return True, "Success"

    return True, "Success"
