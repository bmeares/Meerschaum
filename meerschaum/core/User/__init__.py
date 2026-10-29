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
    Return a `SuccessTuple` indicating whether a given user (or token) may execute actions:
    administrators, and others who hold `actions:execute` and `jobs:execute` (the role
    `'operator'`, or permissions of their own), unless the server runs with `--secure`
    (`api:permissions:actions:non_admin: false`). The API server applies the same rule.

    Meerschaum 4 gated non-administrators on `system:api:permissions:actions:non_admin`, a key
    its default configuration does not define; their permissions decide now.
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

    allow_non_admin = mrsm.get_config('api', 'permissions', 'actions', 'non_admin', warn=False)
    if allow_non_admin is False:
        return False, "The administrator for this server has not allowed users to perform actions."

    if hasattr(user, 'get_scopes'):
        scopes = user.get_scopes(refresh=True, debug=debug)
        ### A literal `*` grants every scope.
        if '*' in scopes:
            return True, "Success"

        for necessary_scope in NECESSARY_SCOPES:
            if necessary_scope not in scopes:
                return False, (
                    f"Missing scope '{necessary_scope}': running actions needs the role "
                    f"'operator' (or '{necessary_scope}' among your own permissions)."
                )

        return True, "Success"

    return True, "Success"
