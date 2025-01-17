#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manager users' metadata via the User class
"""

from typing import Optional

import meerschaum as mrsm
from meerschaum.core.User._User import User


def is_user_allowed_to_execute(
    user: Optional[User],
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Return a `SuccessTuple` indicating whether a given user is allowed to execute actions.
    """
    if user is None:
        return True, "Success"

    user_type = user.instance_connector.get_user_type(user, debug=debug)

    if user_type == 'admin':
        return True, "Success"

    allow_non_admin = mrsm.get_config('system', 'api', 'permissions', 'actions', 'non_admin')
    if not allow_non_admin:
        return False, "The administrator for this server has not allowed users to perform actions."

    return True, "Success"
