#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define methods for managing users.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Union

USERS_TABLE: str = 'mrsm_users'


def register_user(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kwargs: Any
) -> mrsm.SuccessTuple:
    """
    Register a new user.
    """
    from meerschaum.utils.misc import generate_password

    user_id = generate_password(12)

    return True, "Success"


def get_user_id(self, user: mrsm.core.User) -> Union[str, None]:
    """
    Return the ID for a user, or `None`.
    """

