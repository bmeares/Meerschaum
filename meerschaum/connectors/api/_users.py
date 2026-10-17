#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the API (the Rust client, `meerschaum._core.ApiConnector`).
"""

from __future__ import annotations

from uuid import UUID

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Union, List, Optional


def get_users(
    self,
    debug: bool = False,
    **kw: Any
) -> List[str]:
    """Return a list of registered usernames."""
    return self._rust_client().get_users()


def edit_user(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Edit an existing user."""
    return self._rust_client().edit_user(user)


def register_user(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Register a new user."""
    return self._rust_client().register_user(user)


def get_user_id(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> Union[int, str, UUID, None]:
    """Get a user's ID."""
    user_id = self._rust_client().get_user_id(user)
    if isinstance(user_id, str):
        try:
            return UUID(user_id) if len(user_id) == 36 else user_id
        except ValueError:
            return user_id
    return user_id


def delete_user(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Delete a user."""
    return self._rust_client().delete_user(user)


def get_user_attributes(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw
) -> Any:
    """Get a user's attributes."""
    return self._rust_client().get_user_attributes(user)


#############################
# Chaining functions below. #
#############################

def get_user_password_hash(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> Optional[str]:
    """If configured, get a user's password hash."""
    return self._rust_client().get_user_password_hash(user)


def get_user_type(
    self,
    user: mrsm.core.User,
    debug: bool = False,
    **kw: Any
) -> Optional[str]:
    """If configured, get a user's type."""
    return self._rust_client().get_user_type(user)
