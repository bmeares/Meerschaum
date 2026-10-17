#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
User class definition
"""

from __future__ import annotations

import uuid

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, Union, List


__all__ = ('hash_password', 'verify_password', 'User')


def hash_password(
    password: str,
    salt: Optional[bytes] = None,
    rounds: Optional[int] = None,
) -> str:
    """
    Return an encoded hash string from the given password.

    Parameters
    ----------
    password: str
        The password to be hashed.

    salt: Optional[str], default None
        If provided, use these bytes for the salt in the hash.
        Otherwise defaults to 16 random bytes.

    rounds: Optional[int], default None
        If provided, use this number of rounds to generate the hash.
        Defaults to 1,000,000.
        
    Returns
    -------
    An encoded hash string to be stored in a database.
    See the `passlib` documentation on the string format:
    https://passlib.readthedocs.io/en/stable/lib/passlib.hash.pbkdf2_digest.html#format-algorithm
    """
    from meerschaum._core import hash_password as _hash_password
    return _hash_password(password, salt=salt, rounds=rounds)


def verify_password(
    password: str,
    password_hash: str,
) -> bool:
    """
    Return `True` if the password matches the provided hash.

    Parameters
    ----------
    password: str
        The password to be checked.

    password_hash: str
        The encoded hash string as generated from `hash_password()`.

    Returns
    -------
    A `bool` indicating whether `password` matches `password_hash`.
    """
    from meerschaum._core import verify_password as _verify_password
    return _verify_password(password, password_hash)


class User:
    """
    The Meerschaum User object manages authentication to a given instance.
    """

    def __init__(
        self,
        username: str,
        password: Optional[str] = None,
        type: Optional[str] = None,
        email: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
        user_id: Union[int, str, uuid.UUID, None] = None,
        instance: Optional[str] = None
    ):
        if password is None:
            password = ''
        self.password = password
        self.username = username
        self.email = email
        self.type = type
        self._attributes = attributes
        self._user_id = user_id
        self._instance_keys = str(instance) if instance is not None else None

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.username

    @property
    def attributes(self) -> Dict[str, Any]:
        if self._attributes is None:
            self._attributes = {}
        return self._attributes

    @property
    def instance_connector(self) -> mrsm.connectors.InstanceConnector:
        from meerschaum.connectors.parse import parse_instance_keys
        if '_instance_connector' not in self.__dict__:
            self._instance_connector = parse_instance_keys(self._instance_keys)
        return self._instance_connector

    @property
    def user_id(self) -> Union[int, str, uuid.UUID, None]:
        """NOTE: This causes recursion with the API,
              so don't try to get fancy with read-only attributes.
        """
        return self._user_id

    @user_id.setter
    def user_id(self, user_id: Union[int, str, None]):
        self._user_id = user_id

    @property
    def password_hash(self):
        """
        Return the hash of the user's password.
        """
        _password_hash = self.__dict__.get('_password_hash', None)
        if _password_hash is not None:
            return _password_hash

        self._password_hash = hash_password(self.password)
        return self._password_hash

    def get_attributes(self, refresh: bool = False, debug: bool = False) -> Dict[str, Any]:
        """
        Return the user's attributes.
        """
        if not refresh:
            return self.attributes
        self._attributes = self.instance_connector.get_user_attributes(self, debug=debug) or {}
        return self._attributes

    def get_scopes(self, refresh: bool = False, debug: bool = False) -> List[str]:
        """
        Return the scopes for this user.
        """
        from meerschaum._internal.static import STATIC_CONFIG
        _attributes = self.get_attributes(refresh=refresh, debug=debug)
        return _attributes.get('scopes', None) or list(STATIC_CONFIG['tokens']['scopes'])

    def register(self, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
        """
        Register a user to the instance connector.
        """
        return self.instance_connector.register_user(self, debug=debug, **kwargs)
