#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
User class definition
"""

from __future__ import annotations
import os
import hashlib
import hmac
from binascii import b2a_base64, a2b_base64, Error as _BinAsciiError

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, Union
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.warnings import warn


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
        Defaults to 3,000,000.
        
    Returns
    -------
    An encoded hash string to be stored in a database.
    See the `passlib` documentation on the string format:
    https://passlib.readthedocs.io/en/stable/lib/passlib.hash.pbkdf2_digest.html#format-algorithm
    """
    hash_config = STATIC_CONFIG['users']['password_hash']
    if password is None:
        password = ''
    if salt is None:
        salt = os.urandom(hash_config['salt_bytes'])
    if rounds is None:
        rounds = hash_config['pbkdf2_sha256__default_rounds']

    pw_hash = hashlib.pbkdf2_hmac(
        hash_config['algorithm_name'],
        password.encode('utf-8'),
        salt,
        rounds,
    ) 
    return (
        f"$pbkdf2-{hash_config['algorithm_name']}"
        + f"${hash_config['pbkdf2_sha256__default_rounds']}"
        + '$' + ab64_encode(salt).decode('utf-8')
        + '$' + ab64_encode(pw_hash).decode('utf-8')
    )


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
    if password is None or password_hash is None:
        return False
    hash_config = STATIC_CONFIG['users']['password_hash']
    try:
        digest, rounds_str, encoded_salt, encoded_checksum = password_hash.split('$')[1:]
        algorithm_name = digest.split('-')[-1]
        salt = ab64_decode(encoded_salt)
        checksum = ab64_decode(encoded_checksum)
        rounds = int(rounds_str)
    except Exception as e:
        warn(f"Failed to extract context from password hash '{password_hash}'. Is it corrupted?")
        return False

    return hmac.compare_digest(
        checksum,
        hashlib.pbkdf2_hmac(
            algorithm_name,
            password.encode('utf-8'),
            salt,
            rounds,
        )
    )

_BASE64_STRIP = b"=\n"
_BASE64_PAD1 = b"="
_BASE64_PAD2 = b"=="


def ab64_encode(data):
    return b64s_encode(data).replace(b"+", b".")


def ab64_decode(data):
    """
    decode from shortened base64 format which omits padding & whitespace.
    uses custom ``./`` altchars, but supports decoding normal ``+/`` altchars as well.
    """
    if isinstance(data, str):
        # needs bytes for replace() call, but want to accept ascii-unicode ala a2b_base64()
        try:
            data = data.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("string argument should contain only ASCII characters")
    return b64s_decode(data.replace(b".", b"+"))


def b64s_encode(data):
    return b2a_base64(data).rstrip(_BASE64_STRIP)


def b64s_decode(data):
    """
    decode from shortened base64 format which omits padding & whitespace.
    uses default ``+/`` altchars.
    """
    if isinstance(data, str):
        # needs bytes for replace() call, but want to accept ascii-unicode ala a2b_base64()
        try:
            data = data.encode("ascii")
        except UnicodeEncodeError as ue:
            raise ValueError("string argument should contain only ASCII characters") from ue
    off = len(data) & 3
    if off == 0:
        pass
    elif off == 2:
        data += _BASE64_PAD2
    elif off == 3:
        data += _BASE64_PAD1
    else:  # off == 1
        raise ValueError("Invalid base64 input")
    try:
        return a2b_base64(data)
    except _BinAsciiError as err:
        raise TypeError(err) from err


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
        user_id: Optional[int] = None,
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
        self._instance_keys = str(instance)

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
    def instance_connector(self) -> 'mrsm.connectors.Connector':
        from meerschaum.connectors.parse import parse_instance_keys
        if '_instance_connector' not in self.__dict__:
            self._instance_connector = parse_instance_keys(self._instance_keys)
        return self._instance_connector

    @property
    def user_id(self) -> Union[int, str, None]:
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
