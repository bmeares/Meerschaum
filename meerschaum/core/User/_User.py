#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
User class definition
"""

from __future__ import annotations

import uuid

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, Union, List, Tuple


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
        Return the scopes (permissions) this user holds: every permission for an administrator;
        `attributes['scopes']` when set (a literal `'*'` is kept: every scope, plugins' and
        future ones included); otherwise the permissions of their role (`attributes['role']`,
        else the instance's default role, `api:permissions:default_role`).

        Meerschaum 4 gave every scope to a user without a `scopes` attribute; the default role
        now decides (`'editor'` unless configured).

        Parameters
        ----------
        refresh: bool, default False
            If `True`, read the user's attributes (and type) from the instance again. A read
            that fails raises.

        Returns
        -------
        A list of scope names.
        """
        from meerschaum.core.access import user_permissions
        attributes = self.get_attributes(refresh=refresh, debug=debug)
        scopes = attributes.get('scopes', None)
        if isinstance(scopes, (list, tuple)) and '*' in scopes:
            return ['*']
        user_type = self.type
        if not user_type and (refresh or not attributes):
            user_type = self._stored_type(debug=debug)
        return user_permissions(user_type, attributes)

    def register(self, debug: bool = False, **kwargs: Any) -> mrsm.SuccessTuple:
        """
        Register a user to the instance connector.
        """
        return self.instance_connector.register_user(self, debug=debug, **kwargs)

    # ------------------------------------------------------------------------------------------
    # The access model (`meerschaum.core.access`)
    # ------------------------------------------------------------------------------------------

    def _stored_type(self, debug: bool = False) -> Optional[str]:
        if self.type:
            return self.type
        try:
            return self.instance_connector.get_user_type(self, debug=debug)
        except Exception:
            return None

    def _stored_attributes(self, debug: bool = False) -> Dict[str, Any]:
        ### Attributes given (or set) here win; an empty dictionary (`User.attributes` starts as
        ### one) means they were never read.
        ### A read that fails raises: guessing could grant the default role's permissions to a
        ### user whose own are fewer.
        if self._attributes:
            return self._attributes
        return self.instance_connector.get_user_attributes(self, debug=debug) or {}

    @property
    def permissions(self) -> List[str]:
        """
        The user's effective permissions: every permission for an administrator, their explicit
        permissions (`attributes['scopes']`), else their role's (`attributes['role']`, else the
        default role `api:permissions:default_role`). Through an `api:` instance, the server's
        answer. A user whose attributes were set here is computed from them.
        """
        from meerschaum.core.access import user_permissions
        if not self._attributes and not self.type:
            getter = getattr(self.instance_connector, 'get_user_permissions', None)
            if callable(getter):
                return list(getter(self))
        return user_permissions(self._stored_type(), self._stored_attributes())

    @permissions.setter
    def permissions(self, permissions: List[str]) -> None:
        """Set explicit permissions in memory (`set_permissions()` saves them)."""
        from meerschaum.core.access import attributes_with_permissions
        self._attributes = attributes_with_permissions(self._stored_attributes(), permissions)

    @property
    def role(self) -> str:
        """
        The user's role: `'admin'` for administrators, else `'viewer'`, `'editor'` or
        `'operator'` (the preset their permissions equal or their stored role), or `'custom'`
        for explicit permissions that are no preset.
        """
        from meerschaum.core.access import user_role
        if not self._attributes and not self.type:
            getter = getattr(self.instance_connector, 'get_user_role', None)
            if callable(getter):
                return str(getter(self))
        return user_role(self._stored_type(), self._stored_attributes())

    @role.setter
    def role(self, role: str) -> None:
        """Set a role preset in memory (`set_role()` saves it)."""
        from meerschaum.core.access import attributes_with_role
        self._attributes = attributes_with_role(self._stored_attributes(), str(role))

    def set_role(self, role: str, debug: bool = False) -> mrsm.SuccessTuple:
        """
        Give the user a role preset (`'viewer'`, `'editor'` or `'operator'`) and save it: its
        permissions apply from then on (explicit permissions are removed). Administrators are
        made by their user type, not a role.
        """
        role = str(role).strip().lower()
        if role not in ('viewer', 'editor', 'operator'):
            return False, (
                f"Invalid role '{role}': use 'viewer', 'editor' or 'operator' "
                "(administrators are made by their user type)."
            )
        if (self._stored_type(debug=debug) or '').lower() == 'admin':
            return False, f"'{self.username}' is an administrator: change their user type first."
        self.role = role
        return self.instance_connector.set_user_attributes(self, self._attributes, debug=debug)

    def set_permissions(self, permissions: List[str], debug: bool = False) -> mrsm.SuccessTuple:
        """
        Give the user explicit permissions (see `meerschaum.core.access.ALL_PERMISSIONS`) and
        save them.
        """
        from meerschaum.core.access import ALL_PERMISSIONS
        unknown = [p for p in permissions if p not in ALL_PERMISSIONS and p != '*']
        if unknown:
            return False, "Unknown permissions: " + ', '.join(f"'{p}'" for p in unknown) + "."
        self.permissions = list(permissions)
        return self.instance_connector.set_user_attributes(self, self._attributes, debug=debug)

    @property
    def teams(self) -> List[Tuple[str, str]]:
        """The user's teams as `(team, role)` pairs, `Everyone` first."""
        return [tuple(pair) for pair in self.instance_connector.get_user_teams(self)]

    @property
    def primary_team(self) -> str:
        """
        The user's primary team (the default team of the pipes they create):
        `attributes['primary_team']` when they are in it, else their first team other than
        `Everyone`, else `Everyone`.
        """
        from meerschaum.core.access import primary_team
        return primary_team(self._stored_attributes(), self.teams)
