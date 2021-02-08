#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
User class definition
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any

pwd_context = None
def get_pwd_context():
    global pwd_context
    if pwd_context is None:
        from meerschaum.config.static import _static_config
        from meerschaum.utils.packages import attempt_import
        hash_config = _static_config()['users']['password_hash']
        passlib_context = attempt_import('passlib.context')
        pwd_context = passlib_context.CryptContext(
            schemes = hash_config['schemes'],
            default = hash_config['default'],
            pbkdf2_sha256__default_rounds = hash_config['pbkdf2_sha256__default_rounds']
        )
    return pwd_context

class User():
    def __init__(
        self,
        username : str,
        password : str = '',
        type : Optional[str] = None,
        email : Optional[str] = None,
        attributes : Optional[Dict[str, Any]] = None,
        user_id : Optional[int] = None,
        repository : Optional[str] = None
    ):
        self.password = password
        self.password_hash = get_pwd_context().encrypt(password)
        self.username = username
        self.email = email
        self.type = type
        self._attributes = attributes
        self._user_id = user_id
        self._repository_keys = repository

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.username

    @property
    def attributes(self) -> Dict[str, Any]:
        if self._attributes is None:
            self._attributes = dict()
        return self._attributes

    @property
    def repository(self) -> meerschaum.connectors.Connector:
        from meerschaum.connectors.parse import parse_repo_keys
        if '_repository' not in self.__dict__:
            self._repository = parse_repo_keys(self._repository_keys)
        return self._repository

    @property
    def user_id(self) -> int:
        """
        NOTE: This causes recursion with the API,
              so don't try to get fancy with read-only attributes.
        """
        #  if self._user_id is None:
            #  self._user_id = self.repository.get_user_id(self)
        return self._user_id

    @user_id.setter
    def user_id(self, user_id):
        self._user_id = user_id
