#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
User class definition
"""

pwd_context = None
def get_pwd_context():
    global pwd_context
    if pwd_context is None:
        from passlib.context import CryptContext
        pwd_context = CryptContext(
            schemes=["pbkdf2_sha256"],
            default="pbkdf2_sha256",
            pbkdf2_sha256__default_rounds=30000
        )
    return pwd_context

class User():
    def __init__(
        self,
        username : str,
        password : str = '',
        email : str = None,
        attributes : dict = None,
        user_id : int = None,
        repository : str = None
    ):
        self.password = password
        self.password_hash = get_pwd_context().encrypt(password)
        self.username = username
        self.email = email
        self._attributes = attributes
        self._user_id = user_id
        self._repository_keys = repository

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.username

    @property
    def attributes(self):
        if self._attributes is None:
            self._attributes = dict()
        return self._attributes

    @property
    def repository(self):
        from meerschaum.connectors.parse import parse_repo_keys
        if '_repository' not in self.__dict__:
            self._repository = parse_repo_keys(self._repository_keys)
        return self._repository

    @property
    def user_id(self):
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
