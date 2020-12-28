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
        password : str,
        email : str = None,
        attributes : dict = None,
        user_id : int = None
    ):
        self.password = password
        self.password_hash = get_pwd_context().encrypt(password)
        self.username = username
        self.email = email
        self._attributes = attributes
        self.user_id = user_id

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.username

    @property
    def attributes(self):
        if self._attributes is None:
            self._attributes = dict()
        return self._attributes

