#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the API Connector
"""

def get_users(
        self,
        debug : bool = False,
        **kw
    ) -> list:
    """
    Return a list of registered users
    """
    import json
    return json.loads(self.get('/mrsm/users').text)

def login(
        self,
        #  user : 'meerschaum.User',
        **kw
    ) -> tuple:
    """
    Log in and set the session token
    """
    login_data = {
        'username' : self.username,
        'password' : self.password,
    }
    self.post('/mrsm/login', json=login_data)

def edit_user(
        self,
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Edit an existing user
    """
    import json
    r_url = f"/mrsm/users/{user.username}/edit"
    params = {
        'password' : user.password,
        'email' : user.email,
        'attributes' : user.attributes,
    }
    response = self.post(r_url, json=user.attributes, params=params)
    try:
        success_tuple = tuple(json.loads(response.text))
    except:
        if response.text: msg = response.text
        else: msg = f"Failed to edit user '{user}'"
        return False, msg

    return tuple(success_tuple)

def register_user(
        self,
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Register a new user
    """
    import json
    r_url = f"/mrsm/users/{user.username}/register"
    params = {
        'password' : user.password,
        'email' : user.email,
        'attributes' : user.attributes,
    }
    response = self.post(r_url, json=user.attributes, params=params)
    try:
        success_tuple = tuple(json.loads(response.text))
    except:
        if response.text: msg = response.text
        else: msg = f"Failed to register user '{user}'"
        return False, msg

    return tuple(success_tuple)
    
