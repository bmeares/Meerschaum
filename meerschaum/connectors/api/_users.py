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
        **kw
    ) -> tuple:
    """
    Log in and set the session token
    """
    from meerschaum.utils.warnings import warn, info, error
    from meerschaum import User
    import json, datetime
    login_data = {
        'username' : self.username,
        'password' : self.password,
    }
    response = self.post('/mrsm/login', data=login_data, use_token=False)
    if response:
        msg = f"Successfully logged into '{self}' as user '{login_data['username']}'"
        self._token = json.loads(response.text)['access_token']
        self._expires = datetime.datetime.strptime(
            json.loads(response.text)['expires'], 
            '%Y-%m-%dT%H:%M:%S.%f'
        )
    else:
        msg = ''
        if self.get_user_id(User(self.username, self.password), use_token=False) is None:
            msg = f"User '{self.username}' does not exist for '{self}'." + '\n'
        msg += (
            f"Failed to log into '{self}' as user '{login_data['username']}'. " +
            f"Please verify login details for connector '{self}' with `edit config`."
        )
        warn(msg)

    return response.__bool__(), msg

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
    
def get_user_id(
        self,
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> int:
    """
    Get a user's ID
    """
    import json
    r_url = f"/mrsm/users/{user.username}/id"
    response = self.get(r_url, **kw)
    try:
        user_id = int(json.loads(response.text))
    except Exception as e:
        user_id = None
    return user_id

def delete_user(
        self,
        user : 'meerschaum.User',
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Delete a user
    """
    import json
    r_url = f"/mrsm/users/{user.username}/delete"
    response = self.post(r_url)
    try:
        success_tuple = tuple(json.loads(response.text))
    except:
        success_tuple = False, f"Failed to delete user '{user.username}'"
    return success_tuple
