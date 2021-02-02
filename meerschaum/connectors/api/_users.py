#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the API Connector.
"""

from meerschaum.utils.typing import Optional, Any, List, SuccessTuple

def get_users(
        self,
        debug : bool = False,
        **kw : Any
    ) -> List[str]:
    """
    Return a list of registered users.
    """
    from meerschaum.config.static import _static_config
    import json
    return json.loads(
        self.get(
            f"{_static_config()['api']['endpoints']['users']}",
            debug = debug,
            use_token = False
        ).text
    )

def login(
        self,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Log in and set the session token.
    """
    from meerschaum.utils.warnings import warn, info, error
    from meerschaum._internal.User import User
    from meerschaum.config.static import _static_config
    import json, datetime
    try:
        login_data = {
            'username' : self.username,
            'password' : self.password,
        }
    except:
        return False, f"Please provide a username and password for '{self}' with `edit config`."
    response = self.post(
        _static_config()['api']['endpoints']['login'],
        data = login_data,
        use_token = False,
        debug = debug
    )
    if response:
        msg = f"Successfully logged into '{self}' as user '{login_data['username']}'"
        self._token = json.loads(response.text)['access_token']
        self._expires = datetime.datetime.strptime(
            json.loads(response.text)['expires'], 
            '%Y-%m-%dT%H:%M:%S.%f'
        )
    else:
        msg = ''
        if self.get_user_id(User(self.username, self.password), use_token=False, debug=debug) is None:
            msg = f"User '{self.username}' does not exist for '{self}'." + '\n'
        msg += (
            f"Failed to log into '{self}' as user '{login_data['username']}'. " +
            f"Please verify login details for connector '{self}' with `edit config`."
        )
        warn(msg, stack=False)

    return response.__bool__(), msg

def edit_user(
        self,
        user : 'meerschaum._internal.User.User',
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Edit an existing user.
    """
    import json
    from meerschaum.config.static import _static_config
    r_url = f"{_static_config()['api']['endpoints']['users']}/{user.username}/edit"
    params = {
        'password' : user.password,
        'email' : user.email,
        'attributes' : user.attributes,
    }
    response = self.post(r_url, json=user.attributes, params=params, debug=debug)
    try:
        _json = json.loads(response.text)
        if isinstance(_json, dict) and 'detail' in _json:
            return False, _json['detail']
        success_tuple = tuple(_json)
    except:
        if response.text: msg = response.text
        else: msg = f"Failed to edit user '{user}'"
        return False, msg

    return tuple(success_tuple)

def register_user(
        self,
        user : 'meerschaum._internal.User.User',
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Register a new user.
    """
    import json
    from meerschaum.config.static import _static_config
    r_url = f"{_static_config()['api']['endpoints']['users']}/{user.username}/register"
    params = {
        'password' : user.password,
        'email' : user.email,
        'attributes' : user.attributes,
    }
    response = self.post(r_url, json=user.attributes, params=params, debug=debug)
    try:
        _json = json.loads(response.text)
        if isinstance(_json, dict) and 'detail' in _json:
            return False, _json['detail']
        success_tuple = tuple(_json)
    except:
        if response.text: msg = response.text
        else: msg = f"Failed to register user '{user}'"
        return False, msg

    return tuple(success_tuple)
    
def get_user_id(
        self,
        user : 'meerschaum._internal.User.User',
        debug : bool = False,
        **kw
    ) -> Optional[int]:
    """
    Get a user's ID.
    """
    from meerschaum.config.static import _static_config
    import json
    r_url = f"{_static_config()['api']['endpoints']['users']}/{user.username}/id"
    response = self.get(r_url, debug=debug, **kw)
    try:
        user_id = int(json.loads(response.text))
    except Exception as e:
        user_id = None
    return user_id

def delete_user(
        self,
        user : 'meerschaum._internal.User.User',
        debug : bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Delete a user.
    """
    from meerschaum.config.static import _static_config
    import json
    r_url = f"{_static_config()['api']['endpoints']['users']}/{user.username}/delete"
    response = self.post(r_url, debug=debug)
    try:
        _json = json.loads(response.text)
        if isinstance(_json, dict) and 'detail' in _json:
            return False, _json['detail']
        success_tuple = tuple(_json)
    except:
        success_tuple = False, f"Failed to delete user '{user.username}'"
    return success_tuple

def get_user_attributes(
        self,
        user : 'meerschaum._internal.User.User',
        debug : bool = False,
        **kw
    ) -> int:
    """
    Get a user's attributes.
    """
    from meerschaum.config.static import _static_config
    import json
    r_url = f"{_static_config()['api']['endpoints']['users']}/{user.username}/attributes"
    response = self.get(r_url, debug=debug, **kw)
    try:
        attributes = json.loads(response.text)
    except Exception as e:
        attributes = None
    return attributes

