#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage users via the API Connector.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any, List, SuccessTuple

def get_users(
        self,
        debug: bool = False,
        **kw : Any
    ) -> List[str]:
    """
    Return a list of registered usernames.
    """
    from meerschaum.config.static import STATIC_CONFIG
    import json
    response = self.get(
        f"{STATIC_CONFIG['api']['endpoints']['users']}",
        debug = debug,
        use_token = True,
    )
    if not response:
        return []
    try:
        return response.json()
    except Exception as e:
        return []

def edit_user(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """Edit an existing user."""
    import json
    from meerschaum.config.static import STATIC_CONFIG
    r_url = f"{STATIC_CONFIG['api']['endpoints']['users']}/edit"
    data = {
        'username': user.username,
        'password': user.password,
        'type': user.type,
        'email': user.email,
        'attributes': json.dumps(user.attributes),
    }
    response = self.post(r_url, data=data, debug=debug)
    try:
        _json = json.loads(response.text)
        if isinstance(_json, dict) and 'detail' in _json:
            return False, _json['detail']
        success_tuple = tuple(_json)
    except Exception as e:
        msg = response.text if response else f"Failed to edit user '{user}'."
        return False, msg

    return tuple(success_tuple)


def register_user(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """Register a new user."""
    import json
    from meerschaum.config.static import STATIC_CONFIG
    r_url = f"{STATIC_CONFIG['api']['endpoints']['users']}/register"
    data = {
        'username': user.username,
        'password': user.password,
        'attributes': json.dumps(user.attributes),
    }
    if user.type:
        data['type'] = user.type
    if user.email:
        data['email'] = user.email
    response = self.post(r_url, data=data, debug=debug)
    try:
        _json = json.loads(response.text)
        if isinstance(_json, dict) and 'detail' in _json:
            return False, _json['detail']
        success_tuple = tuple(_json)
    except Exception:
        msg = response.text if response else f"Failed to register user '{user}'."
        return False, msg

    return tuple(success_tuple)

    
def get_user_id(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw: Any
    ) -> Optional[int]:
    """Get a user's ID."""
    from meerschaum.config.static import STATIC_CONFIG
    import json
    r_url = f"{STATIC_CONFIG['api']['endpoints']['users']}/{user.username}/id"
    response = self.get(r_url, debug=debug, **kw)
    try:
        user_id = int(json.loads(response.text))
    except Exception as e:
        user_id = None
    return user_id

def delete_user(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """Delete a user."""
    from meerschaum.config.static import STATIC_CONFIG
    import json
    r_url = f"{STATIC_CONFIG['api']['endpoints']['users']}/{user.username}"
    response = self.delete(r_url, debug=debug)
    try:
        _json = json.loads(response.text)
        if isinstance(_json, dict) and 'detail' in _json:
            return False, _json['detail']
        success_tuple = tuple(_json)
    except Exception as e:
        success_tuple = False, f"Failed to delete user '{user.username}'."
    return success_tuple

def get_user_attributes(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw
    ) -> int:
    """Get a user's attributes."""
    from meerschaum.config.static import STATIC_CONFIG
    import json
    r_url = f"{STATIC_CONFIG['api']['endpoints']['users']}/{user.username}/attributes"
    response = self.get(r_url, debug=debug, **kw)
    try:
        attributes = json.loads(response.text)
    except Exception as e:
        attributes = None
    return attributes

#############################
# Chaining functions below. #
#############################

def get_user_password_hash(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw: Any
    ) -> Optional[str]:
    """If configured, get a user's password hash."""
    from meerschaum.config.static import STATIC_CONFIG
    r_url = STATIC_CONFIG['api']['endpoints']['users'] + '/' + user.username + '/password_hash'
    response = self.get(r_url, debug=debug, **kw)
    if not response:
        return None
    return response.json()

def get_user_type(
        self,
        user: 'meerschaum.core.User',
        debug: bool = False,
        **kw: Any
    ) -> Optional[str]:
    """If configured, get a user's type."""
    from meerschaum.config.static import STATIC_CONFIG
    r_url = STATIC_CONFIG['api']['endpoints']['users'] + '/' + user.username + '/type'
    response = self.get(r_url, debug=debug, **kw)
    if not response:
        return None
    return response.json()
