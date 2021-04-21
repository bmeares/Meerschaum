#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Log into the API instance or refresh the token.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any

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
    except AttributeError:
        return False, f"Please provide a username and password for '{self}' with `edit config`."
    response = self.post(
        _static_config()['api']['endpoints']['login'],
        data = login_data,
        use_token = False,
        debug = debug
    )
    if response:
        msg = f"Successfully logged into '{self}' as user '{login_data['username']}'."
        self._token = json.loads(response.text)['access_token']
        self._expires = datetime.datetime.strptime(
            json.loads(response.text)['expires'], 
            '%Y-%m-%dT%H:%M:%S.%f'
        )
    else:
        msg = (
            '' if self.get_user_id(
                User(self.username, self.password),
                use_token = False,
                debug = debug
            ) is not None else f"User '{self.username}' does not exist for '{self}'." + '\n'
        )
        msg += (
            f"   Failed to log into '{self}' as user '{login_data['username']}'.\n" +
            f"   Please verify login details for connector '{self}'."
        )
        warn(msg, stack=False)

    return response.__bool__(), msg

def refresh(
        self,
        debug : bool = False,
        **kw : Any,
    ):
    """
    Refresh the access token.
    """
    pass
