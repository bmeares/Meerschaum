#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Log into the API instance or refresh the token.
"""

from __future__ import annotations

import json
import datetime
from meerschaum.utils.typing import SuccessTuple, Any, Union
from meerschaum.config.static import STATIC_CONFIG
from meerschaum.utils.warnings import warn as _warn


def login(
    self,
    debug: bool = False,
    warn: bool = True,
    **kw: Any
) -> SuccessTuple:
    """Log in and set the session token."""
    try:
        login_data = {
            'username': self.username,
            'password': self.password,
        }
    except AttributeError:
        return False, f"Please login with the command `login {self}`."

    response = self.post(
        STATIC_CONFIG['api']['endpoints']['login'],
        data=login_data,
        use_token=False,
        debug=debug,
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
            f"Failed to log into '{self}' as user '{login_data['username']}'.\n" +
            f"    Please verify login details for connector '{self}'."
        )
        if warn and not self.__dict__.get('_emitted_warning', False):
            _warn(msg, stack=False)
            self._emitted_warning = True

    return response.__bool__(), msg


def test_connection(
    self,
    **kw: Any
) -> Union[bool, None]:
    """Test if a successful connection to the API may be made."""
    from meerschaum.connectors.poll import retry_connect
    _default_kw = {
        'max_retries': 1, 'retry_wait': 0, 'warn': False,
        'connector': self, 'enforce_chaining': False,
        'enforce_login': False,
    }
    _default_kw.update(kw)
    try:
        return retry_connect(**_default_kw)
    except Exception:
        return False
