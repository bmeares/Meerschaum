#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Log into API connectors.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List, Any

def login(
        action: Optional[List[str]] = None,
        connector_keys: Optional[List[str]] = None,
        yes: bool = False,
        noask: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Log into a Meerschaum API instance.
    """
    from meerschaum.utils.prompt import prompt, get_password, yes_no
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.config import get_config
    from meerschaum.config._edit import write_config
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.formatting import print_tuple
    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []
    _possible_keys = action + connector_keys
    _keys = []
    for k in _possible_keys:
        if not k.startswith('api:'):
            warn(f"Connector '{k}' is not an API connector. Skipping...", stack=False)
            continue
        _keys.append(k)

    def _get_creds(connector):
        username = prompt(f"Username for connector '{connector}':")
        password = get_password(username=username, confirm=False)
        return username, password

    def _forget_creds(_dict):
        to_delete = ['username', 'password']
        for cred in to_delete:
            try:
                del _dict[cred]
            except KeyError:
                pass

    _connectors = []
    for k in _keys:
        try:
            _connectors.append(parse_instance_keys(k))
        except Exception as e:
            warn(f"Unable to build connector '{k}'. Is it registered?", stack=False)

    meerschaum_config = get_config('meerschaum')
    successes = 0
    for c in _connectors:
        while True:
            login_change = False
            if 'username' not in c.__dict__ or 'password' not in c.__dict__:
                login_change = True
                try:
                    clear_screen(debug=debug)
                    username, password = _get_creds(c)
                except KeyboardInterrupt:
                    return False, "Cancelled login."
                else:
                    c.username = username
                    c.password = password
            try:
                login_tuple = c.login(debug=debug)
            except Exception as e:
                warn(f"Failed to log in to connector '{c}':\n\n{e}\n\nSkipping '{c}'...", stack=False)
                break
            if not login_tuple[0]:
                if yes_no(
                    f"Would you like to try different login credentials for connector '{c}'?",
                    yes=yes, noask=noask
                ):
                    _forget_creds(c.__dict__)
                    continue
                else:
                    ### Did not have prior creds, was unsuccessful, and wants to abort.
                    if login_change:
                        _forget_creds(c.__dict__)
            print_tuple(login_tuple)
            successes += (1 if login_tuple[0] else 0)
            if login_tuple[0] and login_change:
                if yes_no(
                    f"Would you like to save the new credentials for connector '{c}'?",
                    yes=yes, noask=noask
                ):
                    meerschaum_config['connectors'][c.type][c.label].update(
                        {'username' : c.username, 'password' : c.password}
                    )
                    write_config({'meerschaum' : meerschaum_config})
                else:
                    _forget_creds(c.__dict__)
            break
        
    msg = f"Logged into {successes} connector" + ('s' if successes != 1 else '') + '.'

    return successes > 0, msg

def _complete_login(action: Optional[List[str]] = None, **kw: Any) -> List[str]:
    from meerschaum.utils.misc import get_connector_labels
    search_term = action[-1] if action else ''
    return get_connector_labels('api', search_term=search_term)
