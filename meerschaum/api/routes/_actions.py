#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute Meerschaum Actions via the API
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple

from meerschaum.api import fastapi, app, endpoints, get_api_connector, debug, manager
from meerschaum.actions import actions
import meerschaum._internal.User
actions_endpoint = endpoints['actions']

@app.get(actions_endpoint)
def get_actions() -> list:
    """
    Return a list of available actions
    """
    return list(actions)

@app.post(actions_endpoint + "/{action}")
def do_action(
        action : str,
        keywords : dict = fastapi.Body(...),
        curr_user : 'meerschaum._internal.User.User' = fastapi.Depends(manager),
    ) -> SuccessTuple:
    """
    Perform a Meerschaum action (if permissions allow it).
    """
    if curr_user.type != 'admin':
        from meerschaum.config import get_config
        allow_non_admin = get_config('system', 'api', 'permissions', 'actions', 'non_admin', patch=True)
        if not allow_non_admin:
            return False, (
                "The administrator for this server has not allowed users to perform actions.\n\n" +
                "Please contact the system administrator, or if you are running this server, " +
                "open the configuration file with `edit config system` and search for 'permissions'. " +
                " Under the keys system:api:permissions:actions, you can allow non-admin users to perform actions."
        )

    if action not in actions:
        return False, f"Invalid action '{action}'."
    if 'mrsm_instance' not in keywords:
        keywords['mrsm_instance'] = str(get_api_connector())
    _debug = keywords.get('debug', debug)
    if 'debug' in keywords:
        del keywords['debug']
    return actions[action](debug=_debug, **keywords)
