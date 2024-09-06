#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute Meerschaum Actions via the API
"""

from __future__ import annotations

from meerschaum.utils.typing import SuccessTuple, List, Dict, Any
from meerschaum.api import (
    fastapi, app, endpoints, get_api_connector, debug, manager, private, no_auth
)
from meerschaum.actions import actions
from meerschaum.config import get_config

actions_endpoint = endpoints['actions']

def is_user_allowed_to_execute(user) -> SuccessTuple:
    if user is None:
        return False, "Could not load user."

    if user.type == 'admin':
        return True, "Success"

    allow_non_admin = get_config(
        'system', 'api', 'permissions', 'actions', 'non_admin', patch=True
    )
    if not allow_non_admin:
        return False, (
            "The administrator for this server has not allowed users to perform actions.\n\n"
            + "Please contact the system administrator, or if you are running this server, "
            + "open the configuration file with `edit config system` "
            + "and search for 'permissions'. "
            + "\nUnder the keys 'api:permissions:actions', "
            + "you can allow non-admin users to perform actions."
        )

    return True, "Success"


@app.get(actions_endpoint, tags=['Actions'])
def get_actions(
    curr_user = (
        fastapi.Depends(manager) if private else None
    ),
) -> List[str]:
    """
    Return a list of the available actions.
    """
    return list(actions)


@app.post(actions_endpoint + "/{action}", tags=['Actions'])
def do_action_legacy(
    action: str,
    keywords: Dict[str, Any] = fastapi.Body(...),
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> SuccessTuple:
    """
    Perform a Meerschaum action (if permissions allow).

    Parameters
    ----------
    action: str
        The action to perform.

    keywords: Dict[str, Any]
        The keywords dictionary to pass to the action.

    Returns
    -------
    A `SuccessTuple`.
    """
    if curr_user is not None and curr_user.type != 'admin':
        allow_non_admin = get_config(
            'system', 'api', 'permissions', 'actions', 'non_admin', patch=True
        )
        if not allow_non_admin:
            return False, (
                "The administrator for this server has not allowed users to perform actions.\n\n"
                + "Please contact the system administrator, or if you are running this server, "
                + "open the configuration file with `edit config system` "
                + "and search for 'permissions'. "
                + "\nUnder the keys 'api:permissions:actions', "
                + "you can allow non-admin users to perform actions."
            )

    if action not in actions:
        return False, f"Invalid action '{action}'."

    keywords['mrsm_instance'] = keywords.get('mrsm_instance', str(get_api_connector()))
    _debug = keywords.get('debug', debug)
    keywords.pop('debug', None)
    return actions[action](debug=_debug, **keywords)
