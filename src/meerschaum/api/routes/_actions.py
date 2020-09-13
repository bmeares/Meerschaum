#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute Meerschaum Actions via the API
"""

from meerschaum.api import fast_api, endpoints
from meerschaum.actions import actions
from fastapi import Body

actions_endpoint = endpoints['mrsm'] + "/actions"

@fast_api.get(actions_endpoint)
def get_actions():
    """
    Return a list of available actions
    """
    return list(actions)

@fast_api.post(actions_endpoint + "/{action}")
def do_action(action : str, keywords : dict = Body(...)):
    print(keywords)
    if action not in actions:
        return False, f"Invalid action {action}"
    return actions[action](**keywords)
