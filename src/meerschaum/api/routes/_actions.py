#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Execute Meerschaum Actions via the API
"""

from meerschaum.api import fast_api, endpoints
from meerschaum.actions import actions

actions_endpoint = endpoints['mrsm'] + "/actions"

@fast_api.get(actions_endpoint)
def get_actions():
        return list(actions)

@fast_api.post(actions_endpoint + "/{action}")
def do_action(action : str, params : dict = None):
    print('action', action)
    print('params', params)
    return True
