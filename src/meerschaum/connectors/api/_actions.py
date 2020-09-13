#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions to interact with /mrsm/actions
"""

import requests, json

def get_actions(
        self,
    ) -> list:
    """
    Get available actions from the API server
    """
    return self.get('/mrsm/actions')

def do_action(
        self,
        action : list = [''],
        sysargs : list = None,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Execute a Meerschaum action remotely.

    If sysargs is provided, parse those instead. Otherwise infer everything from keyword arguments.
    
    NOTE: The first index of `action` should NOT be removed!
    Example: action = ['show', 'config']
    
    Returns: tuple (succeeded : bool, message : str)
    """

    if sysargs is not None and action[0] == '':
        from meerschaum.actions.arguments import parse_arguments
        if debug: print(f"Parsing sysargs:\n{sysargs}")
        json = parse_arguments(sysargs)
    else:
        json = kw
        json['action'] = action
        json['debug'] = debug

    root_action = json['action'][0]
    del json['action'][0]
    r_url = f'/mrsm/actions/{root_action}'
    
    if debug:
        from pprintpp import pprint
        print(f"Sending data to '{self.url + r_url}':")
        pprint(json)

    response = self.post(r_url, json=json)
    try:
        response_list = json.loads(response.text)
    except:
        print(f"Invalid response: {response}")
        return False, response.text
    return response_list[0], response_list[1]
