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
        json_dict = parse_arguments(sysargs)
    else:
        json_dict = kw
        json_dict['action'] = action
        json_dict['debug'] = debug

    root_action = json_dict['action'][0]
    del json_dict['action'][0]
    ### ensure 0 index exists (Meerschaum requirement)
    if len(json_dict['action']) == 0: json_dict['action'] = ['']
    r_url = f'/mrsm/actions/{root_action}'
    
    if debug:
        from pprintpp import pprint
        print(f"Sending data to '{self.url + r_url}':")
        pprint(json_dict)

    response = self.post(r_url, json=json_dict)
    try:
        response_list = json.loads(response.text)
    except Exception as e:
        print(f"Invalid response: {response}")
        print(e)
        return False, response.text
    if debug: print(response)
    return response_list[0], response_list[1]
