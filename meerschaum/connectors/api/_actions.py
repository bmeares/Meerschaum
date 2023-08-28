#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions to interact with /mrsm/actions
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List

def get_actions(
        self,
    ) -> list:
    """Get available actions from the API server"""
    from meerschaum.config.static import STATIC_CONFIG
    return self.get(STATIC_CONFIG['api']['endpoints']['actions'])


def do_action(
        self,
        action: Optional[List[str]] = None,
        sysargs: Optional[List[str]] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """Execute a Meerschaum action remotely.
    
    If sysargs is provided, parse those instead. Otherwise infer everything from keyword arguments.
    
    NOTE: The first index of `action` should NOT be removed!
    Example: action = ['show', 'config']
    
    Returns: tuple (succeeded : bool, message : str)

    Parameters
    ----------
    action: Optional[List[str]] :
         (Default value = None)
    sysargs: Optional[List[str]] :
         (Default value = None)
    debug: bool :
         (Default value = False)
    **kw :
        

    Returns
    -------

    """
    import sys, json
    from meerschaum.utils.debug import dprint
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.misc import json_serialize_datetime
    if action is None:
        action = []

    if sysargs is not None and action and action[0] == '':
        from meerschaum._internal.arguments import parse_arguments
        if debug:
            dprint(f"Parsing sysargs:\n{sysargs}")
        json_dict = parse_arguments(sysargs)
    else:
        json_dict = kw
        json_dict['action'] = action
        json_dict['debug'] = debug

    root_action = json_dict['action'][0]
    del json_dict['action'][0]
    r_url = f"{STATIC_CONFIG['api']['endpoints']['actions']}/{root_action}"
    
    if debug:
        from meerschaum.utils.formatting import pprint
        dprint(f"Sending data to '{self.url + r_url}':")
        pprint(json_dict, stream=sys.stderr)

    response = self.post(
        r_url,
        data = json.dumps(json_dict, default=json_serialize_datetime),
        debug = debug,
    )
    try:
        response_list = json.loads(response.text)
        if isinstance(response_list, dict) and 'detail' in response_list:
            return False, response_list['detail']
    except Exception as e:
        print(f"Invalid response: {response}")
        print(e)
        return False, response.text
    if debug:
        dprint(response)
    try:
        return response_list[0], response_list[1]
    except Exception as e:
        return False, f"Failed to parse result from action '{root_action}'"
