#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Get the currently registered connectors.
"""

from fastapi import Body, HTTPException
from meerschaum.api import app, endpoints
from meerschaum.utils.typing import Optional, Dict, List, Union

endpoint = endpoints['connectors']

@app.get(endpoint)
def get_connectors(type : Optional[str] = None) -> Union[Dict[str, List[str]], List[str]]:
    """
    Return the keys of the registered connectors.

    :params type:
        If a `type` is specified, return the list of connectors that belong to that type.
        Otherwise, return a dictionary of types that map to lists of labels.
        Defaults to `None`.
    """
    from meerschaum.config import get_config
    if type is not None and type not in get_config('meerschaum', 'connectors'):
        raise HTTPException(status_code=404, detail=f"No connectors of type '{type}'.")
    types = []
    if type is not None:
        types.append(type)
    else:
        for t in get_config('meerschaum', 'connectors'):
            types.append(t)

    response_dict = dict()
    for t in types:
        response_dict[t] = list(get_config('meerschaum', 'connectors', t))
        response_dict[t].remove('default')
    if type is not None:
        return response_dict[type]
    return response_dict

@app.get(endpoint + "/{type}")
def get_connectors_by_type(type : str):
    """
    Convenience method for `get_connectors()`.
    """
    return get_connectors(type)
