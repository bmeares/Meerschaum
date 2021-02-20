#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register and show connectors
"""

from meerschaum.api import app, endpoints
from fastapi import Body, HTTPException

endpoint = endpoints['connectors']

@app.get(endpoint)
def get_connectors(type : str = None):
    from meerschaum.config import get_config
    if type is not None and type not in get_config('meerschaum', 'connectors'):
        raise HTTPException(status_code=404, detail=f"No connectors of type '{type}'")
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
    if type is not None: return response_dict[type]
    return response_dict

@app.get(endpoint + "/{type}")
def get_connectors_by_type(type : str):
    return get_connectors(type)

