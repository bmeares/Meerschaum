#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register and show connectors
"""

from meerschaum.api import fast_api, endpoints
from fastapi import Body, HTTPException

endpoint = endpoints['mrsm'] + '/connectors'

@fast_api.get(endpoint)
def get_connectors(type : str = None):
    from meerschaum.config import config as cf, get_config
    if type is not None and type not in get_config('meerschaum', 'connectors'):
        raise HTTPException(status_code=404, detail=f"No connectors of type '{type}'")
    types = []
    if type is not None:
        types.append(type)
    else:
        for t in cf['meerschaum']['connectors']:
            types.append(t)

    response_dict = dict()
    for t in types:
        response_dict[t] = list(cf['meerschaum']['connectors'][t])
        response_dict[t].remove('default')
    if type is not None: return response_dict[type]
    return response_dict

@fast_api.get(endpoint + "/{type}")
def get_connectors_by_type(type : str):
    return get_connectors(type)

