#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Get the currently registered connectors.
"""

import fastapi
from fastapi import HTTPException

import meerschaum as mrsm
from meerschaum.api import app, endpoints, no_auth, manager
from meerschaum.utils.typing import Optional, Dict, List, Union

endpoint = endpoints['connectors']


@app.get(endpoint, tags=['Connectors'])
def get_connectors(
    type: Optional[str] = None,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
) -> Union[Dict[str, List[str]], List[str]]:
    """
    Return the keys of the registered connectors.

    Parameters
    ----------
    type: Optional[str], default None
        If provided, return the list of connectors of type `type`.
        Otherwise, return a dictionary of types that map to lists of labels.

    Returns
    -------
    A dictionary of types and labels, or a list of labels.
    """
    if type is not None and type not in mrsm.get_config('meerschaum', 'connectors'):
        raise HTTPException(status_code=404, detail=f"No connectors of type '{type}'.")
    types = [type] if type is not None else mrsm.get_config('meerschaum', 'connectors').keys()
    response_dict = {}
    for typ in types:
        response_dict[typ] = [
            _typ
            for _typ in mrsm.get_config('meerschaum', 'connectors', typ)
            if _typ != 'default'
        ]
    if type is not None:
        return response_dict[type]
    return response_dict


@app.get(endpoint + "/{type}", tags=['Connectors'])
def get_connectors_by_type(
    type: str,
    curr_user = (
        fastapi.Depends(manager) if not no_auth else None
    ),
):
    """
    Convenience method for `get_connectors()`.
    """
    return get_connectors(type)
