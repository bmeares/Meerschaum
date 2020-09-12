#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Preprocessing on the configuration dictionary
"""

def preprocess_config(
        config : dict,
        debug : bool =False,
        **kw
    ) -> dict:
    """
    Apply preprocessing to the configuration dictionary
    """

    ### if `meta` is not set, use `main`
    sql_connectors_config = config['meerschaum']['connectors']['sql']
    if 'meta' not in sql_connectors_config:
        config['meerschaum']['connectors']['sql']['meta'] = sql_connectors_config['main']
    elif len(sql_connectors_config['meta']) == 0:
        config['meerschaum']['connectors']['sql']['meta'] = sql_connectors_config['main']

    return config 
