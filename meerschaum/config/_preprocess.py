#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Preprocessing on the configuration dictionary
"""

def preprocess_config(
        config : dict,
        debug : bool = False,
        **kw
    ) -> dict:
    """
    Apply preprocessing to the configuration dictionary
    config : the config dict
    """
    from meerschaum.utils.misc import parse_config_substitution, search_and_substitute_config

    ### replace Meerschaum substitution syntax with values from keys
    config = search_and_substitute_config(config)

    ### add meta to SQL connectors
    #  sql_connectors_config = config['meerschaum']['connectors']['sql']
    #  if 'meta' in sql_connectors_config and len(sql_connectors_config['meta']) != 0:
        #  pass
    #  else:
        #  config['meerschaum']['connectors']['sql']['meta'] = sql_connectors_config['main']

    return config 

