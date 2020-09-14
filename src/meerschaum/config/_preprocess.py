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
    from meerschaum.utils.misc import parse_config_substitution, search_for_substitution

    #  def recurse_config(node, value):
        #  if isinstance(value, str):
            #  return node, node
        #  try:
            #  for child, value in node.items():
                #  return recurse_config(child, value)
        #  except:
            #  print(node)
            #  return node


    #  for root, value in config.items():
        #  print(recurse_config(root, value))

    search_for_substitution(config)


    ### if `meta` is not set, use `main`
    sql_connectors_config = config['meerschaum']['connectors']['sql']
    if 'meta' not in sql_connectors_config:
        config['meerschaum']['connectors']['sql']['meta'] = sql_connectors_config['main']
    elif len(sql_connectors_config['meta']) == 0:
        config['meerschaum']['connectors']['sql']['meta'] = sql_connectors_config['main']

    return config 


