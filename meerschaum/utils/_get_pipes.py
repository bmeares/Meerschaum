#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the get_pipes() function
"""

def get_pipes(
        location_keys : list = [],
        metric_keys : list = [],
        params : dict = dict(),
        source : str = 'sql',
        as_dict : bool = True,
        debug : bool = False,
        **kw
    )-> 'dict or list':
    """
    Return a dictionary (or list) of Pipe objects.

    location_keys : list
        List of location_keys. A string is converted to a list of length 1.
        If parameter is omitted or is '*', fetch all location_keys.

    metric_keys : list
        List of metric_keys. See location_keys for more information.

    params : dict
        Dictionary of additional parameters to search by. This may include 
    """
    raise NotImplementedError("TODO finish get_pipes")
    ### ensure keys are lists
    if not isinstance(location_keys, list):
        location_keys = [location_keys]
    if not isinstance(metric_keys, list):
        metric_keys = [metric_keys]

    ### fetch meta connector
    from meerschaum.connectors import get_connector
    meta_connector = get_connector(type='sql', label='meta')

    if source == 'sql':
        print("SOURCE", source)
        #  from meerschaum.api.tables import get_tables
        #  pipes_table = get_tables()['pipes']
    ### TODO get pipes from API
    elif source == 'api':
        pass
    else:
        raise NotImplementedError(f"Invalid source '{source}'")

    ### catch all cases
    if location_keys in [[], ['*']] and source == 'sql':
        q = "SELECT DISTINCT location_key FROM pipes"


    q = """
    SELECT * FROM pipes
    """

    for i in meta_connector.read(q).iterrows():
        print(i)
