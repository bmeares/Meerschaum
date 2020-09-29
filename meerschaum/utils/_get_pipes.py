#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the get_pipes() function
"""

def get_pipes(
        connector_keys : list = [],
        location_keys : list = [],
        metric_keys : list = [],
        params : dict = dict(),
        source : str = 'sql',
        as_list : bool = False,
        debug : bool = False,
        **kw
    )-> 'dict or list':
    """
    Return a dictionary (or list) of Pipe objects.

    connector_keys : dict

    location_keys : list
        List of location_keys. A string is converted to a list of length 1.
        If parameter is omitted or is '*', fetch all location_keys.

    metric_keys : list
        List of metric_keys. See location_keys for more information.

    params : dict
        Dictionary of additional parameters to search by. This may include 
    """
    #  raise NotImplementedError("TODO finish get_pipes")
    ### fetch meta connector
    from meerschaum.connectors import get_connector
    meta_connector = get_connector(type='sql', label='meta')

    ### TODO add source options as argument?
    if source == 'sql':
        pass
        #  print("SOURCE", source)
        #  from meerschaum.api.tables import get_tables
        #  pipes_table = get_tables()['pipes']
    ### TODO get pipes from API
    #  elif source == 'api':
        #  pass
    else:
        raise NotImplementedError(f"Invalid source '{source}'")

    ### creates metadata
    from meerschaum.api.tables import get_tables
    tables = get_tables()

    def select_distinct(column : str) -> list:
        """
        Get all distinct values of a single column from the `pipes` table
        """
        q = f"SELECT DISTINCT {column} FROM pipes"
        return list(meta_connector.read(q)[column])

    ### catch all cases
    #  if connector_keys in [None, ['*']] and source == 'sql':
        #  connector_keys = select_distinct('connector_keys')

    #  if metric_keys in [None, ['*']] and source == 'sql':
        #  metric_keys = select_distinct('metric_key')

    #  if location_keys in [None, ['*']] and source == 'sql':
        #  location_keys = select_distinct('location_key')

    q = """
    SELECT DISTINCT pipes.connector_keys, pipes.metric_key, pipes.location_key
    FROM pipes
    """
    ### NOTE implement left joins later?
    """
    LEFT JOIN metrics ON metrics.metric_key = pipes.metric_key AND metrics.connector_keys = pipes.connector_keys
    LEFT JOIN locations ON locations.location_key = pipes.location_key AND locations.connector_keys = pipes.connector_keys
    """

    ### Add three primary keys to params dictionary
    ###   (separated for convenience of arguments)
    cols = {
        'connector_keys' : connector_keys,
        'metric_key' : metric_keys,
        'location_key' : location_keys,
    }
    for col, vals in cols.items():
        if vals not in [None, ['*']]:
            params[col] = vals

    print(params)
    input()
    def build_where():
        where = "\nWHERE 1"
        for key, value in params.items():
            where += f"\n  AND {key} = '{value}'"
        return where
    q += build_where()

    if debug: print(q)
    print(meta_connector.read(q))

    if debug:
        print('Connector keys:', connector_keys)
        print('Metric keys:', metric_keys)
        print('Location keys:', location_keys)

    #  for i in meta_connector.read(q).iterrows():
        #  print(i)
