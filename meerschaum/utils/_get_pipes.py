#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the get_pipes() function
"""

from meerschaum.utils.debug import dprint

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

    connector_keys : list
        List of connector keys.
        If parameter is omitted or is '*', fetch all location_keys.

    metric_keys : list
        List of metric keys.
        See connector_keys for formatting

    location_keys : list
        List of location keys.
        See connector_keys for formatting

    params : dict
        Dictionary of additional parameters to search by. This may include 

    source : str
        ['api', 'sql'] Default "sql"
        Source of pipes data and metadata.
        If 'sql', pull from the `meta` and `main` SQL connectors.
        If 'api', pull from the `main` WebAPI.
    """
    #  raise NotImplementedError("TODO finish get_pipes")
    ### fetch meta connector
    from meerschaum.connectors import get_connector
    meta_connector = get_connector(type='sql', label='meta')

    ### TODO add source options as argument?
    if source == 'sql':
        pass
    ### TODO get pipes from API
    elif source == 'api':
        raise NotImplementedError(f"Source '{source}' has not yet been implemented.")
    else:
        raise NotImplementedError(f"Invalid source '{source}'")

    ### creates metadata
    from meerschaum.api.tables import get_tables
    tables = get_tables()

    q = """SELECT DISTINCT
    pipes.connector_keys, pipes.metric_key, pipes.location_key
FROM pipes
"""
    ### Add three primary keys to params dictionary
    ###   (separated for convenience of arguments)
    cols = {
        'connector_keys' : connector_keys,
        'metric_key' : metric_keys,
        'location_key' : location_keys,
    }
    for col, vals in cols.items():
        if vals not in [None, [], ['*']]:
            params[col] = vals

    def build_where():
        """
        Build the WHERE clause based on the input criteria
        """
        where = ""
        leading_and = "\n    AND "
        for key, value in params.items():
            if isinstance(value, list):
                where += f"{leading_and}{key} IN ("
                for item in value:
                    where += f"'{item}', "
                where = where[:-2] + ")"
                continue
            where += f"{leading_and}{key} = '{value}'"
        if len(where) > 1: where = "WHERE\n    " + where[len(leading_and):]
        return where
    q += build_where()

    pipes = dict()

    from meerschaum import Pipe
    if debug: dprint(q)
    result = meta_connector.engine.execute(q)
    for ck, mk, lk in result:
        if ck not in pipes:
            pipes[ck] = dict()

        if mk not in pipes[ck]:
            pipes[ck][mk] = dict()

        pipes[ck][mk][lk] = Pipe(ck, mk, lk, debug=debug)

    if not as_list: return pipes
    pipes_list = []
    for ck in pipes.values():
        for mk in ck.values():
            pipes_list += list(mk.values())
    return pipes_list

