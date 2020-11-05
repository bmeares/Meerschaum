#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Implement the get_pipes() function
"""

from meerschaum.utils.debug import dprint

def get_pipes(
        connector_keys : 'str or list' = [],
        metric_keys : 'str or list' = [],
        location_keys : 'str or list' = [],
        params : dict = dict(),
        mrsm_instance : str = None,
        as_list : bool = False,
        method : str = 'registered',
        wait : bool = False,
        debug : bool = False,
        **kw
    )-> 'dict or list':
    """
    Return a dictionary (or list) of Pipe objects.

    connector_keys : list
        String or list of connector keys.
        If parameter is omitted or is '*', fetch all connector_keys.

    metric_keys : list
        String or list of metric keys.
        See connector_keys for formatting

    location_keys : list
        String or list of location keys.
        See connector_keys for formatting

    params : dict
        Dictionary of additional parameters to search by. This may include 

    mrsm_instance : str
        ['api', 'sql'] Default "sql"
        Connector keys for the Meerschaum instance of the Pipes.
        
        Source of pipes data and metadata.
        If 'sql', pull from the `meta` and `main` SQL connectors.
        If 'api', pull from the `main` WebAPI.

    as_list : bool : False
        If True, return pipes in a list instead of a hierarchical dictionary.
        False : { connector_keys : { metric_key : { location_key : Pipe } } }
        True  : [ Pipe ]
    
    method : str : 'registered'
        ['registered', 'explicit', 'all']
        TODO implement all (left join with metrics and locations)

        If 'registered', create Pipes based on registered keys in the connector's `pipes` table
            (API or SQL connector, depends on mrsm_instance)

        If 'explicit', create Pipes from provided connector_keys, metric_keys, and location_keys
            instead of consulting the pipes table. Useful for creating non-existent Pipes.

        If 'all', create Pipes from predefined metrics and locations. Required connector_keys.
            NOTE: Not implemented!

    wait : bool : False
        Wait for a connection before getting Pipes. Should only be true for cases where the
        database might not be running (like the API).
    """

    from meerschaum.utils.misc import flatten_pipes_dict
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import error

    #  if connector_keys == "": connector_keys = None
    #  if metric_keys == "": metric_keys = None
    #  if location_keys == "": location_keys = None

    ### Get SQL or API connector (keys come from `connector.fetch_pipes_keys()`).
    ### If `wait`, wait until a connection is made
    if mrsm_instance is None: mrsm_instance = get_config('meerschaum', 'instance', patch=True)
    if isinstance(mrsm_instance, str):
        from meerschaum.utils.misc import parse_instance_keys
        connector = parse_instance_keys(keys=mrsm_instance, wait=wait, debug=debug)
    else: ### NOTE: mrsm_instance MUST be a SQL or API connector for this to work
        connector = mrsm_instance
    if debug: dprint(f"Using instance connector: {connector}")
    if not connector:
        error(f"Could not create connector from keys: '{mrsm_instance}'")

    ### get a list of tuples of keys based on the method type
    result = methods(
        method,
        connector,
        connector_keys = connector_keys,
        metric_keys = metric_keys,
        location_keys = location_keys,
        params = params,
        debug = debug
    )
   
    ### populate the `pipes` dictionary with Pipes based on the keys
    ### obtained from the chosen `method`.
    from meerschaum import Pipe
    pipes = dict()
    for ck, mk, lk in result:
        if ck not in pipes:
            pipes[ck] = dict()

        if mk not in pipes[ck]:
            pipes[ck][mk] = dict()

        pipes[ck][mk][lk] = Pipe(ck, mk, lk, mrsm_instance=connector, debug=debug)

    if not as_list: return pipes
    return flatten_pipes_dict(pipes)

def methods(
        method : str,
        connector : 'Connector (API or SQL)',
        **kw
    ) -> list:
    """
    Return a list of tuples containing (connector_keys, metric_key, location_key)
    based on the method of choosing keys.
    """
    from meerschaum.utils.warnings import error

    def _registered(
            connector_keys : list = [],
            metric_keys : list = [],
            location_keys : list = [],
            params : dict = dict(),
            debug : bool = False,
            **kw
        ) -> list:
        """
        Get keys from the pipes table or the API directly.
        Builds query or URL based on provided keys and parameters.

        Only works for SQL and API Connectors.
        """
        return connector.fetch_pipes_keys(
            connector_keys = connector_keys,
            metric_keys = metric_keys,
            location_keys = location_keys,
            params = params,
            debug = debug
        )

    def _explicit(
            connector_keys : list = [],
            metric_keys : list = [],
            location_keys : list = [],
            params : dict = dict(),
            debug : bool = False,
            **kw
        ) -> list:
        """
        Explicitly build Pipes based on provided keys. 
        Raises an error if connector_keys or metric_keys is empty,
        and assumes location_keys = [None] if empty
        """
        if not isinstance(connector_keys, list): connector_keys = [connector_keys]
        if not isinstance(metric_keys, list): metric_keys = [metric_keys]
        if not isinstance(location_keys, list): location_keys = [location_keys]

        missing_keys = []
        if len(connector_keys) == 0:
            missing_keys.append('connector_keys')
        if len(metric_keys) == 0:
            missing_keys.append('metric_keys')
        if len(location_keys) == 0:
            location_keys.append(None)
        if len(missing_keys) > 0:
            error_message = "Missing parameters: '" + "', '".join(missing_keys) + "'"
            error_message += "\nSee --help for information for passing parameters."
            error(error_message)
        result = []
        for ck in connector_keys:
            for mk in metric_keys:
                for lk in location_keys:
                    result.append((ck, mk, lk))
        return result

    def _all(**kw):
        """
        Fetch all available metrics and locations and create every combination.
        Connector keys are required.
        """
        raise error("Need to implement metrics and locations logic in SQL and API", NotImplementedError)

    methods = {
        'registered' : _registered,
        'explicit'   : _explicit,
        'all'        : _all,
        ### TODO implement 'all'
    }
    if method not in methods:
        error(f"Method '{method}' is not supported!", NotImplementedError)
    return methods[method](**kw)

