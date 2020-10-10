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
        source : str = 'sql',
        as_list : bool = False,
        method : str = 'registered',
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

    source : str
        ['api', 'sql'] Default "sql"
        Connector keys for the source of the Pipes.
        
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
            (API or SQL connector, depends on source)

        If 'explicit', create Pipes from provided connector_keys, metric_keys, and location_keys
            instead of consulting the pipes table. Useful for creating non-existent Pipes.

        If 'all', create Pipes from predefined metrics and locations. Required connector_keys.
            NOTE: Not implemented!
    """

    from meerschaum.connectors import get_connector
    from meerschaum.utils.misc import flatten_pipes_dict

    default_meta_labels = {
        'api' : 'main',
        'sql' : 'meta',
    }

    ### determine where to pull Pipe data from
    source_keys = source.split(':')
    source_type = source_keys[0]
    try:
        meta_label = source_keys[1]
    except:
        meta_label = default_meta_labels[source_type]
    meta_type = source_type

    ### keys for metadata (not source)
    meta_keys = meta_type + ':' + meta_label

    ### Substitute 'sql:meta' to 'sql:main' for source.
    ### There's probably a more scalable way to do this,
    ### but I hope sql:meta / sql:main is the only case.
    source_label = meta_label
    if source_type == "sql" and meta_label == "meta": 
        source_label = "main"

    ### keys for source (not metadata)
    source_keys = source_type + ':' + source_label

    ### get SQL or API connector (keys come from `connector.fetch_pipes_keys()`)
    connector = get_connector(type=meta_type, label=meta_label)

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

        pipes[ck][mk][lk] = Pipe(ck, mk, lk, source=source_keys, debug=debug)

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

