#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Return a dictionary (or list) of pipe objects. See documentation below for more information.
"""

from __future__ import annotations

from meerschaum.utils.typing import (
    Sequence, Optional, Union, Mapping, Any, InstanceConnector, PipesDict, List, Dict, Tuple
)

__pdoc__ = {'get_pipes': True, 'fetch_pipes_keys': True}

def get_pipes(
        connector_keys: Union[str, List[str], None] = None,
        metric_keys: Union[str, List[str], None] = None,
        location_keys: Union[str, List[str], None] = None,
        tags: Optional[List[str], None] = None,
        params: Optional[Dict[str, Any]] = None,
        mrsm_instance: Union[str, InstanceConnector, None] = None,
        instance: Union[str, InstanceConnector, None] = None,
        as_list: bool = False,
        method: str = 'registered',
        wait: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> Union[PipesDict, List['meerschaum.Pipe']]:
    """
    Return a dictionary or list of `meerschaum.Pipe` objects.

    Parameters
    ----------
    connector_keys: Union[str, List[str], None], default None
        String or list of connector keys.
        If omitted or is `'*'`, fetch all possible keys.
        If a string begins with `'_'`, select keys that do NOT match the string.

    metric_keys: Union[str, List[str], None], default None
        String or list of metric keys. See `connector_keys` for formatting.

    location_keys: Union[str, List[str], None], default None
        String or list of location keys. See `connector_keys` for formatting.

    tags: Optional[List[str]], default None
         If provided, only include pipes with these tags.

    params: Optional[Dict[str, Any]], default None
        Dictionary of additional parameters to search by.
        Params are parsed into a SQL WHERE clause.
        E.g. `{'a': 1, 'b': 2}` equates to `'WHERE a = 1 AND b = 2'`

    mrsm_instance: Union[str, InstanceConnector, None], default None
        Connector keys for the Meerschaum instance of the pipes.
        Must be a `meerschaum.connectors.sql.SQLConnector.SQLConnector` or
        `meerschaum.connectors.api.APIConnector.APIConnector`.
        
    as_list: bool, default False
        If `True`, return pipes in a list instead of a hierarchical dictionary.
        `False` : `{connector_keys: {metric_key: {location_key: Pipe}}}`
        `True`  : `[Pipe]`

    method: str, default 'registered'
        Available options: `['registered', 'explicit', 'all']`
        If `'registered'` (default), create pipes based on registered keys in the connector's pipes table
        (API or SQL connector, depends on mrsm_instance).
        If `'explicit'`, create pipes from provided connector_keys, metric_keys, and location_keys
        instead of consulting the pipes table. Useful for creating non-existent pipes.
        If `'all'`, create pipes from predefined metrics and locations. Required `connector_keys`.
        **NOTE:** Method `'all'` is not implemented!

    wait: bool, default False
        Wait for a connection before getting Pipes. Should only be true for cases where the
        database might not be running (like the API).

    **kw: Any:
        Keyword arguments to pass to the `meerschaum.Pipe` constructor.
        

    Returns
    -------
    A dictionary of dictionaries and `meerschaum.Pipe` objects
    in the connector, metric, location hierarchy.
    If `as_list` is `True`, return a list of `meerschaum.Pipe` objects.

    Examples
    --------
    ```
    >>> ### Manual definition:
    >>> pipes = {
    ...     <connector_keys>: {
    ...         <metric_key>: {
    ...             <location_key>: Pipe(
    ...                 <connector_keys>,
    ...                 <metric_key>,
    ...                 <location_key>,
    ...             ),
    ...         },
    ...     },
    ... },
    >>> ### Accessing a single pipe:
    >>> pipes['sql:main']['weather'][None]
    >>> ### Return a list instead:
    >>> get_pipes(as_list=True)
    [sql_main_weather]
    >>> 
    ```
    """

    from meerschaum.config import get_config
    from meerschaum.utils.warnings import error
    from meerschaum.utils.misc import filter_keywords

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    if params is None:
        params = {}
    if tags is None:
        tags = []

    if isinstance(connector_keys, str):
        connector_keys = [connector_keys]
    if isinstance(metric_keys, str):
        metric_keys = [metric_keys]
    if isinstance(location_keys, str):
        location_keys = [location_keys]

    ### Get SQL or API connector (keys come from `connector.fetch_pipes_keys()`).
    ### If `wait`, wait until a connection is made
    if mrsm_instance is None:
        mrsm_instance = instance
    if mrsm_instance is None:
        mrsm_instance = get_config('meerschaum', 'instance', patch=True)
    if isinstance(mrsm_instance, str):
        from meerschaum.connectors.parse import parse_instance_keys
        connector = parse_instance_keys(keys=mrsm_instance, wait=wait, debug=debug)
    else: ### NOTE: mrsm_instance MUST be a SQL or API connector for this to work
        from meerschaum.connectors import Connector
        valid_connector = False
        if issubclass(type(mrsm_instance), Connector):
            if mrsm_instance.type in ('api', 'sql'):
                valid_connector = True
        if not valid_connector:
            error(f"Invalid instance connector: {mrsm_instance}")
        connector = mrsm_instance
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"Using instance connector: {connector}")
    if not connector:
        error(f"Could not create connector from keys: '{mrsm_instance}'")

    ### Get a list of tuples for the keys needed to build pipes.
    result = fetch_pipes_keys(
        method,
        connector,
        connector_keys = connector_keys,
        metric_keys = metric_keys,
        location_keys = location_keys,
        tags = tags,
        params = params,
        debug = debug
    )
    if result is None:
        error(f"Unable to build pipes!")

    ### Populate the `pipes` dictionary with Pipes based on the keys
    ### obtained from the chosen `method`.
    from meerschaum import Pipe
    pipes = {}
    for ck, mk, lk in result:
        if ck not in pipes:
            pipes[ck] = {}

        if mk not in pipes[ck]:
            pipes[ck][mk] = {}

        pipes[ck][mk][lk] = Pipe(
            ck, mk, lk,
            mrsm_instance=connector,
            debug=debug,
            **filter_keywords(Pipe, **kw)
        )

    if not as_list:
        return pipes
    from meerschaum.utils.misc import flatten_pipes_dict
    return flatten_pipes_dict(pipes)


def fetch_pipes_keys(
        method: str,
        connector: 'meerschaum.connectors.Connector',
        **kw: Any
    ) -> List[Tuple[str, str, str]]:
    """
    Fetch keys for pipes according to a method.

    Parameters
    ----------
    method: str
        The method by which to fetch the keys. See `get_pipes()` above.

    connector: meerschaum.connectors.Connector
        The connector to use to fetch the keys.
        Must be of type `meerschaum.connectors.sql.SQLConnector.SQLConnector`
        or `meerschaum.connectors.api.APIConnector.APIConnector`.

    connector_keys: Optional[List[str]], default None
        The list of `connector_keys` to filter by.

    metric_keys: Optional[List[str]], default None
        The list of `metric_keys` to filter by.

    location_keys: Optional[List[str]], default None
        The list of `location_keys` to filter by.

    params: Optional[Dict[str, Any]], default None
        A dictionary of parameters to filter by.

    debug: bool
        Verbosity toggle.

    Returns
    -------
    A list of tuples of strings (or `None` for `location_key`)
    in the form `(connector_keys, metric_key, location_key)`.
    
    Examples
    --------
    >>> fetch_pipes_keys(metric_keys=['weather'])
    [('sql:main', 'weather', None)]
    """
    from meerschaum.utils.warnings import error
    from meerschaum.connectors.sql.tables import get_tables
    tables = get_tables(connector)

    def _registered(
            connector_keys: Optional[List[str]] = None,
            metric_keys: Optional[List[str]] = None,
            location_keys: Optional[List[str]] = None,
            tags: Optional[List[str]] = None,
            params: Optional[Dict[str, Any]] = None,
            debug: bool = False,
            **kw
        ) -> List[Tuple[str, str, str]]:
        """
        Get keys from the pipes table or the API directly.
        Builds query or URL based on provided keys and parameters.
        
        Only works for SQL and API Connectors.
        """
        if connector_keys is None:
            connector_keys = []
        if metric_keys is None:
            metric_keys = []
        if location_keys is None:
            location_keys = []
        if params is None:
            params = {}
        if tags is None:
            tags = []

        return connector.fetch_pipes_keys(
            connector_keys = connector_keys,
            metric_keys = metric_keys,
            location_keys = location_keys,
            tags = tags,
            params = params,
            debug = debug
        )

    def _explicit(
            connector_keys: Optional[List[str]] = None,
            metric_keys: Optional[List[str]] = None,
            location_keys: Optional[List[str]] = None,
            params: Optional[Dict[str, Any]] = None,
            debug: bool = False,
            **kw
        ) -> List[Tuple[str, str, str]]:
        """
        Explicitly build Pipes based on provided keys.
        Raises an error if `connector_keys` or `metric_keys` is empty,
        and assumes `location_keys = [None]` if empty.
        """

        if connector_keys is None:
            connector_keys = []
        if metric_keys is None:
            metric_keys = []
        if location_keys is None:
            location_keys = []
        if params is None:
            params = {}

        if not isinstance(connector_keys, list):
            connector_keys = [connector_keys]
        if not isinstance(metric_keys, list):
            metric_keys = [metric_keys]
        if not isinstance(location_keys, list):
            location_keys = [location_keys]

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
        **NOTE**: Not implemented!
        """
        error(
            "Need to implement metrics and locations logic in SQL and API.",
            NotImplementedError
        )

    _method_functions = {
        'registered' : _registered,
        'explicit'   : _explicit,
        'all'        : _all,
        ### TODO implement 'all'
    }
    if method not in _method_functions:
        error(f"Method '{method}' is not supported!", NotImplementedError)
    return _method_functions[method](**kw)
