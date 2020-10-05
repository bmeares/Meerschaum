#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Directly interact with the SQL server via Meerschaum actions
"""

exec_methods = {
    'read',
    'exec',
}

def sql(
        action : list = [''],
        debug=False,
        **kw
    ):
    """
    Execute a SQL query or launch an interactive CLI. All positional arguments are optional.
    
    Usage:
        `sql {label} {method} {query / table}`

    Options:
        - `sql {label}`
            Launch an interactive CLI. If {label} is omitted, use 'main'.

        - `sql {label} read [query / table]`
            Read a table or query as a pandas DataFrame and print the result
        
        - `sql {label} exec [query]`
            Execute a query and print the success status
    """

    ### input: `sql read` or `sql exec`
    if action[0] in exec_methods and len(action) == 1:
        print(sql.__doc__)
        return (False, 'Query must not be empty')

    from meerschaum.connectors import get_connector

    ### input: `sql`
    if action[0] == '':
        return get_connector().cli(debug=debug) 

    from meerschaum.config import config as cf, get_config
    from meerschaum.utils.debug import dprint

    method = None
    label = None
    query = action[-1]
    for a in action[:]:
        ### check if user specifies an exec method (read vs exec)
        if a in exec_methods:
            method = a
            continue
        ### check if user specifies a label
        if a in get_config('meerschaum', 'connectors', 'sql', patch=True):
            label = a
    if method is None: method = 'read'
    if label is None: label = 'main'

    if (conn := get_connector(type='sql', label=label, debug=debug)) is False:
        return False, (f"Could not create SQL connector '{label}'.\n"
            f"Verify meerschaum:connectors:sql:{label} is defined correctly with `show connectors`," + 
            " and update or add connectors with `edit config`."
        )

    if debug:
        dprint(action)
        dprint(conn)

    ### input: `sql main`
    if query == label:
        if debug: dprint(f"No query provided. Opening CLI on connector '{label}'")
        return conn.cli(debug=debug)

    ### guess the method from the structure of the query
    if 'select' in query.lower() or ' ' not in query:
        if method != 'read':
            print("Method changed to 'read'")
            method = 'read'
    else:
        if method != 'exec':
            print("Method changed to 'exec'")
            method = 'exec'

    result = getattr(conn, method)(query, debug=debug)
    if result is False:
        return (False, "Failed to execute query!")
    
    from meerschaum.utils.misc import attempt_import
    sqlalchemy_engine_result = attempt_import('sqlalchemy.engine.result')
    if isinstance(result, sqlalchemy_engine_result.ResultProxy):
        print("Success")
    else: print(result)

    return (True, "Success")
