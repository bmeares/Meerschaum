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
    Execute a SQL query and print the output.
    Usage: `sql {label} [query / table]`

    Options:
        - `sql read [query / table]`
            - Read a table or query as a pandas DataFrame
              and print the result
        - `sql exec [query]`
            - Execute a query and do not print
    """
    if action[0] == '':
        print(sql.__doc__)
        return (False, 'Query must not be empty')

    from meerschaum.connectors import get_connector
    from meerschaum.config import config as cf

    method = None
    label = None
    query = action[-1]
    for a in action[:-1]:
        if a in exec_methods:
            method = a
            continue
        if a in cf['meerschaum']['connectors']['sql']:
            label = a
    if method is None: method = 'read'
    if label is None: label = 'main'

    if action[0] in exec_methods and len(action) == 1:
        print(sql.__doc__)
        return (False, 'Query must not be empty')

    if (conn := get_connector(type='sql', label=label, debug=debug)) is False:
        return False, ("Could not create SQL connector '{label}'.\n"
            "Verify meerschaum:connectors:sql:{label} is defined with `edit config`")

    if debug:
        print(action)
        print(conn)

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
    print(result)
    return (True, "Success")
