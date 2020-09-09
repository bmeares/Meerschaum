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
    Usage: `sql [query / table]`

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

    if debug:
        print(action)

    import meerschaum.connectors.sql
    conn = meerschaum.connectors.sql.SQLConnector()

    ### guess the method from the actions list
    ### (default to 'read' if not provided)
    if action[0] in exec_methods:
        method = action[0]
        try:
            query = action[1]
        except IndexError:
            print(sql.__doc__)
            return (False, 'Query must not be empty')
    else:
        method = 'read'
        query = action[0]

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
