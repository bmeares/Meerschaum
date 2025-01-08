#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Directly interact with the SQL server via Meerschaum actions
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional

exec_methods = {
    'read',
    'exec',
}


def sql(
    action: Optional[List[str]] = None,
    gui: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Execute a SQL query or launch an interactive CLI. All positional arguments are optional.

    Usage:
        `sql {label} {method} {query / table}`

    Options:
        - `sql {label}`
            Launch an interactive CLI. If {label} is omitted, use 'main'.

        - `sql {label} read [query / table]`
            Read a table or query as a pandas DataFrame and print the result.

        - `sql {label} exec [query]`
            Execute a query and print the success status.

    Examples:
        - `sql`
            Open an interactive CLI for `sql:main`.

        - `sql local`
            Open an interactive CLI for `sql:local`.

        - `sql table`
            Read from `table` on `sql:main`
              (translates to `SELECT * FROM table`).

        - `sql local table`
            Read from `table` on `sql:local`.

        - `sql local read table`
            Read from `table` on `sql:local`.

        - `sql "SELECT * FROM table WHERE id = 1"`
            Execute the above query on `sql:main` and print the results.

        - `sql local exec "INSERT INTO table (id) VALUES (1)"
            Execute the above query on `sql:local`.
    """
    from meerschaum.utils.dataframe import to_json

    if action is None:
        action = []

    ### input: `sql read` or `sql exec`
    if len(action) == 1 and action[0] in exec_methods:
        import textwrap
        print(textwrap.dedent(sql.__doc__))
        return (False, 'Query must not be empty.')

    from meerschaum.connectors import get_connector

    ### input: `sql`
    if len(action) == 0:
        return get_connector('sql').cli(debug=debug) 

    from meerschaum.config import get_config
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
    if method is None:
        method = 'read'
    if label is None:
        label = 'main'

    conn = get_connector(type='sql', label=label, debug=debug)
    if not conn:
        return False, (f"Could not create SQL connector '{label}'.\n"
            f"Verify meerschaum:connectors:sql:{label} is defined correctly with `show connectors`," + 
            " and update or add connectors with `edit config`."
        )

    if debug:
        dprint(action)
        dprint(conn)

    ### input: `sql main`
    if query == label:
        if debug:
            dprint(f"No query provided. Opening CLI on connector '{label}'")
        return conn.cli(debug=debug)

    ### guess the method from the structure of the query
    if 'select' in query.lower() or ' ' not in query:
        if method != 'read':
            method = 'read'
    else:
        if method != 'exec':
            method = 'exec'

    result = getattr(conn, method)(query, debug=debug)
    if result is False:
        return (False, f"Failed to execute query:\n\n{query}")
    
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import print_tuple, pprint
    _ = attempt_import('sqlalchemy.engine.result', lazy=False)
    if 'sqlalchemy' in str(type(result)):
        if not nopretty:
            print_tuple((True, f"Successfully executed query:\n\n{query}"))
    else:
        if not nopretty:
            pprint(result)
        else:
            print(
                to_json(
                    result,
                    orient='split',
                    index=False,
                )
            )

    return (True, "Success")


def _complete_sql(
    action: Optional[List[str]] = None, **kw: Any
) -> List[str]:
    from meerschaum.utils.misc import get_connector_labels
    _text = action[0] if action else ""
    return [
        label.split('sql:', maxsplit=1)[-1]
        for label in get_connector_labels('sql', search_term=_text, ignore_exact_match=True)
    ]
