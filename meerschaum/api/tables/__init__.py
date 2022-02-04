#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create tables using the API instance connector
"""

def get_tables(debug : bool = False, **kw):
    """Call the normal get_tables with the API instance connector instead

    Parameters
    ----------
    debug : bool :
         (Default value = False)
    **kw :
        

    Returns
    -------

    """
    from meerschaum.connectors.sql.tables import get_tables as _get_tables
    from meerschaum.api import get_api_connector
    ### get API instance connector
    conn = get_api_connector()
    return _get_tables(conn, debug=debug, **kw)

