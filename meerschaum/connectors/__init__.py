#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import Connector subclasses
"""

from meerschaum.connectors._Connector import Connector
from meerschaum.connectors.sql import SQLConnector
from meerschaum.connectors.api._APIConnector import APIConnector

### store connectors partitioned by
### type, label for reuse
connectors = {
    'api' : dict(),
    'sql' : dict(),
}
types = {
    'api' : APIConnector,
    'sql' : SQLConnector,
}

def get_connector(
        type : str = "sql",
        label : str = "main",
        debug : bool = False,
        **kw
    ):
    """
    Return existing connector or create new connection and store for reuse.

    type : str (default "sql")
        Connector type (sql, api, etc.)
    label : str (default "main")
        Connector label (e.g. main)

    You can create new connectors if enough parameters are provided for the given type and flavor.
    Example: flavor='sqlite', database='newdb'
    """
    if type not in connectors:
        print(f"Cannot create Connector of type {type}")
        return False        
    if label not in connectors[type]:
        try:
            conn = types[type](label=label, debug=debug, **kw)
        except Exception as e:
            print('Cannot build connector:', e)
            return False
        connectors[type][label] = conn

    return connectors[type][label]
