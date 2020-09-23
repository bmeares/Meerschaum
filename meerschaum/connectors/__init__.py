#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch connectors with get_connector
"""

### store connectors partitioned by
### type, label for reuse
connectors = {
    'api' : dict(),
    'sql' : dict(),
}
### fill this with classes only on execution
### for lazy loading
types = dict()

def get_connector(
        type : str = "sql",
        label : str = "main",
        refresh : bool = False,
        debug : bool = False,
        **kw
    ):
    """
    Return existing connector or create new connection and store for reuse.

    type : str (default "sql")
        Connector type (sql, api, etc.)
    label : str (default "main")
        Connector label (e.g. main)
    refresh : bool (default False)
        Refresh the Connector instance
    kw : dict
        Other arguments to pass to the Connector constructor.

    You can create new connectors if enough parameters are provided for the given type and flavor.
    Example: flavor='sqlite', database='newdb'
    """
    global types, connectors

    if type not in connectors:
        print(f"Cannot create Connector of type '{type}'")
        return False        

    if len(types) == 0:
        from meerschaum.connectors.sql import SQLConnector
        from meerschaum.connectors.api._APIConnector import APIConnector
        types = {
            'api' : APIConnector,
            'sql' : SQLConnector,
        }
    ### determine if we need to call the constructor
    if not refresh:
        ### see if any user-supplied arguments differ from the existing instance
        if label in connectors[type]:
            warning_message = None
            for attribute, value in kw.items():
                if attribute not in connectors[type][label].__dict__:
                    warning_message = (
                        f"Received new attribute '{attribute}' not present in connector " +
                        f"{connectors[type][label]}.\n"
                    )
                elif connectors[type][label].__dict__[attribute] != value:
                    warning_message = (
                        f"Mismatched values for attribute '{attribute}' in connector '{connectors[type][label]}'.\n" +
                        f"  - Keyword value: '{value}'\n" +
                        f"  - Existing value: '{connectors[type][label].__dict__[attribute]}'\n"
                    )
            if warning_message is not None:
                from meerschaum.utils.warnings import warn
                warning_message += f"\nSetting `refresh` to True and recreating connector with type: '{type}' and label '{label}'"
                refresh = True
                warn(warning_message)
        else: ### connector doesn't yet exist
            refresh = True

    ### only create an object if refresh is True (can be manually specified, otherwise determined above)
    if refresh:
        try:
            conn = types[type](label=label, debug=debug, **kw)
        except Exception as e:
            print('Cannot build connector:\n', e)
            return False
        connectors[type][label] = conn

    return connectors[type][label]
