#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create connectors with `meerschaum.connectors.get_connector()`.
For ease of use, you can also import from the root `meerschaum` module:
```
>>> from meerschaum import get_connector
>>> conn = get_connector()
```
"""

from meerschaum.connectors.Connector import Connector

### store connectors partitioned by
### type, label for reuse
connectors = {
    'api'    : dict(),
    'sql'    : dict(),
    'mqtt'   : dict(),
    'plugin' : dict(),
}
### fill this with classes only on execution
### for lazy loading
types = dict()

def get_connector(
        type : str = None,
        label : str = None,
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
    from meerschaum.config import get_config
    if type is None and label is None:
        from meerschaum.utils.misc import parse_instance_keys
        default_instance_keys = get_config('meerschaum', 'instance', patch=True)
        ### recursive call to get_connector
        return parse_instance_keys(default_instance_keys)

    ### NOTE: the default instance connector may not be main.
    ### Only fall back to 'main' if the type is provided by the label is omitted.
    if label is None: label = 'main'

    global types, connectors

    ### type might actually be a label. Check if so and raise a warning.
    if type not in connectors:
        possibilities, poss_msg = [], ""
        for _type in get_config('meerschaum', 'connectors'):
            if type in get_config('meerschaum', 'connectors', _type):
                possibilities.append(f"{_type}:{type}")
        if len(possibilities) > 0:
            poss_msg = " Did you mean"
            for poss in possibilities[:-1]:
                poss_msg += f" '{poss}',"
            if poss_msg.endswith(','): poss_msg = poss_msg[:-1]
            if len(possibilities) > 1: poss_msg += " or"
            poss_msg += f" '{possibilities[-1]}'?"

        from meerschaum.utils.warnings import warn
        warn(f"Cannot create Connector of type '{type}'." + poss_msg, stack=False)
        return None

    if len(types) == 0:
        from meerschaum.connectors.sql import SQLConnector
        from meerschaum.connectors.api import APIConnector
        from meerschaum.connectors.mqtt import MQTTConnector
        from meerschaum.connectors.plugin import PluginConnector
        types = {
            'api'    : APIConnector,
            'sql'    : SQLConnector,
            'mqtt'   : MQTTConnector,
            'plugin' : PluginConnector,
        }
    
    ### always refresh MQTT Connectors NOTE: test this!
    if type == 'mqtt': refresh = True

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
    from meerschaum.utils.warnings import error
    import traceback
    error_msg = None
    if refresh:
        ### will raise an error if configuration is incorrect / missing
        conn = types[type](label=label, debug=debug, **kw)
        connectors[type][label] = conn

    return connectors[type][label]
