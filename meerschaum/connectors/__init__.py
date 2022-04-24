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

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple, Union, Optional, Sequence, Mapping
from meerschaum.utils.threading import Lock, RLock

from meerschaum.connectors.Connector import Connector
from meerschaum.connectors.sql.SQLConnector import SQLConnector
from meerschaum.connectors.api.APIConnector import APIConnector
from meerschaum.connectors.sql._create_engine import flavor_configs as sql_flavor_configs

__all__ = ("Connector", "SQLConnector", "APIConnector", "get_connector", "is_connected")

### store connectors partitioned by
### type, label for reuse
connectors = {
    'api'    : {},
    'sql'    : {},
    'mqtt'   : {},
    'plugin' : {},
}
_locks = {
    'connectors': RLock(),
    'types': RLock(),
}
attributes = {
    'api' : {
        'required' : [
            'host',
            'username',
            'password'
        ],
        'default' : {
            #  'username' : 'mrsm',
            #  'password' : 'mrsm',
            'protocol' : 'http',
            'port'     : 8000,
        },
    },
    'sql' : {
        'flavors' : sql_flavor_configs,
    },
    'mqtt' : {
        'required' : [
            'host',
        ],
        'default' : {
            'port'     : 1883,
            'keepalive': 60,
        },
    },
}
### Fill this with objects only when connectors are first referenced.
types = {}

def get_connector(
        type: str = None,
        label: str = None,
        refresh: bool = False,
        debug: bool = False,
        **kw: Any
    ):
    """
    Return existing connector or create new connection and store for reuse.
    
    You can create new connectors if enough parameters are provided for the given type and flavor.
    

    Parameters
    ----------
    type: Optional[str], default None
        Connector type (sql, api, etc.).
        Defaults to the type of the configured `instance_connector`.

    label: Optional[str], default None
        Connector label (e.g. main). Defaults to `'main'`.

    refresh: bool, default False
        Refresh the Connector instance / construct new object. Defaults to `False`.

    kw: Any
        Other arguments to pass to the Connector constructor.
        If the Connector has already been constructed and new arguments are provided,
        `refresh` is set to `True` and the old Connector is replaced.

    Returns
    -------
    A new Meerschaum connector (e.g. `meerschaum.connectors.api.APIConnector`,
    `meerschaum.connectors.sql.SQLConnector`).
    
    Examples
    --------
    The following parameters would create a new
    `meerschaum.connectors.sql.SQLConnector` that isn't in the configuration file.

    ```
    >>> conn = get_connector(
    ...     type = 'sql',
    ...     label = 'newlabel',
    ...     flavor = 'sqlite',
    ...     database = '/file/path/to/database.db'
    ... )
    >>>
    ```

    """
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.config import get_config
    from meerschaum.config.static import _static_config
    if type is None and label is None:
        default_instance_keys = get_config('meerschaum', 'instance', patch=True)
        ### recursive call to get_connector
        return parse_instance_keys(default_instance_keys)

    ### NOTE: the default instance connector may not be main.
    ### Only fall back to 'main' if the type is provided by the label is omitted.
    label = label if label is not None else _static_config()['connectors']['default_label']

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
            if poss_msg.endswith(','):
                poss_msg = poss_msg[:-1]
            if len(possibilities) > 1:
                poss_msg += " or"
            poss_msg += f" '{possibilities[-1]}'?"

        from meerschaum.utils.warnings import warn
        warn(f"Cannot create Connector of type '{type}'." + poss_msg, stack=False)
        return None

    if len(types) == 0:
        from meerschaum.connectors.sql import SQLConnector
        from meerschaum.connectors.api import APIConnector
        from meerschaum.connectors.mqtt import MQTTConnector
        from meerschaum.connectors.plugin import PluginConnector
        from meerschaum.utils.warnings import warn
        _locks['types'].acquire()
        types = {
            'api'    : APIConnector,
            'sql'    : SQLConnector,
            'mqtt'   : MQTTConnector,
            'plugin' : PluginConnector,
        }
        _locks['types'].release()
    
    ### always refresh MQTT Connectors NOTE: test this!
    if type == 'mqtt':
        refresh = True

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
                        f"Mismatched values for attribute '{attribute}' in connector "
                        + f"'{connectors[type][label]}'.\n" +
                        f"  - Keyword value: '{value}'\n" +
                        f"  - Existing value: '{connectors[type][label].__dict__[attribute]}'\n"
                    )
            if warning_message is not None:
                warning_message += (
                    "\nSetting `refresh` to True and recreating connector with type:"
                    + f" '{type}' and label '{label}'."
                )
                refresh = True
                warn(warning_message)
        else: ### connector doesn't yet exist
            refresh = True

    ### only create an object if refresh is True (can be manually specified, otherwise determined above)
    from meerschaum.utils.warnings import error, warn
    import traceback
    error_msg = None
    if refresh:
        _locks['connectors'].acquire()
        try:
            ### will raise an error if configuration is incorrect / missing
            conn = types[type](label=label, debug=debug, **kw)
            connectors[type][label] = conn
        except Exception as e:
            warn(e, stack=False)
            conn = None
        finally:
            _locks['connectors'].release()
        if conn is None:
            return None

    return connectors[type][label]

def is_connected(keys: str, **kw) -> bool:
    """
    Check if the connector keys correspond to an active connection.
    If the connector has not been created, it will immediately return `False`.
    If the connector exists but cannot communicate with the source, return `False`.
    
    **NOTE:** Only works with instance connectors (`SQLConnectors` and `APIConnectors`).
    Keyword arguments are passed to `meerschaum.utils.misc.retry_connect`.

    Parameters
    ----------
    keys:
        The keys to the connector (e.g. `'sql:main'`).
        
    Returns
    -------
    A `bool` corresponding to whether a successful connection may be made.

    """
    import warnings
    from meerschaum.utils.warnings import error, warn
    if ':' not in keys:
        warn(f"Invalid connector keys '{keys}'")

    try:
        typ, label = keys.split(':')
    except Exception as e:
        return False
    if typ not in ('sql', 'api'):
        return False
    if not (label in connectors.get(typ, {})):
        return False

    from meerschaum.connectors.parse import parse_instance_keys
    conn = parse_instance_keys(keys)
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            return conn.test_connection(**kw)
    except Exception as e:
        return False
