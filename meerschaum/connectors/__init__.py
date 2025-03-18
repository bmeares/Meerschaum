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

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Union, List, Dict
from meerschaum.utils.threading import RLock
from meerschaum.utils.warnings import warn

from meerschaum.connectors._Connector import Connector, InvalidAttributesError
from meerschaum.connectors.sql._SQLConnector import SQLConnector
from meerschaum.connectors.api._APIConnector import APIConnector
from meerschaum.connectors.sql._create_engine import flavor_configs as sql_flavor_configs

__all__ = (
    "make_connector",
    "Connector",
    "SQLConnector",
    "APIConnector",
    "get_connector",
    "is_connected",
    "poll",
    "api",
    "sql",
    "valkey",
    "parse",
)

### store connectors partitioned by
### type, label for reuse
connectors: Dict[str, Dict[str, Connector]] = {
    'api'    : {},
    'sql'    : {},
    'plugin' : {},
    'valkey' : {},
}
instance_types: List[str] = ['sql', 'api']
_locks: Dict[str, RLock] = {
    'connectors'               : RLock(),
    'types'                    : RLock(),
    'custom_types'             : RLock(),
    '_loaded_plugin_connectors': RLock(),
    'instance_types'           : RLock(),
}
attributes: Dict[str, Dict[str, Any]] = {
    'api': {
        'required': [
            'host',
            'username',
            'password',
        ],
        'optional': [
            'port',
        ],
        'default': {
            'protocol': 'http',
        },
    },
    'sql': {
        'flavors': sql_flavor_configs,
    },
}
### Fill this with objects only when connectors are first referenced.
types: Dict[str, Any] = {}
custom_types: set = set()
_loaded_plugin_connectors: bool = False


def get_connector(
    type: str = None,
    label: str = None,
    refresh: bool = False,
    debug: bool = False,
    **kw: Any
) -> Connector:
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
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.warnings import warn
    global _loaded_plugin_connectors
    if isinstance(type, str) and not label and ':' in type:
        type, label = type.split(':', maxsplit=1)

    with _locks['_loaded_plugin_connectors']:
        if not _loaded_plugin_connectors:
            load_plugin_connectors()
            _load_builtin_custom_connectors()
            _loaded_plugin_connectors = True

    if type is None and label is None:
        default_instance_keys = get_config('meerschaum', 'instance', patch=True)
        ### recursive call to get_connector
        return parse_instance_keys(default_instance_keys)

    ### NOTE: the default instance connector may not be main.
    ### Only fall back to 'main' if the type is provided by the label is omitted.
    label = label if label is not None else STATIC_CONFIG['connectors']['default_label']

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

        warn(f"Cannot create Connector of type '{type}'." + poss_msg, stack=False)
        return None

    if 'sql' not in types:
        from meerschaum.connectors.plugin import PluginConnector
        from meerschaum.connectors.valkey import ValkeyConnector
        with _locks['types']:
            types.update({
                'api': APIConnector,
                'sql': SQLConnector,
                'plugin': PluginConnector,
                'valkey': ValkeyConnector,
            })

    ### determine if we need to call the constructor
    if not refresh:
        ### see if any user-supplied arguments differ from the existing instance
        if label in connectors[type]:
            warning_message = None
            for attribute, value in kw.items():
                if attribute not in connectors[type][label].meta:
                    import inspect
                    cls = connectors[type][label].__class__
                    cls_init_signature = inspect.signature(cls)
                    cls_init_params = cls_init_signature.parameters
                    if attribute not in cls_init_params:
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

    ### only create an object if refresh is True
    ### (can be manually specified, otherwise determined above)
    if refresh:
        with _locks['connectors']:
            try:
                ### will raise an error if configuration is incorrect / missing
                conn = types[type](label=label, **kw)
                connectors[type][label] = conn
            except InvalidAttributesError as ie:
                warn(
                    f"Incorrect attributes for connector '{type}:{label}'.\n"
                    + str(ie),
                    stack = False,
                )
                conn = None
            except Exception as e:
                from meerschaum.utils.formatting import get_console
                console = get_console()
                if console:
                    console.print_exception()
                warn(
                    f"Exception when creating connector '{type}:{label}'.\n" + str(e),
                    stack = False,
                )
                conn = None
        if conn is None:
            return None

    return connectors[type][label]


def is_connected(keys: str, **kw) -> bool:
    """
    Check if the connector keys correspond to an active connection.
    If the connector has not been created, it will immediately return `False`.
    If the connector exists but cannot communicate with the source, return `False`.
    
    **NOTE:** Only works with instance connectors (`SQLConnectors` and `APIConnectors`).
    Keyword arguments are passed to `meerschaum.connectors.poll.retry_connect`.

    Parameters
    ----------
    keys:
        The keys to the connector (e.g. `'sql:main'`).
        
    Returns
    -------
    A `bool` corresponding to whether a successful connection may be made.

    """
    import warnings
    if ':' not in keys:
        warn(f"Invalid connector keys '{keys}'")

    try:
        typ, label = keys.split(':')
    except Exception:
        return False
    if typ not in instance_types:
        return False
    if label not in connectors.get(typ, {}):
        return False

    from meerschaum.connectors.parse import parse_instance_keys
    conn = parse_instance_keys(keys)
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore')
            return conn.test_connection(**kw)
    except Exception:
        return False


def make_connector(cls, _is_executor: bool = False):
    """
    Register a class as a `Connector`.
    The `type` will be the lower case of the class name, without the suffix `connector`.

    Parameters
    ----------
    instance: bool, default False
        If `True`, make this connector type an instance connector.
        This requires implementing the various pipes functions and lots of testing.

    Examples
    --------
    >>> import meerschaum as mrsm
    >>> from meerschaum.connectors import make_connector, Connector
    >>> 
    >>> @make_connector
    >>> class FooConnector(Connector):
    ...     REQUIRED_ATTRIBUTES: list[str] = ['username', 'password']
    ... 
    >>> conn = mrsm.get_connector('foo:bar', username='dog', password='cat')
    >>> print(conn.username, conn.password)
    dog cat
    >>> 
    """
    import re
    suffix_regex = (
        r'connector$'
        if not _is_executor
        else r'executor$'
    )
    typ = re.sub(suffix_regex, '', cls.__name__.lower())
    with _locks['types']:
        types[typ] = cls
    with _locks['custom_types']:
        custom_types.add(typ)
    with _locks['connectors']:
        if typ not in connectors:
            connectors[typ] = {}
    if getattr(cls, 'IS_INSTANCE', False):
        with _locks['instance_types']:
            if typ not in instance_types:
                instance_types.append(typ)

    return cls


def load_plugin_connectors():
    """
    If a plugin makes use of the `make_connector` decorator,
    load its module.
    """
    from meerschaum.plugins import get_plugins, import_plugins
    to_import = []
    for plugin in get_plugins():
        if plugin is None:
            continue
        with open(plugin.__file__, encoding='utf-8') as f:
            text = f.read()
        if 'make_connector' in text or 'Connector' in text:
            to_import.append(plugin.name)
    if not to_import:
        return
    import_plugins(*to_import)


def get_connector_plugin(
    connector: Connector,
) -> Union[str, None, mrsm.Plugin]:
    """
    Determine the plugin for a connector.
    This is useful for handling virtual environments for custom instance connectors.

    Parameters
    ----------
    connector: Connector
        The connector which may require a virtual environment.

    Returns
    -------
    A Plugin, 'mrsm', or None.
    """
    if not hasattr(connector, 'type'):
        return None
    plugin_name = (
        connector.__module__.replace('plugins.', '').split('.')[0]
        if connector.type in custom_types else (
            connector.label
            if connector.type == 'plugin'
            else 'mrsm'
        )
    )
    plugin = mrsm.Plugin(plugin_name)
    return plugin if plugin.is_installed() else None


def _load_builtin_custom_connectors():
    """
    Import custom connectors decorated with `@make_connector` or `@make_executor`.
    """
    import meerschaum.jobs.systemd
    import meerschaum.connectors.valkey
