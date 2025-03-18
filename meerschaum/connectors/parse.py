#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for parsing connector keys.
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Union, Optional, Dict, Tuple


def parse_connector_keys(
    keys: str,
    construct: bool = True,
    as_tuple: bool = False,
    **kw:  Any
) -> (
    Union[
        mrsm.connectors.Connector,
        Dict[str, Any],
        Tuple[
            Union[
                mrsm.connectors.Connector,
                Dict[str, Any],
                None,
            ],
            str
        ],
        None
    ]
):
    """
    Convenience function for parsing connector keys and returning Connector objects.

    Parameters
    ----------
    keys: str
        Keys are split by a colon (`':'`) into type and label. If the label is omitted,
        (e.g. 'sql'), pass it along to `meerschaum.connectors.get_connector` to parse.

    construct: bool, default True
        If True, return a Connector. Otherwise return the configuration dictionary for a Connector.
        **NOTE:** This may include passwords, so be careful.

    as_tuple: bool, default False
        If True, return a tuple of (conn, keys). `conn` may be a dict or connector.

    Returns
    -------
    A connector or dictionary of attributes. If `as_tuple`, also return the connector's keys.

    """
    import copy
    from meerschaum.connectors import get_connector
    from meerschaum.config import get_config
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum.utils.warnings import error

    ### `get_connector()` handles the logic for falling back to 'main',
    ### so don't make any decisions here.
    vals = str(keys).split(':')
    _type = vals[0]
    _label = vals[1] if len(vals) > 1 else STATIC_CONFIG['connectors']['default_label']
    _get_connector_kw = {'type': _type, 'label': _label}
    _get_connector_kw.update(kw)

    if construct:
        conn = get_connector(**_get_connector_kw)
        if conn is None:
            error(f"Unable to parse connector keys '{keys}'", stack=False)
    else:
        connectors_config = get_config('meerschaum', 'connectors', patch=True)
        ### invalid type
        if vals[0] not in connectors_config:
            return None
        type_config = get_config('meerschaum', 'connectors', _type)

        default_config = copy.deepcopy(type_config.get('default', {}))
        conn = type_config.get(_label, None)
        if default_config is not None and conn is not None:
            default_config.update(conn)
            conn = default_config

    if as_tuple:
        return conn, _type + ':' + _label
    return conn


def parse_instance_keys(
    keys: Optional[str],
    construct: bool = True,
    as_tuple: bool = False,
    **kw
):
    """
    Parse the Meerschaum instance value into a Connector object.
    """
    from meerschaum.config import get_config

    if keys is None:
        keys = get_config('meerschaum', 'instance')
    keys = str(keys)

    return parse_connector_keys(keys, construct=construct, as_tuple=as_tuple, **kw)


def parse_repo_keys(keys: Optional[str] = None, **kw):
    """Parse the Meerschaum repository value into an APIConnector."""
    from meerschaum.config import get_config
    if keys is None:
        keys = get_config('meerschaum', 'default_repository', patch=True)
    keys = str(keys)
    if ':' not in keys:
        keys = 'api:' + keys

    return parse_connector_keys(keys, **kw)


def parse_executor_keys(keys: Optional[str] = None, **kw):
    """Parse the executor keys into an APIConnector or string."""
    from meerschaum.jobs import get_executor_keys_from_context
    if keys is None:
        keys = get_executor_keys_from_context()

    if keys is None or keys == 'local':
        return 'local'

    keys = str(keys)
    return parse_connector_keys(keys, **kw)


def is_valid_connector_keys(
    keys: str
) -> bool:
    """
    Verify a connector_keys string references a valid connector.
    """
    try:
        success = parse_connector_keys(keys, construct=False) is not None
    except Exception:
        success = False
    return success
