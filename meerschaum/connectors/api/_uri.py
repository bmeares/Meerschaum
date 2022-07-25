#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create an APIConnector from a URI string.
"""

from meerschaum.utils.typing import Optional, Dict, Any, Union
from meerschaum.utils.warnings import warn, error

@classmethod
def from_uri(
        cls,
        uri: str,
        label: Optional[str] = None,
        as_dict: bool = False,
    ) -> Union[
        'meerschaum.connectors.APIConnector',
        Dict[str, Union[str, int]],
    ]:
    """
    Create a new APIConnector from a URI string.

    Parameters
    ----------
    uri: str
        The URI connection string.

    label: Optional[str], default None
        If provided, use this as the connector label.
        Otherwise use the determined database name.

    as_dict: bool, default False
        If `True`, return a dictionary of the keyword arguments
        necessary to create a new `APIConnector`, otherwise create a new object.

    Returns
    -------
    A new APIConnector object or a dictionary of attributes (if `as_dict` is `True`).
    """
    from meerschaum.connectors.sql import SQLConnector
    params = SQLConnector.parse_uri(uri)
    if 'host' not in params:
        error("No host was found in the provided URI.")
    params['protocol'] = params.pop('flavor')
    params['label'] = label or (
        (
            (params['username'] + '@' if 'username' in params else '')
            + params['host']
        ).lower()
    )

    return cls(**params) if not as_dict else params
