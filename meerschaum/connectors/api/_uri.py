#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create an APIConnector from a URI string.
"""

from meerschaum.utils.typing import Optional, Dict, Union


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
    from meerschaum import _core
    params = _core.api_uri_params(uri, label)
    return cls(**params) if not as_dict else params
