#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Compress a Pipe's target table to reduce disk usage.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any


def compress(
    self,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `compress_pipe()` method.

    For TimescaleDB hypertables this enables and applies native compression.
    Other flavors fall back to their respective compression mechanisms where supported.

    Parameters
    ----------
    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    try:
        with Venv(get_connector_plugin(self.instance_connector)):
            if hasattr(self.instance_connector, 'compress_pipe'):
                result = self.instance_connector.compress_pipe(self, debug=debug, **kw)
            else:
                result = (
                    False,
                    (
                        "Cannot compress pipes for instance connectors of type "
                        f"'{self.instance_connector.type}'."
                    )
                )
    except NotImplementedError:
        result = (
            False,
            (
                "Compression is not implemented for instance connectors of type "
                f"'{self.instance_connector.type}'."
            )
        )

    self._clear_cache_key('_exists', debug=debug)
    return result


def decompress(
    self,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `decompress_pipe()` method, the inverse of `compress()`.

    For TimescaleDB hypertables this removes the compression policy and converts compressed
    chunks back to row-store. Other flavors fall back to their respective mechanisms where
    supported.

    Parameters
    ----------
    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    try:
        with Venv(get_connector_plugin(self.instance_connector)):
            if hasattr(self.instance_connector, 'decompress_pipe'):
                result = self.instance_connector.decompress_pipe(self, debug=debug, **kw)
            else:
                result = (
                    False,
                    (
                        "Cannot decompress pipes for instance connectors of type "
                        f"'{self.instance_connector.type}'."
                    )
                )
    except NotImplementedError:
        result = (
            False,
            (
                "Decompression is not implemented for instance connectors of type "
                f"'{self.instance_connector.type}'."
            )
        )

    self._clear_cache_key('_exists', debug=debug)
    return result
