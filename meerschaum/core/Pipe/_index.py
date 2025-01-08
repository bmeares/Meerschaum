#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Index a pipe's table.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, Optional, List


def create_indices(
    self,
    columns: Optional[List[str]] = None,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `create_pipe_indices()` method.

    Parameters
    ----------
    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.

    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    _ = self.__dict__.pop('_columns_indices', None)
    _ = self.__dict__.pop('_columns_indices_timestamp', None)
    _ = self.__dict__.pop('_columns_types_timestamp', None)
    _ = self.__dict__.pop('_columns_types', None)

    if self.cache_pipe is not None:
        cache_success, cache_msg = self.cache_pipe.index(columns=columns, debug=debug, **kw)
        if not cache_success:
            warn(cache_msg)

    with Venv(get_connector_plugin(self.instance_connector)):
        if hasattr(self.instance_connector, 'create_pipe_indices'):
            result = self.instance_connector.create_pipe_indices(
                self,
                columns=columns,
                debug=debug,
                **kw
            )
        else:
            result = (
                False,
                (
                    "Cannot create indices for instance connectors of type "
                    f"'{self.instance_connector.type}'."
                )
            )

    _ = self.__dict__.pop('_columns_indices', None)
    _ = self.__dict__.pop('_columns_indices_timestamp', None)
    _ = self.__dict__.pop('_columns_types_timestamp', None)
    _ = self.__dict__.pop('_columns_types', None)

    return result
