#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Drop a Pipe's table but keep its registration
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, Optional, List


def drop(
    self,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `drop_pipe()` method.

    Parameters
    ----------
    debug: bool, default False:
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.

    """
    self._exists = False
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    if self.cache_pipe is not None:
        _drop_cache_tuple = self.cache_pipe.drop(debug=debug, **kw)
        if not _drop_cache_tuple[0]:
            warn(_drop_cache_tuple[1])

    with Venv(get_connector_plugin(self.instance_connector)):
        if hasattr(self.instance_connector, 'drop_pipe'):
            result = self.instance_connector.drop_pipe(self, debug=debug, **kw)
        else:
            result = (
                False,
                (
                    "Cannot drop pipes for instance connectors of type "
                    f"'{self.instance_connector.type}'."
                )
            )


    _ = self.__dict__.pop('_exists', None)
    _ = self.__dict__.pop('_exists_timestamp', None)

    return result


def drop_indices(
    self,
    columns: Optional[List[str]] = None,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `drop_indices()` method.

    Parameters
    ----------
    columns: Optional[List[str]] = None
        If provided, only drop indices in the given list.

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
        _drop_cache_tuple = self.cache_pipe.drop_indices(columns=columns, debug=debug, **kw)
        if not _drop_cache_tuple[0]:
            warn(_drop_cache_tuple[1])

    with Venv(get_connector_plugin(self.instance_connector)):
        if hasattr(self.instance_connector, 'drop_pipe_indices'):
            result = self.instance_connector.drop_pipe_indices(
                self,
                columns=columns,
                debug=debug,
                **kw
            )
        else:
            result = (
                False,
                (
                    "Cannot drop indices for instance connectors of type "
                    f"'{self.instance_connector.type}'."
                )
            )

    _ = self.__dict__.pop('_columns_indices', None)
    _ = self.__dict__.pop('_columns_indices_timestamp', None)
    _ = self.__dict__.pop('_columns_types_timestamp', None)
    _ = self.__dict__.pop('_columns_types', None)

    return result
