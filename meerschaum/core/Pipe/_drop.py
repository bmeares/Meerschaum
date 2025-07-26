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
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    self._clear_cache_key('_exists', debug=debug)

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

    self._clear_cache_key('_exists', debug=debug)
    self._clear_cache_key('_exists_timestamp', debug=debug)

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
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    self._clear_cache_key('_columns_indices', debug=debug)
    self._clear_cache_key('_columns_indices_timestamp', debug=debug)
    self._clear_cache_key('_columns_types', debug=debug)
    self._clear_cache_key('_columns_types_timestamp', debug=debug)

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

    self._clear_cache_key('_columns_indices', debug=debug)
    self._clear_cache_key('_columns_indices_timestamp', debug=debug)
    self._clear_cache_key('_columns_types', debug=debug)
    self._clear_cache_key('_columns_types_timestamp', debug=debug)

    return result
