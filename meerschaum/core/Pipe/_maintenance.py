#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Run maintenance operations (vacuum, analyze) on a Pipe's target table.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any


def vacuum(
    self,
    full: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `vacuum_pipe()` method to reclaim disk space.

    For PostgreSQL-family tables this runs `VACUUM` (optionally `VACUUM FULL`); other flavors
    fall back to their respective space-reclaiming mechanisms where supported.

    Parameters
    ----------
    full: bool, default False
        If `True` (PostgreSQL family only), run `VACUUM FULL` to return freed space to the OS.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    try:
        with Venv(get_connector_plugin(self.instance_connector)):
            if hasattr(self.instance_connector, 'vacuum_pipe'):
                result = self.instance_connector.vacuum_pipe(self, full=full, debug=debug, **kw)
            else:
                result = (
                    False,
                    (
                        "Cannot vacuum pipes for instance connectors of type "
                        f"'{self.instance_connector.type}'."
                    )
                )
    except NotImplementedError:
        result = (
            False,
            (
                "Vacuuming is not implemented for instance connectors of type "
                f"'{self.instance_connector.type}'."
            )
        )

    self._clear_cache_key('_exists', debug=debug)
    return result


def analyze(
    self,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `analyze_pipe()` method to refresh planner statistics.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.
    """
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    try:
        with Venv(get_connector_plugin(self.instance_connector)):
            if hasattr(self.instance_connector, 'analyze_pipe'):
                result = self.instance_connector.analyze_pipe(self, debug=debug, **kw)
            else:
                result = (
                    False,
                    (
                        "Cannot analyze pipes for instance connectors of type "
                        f"'{self.instance_connector.type}'."
                    )
                )
    except NotImplementedError:
        result = (
            False,
            (
                "Analyzing is not implemented for instance connectors of type "
                f"'{self.instance_connector.type}'."
            )
        )

    return result
