#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Drop a Pipe's table but keep its registration
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any

def drop(
        self,
        debug: bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `drop_pipe()` method

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

    if self.cache_pipe is not None:
        _drop_cache_tuple = self.cache_pipe.drop(debug=debug, **kw)
        if not _drop_cache_tuple[0]:
            warn(_drop_cache_tuple[1])

    with Venv(get_connector_plugin(self.instance_connector)):
        return self.instance_connector.drop_pipe(self, debug=debug, **kw)
