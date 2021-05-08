#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Start subsystems (API server, logging daemon, etc.).
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional, List, Any

def start(
        action : Optional[List[str]] = None,
        **kw : Any,
    ) -> SuccessTuple:
    """
    Start subsystems (API server, logging daemon, etc.).
    """

    from meerschaum.utils.misc import choose_subaction
    options = {
        'api' : _start_api,
    }
    return choose_subaction(action, options, **kw)

def _start_api(action : Optional[List[str]] = None, **kw):
    """
    Start the API server.

    Usage:
        `start api {options}`

    Options:
        - `-p, --port {number}`
            Port to bind the API server to.

        - `-w, --workers {number}`
            How many worker threads to run.
            Defaults to the number of CPU cores or 1 on Android.
    """
    from meerschaum.actions import actions
    return actions['api'](action=['start'], **kw)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
start.__doc__ += _choices_docstring('start')
