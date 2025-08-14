#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Reload the running Meerschaum instance.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple, List, Optional


def reload(
    action: Optional[List[str]] = None,
    debug: bool = False,
    _stop_daemons: bool = True,
    **kw: Any
) -> SuccessTuple:
    """
    Reload the running Meerschaum instance.
    """
    from meerschaum.utils.packages import reload_meerschaum
    from meerschaum.actions import actions

    if _stop_daemons:
        from meerschaum._internal.cli.workers import get_existing_cli_worker_indices
        indices = get_existing_cli_worker_indices()
        cli_action = 'restart' if indices else 'stop'

        stop_daemon_success, stop_daemon_msg = actions[cli_action](['daemons'], debug=debug, **kw)
        if not stop_daemon_success:
            return stop_daemon_success, stop_daemon_msg

    reload_success, reload_msg = reload_meerschaum(debug=debug)
    if not reload_success:
        return reload_success, reload_msg

    return True, "Success"
