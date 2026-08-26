#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pass all other subactions to `mrsm stack`.
"""

from meerschaum.utils.typing import SuccessTuple, Optional, List, Dict, Any


def _compose_logs(
    compose_config: Dict[str, Any],
    action: Optional[List[str]] = None,
    sysargs: Optional[List[str]] = None,
    nopretty: bool = False,
    debug: bool = False,
    **kw,
) -> SuccessTuple:
    """
    Execute Meerschaum actions in the isolated environment.
    """
    from meerschaum.compose.utils import run_mrsm_command

    success, msg = run_mrsm_command(
        ['show', 'logs'] + (['--nopretty'] if nopretty else []),
        compose_config,
        capture_output = False,
        debug = debug,
    )
    return success, msg
