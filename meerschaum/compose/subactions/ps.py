#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Show the jobs' states.
"""

from meerschaum.utils.typing import SuccessTuple, Optional, List, Dict, Any


def _compose_ps(
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
        ['show', 'jobs'] + (['--nopretty'] if nopretty else []),
        compose_config,
        capture_output=False,
        debug=debug,
        _replace=False,
    )
    return success, msg
