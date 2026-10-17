#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose run`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, SuccessTuple


def _compose_run(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Run a single pass of the compose file (`mrsm compose run`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('run', compose_config, debug=debug, **kw)
