#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose ps`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple


def _compose_ps(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the project's jobs (`mrsm compose ps`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('ps', compose_config, debug=debug, **kw)
