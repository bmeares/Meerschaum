#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose logs`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple


def _compose_logs(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Show the project's logs (`mrsm compose logs`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('logs', compose_config, debug=debug, **kw)
