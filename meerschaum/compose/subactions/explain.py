#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose explain`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple


def _compose_explain(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Lint and describe the compose file (`mrsm compose explain`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('explain', compose_config, debug=debug, **kw)
