#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose init`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple, Union
from meerschaum.utils.warnings import info


def _compose_init(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Write a new compose file (`mrsm compose init`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('init', compose_config, debug=debug, **kw)
