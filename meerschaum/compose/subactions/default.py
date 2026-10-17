#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose default`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple
from meerschaum.utils.warnings import info


def _compose_default(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Pass every other subaction to the project's environment (`mrsm compose <action> ...`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('default', compose_config, debug=debug, **kw)
