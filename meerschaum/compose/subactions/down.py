#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose down`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, SuccessTuple
from meerschaum.utils.warnings import info
from meerschaum.utils.formatting import print_options
from meerschaum.utils.prompt import yes_no


def _compose_down(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Stop the project's jobs (`mrsm compose down`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('down', compose_config, debug=debug, **kw)
