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
        **kw: Any
    ) -> SuccessTuple:
    """
    Reload the running Meerschaum instance.
    """
    from meerschaum.utils.packages import reload_meerschaum
    return reload_meerschaum(debug=debug)
