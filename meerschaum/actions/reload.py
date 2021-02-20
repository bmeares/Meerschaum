#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Reload the running Meerschaum instance.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple, List

def reload(
        action : List[str] = [],
        **kw : Any
    ) -> SuccessTuple:
    """
    Reload the running Meerschaum instance.
    """
    from meerschaum.utils.packages import reload_package
    reload_package('meerschaum')
    return True, "Success"
