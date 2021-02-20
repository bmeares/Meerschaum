#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Drop a Pipe's table but keep its registration
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any

def drop(
        self,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Call the Pipe's instance connector's drop_pipe method
    """
    return self.instance_connector.drop_pipe(self, debug=debug, **kw)
