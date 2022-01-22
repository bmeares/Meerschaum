#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Clear pipe data within a bounded or unbounded interval.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any

def clear(
        self,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `clear_pipe` method.
    """
    from meerschaum.utils.warnings import warn
    if self.cache_pipe is not None:
        success, msg = self.cache_pipe.clear(begin=begin, end=end, debug=debug, **kw)
        if not success:
            warn(msg)
    return self.instance_connector.clear_pipe(self, begin=begin, end=end, debug=debug, **kw)
