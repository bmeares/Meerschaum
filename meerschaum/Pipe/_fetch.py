#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for fetching new data into the Pipe
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Any

def fetch(
        self,
        begin : Optional[datetime.datetime] = None,
        end : Optional[datetime.datetime] = None,
        debug : bool = False,
        **kw : Any
    ) -> 'pd.DataFrame or None':
    """
    Fetch a Pipe's latest data from its connector.

    returns : pd.DataFrame of newest unseen data
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn
    if 'fetch' in dir(self.connector):
        if self.connector.type == 'plugin':
            from meerschaum.utils.packages import activate_venv, deactivate_venv
            activate_venv(self.connector.label, debug=debug)
        df = self.connector.fetch(
            self,
            begin = self.sync_time if begin is None else begin,
            end = end,
            debug = debug,
            **kw
        )
        if self.connector.type == 'plugin':
            deactivate_venv(self.connector.label, debug=debug)
        return df
    warn(f"No `fetch()` function defined for connector '{self.connector}'")
    return None
