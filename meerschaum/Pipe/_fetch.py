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
        sync_chunks : bool = False,
        deactivate_plugin_venv: bool = True,
        debug : bool = False,
        **kw : Any
    ) -> 'pd.DataFrame or None':
    """
    Fetch a Pipe's latest data from its connector.

    returns : pd.DataFrame of newest unseen data
    """
    if 'fetch' not in dir(self.connector):
        from meerschaum.utils.warnings import warn
        warn(f"No `fetch()` function defined for connector '{self.connector}'")
        return None

    from meerschaum.utils.debug import dprint
    if self.connector.type == 'plugin':
        from meerschaum.utils.packages import activate_venv, deactivate_venv
        activate_venv(self.connector.label, debug=debug)
    
    _chunk_hook = kw.pop('chunk_hook') if 'chunk_hook' in kw else None

    df = self.connector.fetch(
        self,
        begin = begin,
        end = end,
        chunk_hook = (
            self.sync if sync_chunks and _chunk_hook is None
            else _chunk_hook
        ),
        debug = debug,
        **kw
    )
    if self.connector.type == 'plugin' and deactivate_plugin_venv:
        deactivate_venv(self.connector.label, debug=debug)
    ### Return True if we're syncing in parallel, else continue as usual.
    if sync_chunks:
        return True
    return df
