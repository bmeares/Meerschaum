#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch Pipe data via the API connector
"""

from __future__ import annotations
from datetime import datetime
import copy
import meerschaum as mrsm
from meerschaum.utils.typing import Any, Optional, Dict, Iterator, Union

def fetch(
        self,
        pipe: mrsm.Pipe,
        begin: Union[datetime, str, int] = '',
        end: Union[datetime, int] = None,
        params: Optional[Dict, Any] = None,
        debug: bool = False,
        **kw: Any
    ) -> Iterator['pd.DataFrame']:
    """Get the Pipe data from the remote Pipe."""
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.config._patch import apply_patch_to_config

    fetch_params = pipe.parameters.get('fetch', {})
    if not fetch_params:
        warn(f"Missing 'fetch' parameters for {pipe}.", stack=False)
        return None

    pipe_meta = fetch_params.get('pipe', {})
    ### Legacy: check for `connector_keys`, etc. at the root.
    if not pipe_meta:
        ck, mk, lk = (
            fetch_params.get('connector_keys', None),
            fetch_params.get('metric_key', None),
            fetch_params.get('location_key', None),
        )
        if not ck or not mk:
            warn(f"Missing `fetch:pipe` keys for {pipe}.", stack=False)
            return None

        pipe_meta.update({
            'connector': ck,
            'metric': mk,
            'location': lk,
        })

    pipe_meta['instance'] = self
    source_pipe = mrsm.Pipe(**pipe_meta)

    _params = copy.deepcopy(params) if params is not None else {}
    _params = apply_patch_to_config(_params, fetch_params.get('params', {}))
    select_columns = fetch_params.get('select_columns', [])
    omit_columns = fetch_params.get('omit_columns', [])

    return source_pipe.get_data(
        select_columns = select_columns,
        omit_columns = omit_columns,
        begin = begin,
        end = end,
        params = _params,
        debug = debug,
        as_iterator = True,
    )
