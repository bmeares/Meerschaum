#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch Pipe data via the API connector
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Optional
import datetime

def fetch(
        self,
        pipe : meerschaum.Pipe,
        begin : str = 'now',
        end : Optional[datetime.datetime] = None,
        debug : bool = False,
        **kw : Any
    ) -> pandas.DataFrame:
    """
    Get the Pipe data from the remote Pipe
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error
    from meerschaum.config.static import _static_config

    if 'fetch' not in pipe.parameters:
        warn(f"Missing 'fetch' parameters for Pipe '{pipe}'.", stack=False)
        return None

    instructions = pipe.parameters['fetch']

    if 'connector_keys' not in instructions:
        warn(f"Missing connector_keys in fetch parameters for Pipe '{pipe}'", stack=False)
        return None
    remote_connector_keys = instructions.get('connector_keys', None)
    if 'metric_key' not in instructions:
        warn(f"Missing metric_key in fetch parameters for Pipe '{pipe}'", stack=False)
        return None
    remote_metric_key = instructions.get('metric_key', None)
    remote_location_key = instructions.get('location_key', None)

    from meerschaum import Pipe
    p = Pipe(
        remote_connector_keys,
        remote_metric_key,
        remote_location_key,
        mrsm_instance = self
    )
    return p.get_data(begin=begin, end=end, debug=debug)
