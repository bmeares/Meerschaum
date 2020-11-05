#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch Pipe data via the API connector
"""

import datetime

def fetch(
        self,
        pipe : 'meerschaum.Pipe',
        begin : str = 'now',
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Get the Pipe data from the remote Pipe
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, error

    if 'fetch' not in pipe.parameters:
        warn(f"Missing 'fetch' parameters for Pipe '{pipe}'")
        return None

    instructions = pipe.parameters['fetch']

    if 'connector_keys' not in instructions:
        warn(f"Missing connector_keys in fetch parameters for Pipe '{pipe}'")
        return None
    remote_connector_keys = instructions['connector_keys']
    if 'metric_key' not in instructions:
        warn(f"Missing metric_key in fetch parameters for Pipe '{pipe}'")
        return None
    remote_metric_key = instructions['metric_key']
    if 'location_key' not in instructions: remote_location_key = None
    else: remote_location_key = instructions['location_key']

    from meerschaum import Pipe
    p = Pipe(
        remote_connector_keys,
        remote_metric_key,
        remote_location_key,
        mrsm_instance = self
    )
    return p.get_data(begin=begin, debug=debug)
