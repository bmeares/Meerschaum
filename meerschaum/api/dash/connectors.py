#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting with Meerschaum connectors via the Web Interface.
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import WebState, Union
from meerschaum.connectors.parse import parse_instance_keys
from meerschaum.api import debug, get_api_connector

def get_web_connector(state: WebState) -> Union[
    mrsm.connectors.api.APIConnector,
    mrsm.connectors.sql.SQLConnector,
]:
    """
    Parse the web instance keys into a connector.
    """
    instance_keys = (
        str(get_api_connector())
        if not state.get('instance-select.value', None)
        else state['instance-select.value']
    )
    return parse_instance_keys(instance_keys, debug=debug)
