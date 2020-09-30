#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register a Pipe object
"""

def register(
        self,
        api_connector : 'APIConnector' = None,
        debug : bool = False
    ):
    """
    Send a POST to the Meerschaum API to register a new Pipe
    """
    if api_connector is None:
        from meerschaum import get_connector
        api_connector = get_connector(type='api')
    return api_connector.register_pipe(self, debug=debug)
    
