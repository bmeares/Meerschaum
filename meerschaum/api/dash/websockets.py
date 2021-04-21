#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for interacting via WebSockets.
"""

from meerschaum.config.static import _static_config

def ws_url_from_href(href : str):
    """
    Generate the websocket URL from the webpage href.
    """
    http_protocol = href.split('://')[0]
    ws_protocol = 'wss' if http_protocol == 'https' else 'ws'
    host_and_port = href.replace(http_protocol + '://', '').split('/')[0]
    return (
        ws_protocol + '://' +
        host_and_port +
        _static_config()['api']['endpoints']['websocket']
    )

