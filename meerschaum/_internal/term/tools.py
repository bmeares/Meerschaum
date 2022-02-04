#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions regarding the webterm.
"""

from __future__ import annotations

def is_webterm_running(host: str, port: int) -> int:
    """Determine whether the webterm service is running on a given host and port.

    Parameters
    ----------
    host: str :
        
    port: int :
        

    Returns
    -------

    """
    from meerschaum.utils.networking import find_open_ports, is_port_in_use
    from meerschaum.utils.packages import attempt_import
    requests = attempt_import('requests')
    url = f'http://{host}:{port}'
    try:
        r = requests.get(url)
    except Exception as e:
        return False
    if not r:
        return False
    return '<title>Meerschaum Shell</title>' in r.text
