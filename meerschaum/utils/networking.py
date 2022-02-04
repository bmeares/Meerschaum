#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions pertaining to network functionality (e.g. available ports).
"""

def is_port_in_use(port: int):
    """Try to bind to a port and return whether it is available.

    Parameters
    ----------
    port: int :
        

    Returns
    -------

    """
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


def find_open_ports(start_port: int = 8000, end_port: int = 9001):
    """Check a range of ports and yield open ports.
    
    Found from StackExchange here:
    https://codereview.stackexchange.com/questions/116450/find-available-ports-on-localhost

    Parameters
    ----------
    start_port: int :
         (Default value = 8000)
    end_port: int :
         (Default value = 9001)

    Returns
    -------

    """
    for port in range(start_port, end_port):
        if not is_port_in_use(port):
            yield port

