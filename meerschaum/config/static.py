#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Alias import for the internal static configuration dictionary.
"""

from meerschaum._internal.static import SERVER_ID, STATIC_CONFIG

__all__ = ('STATIC_CONFIG',)


def _static_config():
    """
    Alias function for the global `STATIC_CONFIG` dictionary.
    """
    return STATIC_CONFIG
