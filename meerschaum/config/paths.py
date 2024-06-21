#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
External API for importing Meerschaum paths.
"""

from meerschaum.config._paths import __getattr__, paths
__all__ = tuple(paths.keys())
