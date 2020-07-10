#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""

"""

import yaml, sys, os
try:
    import importlib.resources as pkg_resources
except ImportError:
    import importlib_resources as pkg_resources

from meerschaum._version import __version__ as version

header = "Hello, World!"

try:
    if sys.ps1:
        interactive = True
except AttributeError:
    interactive = False

if interactive:
    print(header)
