#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import configuration modules
and if interactive, print the welcome message
"""

from meerschaum.config._version import __version__
from meerschaum.config._read_yaml import config

### if interactive shell, print welcome header
import sys
__doc__ = f"Meerschaum v{__version__}"
try:
    if sys.ps1:
        interactive = True
except AttributeError:
    interactive = False
if interactive:
    print(__doc__)

