#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utilities for formatting output text
"""

from meerschaum.utils.formatting._shell import make_header

from meerschaum.config import config as cf

ANSI = cf['system']['formatting']['ansi']
UNICODE = cf['system']['formatting']['unicode']
import sys
if 'win' in sys.platform:
    UNICODE = False

### init colorama for Windows color output
import colorama
colorama.init()
