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
if sys.platform == 'win32':
    UNICODE = False
CHARSET = 'unicode' if UNICODE else 'ascii'

from meerschaum.utils.misc import attempt_import

### init colorama for Windows color output
colorama, more_termcolor = attempt_import('colorama', 'more_termcolor')
try:
    colorama.init()
except:
    print(f"Failed to initialize colorama. Ignoring...")

def colored_fallback(*args, **kw):
    return args[0]

try:
    colored = more_termcolor.colored
except:
    print(f"Failed to import more_termcolor. Ignoring color output...")
    colored = colored_fallback
