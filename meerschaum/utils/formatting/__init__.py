#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utilities for formatting output text
"""

from meerschaum.utils.formatting._shell import make_header

from meerschaum.config import config as cf

#  ANSI = get_config('system', 'formatting', 'ansi', patch=True)
ANSI = cf['system']['formatting']['ansi']
#  UNICODE = get_config('system', 'formatting', 'unicode', patch=True)
UNICODE = cf['system']['formatting']['unicode']
CHARSET = 'unicode' if UNICODE else 'ascii'

from meerschaum.utils.misc import attempt_import

### init colorama for Windows color output
colorama, more_termcolor = attempt_import('colorama', 'more_termcolor')
try:
    colorama.init()
except:
    print(f"Failed to initialize colorama. Ignoring...")

def colored_fallback(*args, **kw):
    return ' '.join(args)

try:
    colored = more_termcolor.colored
except:
    print(f"Failed to import more_termcolor. Ignoring color output...")
    colored = colored_fallback


def print_tuple(tup : tuple):
    """
    Print Meerschaum return tuple
    """
    from meerschaum.utils.formatting import ANSI, CHARSET, colored
    from meerschaum.config import config as cf, get_config

    status = 'success' if tup[0] else 'failure'

    status_config = get_config('system', status, patch=True)

    msg = ' ' + status_config[CHARSET]['icon'] + ' ' + str(tup[1])
    if ANSI:
        msg = colored(msg, *status_config['ansi']['color'])

    print(msg)

