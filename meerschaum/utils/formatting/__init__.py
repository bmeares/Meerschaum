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


def print_tuple(tup : tuple, skip_common : bool = True, common_only : bool = False):
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

    omit_messages = { 'Success', 'Succeeded', 'success', '', None }

    do_print = True

    if common_only:
        skip_common = False
        do_print = tup[1] in omit_messages

    if skip_common:
        do_print = tup[1] not in omit_messages

    if do_print:
        print(msg)

