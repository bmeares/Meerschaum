#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utilities for formatting output text
"""

from meerschaum.utils.formatting._shell import make_header
from meerschaum.utils.formatting._pprint import pprint
from meerschaum.utils.formatting._pipes import pprint_pipes

from meerschaum.config import config as cf

#  ANSI = get_config('system', 'formatting', 'ansi', patch=True)
ANSI = cf['system']['formatting']['ansi']
#  UNICODE = get_config('system', 'formatting', 'unicode', patch=True)
UNICODE = cf['system']['formatting']['unicode']
CHARSET = 'unicode' if UNICODE else 'ascii'

def colored_fallback(*args, **kw):
    return ' '.join(args)

from meerschaum.utils.packages import attempt_import
from meerschaum.utils.warnings import warn

### init colorama for Windows color output
colorama, more_termcolor, rich_console, rich_pretty, rich_traceback = attempt_import(
    'colorama',
    'more_termcolor',
    'rich.console',
    'rich.pretty',
    'rich.traceback',
)

try:
    colorama.init()
except:
    warn(f"Failed to initialize colorama. Ignoring...", stack=False)
    ANSI, UNICODE, CHARSET = False, False, 'ascii'

try:
    colored = more_termcolor.colored
except:
    warn(f"Failed to import more_termcolor. Ignoring color output...", stack=False)
    colored = colored_fallback
    ANSI, UNICODE, CHARSET = False, False, 'ascii'

try:
    console = rich_console.Console()
    #  if ANSI:
        #  rich_pretty.install()
        #  rich_traceback.install(console=console, extra_lines=10)
except:
    warn(f"Failed to import rich. Ignoring color output...", stack=False)
    console = None
    ANSI, UNICODE, CHARSET = False, False, 'ascii'

def print_tuple(tup : tuple, skip_common : bool = True, common_only : bool = False):
    """
    Print Meerschaum return tuple
    """
    from meerschaum.utils.formatting import ANSI, CHARSET, colored
    from meerschaum.config import config as cf, get_config

    try:
        status = 'success' if tup[0] else 'failure'
    except TypeError:
        status = 'failure'
        tup = None, None

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

