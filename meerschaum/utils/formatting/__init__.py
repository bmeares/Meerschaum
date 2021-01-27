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

### I encountered a bug in git bash on Windows.
### This seems to resolve it; not sure if this is the best way.
import os
if 'PYTHONIOENCODING' not in os.environ:
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from meerschaum.utils.packages import attempt_import
from meerschaum.utils.warnings import warn

### init colorama for Windows color output
colorama, more_termcolor = attempt_import(
    'colorama',
    'more_termcolor',
)

try:
    colorama.init()
except:
    warn(f"Failed to initialize colorama. Ignoring...", stack=False)
    ANSI, UNICODE, CHARSET = False, False, 'ascii'

def colored_fallback(*args, **kw):
    return ' '.join(args)

def translate_rich_to_termcolor(*colors) -> tuple:
    """
    Translate between rich and more_termcolor terminology.
    This is probably prone to breaking
    """
    _colors = []
    for c in colors:
        _c_list = []
        ### handle 'bright'
        c = c.replace('bright_', 'bright ')

        ### handle 'on'
        if ' on ' in c:
            _on = c.split(' on ')
            _colors.append(_on[0])
            for _c in _on[1:]:
                _c_list.append('on ' + _c)
        else:
            _c_list += [c]

        _colors += _c_list

    return tuple(_colors)

def colored(text : str, *colors, **kw):
    try:
        colored_text = more_termcolor.colored(text, *colors, **kw)
    except:
        colored_text = None

    if colored_text is not None: return colored_text

    try:
        _colors = translate_rich_to_termcolor(*colors)
        colored_text = more_termcolor.colored(text, *_colors, **kw)
    except:
        colored_text = None

    if colored_text is None:
        ### TODO warn here?
        return text

    return colored_text

if more_termcolor is None:
    warn(f"Failed to import more_termcolor. Ignoring color output...", stack=False)
    colored = colored_fallback
    ANSI, UNICODE, CHARSET = False, False, 'ascii'

console = None
def get_console():
    global console
    from meerschaum.utils.packages import import_rich
    if not ANSI and not UNICODE:
        return None
    rich = import_rich()
    try:
        console = rich.console.Console()
    except:
        console = None
    return console

#  try:
    #  console = rich_console.Console()
#  except:
    #  warn(f"Failed to import rich. Ignoring color output...", stack=False)
    #  console = None
    #  ANSI, UNICODE, CHARSET = False, False, 'ascii'

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
