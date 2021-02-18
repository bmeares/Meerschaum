#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utilities for formatting output text
"""

from meerschaum.utils.formatting._shell import make_header
from meerschaum.utils.formatting._pprint import pprint
from meerschaum.utils.formatting._pipes import pprint_pipes

_attrs = {
    'ANSI' : None,
    'UNICODE' : None,
    'CHARSET' : None,
}
__all__ = sorted([
    'ANSI', 'CHARSET', 'UNICODE',
    'colored',
    'translate_rich_to_termcolor',
    'get_console',
    'print_tuple',
])
__pdoc__ = {}


### I encountered a bug in git bash on Windows.
### This seems to resolve it; not sure if this is the best way.
import os
if 'PYTHONIOENCODING' not in os.environ:
    os.environ['PYTHONIOENCODING'] = 'utf-8'


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

def _init():
    global attrs
    from meerschaum.utils.packages import attempt_import
    ### init colorama for Windows color output
    colorama, more_termcolor = attempt_import(
        'colorama',
        'more_termcolor',
        lazy = False,
        warn = False,
        color = False,
    )
    try:
        colorama.init()
        success = True
    except:
        #  warn(f"Failed to initialize colorama. Ignoring...", stack=False)
        _attrs['ANSI'], _attrs['UNICODE'], _attrs['CHARSET'] = False, False, 'ascii'
        success = False

    if more_termcolor is None:
        #  warn(f"Failed to import more_termcolor. Ignoring color output...", stack=False)
        #  colored = colored_fallback
        _attrs['ANSI'], _attrs['UNICODE'], _attrs['CHARSET'] = False, False, 'ascii'
        success = False

    return success

_colorama_init = False
def colored(text : str, *colors, **kw) -> str:
    from meerschaum.utils.packages import attempt_import
    global _colorama_init
    _colorama_init = _init() if not _colorama_init else True
    more_termcolor = attempt_import('more_termcolor', install=False, lazy=False)
    try:
        colored_text = more_termcolor.colored(text, *colors, **kw)
    except Exception as e:
        colored_text = None

    if colored_text is not None:
        return colored_text

    try:
        _colors = translate_rich_to_termcolor(*colors)
        colored_text = more_termcolor.colored(text, *_colors, **kw)
    except:
        colored_text = None

    if colored_text is None:
        ### NOTE: warn here?
        return text

    return colored_text

console = None
def get_console():
    global console
    from meerschaum.utils.packages import import_rich
    if not __getattr__('ANSI') and not __getattr__('UNICODE'):
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

def print_tuple(tup : tuple, skip_common : bool = True, common_only : bool = False) -> None:
    """
    Print Meerschaum return tuple
    """
    from meerschaum.utils.formatting import ANSI, CHARSET, colored
    from meerschaum.config import get_config

    try:
        status = 'success' if tup[0] else 'failure'
    except TypeError:
        status = 'failure'
        tup = None, None

    status_config = get_config('formatting', status, patch=True)

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


def __getattr__(name : str) -> str:
    """
    Lazily load module-level variables
    """
    global attrs
    if name in _attrs:
        if _attrs[name] is not None:
            return _attrs[name]
        from meerschaum.config import get_config
        if name.lower() in get_config('formatting'):
            _attrs[name] = get_config('formatting', name.lower())
        elif name == 'CHARSET':
            _attrs[name] = 'unicode' if __getattr__('UNICODE') else 'ascii'
        return _attrs[name]
    
    if name == '__wrapped__':
        import sys
        return sys.modules[__name__]
    if name == '__all__':
        return __all__

    try:
        return globals()[name]
    except KeyError:
        raise AttributeError(f"Could not find '{name}'")

