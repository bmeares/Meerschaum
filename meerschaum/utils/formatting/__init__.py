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
    This is probably prone to breaking.
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

def rich_text_to_str(text) -> str:
    """
    Convert a `rich.text.Text` object to a string with ANSI in-tact.
    """
    _console = get_console()
    if _console is None:
        return str(text)
    with console.capture() as cap:
        console.print(text)
    return cap.get()[:-1]

#  def translate_termcolor_to_rich()

def _init():
    global _attrs
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
    except Exception as e:
        _attrs['ANSI'], _attrs['UNICODE'], _attrs['CHARSET'] = False, False, 'ascii'
        success = False

    if more_termcolor is None:
        _attrs['ANSI'], _attrs['UNICODE'], _attrs['CHARSET'] = False, False, 'ascii'
        success = False

    return success

_colorama_init = False
def colored(text : str, *colors, **kw) -> str:
    """
    Apply colors and rich styles to a string.
    If a `style` keyword is provided, a `rich.text.Text` object will be parsed into a string.
    Otherwise attempt to use the legacy `more_termcolor.colored` method.
    """
    from meerschaum.utils.packages import import_rich, attempt_import

    if 'style' in kw:
        rich = import_rich()
        rich_text = attempt_import('rich.text')
        return rich_text_to_str(rich_text.Text(text, **kw))

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
    except Exception as e:
        colored_text = None

    if colored_text is None:
        ### NOTE: warn here?
        return text

    return colored_text

console = None
def get_console():
    """
    Return a `rich` console object for reuse.
    """
    global console
    from meerschaum.utils.packages import import_rich, attempt_import
    rich = import_rich()
    rich_console = attempt_import('rich.console')
    try:
        console = rich_console.Console(force_terminal=True, color_system='truecolor')
    except Exception as e:
        console = None
    return console

def print_tuple(
        tup : tuple,
        skip_common : bool = True,
        common_only : bool = False,
        upper_padding : int = 0,
        lower_padding : int = 0,
    ) -> None:
    """
    Print Meerschaum SuccessTuple.
    """
    from meerschaum.config.static import _static_config
    try:
        status = 'success' if tup[0] else 'failure'
    except TypeError:
        status = 'failure'
        tup = None, None

    omit_messages = _static_config()['system']['success']['ignore']

    do_print = True

    if common_only:
        skip_common = False
        do_print = tup[1] in omit_messages

    if skip_common:
        do_print = tup[1] not in omit_messages

    if do_print:
        from meerschaum.utils.formatting import ANSI, CHARSET, colored
        from meerschaum.config import get_config
        status_config = get_config('formatting', status, patch=True)

        msg = ' ' + status_config[CHARSET]['icon'] + ' ' + str(tup[1])
        if ANSI:
            msg = colored(msg, **status_config['ansi']['rich'])

        for i in range(upper_padding):
            print()
        print(msg)
        for i in range(lower_padding):
            print()

def __getattr__(name : str) -> str:
    """
    Lazily load module-level variables
    """
    if name.startswith('__') and name.endswith('__'):
        raise AttributeError("Cannot import dunders from this module.")

    global _attrs
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
