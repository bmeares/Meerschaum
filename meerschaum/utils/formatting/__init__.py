#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utilities for formatting output text
"""

from __future__ import annotations
import platform
import os
import sys
from meerschaum.utils.typing import Optional, Union, Any
from meerschaum.utils.formatting._shell import make_header
from meerschaum.utils.formatting._pprint import pprint
from meerschaum.utils.formatting._pipes import (
    pprint_pipes,
    highlight_pipes,
    format_pipe_success_tuple,
    print_pipes_results,
    extract_stats_from_message,
    pipe_repr,
)
from meerschaum.utils.threading import Lock, RLock

_attrs = {
    'ANSI': None,
    'UNICODE': None,
    'CHARSET': None,
}
__all__ = sorted([
    'ANSI', 'CHARSET', 'UNICODE',
    'colored',
    'translate_rich_to_termcolor',
    'get_console',
    'print_tuple',
    'print_options',
    'fill_ansi',
    'pprint',
    'highlight_pipes',
    'pprint_pipes',
    'make_header',
    'pipe_repr',
    'print_pipes_results',
    'extract_stats_from_message',
])
__pdoc__ = {}
_locks = {
    '_colorama_init': RLock(),
}


def colored_fallback(*args, **kw):
    return ' '.join(args)

def translate_rich_to_termcolor(*colors) -> tuple:
    """Translate between rich and more_termcolor terminology.
    This is probably prone to breaking.

    Parameters
    ----------
    *colors :
        

    Returns
    -------

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


def rich_text_to_str(text: 'rich.text.Text') -> str:
    """Convert a `rich.text.Text` object to a string with ANSI in-tact."""
    _console = get_console()
    if _console is None:
        return str(text)
    with console.capture() as cap:
        console.print(text)
    string = cap.get()
    return string[:-1]


def _init():
    """
    Initial color settings (mostly for Windows).
    """
    if platform.system() != "Windows":
        return
    if 'PYTHONIOENCODING' not in os.environ:
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    if 'PYTHONLEGACYWINDOWSSTDIO' not in os.environ:
        os.environ['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'
    sys.stdin.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

    from ctypes import windll
    k = windll.kernel32
    k.SetConsoleMode(k.GetStdHandle(-11), 7)
    os.system("color")

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
        colorama.init(autoreset=False)
        success = True
    except Exception as e:
        import traceback
        traceback.print_exc()
        _attrs['ANSI'], _attrs['UNICODE'], _attrs['CHARSET'] = False, False, 'ascii'
        success = False

    if more_termcolor is None:
        _attrs['ANSI'], _attrs['UNICODE'], _attrs['CHARSET'] = False, False, 'ascii'
        success = False

    return success

_colorama_init = False
def colored(text: str, *colors, as_rich_text: bool=False, **kw) -> Union[str, 'rich.text.Text']:
    """Apply colors and rich styles to a string.
    If a `style` keyword is provided, a `rich.text.Text` object will be parsed into a string.
    Otherwise attempt to use the legacy `more_termcolor.colored` method.

    Parameters
    ----------
    text: str
        The string to apply formatting to.
        
    *colors:
        A list of colors to pass to `more_termcolor.colored()`.
        Has no effect if `style` is provided.

    style: str, default None
        If provided, pass to `rich` for processing.

    as_rich_text: bool, default False
        If `True`, return a `rich.Text` object.
        `style` must be provided.
        
    **kw:
        Keyword arguments to pass to `rich.text.Text` or `more_termcolor`.
        

    Returns
    -------
    An ANSI-formatted string or a `rich.text.Text` object if `as_rich_text` is `True`.

    """
    from meerschaum.utils.packages import import_rich, attempt_import
    global _colorama_init
    _init()
    with _locks['_colorama_init']:
        if not _colorama_init:
            _colorama_init = _init()

    if 'style' in kw:
        rich = import_rich()
        rich_text = attempt_import('rich.text')
        text_obj = rich_text.Text(text, **kw)
        if as_rich_text:
            return text_obj
        return rich_text_to_str(text_obj)

    more_termcolor = attempt_import('more_termcolor', lazy=False)
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
    """Get the rich console."""
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
        tup: tuple,
        skip_common: bool = True,
        common_only: bool = False,
        upper_padding: int = 0,
        lower_padding: int = 0,
        calm: bool = False,
        _progress: Optional['rich.progress.Progress'] = None,
    ) -> None:
    """
    Print `meerschaum.utils.typing.SuccessTuple`.

    Parameters
    ----------
    skip_common: bool, default True
        If `True`, do not print common success tuples (i.e. `(True, "Success")`).

    common_only: bool, default False
        If `True`, only print if the success tuple is common.

    upper_padding: int, default 0
        How many newlines to prepend to the message.

    lower_padding: int, default 0
        How many newlines to append to the message.

    calm: bool, default False
        If `True`, use the default emoji and color scheme.
    """
    from meerschaum.config.static import STATIC_CONFIG
    _init()
    try:
        status = 'success' if tup[0] else 'failure'
    except TypeError:
        status = 'failure'
        tup = None, None

    if calm:
        status += '_calm'

    omit_messages = STATIC_CONFIG['system']['success']['ignore']

    do_print = True

    if common_only:
        skip_common = False
        do_print = tup[1] in omit_messages

    if skip_common:
        do_print = tup[1] not in omit_messages

    if do_print:
        ANSI, CHARSET = __getattr__('ANSI'), __getattr__('CHARSET')
        from meerschaum.config import get_config
        status_config = get_config('formatting', status, patch=True)

        msg = ' ' + status_config[CHARSET]['icon'] + ' ' + str(tup[1])
        lines = msg.split('\n')
        lines = [lines[0]] + [
            (('    ' + line if not line.startswith(' ') else line))
            for line in lines[1:]
        ]
        if ANSI:
            lines[0] = fill_ansi(highlight_pipes(lines[0]), **status_config['ansi']['rich'])
        msg = '\n'.join(lines)
        msg = ('\n' * upper_padding) + msg + ('\n' * lower_padding)
        print(msg)


def print_options(
        options: Optional[Dict[str, Any]] = None,
        nopretty: bool = False,
        no_rich: bool = False,
        name: str = 'options',
        header: Optional[str] = None,
        num_cols: Optional[int] = None,
        adjust_cols: bool = True,
        **kw
    ) -> None:
    """
    Print items in an iterable as a fancy table.

    Parameters
    ----------
    options: Optional[Dict[str, Any]], default None
        The iterable to be printed.

    nopretty: bool, default False
        If `True`, don't use fancy formatting.

    no_rich: bool, default False
        If `True`, don't use `rich` to format the output.

    name: str, default 'options'
        The text in the default header after `'Available'`.

    header: Optional[str], default None
        If provided, override `name` and use this as the header text.

    num_cols: Optional[int], default None
        How many columns in the table. Depends on the terminal size. If `None`, use 8.

    adjust_cols: bool, default True
        If `True`, adjust the number of columns depending on the terminal size.

    """
    import os
    from meerschaum.utils.packages import import_rich
    from meerschaum.utils.formatting import make_header, highlight_pipes
    from meerschaum.actions import actions as _actions
    from meerschaum.utils.misc import get_cols_lines, string_width, iterate_chunks


    if options is None:
        options = {}
    _options = []
    for o in options:
        _options.append(str(o))
    _header = f"Available {name}" if header is None else header

    if num_cols is None:
        num_cols = 8

    def _print_options_no_rich():
        if not nopretty:
            print()
            print(make_header(_header))
        ### print actions
        for option in sorted(_options):
            if not nopretty:
                print("  - ", end="")
            print(option)
        if not nopretty:
            print()

    rich = import_rich()
    if rich is None or nopretty or no_rich:
        _print_options_no_rich()
        return None

    ### Prevent too many options from being truncated on small terminals.
    if adjust_cols and _options:
        _cols, _lines = get_cols_lines()
        while num_cols > 1:
            cell_len = int(((_cols - 4) - (3 * (num_cols - 1))) / num_cols)
            num_too_big = sum([(1 if string_width(o) > cell_len else 0) for o in _options])
            if num_too_big > int(len(_options) / 3):
                num_cols -= 1
                continue
            break

    from meerschaum.utils.formatting import pprint, get_console
    from meerschaum.utils.packages import attempt_import
    rich_columns = attempt_import('rich.columns')
    rich_panel = attempt_import('rich.panel')
    rich_table = attempt_import('rich.table')
    Text = attempt_import('rich.text').Text
    box = attempt_import('rich.box')
    Panel = rich_panel.Panel
    Columns = rich_columns.Columns
    Table = rich_table.Table

    if _header is not None:
        table = Table(
            title = '\n' + _header,
            box = box.SIMPLE,
            show_header = False,
            show_footer = False,
            title_style = '',
            expand = True,
        )
    else:
        table = Table.grid(padding=(0, 2))
    for i in range(num_cols):
        table.add_column()

    chunks = iterate_chunks(
        [Text.from_ansi(highlight_pipes(o)) for o in sorted(_options)],
        num_cols,
        fillvalue=''
    )
    for c in chunks:
        table.add_row(*c)

    get_console().print(table)
    return None


def fill_ansi(string: str, style: str = '') -> str:
    """
    Fill in non-formatted segments of ANSI text.

    Parameters
    ----------
    string: str
        A string which contains ANSI escape codes.

    style: str
        Style arguments to pass to `rich.text.Text`.

    Returns
    -------
    A string with ANSI styling applied to the segments which don't yet have a style applied.
    """
    from meerschaum.utils.packages import import_rich, attempt_import
    from meerschaum.utils.misc import iterate_chunks
    rich = import_rich()
    Text = attempt_import('rich.text').Text
    try:
        msg = Text.from_ansi(string)
    except AttributeError as e:
        import traceback
        traceback.print_stack()
        msg = ''
    plain_indices = []
    for left_span, right_span in iterate_chunks(msg.spans, 2, fillvalue=len(msg)):
        left = left_span.end
        right = right_span.start if not isinstance(right_span, int) else right_span
        if left != right:
            plain_indices.append((left, right))
    if msg.spans:
        if msg.spans[0].start != 0:
            plain_indices = [(0, msg.spans[0].start)] + plain_indices
        if plain_indices and msg.spans[-1].end != len(msg) and plain_indices[-1][1] != len(msg):
            plain_indices.append((msg.spans[-1].end, len(msg)))

    if plain_indices:
        for left, right in plain_indices:
            msg.stylize(style, left, right)
    else:
        msg = Text(str(msg), style)

    return rich_text_to_str(msg)

def __getattr__(name: str) -> str:
    """
    Lazily load module-level variables.
    """
    if name.startswith('__') and name.endswith('__'):
        raise AttributeError("Cannot import dunders from this module.")

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
