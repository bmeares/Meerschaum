#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Formatting functions for the interactive shell
"""

from re import sub
from meerschaum.utils.threading import Lock
_locks = {'_tried_clear_command': Lock()}


def make_header(
    message: str,
    ruler: str = '─',
) -> str:
    """Format a message string with a ruler.
    Length of the ruler is the length of the longest word.
    
    Example:
        'My\nheader' -> 'My\nheader\n──────'
    """

    from meerschaum.utils.formatting import ANSI, UNICODE, colored
    if not UNICODE:
        ruler = '-'
    words = message.split('\n')
    max_length = 0
    for w in words:
        length = len(w)
        if length > max_length:
            max_length = length

    s = message + "\n"
    for i in range(max_length):
        s += ruler
    return s


_tried_clear_command = None
def clear_screen(debug: bool = False) -> bool:
    """Clear the terminal window of all text. If ANSI is enabled,
    print the ANSI code for clearing. Otherwise, execute `clear` or `cls`.
    """
    import platform
    import os
    from meerschaum.utils.formatting import ANSI, get_console
    from meerschaum.utils.debug import dprint
    from meerschaum.config import get_config
    from meerschaum.utils.daemon import running_in_daemon
    global _tried_clear_command

    if running_in_daemon():
        return True

    if not get_config('shell', 'clear_screen'):
        return True

    print("", end="", flush=True)
    if debug:
        dprint("Skipping screen clear.")
        return True
    if ANSI and platform.system() != 'Windows':
        if get_console() is not None:
            get_console().clear()
            print("", end="", flush=True)
            return True
        clear_string = '\033[2J'
        reset_string = '\033[0m'
        print(clear_string + reset_string, end="")
        print("", end="", flush=True)
        return True
    ### ANSI support is disabled, try system level instead
    if _tried_clear_command is not None:
        return os.system(_tried_clear_command) == 0
    
    rc = -1
    command = 'clear' if platform.system() != 'Windows' else 'cls'
    try:
        rc = os.system(command)
    except Exception as e:
        pass
    if rc == 0:
        with _locks['_tried_clear_command']:
            _tried_clear_command = command
    return rc == 0


def flush_with_newlines(debug: bool = False) -> None:
    """Print newlines such that the entire terminal is cleared and new text will show up at the bottom."""
    import sys
    from meerschaum.utils.misc import get_cols_lines
    from meerschaum.utils.debug import dprint
    if debug:
        dprint("Skipping screen clear.")
        return
    cols, lines = get_cols_lines()
    sys.stderr.write('\n' * lines)


def progress(transient: bool = True, **kw):
    """
    Return the shell's `Progress` object.
    """
    from meerschaum.utils.packages import import_rich, attempt_import
    from meerschaum.utils.formatting import get_console, _init
    _init()
    _ = import_rich()
    rich_progress = attempt_import('rich.progress')
    return rich_progress.Progress(
        rich_progress.TextColumn(''),
        rich_progress.SpinnerColumn('clock'),
        rich_progress.TimeElapsedColumn(),
        rich_progress.TextColumn(''),
        rich_progress.BarColumn(bar_width=None),
        transient=transient,
        console=get_console(),
        **kw
    )

def live(**kw):
    """
    Return the shell's `Live` object.
    """
    from meerschaum.utils.packages import import_rich, attempt_import
    _ = import_rich()
    rich_live = attempt_import('rich.live')
    return rich_live.Live(**kw)
