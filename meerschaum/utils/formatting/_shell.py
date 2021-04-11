#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Formatting functions for the interactive shell
"""

def make_header(
        message : str,
        ruler : str = '─',
    ) -> str:
    """
    Format a message string with a ruler.
    Length of the ruler is the length of the longest word

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

def clear_screen(debug : bool = False) -> bool:
    """
    Clear the terminal window of all text. If ANSI is enabled,
    print the ANSI code for clearing. Otherwise, execute `clear` or `cls`.
    """
    from meerschaum.utils.formatting import ANSI, get_console
    from meerschaum.utils.debug import dprint
    print("", end="", flush=True)
    if debug:
        dprint("Skipping screen clear.")
        return True
    if ANSI:
        if get_console() is not None:
            get_console().clear()
            print("", end="", flush=True)
            return True
        clear_string, reset_string = '\033c', '\033[0m'
        print(clear_string + reset_string, end="")
        print("", end="", flush=True)
        return True
    ### ANSI support is disabled, try system level instead
    import platform, subprocess
    command = 'clear' if platform.system() != "Windows" else "cls"
    rc = subprocess.call(command, shell=False)
    return rc == 0
