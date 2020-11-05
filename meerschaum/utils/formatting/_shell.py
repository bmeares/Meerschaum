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
    from meerschaum.utils.formatting import ANSI
    from meerschaum.utils.debug import dprint
    if debug:
        dprint("Skipping screen clear.")
        return True
    if ANSI:
        clear_string = '\033c'
        print(clear_string, end="", flush=True)
        return True
    ### ANSI support is disabled, try system level instead
    import platform, os
    if platform.system() == 'Windows':
        command = 'cls'
    command = 'clear' if platform.system() != "Windows" else "cls"
    rc = os.system(command)
    return rc == 0
