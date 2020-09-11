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
        if (length := len(w)) > max_length:
            max_length = length
    
    s = message + "\n"
    for i in range(max_length):
        s += ruler
    return s
